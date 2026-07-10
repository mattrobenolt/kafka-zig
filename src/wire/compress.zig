//! Compression codecs for v2 record batches.
//!
//! The record batch stores its records region either verbatim (`none`) or
//! compressed with one codec (compression bits 0–2 of the attributes field).
//! Implemented codecs: plaintext + snappy + zstd. gzip (codec 1) and lz4
//! (codec 3) are NOT yet implemented — the enum values exist as wire-format
//! constants but `compress`/`decompress` return `error.NotImplemented`.
//!
//! Snappy uses Kafka's Xerial-framed format (what org.xerial.snappy's
//! SnappyOutputStream produces and SnappyInputStream consumes — the format the
//! Kafka Java client's DefaultRecordBatch.compressedIterator expects). A raw
//! Snappy block would be rejected by a Java kafka-console-consumer. The frame
//! is a 16-byte header (8-byte magic `[0x82,'S','N','A','P','P','Y',0]` +
//! BE int32 version=1 + BE int32 compatible_version=1) followed by one or more
//! blocks of (BE int32 compressed_size + a raw snappy block of a <=32KB chunk).
//! Verified against kafka-python's snappy_encode(xerial_compatible=True) and
//! xerial/snappy-java SnappyOutputStream.
//!
//! The underlying raw-block codec lives in `src/snappy/` (its own module): a
//! hash-table match-finder encoder (real back-references, not literal-only)
//! and a SIMD-accelerated decoder that handles all tag types. The decoder
//! accepts blocks produced by any snappy compressor, including the reference
//! C++ snappy. `snappyDecompress` accepts both Xerial-framed input (detected
//! via the magic header) and bare raw blocks (backward compat). Snappy is
//! always available (no build flag, no C dep).
//! Spec: https://github.com/google/snappy/blob/main/format_description.txt
//!
//! gzip (codec 1) and lz4 (codec 3) are not yet implemented. The enum values
//! are kept as wire-format constants (they match the Kafka attributes field
//! compression bits), but `compress`/`decompress`/`compressBound`/
//! `decompressedLen` return `error.NotImplemented` for these codecs. They need
//! real implementations (link a C lib or do a proven port) rather than the
//! "valid but doesn't compress" placeholders that were backed out.
//!
//! zstd is optional and gated behind `-Dzstd=true`. The build wires
//! `build_options.zstd_enabled`; when false the zstd paths return
//! `error.NotImplemented` at runtime (the module still compiles and links no
//! extra library). When true we `@cImport("zstd.h")` and call libzstd's
//! single-shot simple API (`ZSTD_compress` / `ZSTD_decompress`). The include
//! path and static archive come from `mod.linkSystemLibrary("zstd", …)` in
//! build.zig (pkg-config `--cflags` supplies the header path).
//!
//! Allocator policy: compress/decompress are ZERO heap allocation — they read
//! `input` and write into a caller-provided `out` buffer, returning the number
//! of bytes written. `error.BufferTooSmall` when `out` is too small. There is
//! NO `std.heap.*` / page_allocator fallback anywhere; the caller sizes `out`
//! via `compressBound`. The record-batch decode path (cold, broker responses)
//! may pass an allocator and use `decompressedLen` to size its buffer.

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const snappy = @import("snappy");

const build_options = @import("build_options");
pub const zstd_enabled = build_options.zstd_enabled;

/// libzstd bindings, imported only when the build enables zstd. The `if` is
/// comptime-known, so when zstd is disabled this branch is never analyzed and
/// the missing header / unlinked library never matters.
const c = if (zstd_enabled) @cImport({
    @cInclude("zstd.h");
}) else struct {};

/// Default zstd compression level (ZSTD_CLEVEL_DEFAULT is 3). Chosen for a
/// sane speed/ratio balance on the produce path; callers may override.
pub const default_level: i32 = 3;

/// Compression codec = the attributes-field compression bits (0–2).
pub const Codec = enum(u3) {
    none = 0,
    gzip = 1,
    snappy = 2,
    lz4 = 3,
    zstd = 4,
};

pub const CompressError = error{ BufferTooSmall, NotImplemented, CompressionFailed };
pub const DecompressError = error{ BufferTooSmall, NotImplemented, DecompressionFailed };

/// Compress `input` into `out` with `codec` at `level`, returning the number
/// of bytes written to `out`. `none` is an identity copy (level ignored).
/// `zstd` requires `-Dzstd=true`; otherwise returns `error.NotImplemented`.
///
/// Zero heap allocation: writes only into `out`. `error.BufferTooSmall` when
/// `out` cannot hold the compressed output — size `out` via `compressBound`.
pub fn compress(codec: Codec, input: []const u8, out: []u8, level: i32) CompressError!usize {
    switch (codec) {
        .none => {
            if (out.len < input.len) return error.BufferTooSmall;
            @memcpy(out[0..input.len], input);
            return input.len;
        },
        .snappy => return snappyCompress(input, out),
        .zstd => {
            if (!zstd_enabled) return error.NotImplemented;
            // Check the dst bound up front via compressBound so a too-small
            // `out` is reported as BufferTooSmall without relying on
            // ZSTD_getErrorCode (not exported by all libzstd headers, e.g.
            // older Ubuntu libzstd-dev — broke CI). ZSTD_compress itself
            // also returns an error on too-small dst, caught by ZSTD_isError.
            if (out.len < c.ZSTD_compressBound(input.len)) return error.BufferTooSmall;
            const written = c.ZSTD_compress(out.ptr, out.len, input.ptr, input.len, level);
            if (c.ZSTD_isError(written) != 0) return error.CompressionFailed;
            assert(written <= out.len);
            return written;
        },
        .gzip, .lz4 => return error.NotImplemented,
    }
}

/// Worst-case compressed size for `input_len` bytes with `codec`. The caller
/// sizes its `out` buffer to this before calling `compress`. `none` is the
/// identity bound (`input_len`). For `zstd` when disabled the value is unused
/// (compress returns NotImplemented first) and falls back to `input_len`.
pub fn compressBound(codec: Codec, input_len: usize) usize {
    switch (codec) {
        .none => return input_len,
        .snappy => return snappyCompressBound(input_len),
        .zstd => {
            if (!zstd_enabled) return input_len;
            return c.ZSTD_compressBound(input_len);
        },
        // gzip/lz4 are not yet implemented. compressBound returns input_len
        // as a harmless fallback — compress() will error with NotImplemented
        // before the caller uses this bound to size a buffer.
        .gzip, .lz4 => return input_len,
    }
}

/// Decompress `input` into `out` with `codec`, returning the number of bytes
/// written. `none` is an identity copy. `zstd` requires `-Dzstd=true`.
/// `error.BufferTooSmall` when `out` cannot hold the decompressed output —
/// size it via `decompressedLen`.
pub fn decompress(codec: Codec, input: []const u8, out: []u8) DecompressError!usize {
    switch (codec) {
        .none => {
            if (out.len < input.len) return error.BufferTooSmall;
            @memcpy(out[0..input.len], input);
            return input.len;
        },
        .snappy => return snappyDecompress(input, out),
        .zstd => {
            if (!zstd_enabled) return error.NotImplemented;
            // Size `out` via decompressedLen; a too-small dst is reported as
            // BufferTooSmall without ZSTD_getErrorCode (portability — see
            // compress). ZSTD_isError catches any other decompression failure.
            const need = decompressedLen(.zstd, input) catch return error.DecompressionFailed;
            if (out.len < need) return error.BufferTooSmall;
            const written = c.ZSTD_decompress(out.ptr, out.len, input.ptr, input.len);
            if (c.ZSTD_isError(written) != 0) return error.DecompressionFailed;
            return written;
        },
        .gzip, .lz4 => return error.NotImplemented,
    }
}

/// Decompressed byte length of `input` (needed to size the `out` buffer for
/// `decompress`). For `none` it is `input.len`. For `zstd` it reads the frame
/// header's content size (`ZSTD_getFrameContentSize`); returns
/// `error.DecompressionFailed` if the size is unknown/corrupt. Requires
/// `-Dzstd=true` for the zstd path.
pub fn decompressedLen(codec: Codec, input: []const u8) DecompressError!usize {
    switch (codec) {
        .none => return input.len,
        .snappy => return snappyDecompressedLen(input),
        .zstd => {
            if (!zstd_enabled) return error.NotImplemented;
            const size = c.ZSTD_getFrameContentSize(input.ptr, input.len);
            // ZSTD_CONTENTSIZE_UNKNOWN = (0ULL - 1), ZSTD_CONTENTSIZE_ERROR =
            // (0ULL - 2). We compute the sentinels directly: @cImport renders
            // the header macros as `@as(c_ulonglong, 0) - 1`, which overflows
            // at comptime in Zig, so we can't reference `c.ZSTD_CONTENTSIZE_*`.
            const contentsize_unknown: c_ulonglong = ~@as(c_ulonglong, 0); // -1
            const contentsize_error: c_ulonglong = ~@as(c_ulonglong, 0) - 1; // -2
            if (size == contentsize_unknown or size == contentsize_error) {
                return error.DecompressionFailed;
            }
            return @intCast(size);
        },
        .gzip, .lz4 => return error.NotImplemented,
    }
}

// ---------------------------------------------------------------------------
// Snappy — Xerial-framed on the wire, raw-block codec delegated to `snappy`
//
// Kafka's snappy codec (attribute 2) is Xerial-framed: a 16-byte header plus
// blocks of (BE int32 compressed_size + a raw snappy block of a <=32KB chunk).
// This is NOT the raw snappy block format on its own, and NOT the
// framed/streaming format with the "\xff\x06" magic — it is the org.xerial
// SnappyOutputStream format the Kafka Java client speaks.
//
// This file holds only the framing layer (snappyCompress / snappyDecompress /
// snappyCompressBound / snappyDecompressedLen). The raw-block codec — a
// hash-table match-finder encoder and a SIMD-accelerated decoder — lives in
// `src/snappy/` (imported as `snappy`), so it is reusable and testable in
// isolation. Zero heap allocation throughout.
// ---------------------------------------------------------------------------

/// Xerial frame magic: `[0x82,'S','N','A','P','P','Y',0]` (0x82 == -126 as a
/// signed byte). Followed by BE int32 version=1 and BE int32 compatible=1.
/// Total header = 16 bytes. Verified against kafka-python / xerial snappy-java.
const xerial_magic = [8]u8{ 0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0 };
const xerial_header = xerial_magic ++ [8]u8{ 0, 0, 0, 1, 0, 0, 0, 1 };

/// Xerial block chunk size: snappy-java / kafka-python default is 32 KiB. Each
/// block covers at most this many uncompressed bytes.
const xerial_block_size: usize = 32 * 1024;

/// True when `input` starts with the 16-byte Xerial frame (magic present and at
/// least the full header available). Bare raw blocks lack the magic.
fn isXerial(input: []const u8) bool {
    return input.len >= xerial_header.len and
        std.mem.eql(u8, input[0..xerial_magic.len], &xerial_magic);
}

/// Worst-case Xerial-framed compressed size for `input_len` bytes: the 16-byte
/// header plus, per <=32KB chunk, a 4-byte BE length prefix and that chunk's
/// raw-block bound. Cold path (sized once at producer init); the exact per-chunk
/// loop is clearer than an over-estimate.
fn snappyCompressBound(input_len: usize) usize {
    var bound: usize = xerial_header.len;
    var remaining = input_len;
    while (remaining > 0) {
        const chunk = @min(remaining, xerial_block_size);
        bound += 4 + snappy.maxCompressedLength(chunk);
        remaining -= chunk;
    }
    return bound;
}

/// Compress `input` into `out` as Xerial-framed snappy: the 16-byte header then
/// one block per <=32KB chunk (BE int32 compressed_size + the raw snappy block).
/// Returns bytes written. `error.BufferTooSmall` when `out` is too small — size
/// it via `snappyCompressBound`. Zero heap allocation. Empty input yields just
/// the header (no blocks). Output is decompressible by a Kafka Java consumer's
/// SnappyInputStream.
fn snappyCompress(input: []const u8, out: []u8) CompressError!usize {
    if (out.len < xerial_header.len) return error.BufferTooSmall;
    @memcpy(out[0..xerial_header.len], &xerial_header);
    var out_pos: usize = xerial_header.len;

    var in_pos: usize = 0;
    while (in_pos < input.len) {
        const chunk = @min(input.len - in_pos, xerial_block_size);
        // Reserve the 4-byte BE length prefix, compress the chunk after it,
        // then backfill the prefix with the block's compressed size.
        if (out_pos + 4 > out.len) return error.BufferTooSmall;
        const block_out = out[out_pos + 4 ..];
        const block_len = try snappy.compressBlock(input[in_pos..][0..chunk], block_out);
        assert(block_len <= 0xFFFFFFFF);
        std.mem.writeInt(u32, out[out_pos..][0..4], @intCast(block_len), .big);
        out_pos += 4 + block_len;
        in_pos += chunk;
    }
    return out_pos;
}

/// Decompressed byte length of a snappy input. Xerial-framed input: sum each
/// block's raw-block uncompressed length (the varint at the start of every raw
/// block). Bare raw block: the leading varint. `error.DecompressionFailed` on
/// truncation/corruption. Cold path (broker responses).
fn snappyDecompressedLen(input: []const u8) DecompressError!usize {
    if (!isXerial(input)) return snappy.decompressedBlockLen(input);
    var pos: usize = xerial_header.len;
    var total: usize = 0;
    while (pos < input.len) {
        if (pos + 4 > input.len) return error.DecompressionFailed;
        const block_size = std.mem.readInt(u32, input[pos..][0..4], .big);
        pos += 4;
        if (pos + block_size > input.len) return error.DecompressionFailed;
        total += try snappy.decompressedBlockLen(input[pos..][0..block_size]);
        pos += block_size;
    }
    return total;
}

/// Decompress a snappy input into `out`, returning bytes written. Handles both
/// Xerial-framed input (detected via the magic header — skip the 16-byte header,
/// then loop over BE int32 block_size + raw block) and a bare raw block
/// (backward compat). `error.BufferTooSmall` when `out` is too small (size via
/// `snappyDecompressedLen`); `error.DecompressionFailed` on corrupt input. Zero
/// heap allocation.
fn snappyDecompress(input: []const u8, out: []u8) DecompressError!usize {
    if (!isXerial(input)) return snappy.decompressBlock(input, out);
    var in_pos: usize = xerial_header.len;
    var out_pos: usize = 0;
    while (in_pos < input.len) {
        if (in_pos + 4 > input.len) return error.DecompressionFailed;
        const block_size = std.mem.readInt(u32, input[in_pos..][0..4], .big);
        in_pos += 4;
        if (in_pos + block_size > input.len) return error.DecompressionFailed;
        out_pos += try snappy.decompressBlock(input[in_pos..][0..block_size], out[out_pos..]);
        in_pos += block_size;
    }
    return out_pos;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "compress none: identity copy" {
    const input = "hello, kafka records region";
    var out: [64]u8 = undefined;
    const n = try compress(.none, input, &out, default_level);
    try testing.expectEqual(input.len, n);
    try testing.expectEqualSlices(u8, input, out[0..n]);
}

test "compressBound none: identity bound" {
    try testing.expectEqual(@as(usize, 0), compressBound(.none, 0));
    try testing.expectEqual(@as(usize, 1234), compressBound(.none, 1234));
}

test "decompress none: identity copy" {
    const input = "some compressed-looking bytes";
    var out: [64]u8 = undefined;
    const n = try decompress(.none, input, &out);
    try testing.expectEqualSlices(u8, input, out[0..n]);
}

test "compress none: BufferTooSmall" {
    const input = "0123456789";
    var out: [4]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, compress(.none, input, &out, default_level));
}

test "decompress none: BufferTooSmall" {
    const input = "0123456789";
    var out: [4]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, decompress(.none, input, &out));
}

test "gzip/lz4: compress returns NotImplemented" {
    var out: [64]u8 = undefined;
    try testing.expectError(error.NotImplemented, compress(.gzip, "hello", &out, default_level));
    try testing.expectError(error.NotImplemented, compress(.lz4, "hello", &out, default_level));
}

test "gzip/lz4: decompress returns NotImplemented" {
    var out: [64]u8 = undefined;
    try testing.expectError(error.NotImplemented, decompress(.gzip, "hello", &out));
    try testing.expectError(error.NotImplemented, decompress(.lz4, "hello", &out));
}

test "gzip/lz4: decompressedLen returns NotImplemented" {
    try testing.expectError(error.NotImplemented, decompressedLen(.gzip, "hello"));
    try testing.expectError(error.NotImplemented, decompressedLen(.lz4, "hello"));
}

// --- Snappy tests ---

test "snappy: empty input round-trips" {
    const input = "";
    var comp: [16]u8 = undefined;
    const clen = try compress(.snappy, input, &comp, 0);
    // Empty input yields just the 16-byte Xerial header, no blocks.
    try testing.expectEqual(@as(usize, 16), clen);
    try testing.expectEqualSlices(u8, &xerial_header, comp[0..16]);

    const dlen = try decompressedLen(.snappy, comp[0..clen]);
    try testing.expectEqual(@as(usize, 0), dlen);

    var back: [8]u8 = undefined;
    const blen = try decompress(.snappy, comp[0..clen], &back);
    try testing.expectEqual(@as(usize, 0), blen);
}

test "snappy: Xerial header is present in compressed output" {
    // The compressed output must begin with the 8-byte Xerial magic so a Kafka
    // Java consumer's SnappyInputStream accepts it. Magic = [0x82, S,N,A,P,P,Y, 0].
    const input = "kafka interop requires the xerial frame";
    var comp: [128]u8 = undefined;
    const clen = try compress(.snappy, input, &comp, 0);
    try testing.expect(clen > 16);
    try testing.expectEqualSlices(u8, &xerial_magic, comp[0..8]);
    // version = 1, compatible = 1 (BE int32 each).
    try testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, comp[8..12], .big));
    try testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, comp[12..16], .big));
}

test "snappy: decompresses bare raw block (no Xerial header, backward compat)" {
    // A bare raw snappy block (no magic) must still decompress — this is what an
    // older producer or a hand-built block looks like. Block for "ABABABAB":
    // varint(8)=0x08, literal "AB" (tag 0x04), copy len6 off2 (0x09 0x02).
    const raw_block = [_]u8{ 0x08, 0x04, 'A', 'B', 0x09, 0x02 };
    try testing.expect(!isXerial(&raw_block));
    const dlen = try decompressedLen(.snappy, &raw_block);
    try testing.expectEqual(@as(usize, 8), dlen);
    var out: [8]u8 = undefined;
    const blen = try decompress(.snappy, &raw_block, &out);
    try testing.expectEqualSlices(u8, "ABABABAB", out[0..blen]);
}

test "snappy: small input round-trips" {
    const input = "hello, kafka!";
    var comp: [64]u8 = undefined;
    const clen = try compress(.snappy, input, &comp, 0);
    try testing.expect(clen > 0);
    try testing.expect(compressBound(.snappy, input.len) >= clen);

    const dlen = try decompressedLen(.snappy, comp[0..clen]);
    try testing.expectEqual(input.len, dlen);

    var back: [64]u8 = undefined;
    const blen = try decompress(.snappy, comp[0..clen], &back);
    try testing.expectEqualSlices(u8, input, back[0..blen]);
}

test "snappy: compressible input shrinks via match-finding" {
    // Repetitive input >= min_non_literal_block_size exercises the match
    // finder (the small-input tests above all bail to a single literal).
    const input = "the quick brown fox" ** 400; // 7600 bytes, crosses one 32KB? no, one block
    const bound = compressBound(.snappy, input.len);
    const comp = try testing.allocator.alloc(u8, bound);
    defer testing.allocator.free(comp);
    const clen = try compress(.snappy, input, comp, 0);
    try testing.expect(clen < input.len / 2);
    try testing.expectEqualSlices(u8, &xerial_magic, comp[0..8]);

    const dlen = try decompressedLen(.snappy, comp[0..clen]);
    try testing.expectEqual(input.len, dlen);
    const back = try testing.allocator.alloc(u8, dlen);
    defer testing.allocator.free(back);
    const blen = try decompress(.snappy, comp[0..clen], back);
    try testing.expectEqualSlices(u8, input, back[0..blen]);
}

test "snappy: known encoding (Xerial frame + literal-only block)" {
    // Verify the exact bytes for a 13-byte input: 16-byte Xerial header, then
    // BE int32 block_size, then the raw snappy block.
    //   raw block = varint(13)=0x0D + literal tag (13-1)<<2=0x30 + data (13B)
    //   block_size = 2 + 13 = 15 → BE int32 {0,0,0,15}
    const input = "hello, kafka!";
    var comp: [64]u8 = undefined;
    const clen = try compress(.snappy, input, &comp, 0);
    const expected = xerial_header ++ [_]u8{ 0, 0, 0, 15, 0x0D, 0x30 } ++ input.*;
    try testing.expectEqualSlices(u8, &expected, comp[0..clen]);
}

test "snappy: 60-byte boundary (6-bit literal max)" {
    // 60 bytes fits in a single 6-bit literal tag (max for that encoding).
    var input: [60]u8 = undefined;
    for (&input, 0..) |*b, i| b.* = @intCast(i % 256);
    var comp: [128]u8 = undefined;
    const clen = try compress(.snappy, &input, &comp, 0);
    // raw block: varint(60)=0x3C, tag (60-1)<<2=0xEC, data=60 → 62 bytes.
    // Xerial: 16-byte header + 4-byte block_size + 62 = 82 total.
    try testing.expectEqual(@as(usize, 82), clen);

    var back: [60]u8 = undefined;
    const blen = try decompress(.snappy, comp[0..clen], &back);
    try testing.expectEqualSlices(u8, &input, back[0..blen]);
}

test "snappy: 61-byte input (1-byte extended literal)" {
    // 61 bytes exceeds the 6-bit literal max (60), so the encoder uses the
    // 1-byte extended literal: tag 0xF0 + 1 length byte + data.
    var input: [61]u8 = undefined;
    for (&input, 0..) |*b, i| b.* = @intCast(i % 256);
    var comp: [128]u8 = undefined;
    const clen = try compress(.snappy, &input, &comp, 0);
    // raw block: varint(61)=0x3D, tag 0xF0, length-1=60=0x3C, data=61 → 64 bytes.
    // Xerial: 16-byte header + 4-byte block_size + 64 = 84 total. The raw block
    // starts at offset 20 (16 header + 4 size prefix).
    try testing.expectEqual(@as(usize, 84), clen);
    try testing.expectEqual(@as(u8, 0xF0), comp[21]);
    try testing.expectEqual(@as(u8, 60), comp[22]);

    var back: [61]u8 = undefined;
    const blen = try decompress(.snappy, comp[0..clen], &back);
    try testing.expectEqualSlices(u8, &input, back[0..blen]);
}

test "snappy: 257-byte input (2-byte extended literal)" {
    // 257 bytes exceeds the 1-byte extended literal max (256), so the encoder
    // uses the 2-byte extended literal.
    var input: [257]u8 = undefined;
    for (&input, 0..) |*b, i| b.* = @intCast(i % 256);
    var comp: [520]u8 = undefined;
    const clen = try compress(.snappy, &input, &comp, 0);
    // raw block: varint(257)=0x81 0x02, tag 0xF4, length-1=256=0x00 0x01 LE,
    // data=257 → 2 + 3 + 257 = 262 bytes. Xerial: 16 + 4 + 262 = 282 total.
    // The raw block starts at offset 20; the 0xF4 tag is after the 2-byte varint.
    try testing.expectEqual(@as(usize, 282), clen);
    try testing.expectEqual(@as(u8, 0xF4), comp[22]);

    var back: [257]u8 = undefined;
    const blen = try decompress(.snappy, comp[0..clen], &back);
    try testing.expectEqualSlices(u8, &input, back[0..blen]);
}

test "snappy: large input round-trips" {
    var input: [16 * 1024]u8 = undefined;
    var rng = std.Random.DefaultPrng.init(0x5A4BEEF);
    for (&input, 0..) |*b, i| b.* = @truncate(rng.random().int(u8) ^ @as(u8, @truncate(i)));

    const bound = compressBound(.snappy, input.len);
    const comp = try testing.allocator.alloc(u8, bound);
    defer testing.allocator.free(comp);
    const clen = try compress(.snappy, &input, comp, 0);
    try testing.expect(bound >= clen);

    const dlen = try decompressedLen(.snappy, comp[0..clen]);
    try testing.expectEqual(input.len, dlen);

    const back = try testing.allocator.alloc(u8, dlen);
    defer testing.allocator.free(back);
    const blen = try decompress(.snappy, comp[0..clen], back);
    try testing.expectEqualSlices(u8, &input, back[0..blen]);
}

test "snappy: multi-block round-trips (input > 32KB Xerial chunk)" {
    // Input larger than xerial_block_size (32KB) must split into multiple
    // blocks and reassemble on decompress. 80KB → 3 blocks (32K + 32K + 16K).
    var input: [80 * 1024]u8 = undefined;
    var rng = std.Random.DefaultPrng.init(0xB10C5);
    for (&input, 0..) |*b, i| b.* = @truncate(rng.random().int(u8) ^ @as(u8, @truncate(i)));

    const bound = compressBound(.snappy, input.len);
    const comp = try testing.allocator.alloc(u8, bound);
    defer testing.allocator.free(comp);
    const clen = try compress(.snappy, &input, comp, 0);
    try testing.expect(bound >= clen);
    try testing.expectEqualSlices(u8, &xerial_magic, comp[0..8]);

    const dlen = try decompressedLen(.snappy, comp[0..clen]);
    try testing.expectEqual(input.len, dlen);

    const back = try testing.allocator.alloc(u8, dlen);
    defer testing.allocator.free(back);
    const blen = try decompress(.snappy, comp[0..clen], back);
    try testing.expectEqualSlices(u8, &input, back[0..blen]);
}

test "snappy: decompresses copy tags (back-references)" {
    // The decoder must handle copy tags, not just literals. We hand-construct
    // a snappy block with a copy operation that the reference C++ snappy
    // would produce for repetitive data.
    //
    // Decompressed: "ABABABAB" (8 bytes)
    // Encoding:
    //   varint(8) = 0x08
    //   literal tag: length 2 → (2-1)<<2 = 0x04, data "AB"
    //   copy with 1-byte offset: length 6, offset 2
    //     tag = 01 | ((6 - 4) << 2) | ((2 >> 8) << 5) = 01 | 0x08 | 0x00 = 0x09
    //     offset byte = 2 & 0xFF = 0x02
    //
    // The decoder reads the literal "AB", then copies from offset 2 (back to
    // "AB") for 6 bytes, producing "ABABABAB".
    const snappy_block = [_]u8{ 0x08, 0x04, 'A', 'B', 0x09, 0x02 };
    var out: [8]u8 = undefined;
    const dlen = try decompressedLen(.snappy, &snappy_block);
    try testing.expectEqual(@as(usize, 8), dlen);
    const blen = try decompress(.snappy, &snappy_block, &out);
    try testing.expectEqualSlices(u8, "ABABABAB", out[0..blen]);
}

test "snappy: decompresses copy with 2-byte offset" {
    // Decompressed: "XXXX" + 64 bytes of 'Y' (68 bytes total)
    // We use a 2-byte offset copy to copy 'Y' * 64 from offset 4.
    //
    // Encoding:
    //   varint(68) = 0x44
    //   literal tag: length 4 → (4-1)<<2 = 0x0C, data "XXXX"
    //   copy with 2-byte offset: length 64, offset 4
    //     tag = 10 | ((64 - 1) << 2) = 0x02 | 0xFC = 0xFE
    //     offset LE = 0x04 0x00
    //
    // Wait, tag type 10 is 2 in decimal. tag = (length-1) << 2 | 2.
    // (64 - 1) << 2 = 252 = 0xFC. 0xFC | 0x02 = 0xFE.
    // offset = 4 → 0x04 0x00 (LE).
    const snappy_block = [_]u8{ 0x44, 0x0C, 'X', 'X', 'X', 'X', 0xFE, 0x04, 0x00 };
    var out: [68]u8 = undefined;
    const blen = try decompress(.snappy, &snappy_block, &out);
    try testing.expectEqual(@as(usize, 68), blen);
    // First 4 bytes are "XXXX"
    try testing.expectEqualSlices(u8, "XXXX", out[0..4]);
    // Remaining 64 bytes are copies of "XXXX" repeated
    for (out[4..68], 0..) |b, i| {
        try testing.expectEqual(out[i % 4], b);
    }
}

test "snappy: decompresses overlapping copy (offset < length)" {
    // Overlapping copy: offset 1, length 4, after a single byte 'A'.
    // This produces "AAAAA" (1 literal + 4 copied from offset 1 → run of 'A').
    //
    // Encoding:
    //   varint(5) = 0x05
    //   literal tag: length 1 → (1-1)<<2 = 0x00, data "A"
    //   copy with 1-byte offset: length 4, offset 1
    //     tag = 01 | ((4 - 4) << 2) | ((1 >> 8) << 5) = 0x01
    //     offset byte = 0x01
    const snappy_block = [_]u8{ 0x05, 0x00, 'A', 0x01, 0x01 };
    var out: [5]u8 = undefined;
    const blen = try decompress(.snappy, &snappy_block, &out);
    try testing.expectEqual(@as(usize, 5), blen);
    try testing.expectEqualSlices(u8, "AAAAA", out[0..blen]);
}

test "snappy: too-small out buffer returns BufferTooSmall" {
    const input = "hello, kafka compression test";
    var comp: [64]u8 = undefined;
    const clen = try compress(.snappy, input, &comp, 0);

    var back: [4]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, decompress(.snappy, comp[0..clen], &back));
}

test "snappy: compress too-small out buffer returns BufferTooSmall" {
    const input = "this is too long for a 2-byte buffer";
    var out: [2]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, compress(.snappy, input, &out, 0));
}

test "snappy: corrupt block returns DecompressionFailed" {
    // Truncated varint (continuation bit set, no more data).
    const bad = [_]u8{0x80};
    var out: [8]u8 = undefined;
    try testing.expectError(error.DecompressionFailed, decompress(.snappy, &bad, &out));
    try testing.expectError(error.DecompressionFailed, decompressedLen(.snappy, &bad));
}

test "snappy: compressBound is always >= actual compressed size" {
    // Test several sizes that cross the tag-format boundaries.
    inline for (.{ 0, 1, 60, 61, 256, 257, 65536, 65537 }) |size| {
        const input = try testing.allocator.alloc(u8, size);
        defer testing.allocator.free(input);
        @memset(input, 0xAB);
        const bound = compressBound(.snappy, size);
        const comp = try testing.allocator.alloc(u8, bound);
        defer testing.allocator.free(comp);
        const clen = try compress(.snappy, input, comp, 0);
        try testing.expect(bound >= clen);
    }
}
