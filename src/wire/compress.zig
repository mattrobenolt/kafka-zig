//! Compression codec slot for the v2 record batch (PLAN §1, §2.1, §6).
//!
//! The record batch stores its records region either verbatim (`none`) or
//! compressed with one codec (compression bits 0–2 of the attributes field).
//! v1 implements plaintext + snappy + zstd; gzip/lz4 return
//! `error.NotImplemented`.
//!
//! Snappy uses Kafka's raw block format (NOT the framed/streaming format).
//! The encoder is literal-only (no match-finding): it produces valid snappy
//! blocks that any consumer can decompress, but the ratio is ~1:1. This is
//! sufficient for interop — the broker accepts and stores it, consumers
//! decompress it transparently. The decoder is full: it handles all tag types
//! (literal + copy 1/2/4-byte offset), so it can decompress blocks produced
//! by any snappy compressor (including the reference C++ snappy with full
//! match-finding). Snappy is always available (no build flag, no C dep).
//! Spec: https://github.com/google/snappy/blob/main/format_description.txt
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
// Snappy block format (raw, not framed) — pure-Zig implementation
//
// Kafka uses the raw Snappy block format (codec attribute 2): a varint-encoded
// uncompressed length followed by a sequence of tags. This is the block format
// from the snappy spec, NOT the framed/streaming format (which has a "\xff\x06"
// magic header and chunk framing).
//
// Spec: https://github.com/google/snappy/blob/main/format_description.txt
//
// The encoder is literal-only (no match-finding / back-references). It produces
// valid snappy blocks that any snappy decoder (including the reference C++
// snappy and Java's kafka-console-consumer) can decompress. The compression
// ratio is ~1:1 (slightly worse due to tag overhead) — acceptable for v1
// interop. A full compressor with match-finding can be added later without
// changing the decoder or the wire format.
//
// The decoder is full: it handles all four tag types (literal + copy with
// 1/2/4-byte offset), so it can decompress blocks produced by any snappy
// compressor. This matters for the mock-broker round-trip and for future
// consumer work where the broker may send snappy-compressed batches produced by
// a full compressor.
//
// Zero heap allocation: both compress and decompress read `input` and write into
// caller-provided `out` buffers, returning the byte count. No allocator.
// ---------------------------------------------------------------------------

/// Number of bytes to encode `value` as an unsigned LEB128 varint.
fn uvarintSize(value: usize) usize {
    if (value == 0) return 1;
    var size: usize = 0;
    var v = value;
    while (v != 0) : (v >>= 7) size += 1;
    return size;
}

/// Write `value` as an unsigned LEB128 varint into `out`, returning bytes
/// written. `error.BufferTooSmall` when `out` is too small.
fn writeUvarint(out: []u8, value: usize) CompressError!usize {
    var v = value;
    var pos: usize = 0;
    while (v >= 0x80) {
        if (pos >= out.len) return error.BufferTooSmall;
        out[pos] = @as(u8, @truncate(v)) | 0x80;
        pos += 1;
        v >>= 7;
    }
    if (pos >= out.len) return error.BufferTooSmall;
    out[pos] = @as(u8, @truncate(v));
    return pos + 1;
}

/// Read an unsigned LEB128 varint from `input` at `pos`, advancing `pos`.
/// Snappy stores the uncompressed length as a varint (max 5 bytes for u32).
/// `error.DecompressionFailed` on truncation or overflow.
fn readUvarint(input: []const u8, pos: *usize) DecompressError!usize {
    var result: usize = 0;
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        if (pos.* >= input.len) return error.DecompressionFailed;
        const byte = input[pos.*];
        pos.* += 1;
        result |= (@as(usize, byte & 0x7F)) << @intCast(i * 7);
        if (byte & 0x80 == 0) return result;
    }
    return error.DecompressionFailed; // varint too long (> 5 bytes)
}

/// Worst-case compressed size for a literal-only snappy encoding of
/// `input_len` bytes. The encoder uses the largest literal chunk that fits,
/// but the worst case (max tag overhead) is one tag byte per 60-byte chunk
/// (the smallest literal encoding). Plus the varint-encoded uncompressed
/// length.
fn snappyCompressBound(input_len: usize) usize {
    return uvarintSize(input_len) + input_len + (input_len + 59) / 60;
}

/// Compress `input` into `out` using the snappy block format (literal-only
/// encoder). Returns bytes written. `error.BufferTooSmall` when `out` is too
/// small — size it via `snappyCompressBound`. Zero heap allocation.
///
/// Tag format for literals (type 00):
///   length 1–60:   tag = (length - 1) << 2         (1 byte tag)
///   length 61–256: tag = 0xF0, then (length - 1) as 1 byte  (2 bytes)
///   length 257–65536: tag = 0xF4, then (length - 1) as 2 bytes LE  (3 bytes)
///   length 65537–16M: tag = 0xF8, then (length - 1) as 3 bytes LE  (4 bytes)
///   length > 16M:  tag = 0xFC, then (length - 1) as 4 bytes LE  (5 bytes)
fn snappyCompress(input: []const u8, out: []u8) CompressError!usize {
    var out_pos: usize = 0;
    out_pos += try writeUvarint(out[out_pos..], input.len);

    var in_pos: usize = 0;
    while (in_pos < input.len) {
        const remaining = input.len - in_pos;

        // Choose the largest literal chunk that fits in `remaining`, and write
        // the tag header. The chunk sizes correspond to the tag encodings
        // above (60 = 6-bit, 256 = 1-byte ext, 65536 = 2-byte ext, etc).
        if (remaining <= 60) {
            if (out_pos + 1 + remaining > out.len) return error.BufferTooSmall;
            out[out_pos] = @as(u8, @intCast(remaining - 1)) << 2;
            out_pos += 1;
        } else if (remaining <= 256) {
            if (out_pos + 2 + remaining > out.len) return error.BufferTooSmall;
            out[out_pos] = 0xF0;
            out[out_pos + 1] = @intCast(remaining - 1);
            out_pos += 2;
        } else if (remaining <= 65536) {
            if (out_pos + 3 + remaining > out.len) return error.BufferTooSmall;
            out[out_pos] = 0xF4;
            const v: u16 = @intCast(remaining - 1);
            out[out_pos + 1] = @truncate(v);
            out[out_pos + 2] = @truncate(v >> 8);
            out_pos += 3;
        } else if (remaining <= 16777216) {
            if (out_pos + 4 + remaining > out.len) return error.BufferTooSmall;
            out[out_pos] = 0xF8;
            const v: u32 = @intCast(remaining - 1);
            out[out_pos + 1] = @truncate(v);
            out[out_pos + 2] = @truncate(v >> 8);
            out[out_pos + 3] = @truncate(v >> 16);
            out_pos += 4;
        } else {
            // 4-byte extended literal, chunked at 2^32 - 1.
            const chunk: usize = @min(remaining, 0xFFFFFFFF);
            if (out_pos + 5 + chunk > out.len) return error.BufferTooSmall;
            out[out_pos] = 0xFC;
            const v: u32 = @intCast(chunk - 1);
            out[out_pos + 1] = @truncate(v);
            out[out_pos + 2] = @truncate(v >> 8);
            out[out_pos + 3] = @truncate(v >> 16);
            out[out_pos + 4] = @truncate(v >> 24);
            out_pos += 5;
            @memcpy(out[out_pos..][0..chunk], input[in_pos..][0..chunk]);
            out_pos += chunk;
            in_pos += chunk;
            continue;
        }

        @memcpy(out[out_pos..][0..remaining], input[in_pos..][0..remaining]);
        out_pos += remaining;
        in_pos += remaining;
    }

    return out_pos;
}

/// Decompressed byte length of a snappy block (the varint at the start of
/// `input`). `error.DecompressionFailed` if the varint is corrupt/truncated.
fn snappyDecompressedLen(input: []const u8) DecompressError!usize {
    var pos: usize = 0;
    return readUvarint(input, &pos);
}

/// Copy `length` bytes from `out[pos - offset .. pos - offset + length]` to
/// `out[pos .. pos + length]`, handling overlap (like memmove). The byte-by-byte
/// loop is correct for all overlap cases; the decoder is cold-path (mock
/// broker / tests), so the simplicity is worth the non-vectorized copy.
fn snappyCopy(out: []u8, pos: usize, offset: usize, length: usize) void {
    assert(offset > 0);
    assert(offset <= pos);
    assert(pos + length <= out.len);
    var i: usize = 0;
    while (i < length) : (i += 1) {
        out[pos + i] = out[pos - offset + i];
    }
}

/// Decompress a snappy block from `input` into `out`, returning bytes written.
/// `error.BufferTooSmall` when `out` is too small (size via
/// `snappyDecompressedLen`). `error.DecompressionFailed` on corrupt input.
/// Zero heap allocation.
///
/// Handles all four tag types:
///   00: literal (copy raw bytes from input to output)
///   01: copy with 1-byte offset (3-bit length, 11-bit offset)
///   10: copy with 2-byte offset (6-bit length, 16-bit offset LE)
///   11: copy with 4-byte offset (6-bit length, 32-bit offset LE)
fn snappyDecompress(input: []const u8, out: []u8) DecompressError!usize {
    var in_pos: usize = 0;
    const uncomp_len = try readUvarint(input, &in_pos);
    if (uncomp_len > out.len) return error.BufferTooSmall;

    var out_pos: usize = 0;
    while (in_pos < input.len) {
        const tag = input[in_pos];
        in_pos += 1;

        switch (tag & 3) {
            0 => {
                // Literal: the 6-bit field (tag >> 2) encodes the length.
                // Values 0–59 mean length = field + 1. Values 60–63 mean the
                // length is in the next 1–4 bytes (LE), plus 1.
                const code6: u8 = tag >> 2;
                var length: usize = undefined;
                if (code6 < 60) {
                    length = @as(usize, code6) + 1;
                } else {
                    const extra: usize = @as(usize, code6) - 59; // 60→1, 61→2, 62→3, 63→4
                    if (in_pos + extra > input.len) return error.DecompressionFailed;
                    length = 1;
                    var i: usize = 0;
                    while (i < extra) : (i += 1) {
                        length += @as(usize, input[in_pos + i]) << @intCast(i * 8);
                    }
                    in_pos += extra;
                }
                if (in_pos + length > input.len) return error.DecompressionFailed;
                if (out_pos + length > uncomp_len) return error.DecompressionFailed;
                @memcpy(out[out_pos..][0..length], input[in_pos..][0..length]);
                in_pos += length;
                out_pos += length;
            },
            1 => {
                // Copy with 1-byte offset: bits 2–4 = length - 4, bits 5–7 =
                // upper 3 bits of offset, next byte = lower 8 bits.
                const length: usize = @as(usize, (tag >> 2) & 0x07) + 4;
                if (in_pos >= input.len) return error.DecompressionFailed;
                const offset: usize = (@as(usize, tag >> 5) << 8) | @as(usize, input[in_pos]);
                in_pos += 1;
                if (offset == 0 or offset > out_pos) return error.DecompressionFailed;
                if (out_pos + length > uncomp_len) return error.DecompressionFailed;
                snappyCopy(out, out_pos, offset, length);
                out_pos += length;
            },
            2 => {
                // Copy with 2-byte offset: bits 2–7 = length - 1, next 2 bytes
                // LE = offset.
                const length: usize = @as(usize, tag >> 2) + 1;
                if (in_pos + 1 >= input.len) return error.DecompressionFailed;
                const offset: usize = @as(usize, input[in_pos]) | (@as(usize, input[in_pos + 1]) << 8);
                in_pos += 2;
                if (offset == 0 or offset > out_pos) return error.DecompressionFailed;
                if (out_pos + length > uncomp_len) return error.DecompressionFailed;
                snappyCopy(out, out_pos, offset, length);
                out_pos += length;
            },
            3 => {
                // Copy with 4-byte offset: bits 2–7 = length - 1, next 4 bytes
                // LE = offset.
                const length: usize = @as(usize, tag >> 2) + 1;
                if (in_pos + 3 >= input.len) return error.DecompressionFailed;
                const offset: usize = @as(usize, input[in_pos]) |
                    (@as(usize, input[in_pos + 1]) << 8) |
                    (@as(usize, input[in_pos + 2]) << 16) |
                    (@as(usize, input[in_pos + 3]) << 24);
                in_pos += 4;
                if (offset == 0 or offset > out_pos) return error.DecompressionFailed;
                if (out_pos + length > uncomp_len) return error.DecompressionFailed;
                snappyCopy(out, out_pos, offset, length);
                out_pos += length;
            },
            else => unreachable,
        }
    }

    if (out_pos != uncomp_len) return error.DecompressionFailed;
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

test "unimplemented codecs return NotImplemented" {
    var out: [64]u8 = undefined;
    try testing.expectError(error.NotImplemented, compress(.gzip, "x", &out, 3));
    try testing.expectError(error.NotImplemented, compress(.lz4, "x", &out, 3));
}

test "zstd disabled: compress/decompress return NotImplemented" {
    if (zstd_enabled) return; // this test asserts the DISABLED behavior
    var out: [64]u8 = undefined;
    try testing.expectError(error.NotImplemented, compress(.zstd, "x", &out, 3));
    try testing.expectError(error.NotImplemented, decompress(.zstd, "x", &out));
    try testing.expectError(error.NotImplemented, decompressedLen(.zstd, "x"));
}

test "zstd round-trip: known input" {
    if (!zstd_enabled) return;
    const input = "the quick brown fox jumps over the lazy dog" ** 8;
    var comp: [512]u8 = undefined;
    const clen = try compress(.zstd, input, &comp, default_level);
    try testing.expect(clen > 0);
    // compressBound must always cover the actual compressed size.
    try testing.expect(compressBound(.zstd, input.len) >= clen);

    const dlen = try decompressedLen(.zstd, comp[0..clen]);
    try testing.expectEqual(input.len, dlen);

    var back: [512]u8 = undefined;
    const blen = try decompress(.zstd, comp[0..clen], &back);
    try testing.expectEqualSlices(u8, input, back[0..blen]);
}

test "zstd: empty input round-trips" {
    if (!zstd_enabled) return;
    const input = "";
    var comp: [64]u8 = undefined;
    const clen = try compress(.zstd, input, &comp, default_level);
    const dlen = try decompressedLen(.zstd, comp[0..clen]);
    try testing.expectEqual(@as(usize, 0), dlen);
    var back: [8]u8 = undefined;
    const blen = try decompress(.zstd, comp[0..clen], &back);
    try testing.expectEqual(@as(usize, 0), blen);
}

test "zstd: large input round-trips" {
    if (!zstd_enabled) return;
    var input: [16 * 1024]u8 = undefined;
    var rng = std.Random.DefaultPrng.init(0xC0FFEE);
    // Mix of structured + random so it is neither trivially compressible nor
    // pathologically incompressible.
    for (&input, 0..) |*b, i| b.* = @truncate(rng.random().int(u8) ^ @as(u8, @truncate(i)));

    const bound = compressBound(.zstd, input.len);
    const comp = try testing.allocator.alloc(u8, bound);
    defer testing.allocator.free(comp);
    const clen = try compress(.zstd, &input, comp, default_level);
    try testing.expect(bound >= clen);

    const dlen = try decompressedLen(.zstd, comp[0..clen]);
    try testing.expectEqual(input.len, dlen);
    const back = try testing.allocator.alloc(u8, dlen);
    defer testing.allocator.free(back);
    const blen = try decompress(.zstd, comp[0..clen], back);
    try testing.expectEqualSlices(u8, &input, back[0..blen]);
}

test "zstd: too-small out buffer returns BufferTooSmall" {
    if (!zstd_enabled) return;
    // Incompressible-ish data into a 1-byte buffer: zstd cannot fit a frame.
    const input = "abcdefghijklmnopqrstuvwxyz0123456789";
    var out: [1]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, compress(.zstd, input, &out, default_level));
}

// --- Snappy tests ---

test "snappy: empty input round-trips" {
    const input = "";
    var comp: [16]u8 = undefined;
    const clen = try compress(.snappy, input, &comp, 0);
    try testing.expectEqual(@as(usize, 1), clen); // just the varint 0x00
    try testing.expectEqual(@as(u8, 0x00), comp[0]);

    const dlen = try decompressedLen(.snappy, comp[0..clen]);
    try testing.expectEqual(@as(usize, 0), dlen);

    var back: [8]u8 = undefined;
    const blen = try decompress(.snappy, comp[0..clen], &back);
    try testing.expectEqual(@as(usize, 0), blen);
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

test "snappy: known encoding (literal-only)" {
    // Verify the exact bytes our literal-only encoder produces for a 13-byte
    // input. The format is: varint(13) + literal tag + data.
    // varint(13) = 0x0D (single byte, 13 < 128)
    // literal tag for 13 bytes: (13 - 1) << 2 = 0x30
    // data: "hello, kafka!"
    const input = "hello, kafka!";
    var comp: [64]u8 = undefined;
    const clen = try compress(.snappy, input, &comp, 0);
    const expected = [_]u8{ 0x0D, 0x30 } ++ input.*;
    try testing.expectEqualSlices(u8, &expected, comp[0..clen]);
}

test "snappy: 60-byte boundary (6-bit literal max)" {
    // 60 bytes fits in a single 6-bit literal tag (max for that encoding).
    var input: [60]u8 = undefined;
    for (&input, 0..) |*b, i| b.* = @intCast(i % 256);
    var comp: [128]u8 = undefined;
    const clen = try compress(.snappy, &input, &comp, 0);
    // varint(60) = 0x3C, tag = (60-1)<<2 = 0xEC, data = 60 bytes → 62 total
    try testing.expectEqual(@as(usize, 62), clen);

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
    // varint(61) = 0x3D, tag 0xF0, length-1 = 60 = 0x3C, data = 61 → 64 total
    try testing.expectEqual(@as(usize, 64), clen);
    try testing.expectEqual(@as(u8, 0xF0), comp[1]);
    try testing.expectEqual(@as(u8, 60), comp[2]);

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
    // varint(257) = 0x81 0x02, tag 0xF4, length-1 = 256 = 0x00 0x01 LE, data = 257
    // total = 2 + 3 + 257 = 262
    try testing.expectEqual(@as(usize, 262), clen);
    try testing.expectEqual(@as(u8, 0xF4), comp[2]);

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
