//! Compression codec slot for the v2 record batch (PLAN §1, §2.1, §6).
//!
//! The record batch stores its records region either verbatim (`none`) or
//! compressed with one codec (compression bits 0–2 of the attributes field).
//! v1 implements plaintext + snappy + zstd; gzip and lz4 are now implemented
//! for interop completeness (issue #10).
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
//! gzip (codec 1) uses the full gzip container format (RFC 1952) — NOT raw
//! DEFLATE (RFC 1951). Kafka's `GzipCompression` wraps `java.util.zip.
//! GZIPOutputStream`/`GZIPInputStream`, which produce/consume the gzip
//! container (10-byte header + DEFLATE body + 8-byte CRC32/ISIZE footer).
//! Verified against the Kafka Java client `GzipCompression` / `GzipOutputStream`
//! source (trunk). The encoder writes stored DEFLATE blocks (no Huffman/LZ77
//! match-finding) — valid gzip that any `GZIPInputStream` can decompress, with
//! ~1:1 ratio. The decoder uses `std.compress.flate.Decompress` with the
//! `.gzip` container, which handles all DEFLATE block types (stored, fixed,
//! dynamic). Gzip is always available (no build flag, no C dep).
//! Spec: RFC 1952 (gzip), RFC 1951 (DEFLATE).
//!
//! lz4 (codec 3) uses the LZ4 Frame format (v1.5.1) — NOT the raw LZ4 block
//! format and NOT lz4-java's `LZ4BlockOutputStream` "LZ4Block" format. Kafka's
//! `Lz4Compression` wraps `org.apache.kafka.common.compress.Lz4BlockOutputStream`,
//! which is a partial implementation of the LZ4 Frame format with magic
//! 0x184D2204. This is a format surprise comparable to the snappy Xerial
//! framing: the skill originally said "lz4 block format" but Kafka actually
//! uses the frame format. The frame is: 4-byte magic (LE) + FLG byte + BD byte
//! + HC byte (XXHash32 of FLG+BD, high byte) + blocks (4-byte LE block_size
//! with high bit = uncompressed flag + block data) + 4-byte LE end mark (0).
//! The encoder writes uncompressed blocks (high bit set, raw data) — valid
//! LZ4 frame that any LZ4 frame decoder can decompress, with ~1:1 ratio. The
//! decoder handles both compressed and uncompressed blocks, with a full LZ4
//! block decoder for compressed blocks. Block checksums are optional (FLG bit
//! 4); Kafka does not set them by default. The frame content size flag (FLG
//! bit 3) is not set by Kafka, so `decompressedLen` scans all blocks. LZ4 is
//! always available (no build flag, no C dep).
//! Spec: https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md
//! Verified against the Kafka Java client `Lz4BlockOutputStream` /
//! `Lz4BlockInputStream` source (trunk).
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
        .gzip => return gzipCompress(input, out),
        .lz4 => return lz4Compress(input, out),
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
        .gzip => return gzipCompressBound(input_len),
        .lz4 => return lz4CompressBound(input_len),
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
        .gzip => return gzipDecompress(input, out),
        .lz4 => return lz4Decompress(input, out),
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
        .gzip => return gzipDecompressedLen(input),
        .lz4 => return lz4DecompressedLen(input),
    }
}

// ---------------------------------------------------------------------------
// Gzip — full gzip container (RFC 1952), stored DEFLATE blocks
//
// Kafka's gzip codec (attribute 1) uses the full gzip container format, NOT
// raw DEFLATE. Kafka's `GzipCompression` wraps `java.util.zip.GZIPOutputStream`
// which produces a gzip stream: 10-byte header + DEFLATE body + 8-byte footer
// (CRC32 + ISIZE). Verified against the Kafka Java client source (trunk):
// `GzipCompression.wrapForOutput` → `GzipOutputStream extends GZIPOutputStream`,
// `GzipCompression.wrapForInput` → `GZIPInputStream`.
//
// The encoder writes stored DEFLATE blocks (BTYPE=00, no Huffman/LZ77): valid
// gzip that any `GZIPInputStream` can decompress, with ~1:1 ratio. Stored
// blocks are limited to 65535 bytes each, so inputs larger than that chain
// multiple non-final blocks before a final block. This is the same approach as
// the snappy literal-only encoder — valid for interop, not for compression
// ratio. The decoder uses `std.compress.flate.Decompress` with the `.gzip`
// container, which handles all DEFLATE block types (stored, fixed, dynamic).
//
// Zero heap allocation: both compress and decompress read `input` and write
// into caller-provided `out` buffers. No allocator.
//
// Spec: RFC 1952 (gzip), RFC 1951 (DEFLATE).
// ---------------------------------------------------------------------------

/// Gzip header: ID1=0x1f, ID2=0x8b, CM=8 (deflate), FLG=0, MTIME=0, XFL=0,
/// OS=3 (Unix). 10 bytes total. (RFC 1952 §2.3.1.)
const gzip_header = [10]u8{ 0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03 };

/// Gzip footer: 4 bytes CRC32 (LE) + 4 bytes ISIZE (LE, uncompressed size
/// mod 2^32). 8 bytes total. (RFC 1952 §2.3.1.)
const gzip_footer_len: usize = 8;

/// Stored DEFLATE block size limit: 65535 bytes per block (RFC 1951 §3.2.4).
const deflate_stored_max: usize = 65535;

/// Dummy mutable buffer for flate.Decompress when `out` is smaller than
/// max_window_len. Zero-length slice triggers the decompressor's direct mode
/// (no history window), which works for stored blocks (no back-references).
var decompress_dummy_window: [0]u8 = @splat(0);

/// Worst-case gzip compressed size: 10-byte header + per-block (5-byte stored
/// header + chunk) + 8-byte footer. Each stored block covers at most 65535
/// bytes. Empty input yields header + empty final block (5 bytes) + footer =
/// 23 bytes.
fn gzipCompressBound(input_len: usize) usize {
    if (input_len == 0) return gzip_header.len + 5 + gzip_footer_len;
    const num_blocks = (input_len + deflate_stored_max - 1) / deflate_stored_max;
    return gzip_header.len + num_blocks * (5 + deflate_stored_max) + gzip_footer_len;
}

/// Compress `input` into `out` as a gzip container with stored DEFLATE blocks.
/// Returns bytes written. `error.BufferTooSmall` when `out` is too small —
/// size it via `gzipCompressBound`. Zero heap allocation.
///
/// The output is a valid gzip stream (RFC 1952) that any `GZIPInputStream` can
/// decompress. The compression ratio is ~1:1 (stored blocks add 5 bytes per
/// 65535-byte chunk plus 18 bytes of header/footer overhead).
fn gzipCompress(input: []const u8, out: []u8) CompressError!usize {
    var pos: usize = 0;

    // Gzip header.
    if (out.len < gzip_header.len + 5 + gzip_footer_len) return error.BufferTooSmall;
    @memcpy(out[pos..][0..gzip_header.len], &gzip_header);
    pos += gzip_header.len;

    // CRC32 of the uncompressed data (RFC 1952 uses standard CRC32, NOT CRC32C).
    var crc: std.hash.Crc32 = .init();
    crc.update(input);

    // Stored DEFLATE blocks. Each block: 3 bits (BFINAL + BTYPE) padded to a
    // byte boundary, then 2-byte LEN (LE) + 2-byte NLEN = ~LEN (LE) + LEN
    // bytes of raw data. The 3 bits pack into 1 byte (the rest is zero
    // padding to the next byte boundary, per RFC 1951 §3.2.4).
    var in_pos: usize = 0;
    if (input.len == 0) {
        // Single final empty stored block: BFINAL=1, BTYPE=00, LEN=0, NLEN=0xFFFF.
        out[pos] = 0x01; // BFINAL=1, BTYPE=00
        pos += 1;
        std.mem.writeInt(u16, out[pos..][0..2], 0, .little);
        pos += 2;
        std.mem.writeInt(u16, out[pos..][0..2], 0xFFFF, .little);
        pos += 2;
    } else {
        while (in_pos < input.len) {
            const chunk = @min(input.len - in_pos, deflate_stored_max);
            const is_final = (in_pos + chunk == input.len);
            // BFINAL = 1 if last block, BTYPE = 00 (stored). Packed into 1 byte.
            out[pos] = if (is_final) 0x01 else 0x00;
            pos += 1;
            const len: u16 = @intCast(chunk);
            std.mem.writeInt(u16, out[pos..][0..2], len, .little);
            pos += 2;
            std.mem.writeInt(u16, out[pos..][0..2], ~len, .little);
            pos += 2;
            if (pos + chunk > out.len) return error.BufferTooSmall;
            @memcpy(out[pos..][0..chunk], input[in_pos..][0..chunk]);
            pos += chunk;
            in_pos += chunk;
        }
    }

    // Gzip footer: CRC32 (LE) + ISIZE (LE).
    if (pos + gzip_footer_len > out.len) return error.BufferTooSmall;
    std.mem.writeInt(u32, out[pos..][0..4], crc.final(), .little);
    pos += 4;
    std.mem.writeInt(u32, out[pos..][0..4], @as(u32, @truncate(input.len)), .little);
    pos += 4;

    return pos;
}

/// Decompressed byte length of a gzip input. The gzip footer (last 8 bytes)
/// contains ISIZE = uncompressed size mod 2^32 (LE). For Kafka record batches
/// the records region is always < 4 GB, so ISIZE is exact.
/// `error.DecompressionFailed` if the input is too short for a gzip footer.
fn gzipDecompressedLen(input: []const u8) DecompressError!usize {
    if (input.len < gzip_header.len + gzip_footer_len) return error.DecompressionFailed;
    const footer = input[input.len - gzip_footer_len ..];
    const isize_val = std.mem.readInt(u32, footer[4..8], .little);
    return @intCast(isize_val);
}

/// Decompress a gzip input into `out`, returning bytes written. Uses
/// `std.compress.flate.Decompress` with the `.gzip` container, which handles
/// all DEFLATE block types (stored, fixed, dynamic). `error.BufferTooSmall`
/// when `out` is too small (size via `gzipDecompressedLen`);
/// `error.DecompressionFailed` on corrupt input. Zero heap allocation (the
/// decompressor uses `out` as its history window).
fn gzipDecompress(input: []const u8, out: []u8) DecompressError!usize {
    const need = gzipDecompressedLen(input) catch return error.DecompressionFailed;
    if (out.len < need) return error.BufferTooSmall;

    // flate.Decompress needs a history window buffer for back-references.
    // We cannot use `out` as the window because the decompressor writes to `out`
    // via a Writer and reads from the window for matches — using the same
    // buffer causes @memcpy aliasing panics. Instead, we use direct mode
    // (zero-length buffer) which works for all block types: stored blocks need
    // no window, and fixed/dynamic blocks use the Writer's own buffer as the
    // history (the Decompress streamDirect path handles this internally).
    var in: std.Io.Reader = .fixed(input);
    var decomp = std.compress.flate.Decompress.init(&in, .gzip, decompress_dummy_window[0..0]);

    var w: std.Io.Writer = .fixed(out);
    const n = decomp.reader.streamRemaining(&w) catch return error.DecompressionFailed;
    assert(n <= out.len);
    return n;
}

// ---------------------------------------------------------------------------
// LZ4 — LZ4 Frame format (v1.5.1), pure-Zig codec
//
// Kafka's lz4 codec (attribute 3) uses the LZ4 Frame format (v1.5.1), NOT the
// raw LZ4 block format and NOT lz4-java's "LZ4Block" format. This is a format
// surprise comparable to the snappy Xerial framing: the skill originally said
// "lz4 block format" but Kafka actually uses the frame format. Verified
// against the Kafka Java client source (trunk): `Lz4Compression.wrapForOutput`
// → `org.apache.kafka.common.compress.Lz4BlockOutputStream`, which is
// "A partial implementation of the v1.5.1 LZ4 Frame format" with magic
// 0x184D2204.
//
// The LZ4 Frame format:
//   - 4-byte magic (LE): 0x184D2204
//   - FLG byte: bits 0-1 reserved, bit 2 contentChecksum, bit 3 contentSize,
//     bit 4 blockChecksum, bit 5 blockIndependence, bits 6-7 version (must be 1)
//   - BD byte: bits 0-3 reserved, bits 4-6 blockMaxSize (4=64KB..7=4MB), bit 7 reserved
//   - HC byte: XXHash32(FLG..end_of_descriptor, seed=0) >> 8
//   - Blocks: 4-byte LE block_size (high bit 31 = uncompressed flag), then block_size bytes
//   - End mark: 4-byte LE 0x00000000
//   - Optional content checksum (if FLG bit 2 set): 4-byte LE XXHash32
//
// The encoder writes uncompressed blocks (high bit set, raw data) — valid LZ4
// frame that any LZ4 frame decoder can decompress, with ~1:1 ratio. The
// decoder handles both compressed and uncompressed blocks, with a full LZ4
// block decoder for compressed blocks (handles all token types: literals +
// matches with 2-byte offset). Block checksums and content checksum are
// optional; Kafka does not set them by default but the decoder skips them if
// present.
//
// Zero heap allocation: both compress and decompress read `input` and write
// into caller-provided `out` buffers. No allocator.
//
// Spec: https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md
// ---------------------------------------------------------------------------

/// LZ4 Frame magic number (LE): 0x184D2204.
const lz4_frame_magic: u32 = 0x184D2204;

/// LZ4 Frame header: 4-byte magic (LE) + FLG + BD + HC = 7 bytes.
const lz4_frame_header_len: usize = 7;

/// FLG byte: version=1 (bits 6-7), blockIndependence=1 (bit 5), no checksums.
/// 0b0110_0000 = 0x60.
const lz4_flg: u8 = 0x60;

/// BD byte: blockMaxSize=4 (64KB, bits 4-6). 0b0100_0000 = 0x40.
/// The block max size only affects the decoder's buffer allocation; for
/// uncompressed blocks it doesn't limit the block size we write.
const lz4_bd: u8 = 0x40;

/// LZ4 Frame incompressible mask: bit 31 of block_size = block is uncompressed.
const lz4_incompressible_mask: u32 = 0x80000000;

/// LZ4 block max size for our encoder. We use 64KB (the Kafka default) but can
/// write larger uncompressed blocks since the block_size field is 32-bit.
/// For simplicity we write the entire input as a single uncompressed block
/// when it fits in a u32, or split at 64KB boundaries.
const lz4_block_chunk: usize = 64 * 1024;

/// XXHash32 of FLG+BD with seed 0, high byte (>> 8) = HC. (LZ4 Frame spec §2.1.)
fn lz4Hc() u8 {
    const pair = [_]u8{ lz4_flg, lz4_bd };
    const hash = std.hash.XxHash32.hash(0, &pair);
    return @truncate(hash >> 8);
}

/// Worst-case LZ4 frame compressed size: 7-byte header + per-block (4-byte
/// size + chunk) + 4-byte end mark. Each block covers at most lz4_block_chunk
/// bytes. Empty input yields header + end mark = 11 bytes.
fn lz4CompressBound(input_len: usize) usize {
    if (input_len == 0) return lz4_frame_header_len + 4;
    const num_blocks = (input_len + lz4_block_chunk - 1) / lz4_block_chunk;
    return lz4_frame_header_len + num_blocks * (4 + lz4_block_chunk) + 4;
}

/// Compress `input` into `out` as an LZ4 frame with uncompressed blocks.
/// Returns bytes written. `error.BufferTooSmall` when `out` is too small —
/// size it via `lz4CompressBound`. Zero heap allocation.
///
/// The output is a valid LZ4 frame (v1.5.1) that any LZ4 frame decoder can
/// decompress. The compression ratio is ~1:1 (uncompressed blocks add 4 bytes
/// per 64KB chunk plus 11 bytes of header/end-mark overhead).
fn lz4Compress(input: []const u8, out: []u8) CompressError!usize {
    var pos: usize = 0;

    // Frame header: magic (LE) + FLG + BD + HC.
    if (out.len < lz4_frame_header_len + 4) return error.BufferTooSmall;
    std.mem.writeInt(u32, out[pos..][0..4], lz4_frame_magic, .little);
    pos += 4;
    out[pos] = lz4_flg;
    pos += 1;
    out[pos] = lz4_bd;
    pos += 1;
    out[pos] = lz4Hc();
    pos += 1;

    // Uncompressed blocks: 4-byte LE (chunk_size | INCOMPRESSIBLE_MASK) + raw data.
    var in_pos: usize = 0;
    while (in_pos < input.len) {
        const chunk = @min(input.len - in_pos, lz4_block_chunk);
        if (pos + 4 + chunk > out.len) return error.BufferTooSmall;
        const block_size: u32 = @as(u32, @intCast(chunk)) | lz4_incompressible_mask;
        std.mem.writeInt(u32, out[pos..][0..4], block_size, .little);
        pos += 4;
        @memcpy(out[pos..][0..chunk], input[in_pos..][0..chunk]);
        pos += chunk;
        in_pos += chunk;
    }

    // End mark: 4-byte LE 0.
    if (pos + 4 > out.len) return error.BufferTooSmall;
    std.mem.writeInt(u32, out[pos..][0..4], 0, .little);
    pos += 4;

    return pos;
}

/// Decompressed byte length of an LZ4 frame. Scans the frame header + all
/// blocks, summing the decompressed size of each. For uncompressed blocks the
/// decompressed size is `block_size & ~INCOMPRESSIBLE_MASK`. For compressed
/// blocks, scans the LZ4 block data to count output bytes (literals + matches)
/// without writing them. `error.DecompressionFailed` on corrupt/truncated input.
fn lz4DecompressedLen(input: []const u8) DecompressError!usize {
    if (input.len < lz4_frame_header_len) return error.DecompressionFailed;

    // Parse frame header.
    const magic = std.mem.readInt(u32, input[0..4], .little);
    if (magic != lz4_frame_magic) return error.DecompressionFailed;

    const flg = input[4];
    const bd = input[5];
    // Skip HC byte (index 6).

    // Parse FLG flags.
    const block_checksum = (flg >> 4) & 1 == 1;
    const content_checksum = (flg >> 2) & 1 == 1;
    const content_size_flag = (flg >> 3) & 1 == 1;

    // Parse BD block max size (not needed for decompressedLen, but validate).
    const block_max_value = (bd >> 4) & 7;
    if (block_max_value < 4 or block_max_value > 7) return error.DecompressionFailed;

    var pos: usize = 7; // after magic + FLG + BD + HC

    // Optional content size (8 bytes, LE) if flag set.
    if (content_size_flag) {
        if (pos + 8 > input.len) return error.DecompressionFailed;
        // We could read the content size directly, but still scan blocks
        // for validation. For now, skip it and scan.
        pos += 8;
    }

    var total: usize = 0;
    while (pos + 4 <= input.len) {
        const block_size_raw = std.mem.readInt(u32, input[pos..][0..4], .little);
        pos += 4;
        const compressed = (block_size_raw & lz4_incompressible_mask) == 0;
        const block_size: usize = @intCast(block_size_raw & ~lz4_incompressible_mask);

        // End mark: block_size_raw == 0 (regardless of compressed flag).
        if (block_size == 0) {
            // Optional content checksum.
            if (content_checksum) {
                if (pos + 4 > input.len) return error.DecompressionFailed;
                pos += 4;
            }
            return total;
        }

        if (pos + block_size > input.len) return error.DecompressionFailed;

        if (!compressed) {
            // Uncompressed block: decompressed size = block_size.
            total += block_size;
        } else {
            // Compressed block: scan the LZ4 block to count output bytes.
            total += try lz4BlockCountOutput(input[pos..][0..block_size]);
        }
        pos += block_size;

        // Optional block checksum (4 bytes LE).
        if (block_checksum) {
            if (pos + 4 > input.len) return error.DecompressionFailed;
            pos += 4;
        }
    }

    return error.DecompressionFailed; // no end mark found
}

/// Decompress an LZ4 frame into `out`, returning bytes written. Handles both
/// uncompressed blocks (high bit set — just copy) and compressed blocks (full
/// LZ4 block decode). `error.BufferTooSmall` when `out` is too small (size via
/// `lz4DecompressedLen`); `error.DecompressionFailed` on corrupt input. Zero
/// heap allocation.
fn lz4Decompress(input: []const u8, out: []u8) DecompressError!usize {
    if (input.len < lz4_frame_header_len) return error.DecompressionFailed;

    // Parse frame header.
    const magic = std.mem.readInt(u32, input[0..4], .little);
    if (magic != lz4_frame_magic) return error.DecompressionFailed;

    const flg = input[4];
    const bd = input[5];
    const block_checksum = (flg >> 4) & 1 == 1;
    const content_checksum = (flg >> 2) & 1 == 1;
    const content_size_flag = (flg >> 3) & 1 == 1;

    // Parse BD block max size.
    const block_max_value = (bd >> 4) & 7;
    if (block_max_value < 4 or block_max_value > 7) return error.DecompressionFailed;

    var pos: usize = 7;
    if (content_size_flag) {
        if (pos + 8 > input.len) return error.DecompressionFailed;
        pos += 8;
    }

    var out_pos: usize = 0;
    while (pos + 4 <= input.len) {
        const block_size_raw = std.mem.readInt(u32, input[pos..][0..4], .little);
        pos += 4;
        const compressed = (block_size_raw & lz4_incompressible_mask) == 0;
        const block_size: usize = @intCast(block_size_raw & ~lz4_incompressible_mask);

        // End mark: block_size_raw == 0 (regardless of compressed flag).
        if (block_size == 0) {
            if (content_checksum) {
                if (pos + 4 > input.len) return error.DecompressionFailed;
                pos += 4;
            }
            return out_pos;
        }

        if (pos + block_size > input.len) return error.DecompressionFailed;

        if (!compressed) {
            // Uncompressed block: copy raw data.
            if (out_pos + block_size > out.len) return error.BufferTooSmall;
            @memcpy(out[out_pos..][0..block_size], input[pos..][0..block_size]);
            out_pos += block_size;
        } else {
            // Compressed block: full LZ4 block decode.
            const written = lz4BlockDecompress(input[pos..][0..block_size], out[out_pos..]) catch
                return error.DecompressionFailed;
            out_pos += written;
        }
        pos += block_size;

        // Optional block checksum (4 bytes LE).
        if (block_checksum) {
            if (pos + 4 > input.len) return error.DecompressionFailed;
            pos += 4;
        }
    }

    return error.DecompressionFailed;
}

/// Count the decompressed output bytes of an LZ4 block without writing them.
/// Scans all sequences (token + literal length extensions + match length
/// extensions) to sum literals + match lengths. The last sequence is
/// literals-only (no match). `error.DecompressionFailed` on corrupt input.
///
/// LZ4 block format (https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md):
/// Each sequence: token byte (high nibble = literal length, low nibble =
/// match length - 4), optional literal length extension bytes (if nibble == 15,
/// add 255 for each 0xFF, then add the first non-0xFF byte), literal data,
/// 2-byte LE offset (if not the last sequence), optional match length
/// extension bytes (if nibble == 15). The last sequence has no match.
fn lz4BlockCountOutput(input: []const u8) DecompressError!usize {
    var pos: usize = 0;
    var total: usize = 0;

    while (pos < input.len) {
        const token = input[pos];
        pos += 1;

        // Literal length.
        var lit_len: usize = token >> 4;
        if (lit_len == 15) {
            while (true) {
                if (pos >= input.len) return error.DecompressionFailed;
                const b = input[pos];
                pos += 1;
                lit_len += b;
                if (b != 255) break;
            }
        }

        // Literal data.
        if (pos + lit_len > input.len) return error.DecompressionFailed;
        total += lit_len;
        pos += lit_len;

        // The last sequence has only literals (no match). The decoder knows
        // the block is done when all input is consumed. If we've consumed all
        // input, this was the last sequence.
        if (pos == input.len) break;

        // Match: 2-byte LE offset.
        if (pos + 2 > input.len) return error.DecompressionFailed;
        pos += 2; // skip offset

        // Match length.
        var match_len: usize = (token & 0x0F) + 4; // encoded as length - 4
        if ((token & 0x0F) == 15) {
            while (true) {
                if (pos >= input.len) return error.DecompressionFailed;
                const b = input[pos];
                pos += 1;
                match_len += b;
                if (b != 255) break;
            }
        }
        total += match_len;
    }

    return total;
}

/// Decompress an LZ4 block from `input` into `out`, returning bytes written.
/// Handles all token types: literals + matches with 2-byte LE offset.
/// `error.DecompressionFailed` on corrupt input (truncated, bad offset, etc.).
/// Zero heap allocation.
///
/// LZ4 block format: see `lz4BlockCountOutput` for the format description.
fn lz4BlockDecompress(input: []const u8, out: []u8) DecompressError!usize {
    var in_pos: usize = 0;
    var out_pos: usize = 0;

    while (in_pos < input.len) {
        const token = input[in_pos];
        in_pos += 1;

        // Literal length.
        var lit_len: usize = token >> 4;
        if (lit_len == 15) {
            while (true) {
                if (in_pos >= input.len) return error.DecompressionFailed;
                const b = input[in_pos];
                in_pos += 1;
                lit_len += b;
                if (b != 255) break;
            }
        }

        // Copy literal data.
        if (in_pos + lit_len > input.len) return error.DecompressionFailed;
        if (out_pos + lit_len > out.len) return error.BufferTooSmall;
        @memcpy(out[out_pos..][0..lit_len], input[in_pos..][0..lit_len]);
        in_pos += lit_len;
        out_pos += lit_len;

        // Last sequence: only literals, no match.
        if (in_pos == input.len) break;

        // Match: 2-byte LE offset.
        if (in_pos + 2 > input.len) return error.DecompressionFailed;
        const offset: usize = @as(usize, input[in_pos]) | (@as(usize, input[in_pos + 1]) << 8);
        in_pos += 2;
        if (offset == 0 or offset > out_pos) return error.DecompressionFailed;

        // Match length.
        var match_len: usize = (token & 0x0F) + 4;
        if ((token & 0x0F) == 15) {
            while (true) {
                if (in_pos >= input.len) return error.DecompressionFailed;
                const b = input[in_pos];
                in_pos += 1;
                match_len += b;
                if (b != 255) break;
            }
        }

        // Copy match (handles overlap like memmove).
        if (out_pos + match_len > out.len) return error.BufferTooSmall;
        var i: usize = 0;
        while (i < match_len) : (i += 1) {
            out[out_pos + i] = out[out_pos - offset + i];
        }
        out_pos += match_len;
    }

    return out_pos;
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

// --- Gzip tests ---

test "gzip: empty input round-trips" {
    const input = "";
    var comp: [64]u8 = undefined;
    const clen = try compress(.gzip, input, &comp, default_level);
    // Empty gzip: 10-byte header + 5-byte empty stored block + 8-byte footer = 23.
    try testing.expectEqual(@as(usize, 23), clen);

    const dlen = try decompressedLen(.gzip, comp[0..clen]);
    try testing.expectEqual(@as(usize, 0), dlen);

    var back: [8]u8 = undefined;
    const blen = try decompress(.gzip, comp[0..clen], &back);
    try testing.expectEqual(@as(usize, 0), blen);
}

test "gzip: small input round-trips" {
    const input = "hello, kafka gzip!";
    var comp: [128]u8 = undefined;
    const clen = try compress(.gzip, input, &comp, default_level);
    try testing.expect(clen > 0);
    try testing.expect(compressBound(.gzip, input.len) >= clen);

    const dlen = try decompressedLen(.gzip, comp[0..clen]);
    try testing.expectEqual(input.len, dlen);

    var back: [128]u8 = undefined;
    const blen = try decompress(.gzip, comp[0..clen], &back);
    try testing.expectEqualSlices(u8, input, back[0..blen]);
}

test "gzip: header is present in compressed output" {
    // The compressed output must begin with the gzip magic 0x1f 0x8b so a
    // Java GZIPInputStream accepts it.
    const input = "kafka interop requires the gzip container";
    var comp: [128]u8 = undefined;
    const clen = try compress(.gzip, input, &comp, default_level);
    try testing.expect(clen > 10);
    try testing.expectEqual(@as(u8, 0x1f), comp[0]);
    try testing.expectEqual(@as(u8, 0x8b), comp[1]);
    try testing.expectEqual(@as(u8, 0x08), comp[2]); // CM = deflate
}

test "gzip: round-trips through std.compress.flate.Decompress" {
    // Verify our hand-rolled gzip store-block encoder produces output that
    // std.compress.flate.Decompress (the same code GZIPInputStream-equivalent
    // uses) can decompress. This is the interop guarantee.
    const input = "the quick brown fox jumps over the lazy dog" ** 8;
    var comp: [512]u8 = undefined;
    const clen = try compress(.gzip, input, &comp, default_level);

    var back: [512]u8 = undefined;
    const blen = try decompress(.gzip, comp[0..clen], &back);
    try testing.expectEqualSlices(u8, input, back[0..blen]);
}

test "gzip: large input round-trips (> 65535, multi-block)" {
    // Input larger than deflate_stored_max (65535) must chain multiple stored
    // blocks and still round-trip.
    var input: [128 * 1024]u8 = undefined;
    var rng = std.Random.DefaultPrng.init(0xC0FFEE);
    for (&input, 0..) |*b, i| b.* = @truncate(rng.random().int(u8) ^ @as(u8, @truncate(i)));

    const bound = compressBound(.gzip, input.len);
    const comp = try testing.allocator.alloc(u8, bound);
    defer testing.allocator.free(comp);
    const clen = try compress(.gzip, &input, comp, default_level);
    try testing.expect(bound >= clen);

    const dlen = try decompressedLen(.gzip, comp[0..clen]);
    try testing.expectEqual(input.len, dlen);

    const back = try testing.allocator.alloc(u8, dlen);
    defer testing.allocator.free(back);
    const blen = try decompress(.gzip, comp[0..clen], back);
    try testing.expectEqualSlices(u8, &input, back[0..blen]);
}

test "gzip: too-small out buffer returns BufferTooSmall" {
    const input = "hello, gzip compression test";
    var comp: [128]u8 = undefined;
    const clen = try compress(.gzip, input, &comp, default_level);

    var back: [4]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, decompress(.gzip, comp[0..clen], &back));
}

test "gzip: compress too-small out buffer returns BufferTooSmall" {
    const input = "this is too long for a 2-byte buffer";
    var out: [2]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, compress(.gzip, input, &out, default_level));
}

test "gzip: corrupt input returns DecompressionFailed" {
    // Too short for a gzip footer.
    const bad = [_]u8{ 0x1f, 0x8b, 0x08 };
    var out: [8]u8 = undefined;
    try testing.expectError(error.DecompressionFailed, decompress(.gzip, &bad, &out));
    try testing.expectError(error.DecompressionFailed, decompressedLen(.gzip, &bad));
}

test "gzip: compressBound is always >= actual compressed size" {
    inline for (.{ 0, 1, 100, 65535, 65536, 131072 }) |size| {
        const input = try testing.allocator.alloc(u8, size);
        defer testing.allocator.free(input);
        @memset(input, 0xAB);
        const bound = compressBound(.gzip, size);
        const comp = try testing.allocator.alloc(u8, bound);
        defer testing.allocator.free(comp);
        const clen = try compress(.gzip, input, comp, default_level);
        try testing.expect(bound >= clen);
    }
}

// --- LZ4 tests ---

test "lz4: empty input round-trips" {
    const input = "";
    var comp: [32]u8 = undefined;
    const clen = try compress(.lz4, input, &comp, default_level);
    // Empty lz4 frame: 7-byte header + 4-byte end mark = 11.
    try testing.expectEqual(@as(usize, 11), clen);

    const dlen = try decompressedLen(.lz4, comp[0..clen]);
    try testing.expectEqual(@as(usize, 0), dlen);

    var back: [8]u8 = undefined;
    const blen = try decompress(.lz4, comp[0..clen], &back);
    try testing.expectEqual(@as(usize, 0), blen);
}

test "lz4: small input round-trips" {
    const input = "hello, kafka lz4!";
    var comp: [128]u8 = undefined;
    const clen = try compress(.lz4, input, &comp, default_level);
    try testing.expect(clen > 0);
    try testing.expect(compressBound(.lz4, input.len) >= clen);

    const dlen = try decompressedLen(.lz4, comp[0..clen]);
    try testing.expectEqual(input.len, dlen);

    var back: [128]u8 = undefined;
    const blen = try decompress(.lz4, comp[0..clen], &back);
    try testing.expectEqualSlices(u8, input, back[0..blen]);
}

test "lz4: frame magic and header present" {
    // The compressed output must begin with the LZ4 frame magic 0x184D2204
    // (LE) so a Kafka Java Lz4BlockInputStream accepts it.
    const input = "kafka lz4 frame format interop";
    var comp: [128]u8 = undefined;
    const clen = try compress(.lz4, input, &comp, default_level);
    try testing.expect(clen > 7);
    const magic = std.mem.readInt(u32, comp[0..4], .little);
    try testing.expectEqual(@as(u32, 0x184D2204), magic);
    // FLG: version=1, blockIndependence=1 → 0x60.
    try testing.expectEqual(@as(u8, 0x60), comp[4]);
    // BD: blockMaxSize=4 (64KB) → 0x40.
    try testing.expectEqual(@as(u8, 0x40), comp[5]);
}

test "lz4: large input round-trips (> 64KB, multi-block)" {
    // Input larger than lz4_block_chunk (64KB) must split into multiple
    // uncompressed blocks and still round-trip.
    var input: [128 * 1024]u8 = undefined;
    var rng = std.Random.DefaultPrng.init(0xDEADBEEF);
    for (&input, 0..) |*b, i| b.* = @truncate(rng.random().int(u8) ^ @as(u8, @truncate(i)));

    const bound = compressBound(.lz4, input.len);
    const comp = try testing.allocator.alloc(u8, bound);
    defer testing.allocator.free(comp);
    const clen = try compress(.lz4, &input, comp, default_level);
    try testing.expect(bound >= clen);

    const dlen = try decompressedLen(.lz4, comp[0..clen]);
    try testing.expectEqual(input.len, dlen);

    const back = try testing.allocator.alloc(u8, dlen);
    defer testing.allocator.free(back);
    const blen = try decompress(.lz4, comp[0..clen], back);
    try testing.expectEqualSlices(u8, &input, back[0..blen]);
}

test "lz4: decompresses compressed blocks (with matches)" {
    // The decoder must handle compressed LZ4 blocks (with back-references),
    // not just uncompressed blocks. We hand-construct a compressed LZ4 block
    // for "ABABABABABAB" (12 bytes) and wrap it in an LZ4 frame.
    //
    // LZ4 block for "ABABABABABAB":
    //   token: lit_len=2 (high nibble), match_len-4=6 (low nibble) → 0x26
    //   literals: "AB"
    //   offset: 2 (LE) → 0x02 0x00
    //   match_len: 6 (from token, no extension)
    //   Then final sequence: token 0x40 (lit_len=4), literals "ABAB"
    //
    // Wait — let me construct this more carefully. The block decodes to:
    //   seq 1: lit "AB" (2 bytes), match len=6 off=2 → produces "AB" + "ABABAB" = 8 bytes
    //   seq 2 (last): lit "ABAB" (4 bytes) → 4 bytes
    //   Total: 12 bytes = "ABABABABABAB"
    //
    // Token 1: lit_len=2 → high nibble=2, match_len=6 → low nibble=2 (6-4=2) → 0x22
    //   literals: 'A' 'B'
    //   offset: 0x02 0x00
    // Token 2 (last): lit_len=4 → high nibble=4, no match → 0x40
    //   literals: 'A' 'B' 'A' 'B'
    const lz4_block = [_]u8{ 0x22, 'A', 'B', 0x02, 0x00, 0x40, 'A', 'B', 'A', 'B' };

    // Wrap in an LZ4 frame: header + compressed block + end mark.
    var frame: [64]u8 = undefined;
    var pos: usize = 0;
    std.mem.writeInt(u32, frame[pos..][0..4], 0x184D2204, .little);
    pos += 4;
    frame[pos] = 0x60; // FLG
    pos += 1;
    frame[pos] = 0x40; // BD
    pos += 1;
    frame[pos] = lz4Hc(); // HC
    pos += 1;
    // Compressed block: block_size = lz4_block.len (no incompressible mask).
    std.mem.writeInt(u32, frame[pos..][0..4], @as(u32, lz4_block.len), .little);
    pos += 4;
    @memcpy(frame[pos..][0..lz4_block.len], &lz4_block);
    pos += lz4_block.len;
    // End mark.
    std.mem.writeInt(u32, frame[pos..][0..4], 0, .little);
    pos += 4;

    const dlen = try decompressedLen(.lz4, frame[0..pos]);
    try testing.expectEqual(@as(usize, 12), dlen);

    var back: [12]u8 = undefined;
    const blen = try decompress(.lz4, frame[0..pos], &back);
    try testing.expectEqual(@as(usize, 12), blen);
    try testing.expectEqualSlices(u8, "ABABABABABAB", back[0..blen]);
}

test "lz4: too-small out buffer returns BufferTooSmall" {
    const input = "hello, lz4 compression test";
    var comp: [128]u8 = undefined;
    const clen = try compress(.lz4, input, &comp, default_level);

    var back: [4]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, decompress(.lz4, comp[0..clen], &back));
}

test "lz4: compress too-small out buffer returns BufferTooSmall" {
    const input = "this is too long for a 2-byte buffer";
    var out: [2]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, compress(.lz4, input, &out, default_level));
}

test "lz4: corrupt input returns DecompressionFailed" {
    // Too short for an LZ4 frame header.
    const bad = [_]u8{ 0x04, 0x22, 0x4d };
    var out: [8]u8 = undefined;
    try testing.expectError(error.DecompressionFailed, decompress(.lz4, &bad, &out));
    try testing.expectError(error.DecompressionFailed, decompressedLen(.lz4, &bad));
}

test "lz4: wrong magic returns DecompressionFailed" {
    const bad = [_]u8{ 0x00, 0x00, 0x00, 0x00, 0x60, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00 };
    var out: [8]u8 = undefined;
    try testing.expectError(error.DecompressionFailed, decompress(.lz4, &bad, &out));
}

test "lz4: compressBound is always >= actual compressed size" {
    inline for (.{ 0, 1, 100, 65536, 131072 }) |size| {
        const input = try testing.allocator.alloc(u8, size);
        defer testing.allocator.free(input);
        @memset(input, 0xAB);
        const bound = compressBound(.lz4, size);
        const comp = try testing.allocator.alloc(u8, bound);
        defer testing.allocator.free(comp);
        const clen = try compress(.lz4, input, comp, default_level);
        try testing.expect(bound >= clen);
    }
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
