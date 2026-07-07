//! Compression codec slot for the v2 record batch (PLAN §1, §2.1, §6).
//!
//! The record batch stores its records region either verbatim (`none`) or
//! compressed with one codec (compression bits 0–2 of the attributes field).
//! v1 implements plaintext + zstd only; gzip/snappy/lz4 return
//! `error.NotImplemented`.
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
        .gzip, .snappy, .lz4 => return error.NotImplemented,
    }
}

/// Worst-case compressed size for `input_len` bytes with `codec`. The caller
/// sizes its `out` buffer to this before calling `compress`. `none` is the
/// identity bound (`input_len`). For `zstd` when disabled the value is unused
/// (compress returns NotImplemented first) and falls back to `input_len`.
pub fn compressBound(codec: Codec, input_len: usize) usize {
    switch (codec) {
        .none => return input_len,
        .zstd => {
            if (!zstd_enabled) return input_len;
            return c.ZSTD_compressBound(input_len);
        },
        .gzip, .snappy, .lz4 => return input_len,
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
        .gzip, .snappy, .lz4 => return error.NotImplemented,
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
        .gzip, .snappy, .lz4 => return error.NotImplemented,
    }
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
    try testing.expectError(error.NotImplemented, compress(.snappy, "x", &out, 3));
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
