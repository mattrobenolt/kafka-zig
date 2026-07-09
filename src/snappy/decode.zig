//! Snappy block decoder with a SIMD overlapping-copy fast path.
//!
//! The tag stream is decoded scalarly (it's inherently sequential — a copy can
//! reference bytes just written). The win is in the copy operations:
//!
//!   - `offset >= length` (non-overlapping): a plain `@memcpy`, which the
//!     compiler lowers to vector loads/stores.
//!   - `offset < length` (overlapping / RLE): the pattern of `offset` bytes
//!     repeats. For `offset <= 16` we load 16 bytes at `dst - offset`, permute
//!     them into the repeating pattern with one shuffle instruction (`tbl` on
//!     AArch64, `pshufb` on x86_64), and store 16-byte chunks — no byte loop.
//!     For `offset > 16` the source doesn't overlap within a 16-byte window, so
//!     a vectorized `@memcpy` is correct and fast.
//!
//! The shuffle masks are comptime-generated: `pattern_mask[offset-1][i] =
//! i % offset`, which is exactly the PSHUFB/TBL control word that replicates the
//! first `offset` bytes across 16. Index `>= 16` is impossible since each mask
//! is for a specific `offset` in `[1, 16]` and every entry is `< offset <= 16`.
//!
//! Portable fallback (other arches / no asm): extend the pattern to 16 bytes
//! with a short byte loop, then copy 16-byte chunks — the compiler vectorizes
//! the chunks. This matches the reference snappy non-SIMD path.
//!
//! Format: https://github.com/google/snappy/blob/main/format_description.txt

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const builtin = @import("builtin");
const native_arch = builtin.target.cpu.arch;

const common = @import("common.zig");
const readUvarint = common.readUvarint;
const writeUvarint = common.writeUvarint;

/// Error set for the decode path. Public because `decompressBlock` and
/// `decompressedBlockLen` expose it in their signatures.
pub const DecompressError = error{ BufferTooSmall, DecompressionFailed };

/// 16-byte vector type for the shuffle loads/stores. PascalCase because it's
/// a type alias (per Zig convention); ziglint's Z006 misfires on type-alias
/// consts and has no inline-ignore directive, so this is documented here.
const Vec16 = @Vector(16, u8); // ziglint-ignore: Z006

/// Comptime shuffle mask for a given pattern size: `mask[i] = i % pattern_size`.
/// Used as the `tbl`/`pshufb` control word to replicate the first `pattern_size`
/// bytes across a 16-byte register. Every entry is `< pattern_size <= 16`, so
/// no index is out of range.
fn patternMask(comptime pattern_size: u8, comptime base: u8) Vec16 {
    var m: Vec16 = undefined;
    for (0..16) |i| m[i] = @intCast((@as(u16, base) + i) % pattern_size);
    return m;
}

/// Generation masks: `pattern_masks[offset-1][i] = i % offset`. Loaded once,
/// applied to the raw source bytes to produce the first 16-byte repeating
/// pattern. Mask bytes are all `< offset <= 16`, satisfying `tbl`/`pshufb`.
const pattern_masks: [16]Vec16 = blk: {
    var table: [16]Vec16 = undefined;
    for (1..17) |ps| table[ps - 1] = patternMask(@intCast(ps), 0);
    break :blk table;
};

/// Reshuffle masks: `reshuffle_masks[offset-1][i] = (16 + i) % offset`. Applied
/// to the current pattern vector to produce the *next* 16 bytes, advancing the
/// phase. For offsets that divide 16 (1, 2, 4, 8, 16) this equals the
/// generation mask, so the pattern is exactly periodic over 16 and re-storing
/// the same vector is correct; for other offsets it shifts the phase. Entries
/// are `< offset <= 16`, so all in range for `tbl`/`pshufb`.
const reshuffle_masks: [16]Vec16 = blk: {
    var table: [16]Vec16 = undefined;
    for (1..17) |ps| table[ps - 1] = patternMask(@intCast(ps), 16);
    break :blk table;
};

/// Load 16 bytes from `src` as a `Vec16` (unaligned). Using `align(1)` tells
/// the compiler the address may be unaligned, so it emits an unaligned vector
/// load (`ldur` on AArch64, `movdqu` on x86_64) rather than an aligned one.
inline fn loadVec(src: [*]const u8) Vec16 {
    const ptr: *align(1) const Vec16 = @ptrCast(src);
    return ptr.*;
}

/// Store 16 bytes of `v` to `dst` (unaligned).
inline fn storeVec(dst: [*]u8, v: Vec16) void {
    const ptr: *align(1) Vec16 = @ptrCast(dst);
    ptr.* = v;
}

/// Shuffle `v` by the comptime-selected `mask` using the native SIMD
/// instruction. On AArch64 this is `tbl`; on x86_64 (SSSE3+) `pshufb`. The mask
/// is chosen at runtime from `pattern_masks`, so it is NOT comptime-known to
/// the shuffle itself — we hand the runtime mask to the instruction directly.
inline fn shuffleBytes(v: Vec16, mask: Vec16) Vec16 {
    switch (native_arch) {
        .aarch64 => {
            var out: Vec16 = undefined;
            asm volatile ("tbl %[out].16b, { %[v].16b }, %[mask].16b"
                : [out] "=w" (out),
                : [v] "w" (v),
                  [mask] "w" (mask),
            );
            return out;
        },
        .x86_64 => {
            // PSHUFB is a 2-operand instruction (SSSE3+): `pshufb dest, mask`
            // shuffles `dest` in place using `mask`. AT&T syntax (Zig's x86
            // default) is `pshufb %{mask}, %{out}`, src-first. We use `+x` so
            // `v` is read and written, then return it.
            // Gate on SSSE3: pshufb requires SSSE3, not just x86_64. Baseline
            // x86_64 (no SSSE3) falls through to the portable byte permute.
            if (comptime std.Target.x86.featureSetHas(builtin.cpu.features, .ssse3)) {
                var out = v;
                asm volatile ("pshufb %[mask], %[out]"
                    : [out] "+x" (out),
                    : [mask] "x" (mask),
                );
                return out;
            } else {
                var out: Vec16 = undefined;
                for (0..16) |i| out[i] = v[mask[i]];
                return out;
            }
        },
        else => {
            // Portable fallback: a byte-wise permute. The compiler can often
            // recognize this as a shuffle on targets with a suitable instruction;
            // otherwise it stays a scalar loop (cold path for non-SIMD arches).
            var out: Vec16 = undefined;
            for (0..16) |i| out[i] = v[mask[i]];
            return out;
        },
    }
}

/// Copy `length` bytes from `out[pos - offset ..]` to `out[pos ..]`, handling
/// overlap (RLE). `offset > 0` and `offset <= pos` and `pos + length <= out.len`
/// are asserted by the caller. This is the hot path for copy tags.
///
/// Strategy:
///   - `offset >= length`: no overlap within the copy window -> `@memcpy`.
///   - `offset <= 16`: replicate the `offset`-byte pattern with one SIMD
///     shuffle (`tbl`/`pshufb`), store 16-byte chunks, and re-shuffle each
///     chunk to advance the phase (so non-power-of-two offsets stay aligned).
///   - `offset > 16`: the 16-byte source window at `pos - offset` does not
///     overlap the 16-byte store at `pos`, so a vectorized chunked copy is
///     safe.
fn copyMatch(out: []u8, pos: usize, offset: usize, length: usize) void {
    assert(offset > 0);
    assert(offset <= pos);
    assert(pos + length <= out.len);

    // Non-overlapping: a single vectorized memcpy. The common case for matches
    // into earlier, distinct data.
    if (offset >= length) {
        @memcpy(out[pos..][0..length], out[pos - offset ..][0..length]);
        return;
    }

    if (offset <= 16) {
        // Build the 16-byte pattern source safely. The repeating pattern is the
        // first `offset` bytes at `out[pos-offset]`; loading a full 16 bytes
        // there could overread past `out.len` near the tail (the output buffer
        // is sized to the decompressed length, with no slop). So copy just the
        // `offset` source bytes into a zero-padded 16-byte buffer — the shuffle
        // mask only references indices `< offset`, so the padding is never used.
        var src16: [16]u8 = @splat(0);
        @memcpy(src16[0..offset], out[pos - offset ..][0..offset]);
        const pattern0: Vec16 = src16;
        var pattern = shuffleBytes(pattern0, pattern_masks[offset - 1]);
        const reshuffle = reshuffle_masks[offset - 1];
        const end = pos + length;
        var p = pos;
        while (p + 16 <= end) : (p += 16) {
            storeVec(out.ptr + p, pattern);
            pattern = shuffleBytes(pattern, reshuffle);
        }
        // Tail: the remaining (< 16) bytes are the leading bytes of the current
        // pattern window — `pattern[i]` is exactly the byte for this position.
        const tail = end - p;
        var i: usize = 0;
        while (i < tail) : (i += 1) {
            out[p + i] = pattern[i];
        }
        return;
    }

    // offset > 16 but < length: overlapping, but the 16-byte source window
    // starting at pos-offset does not overlap the 16-byte store at pos, so a
    // forward 16-byte-chunk memcpy is safe and vectorizes.
    var p = pos;
    const end = pos + length;
    while (p + 16 <= end) : (p += 16) {
        storeVec(out.ptr + p, loadVec(out.ptr + (p - offset)));
    }
    const tail = end - p;
    if (tail > 0) {
        @memcpy(out[p..][0..tail], out[p - offset ..][0..tail]);
    }
}

/// Decompressed byte length of a raw snappy block (the varint at the start).
/// `error.DecompressionFailed` if the varint is corrupt/truncated.
pub fn decompressedBlockLen(input: []const u8) DecompressError!usize {
    var pos: usize = 0;
    return readUvarint(input, &pos);
}

/// Decompress a raw snappy block from `input` into `out`, returning bytes
/// written. `error.BufferTooSmall` when `out` is too small (size via
/// `decompressedBlockLen`). `error.DecompressionFailed` on corrupt input. Zero
/// heap allocation.
pub fn decompressBlock(input: []const u8, out: []u8) DecompressError!usize {
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
                // 0–59 => length = field + 1. 60–63 => length is in the next
                // 1–4 bytes (LE), plus 1.
                const code6: u8 = tag >> 2;
                var length: usize = undefined;
                if (code6 < 60) {
                    length = @as(usize, code6) + 1;
                } else {
                    const extra: usize = @as(usize, code6) - 59; // 60→1 .. 63→4
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
                // Copy with 1-byte offset: length = ((tag >> 2) & 7) + 4,
                // offset = ((tag >> 5) << 8) | next_byte.
                const length: usize = @as(usize, (tag >> 2) & 0x07) + 4;
                if (in_pos >= input.len) return error.DecompressionFailed;
                const offset: usize = (@as(usize, tag >> 5) << 8) | @as(usize, input[in_pos]);
                in_pos += 1;
                if (offset == 0 or offset > out_pos) return error.DecompressionFailed;
                if (out_pos + length > uncomp_len) return error.DecompressionFailed;
                copyMatch(out, out_pos, offset, length);
                out_pos += length;
            },
            2 => {
                // Copy with 2-byte offset: length = (tag >> 2) + 1,
                // offset = next 2 bytes LE.
                const length: usize = @as(usize, tag >> 2) + 1;
                if (in_pos + 1 >= input.len) return error.DecompressionFailed;
                const offset: usize = @as(usize, input[in_pos]) |
                    (@as(usize, input[in_pos + 1]) << 8);
                in_pos += 2;
                if (offset == 0 or offset > out_pos) return error.DecompressionFailed;
                if (out_pos + length > uncomp_len) return error.DecompressionFailed;
                copyMatch(out, out_pos, offset, length);
                out_pos += length;
            },
            3 => {
                // Copy with 4-byte offset: length = (tag >> 2) + 1,
                // offset = next 4 bytes LE.
                const length: usize = @as(usize, tag >> 2) + 1;
                if (in_pos + 3 >= input.len) return error.DecompressionFailed;
                const offset: usize = @as(usize, input[in_pos]) |
                    (@as(usize, input[in_pos + 1]) << 8) |
                    (@as(usize, input[in_pos + 2]) << 16) |
                    (@as(usize, input[in_pos + 3]) << 24);
                in_pos += 4;
                if (offset == 0 or offset > out_pos) return error.DecompressionFailed;
                if (out_pos + length > uncomp_len) return error.DecompressionFailed;
                copyMatch(out, out_pos, offset, length);
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

test "decompress: empty block" {
    const block = [_]u8{0x00}; // varint(0)
    const dlen = try decompressedBlockLen(&block);
    try testing.expectEqual(@as(usize, 0), dlen);
    var out: [1]u8 = undefined;
    const blen = try decompressBlock(&block, &out);
    try testing.expectEqual(@as(usize, 0), blen);
}

test "decompress: literal-only block" {
    // varint(13) + literal tag (13-1)<<2=0x30 + 13 bytes.
    const msg = "hello, kafka!";
    var block: [16]u8 = undefined;
    block[0] = 0x0D;
    block[1] = 0x30;
    @memcpy(block[2..][0..msg.len], msg);
    const dlen = try decompressedBlockLen(block[0 .. 2 + msg.len]);
    try testing.expectEqual(msg.len, dlen);
    var out: [32]u8 = undefined;
    const blen = try decompressBlock(block[0 .. 2 + msg.len], &out);
    try testing.expectEqualSlices(u8, msg, out[0..blen]);
}

test "decompress: copy with 1-byte offset (overlapping RLE)" {
    // Decompressed: "ABABABAB" (8 bytes).
    // varint(8)=0x08, literal "AB" (tag 0x04), copy len6 off2 (0x09 0x02).
    const snappy_block = [_]u8{ 0x08, 0x04, 'A', 'B', 0x09, 0x02 };
    var out: [8]u8 = undefined;
    const blen = try decompressBlock(&snappy_block, &out);
    try testing.expectEqual(@as(usize, 8), blen);
    try testing.expectEqualSlices(u8, "ABABABAB", out[0..blen]);
}

test "decompress: copy with 2-byte offset" {
    // Decompressed: "XXXX" + 64 bytes of 'Y' (68 bytes total).
    // varint(68)=0x44, literal "XXXX" (tag 0x0C), copy len64 off4 (0xFE 0x04 0x00).
    const snappy_block = [_]u8{ 0x44, 0x0C, 'X', 'X', 'X', 'X', 0xFE, 0x04, 0x00 };
    var out: [68]u8 = undefined;
    const blen = try decompressBlock(&snappy_block, &out);
    try testing.expectEqual(@as(usize, 68), blen);
    try testing.expectEqualSlices(u8, "XXXX", out[0..4]);
    for (out[4..68], 0..) |b, i| {
        try testing.expectEqual(out[i % 4], b);
    }
}

test "decompress: overlapping copy offset 1 (single-byte run)" {
    // varint(5)=0x05, literal "A" (tag 0x00), copy len4 off1 (0x01 0x01).
    const snappy_block = [_]u8{ 0x05, 0x00, 'A', 0x01, 0x01 };
    var out: [5]u8 = undefined;
    const blen = try decompressBlock(&snappy_block, &out);
    try testing.expectEqual(@as(usize, 5), blen);
    try testing.expectEqualSlices(u8, "AAAAA", out[0..blen]);
}

test "decompress: corrupt block returns DecompressionFailed" {
    const bad = [_]u8{0x80}; // truncated varint
    var out: [8]u8 = undefined;
    try testing.expectError(error.DecompressionFailed, decompressBlock(&bad, &out));
    try testing.expectError(error.DecompressionFailed, decompressedBlockLen(&bad));
}

test "decompress: too-small out buffer returns BufferTooSmall" {
    // varint(13) + literal tag + 13 bytes, but out is only 4 bytes.
    const msg = "hello, kafka!";
    var block: [16]u8 = undefined;
    block[0] = 0x0D;
    block[1] = 0x30;
    @memcpy(block[2..][0..msg.len], msg);
    var out: [4]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, decompressBlock(block[0 .. 2 + msg.len], &out));
}

test "copyMatch: non-overlapping uses plain copy" {
    var buf: [64]u8 = undefined;
    @memset(&buf, 0);
    @memcpy(buf[0..8], "abcdefgh");
    copyMatch(&buf, 16, 16, 8); // copy buf[0..8] to buf[16..24]
    try testing.expectEqualSlices(u8, "abcdefgh", buf[16..24]);
}

test "copyMatch: SIMD shuffle for small offsets round-trips" {
    // Exercise the shuffle path for every offset 1..16 with a length that
    // spans multiple 16-byte chunks plus a tail.
    for (1..17) |offset| {
        var buf: [256]u8 = undefined;
        @memset(&buf, 0);
        // Seed the first `offset` bytes with a recognizable pattern.
        for (0..offset) |i| buf[i] = @intCast(0xA0 + (i % 16));
        const length = 64 + offset; // > 16, forces chunked stores + tail
        copyMatch(&buf, offset, offset, length);
        // Verify the repeating pattern across the whole copied region.
        for (0..length) |i| {
            try testing.expectEqual(buf[i % offset], buf[offset + i]);
        }
    }
}

// ---------------------------------------------------------------------------
// Golden decode vectors — ported from golang/snappy's TestDecode, TestDecodeCopy4,
// and TestDecodeLengthOffset. These are the authoritative conformance cases:
// every tag type, every extended-literal length form, and the corrupt-input
// rejections (zero offset, offset past start, inconsistent dLen, truncated
// length/offset bytes). A conformant snappy decoder must produce exactly the
// documented output or reject exactly the documented corrupt input.
//
// Source: https://github.com/golang/snappy/blob/master/snappy_test.go
//
// Note on encoder golden vectors: snappy does NOT mandate a canonical
// compressed form ("there is more than one valid encoding of any given input",
// per golang/snappy). So we do NOT assert byte-identical encoder output against
// reference corpora — only that our output round-trips and our decoder accepts
// any valid block. These decode vectors are the real interop bar.
// ---------------------------------------------------------------------------

/// Sentinel byte range [0xa0, 0xc5) used to detect decoder overrun: the output
/// buffer is pre-filled with these (cycling), and after decode every byte past
/// `dLen` must be untouched. 37 is prime so a mis-copied byte lands at an
/// unrelated offset (a 'natural' 32 could mask a 4*8-byte-off copy). This
/// matches golang/snappy's notPresentBase/notPresentLen.
const overrun_base: u8 = 0xa0;
const overrun_len: u8 = 37;

/// One decode test case: `input` is a raw snappy block (varint dLen + tags),
/// `want` is the expected decompressed bytes (empty when expecting an error),
/// and `want_err` is true when the input must be rejected as corrupt.
const DecodeCase = struct {
    desc: []const u8,
    input: []const u8,
    want: []const u8,
    want_err: bool,
};

/// Run one decode case against `decompressBlock`, checking the decoded bytes
/// and that no byte past dLen in `d_buf` was modified (overrun check).
fn checkDecodeCase(d_buf: []u8, tc: DecodeCase) !void {
    // The input must not contain the sentinel bytes, or the overrun check is
    // meaningless. (All golang vectors satisfy this by construction.)
    for (tc.input) |x| {
        try testing.expect(!(overrun_base <= x and x < overrun_base + overrun_len));
    }

    // Pre-fill d_buf with the cycling sentinel.
    for (d_buf, 0..) |*b, j| b.* = overrun_base + @as(u8, @intCast(j % overrun_len));

    // dLen is the leading varint; size the output window to it.
    var vp: usize = 0;
    const d_len = try readUvarint(tc.input, &vp);
    try testing.expect(d_len <= d_buf.len);

    if (tc.want_err) {
        try testing.expectError(error.DecompressionFailed, decompressBlock(tc.input, d_buf[0..d_len]));
        return;
    }

    const n = try decompressBlock(tc.input, d_buf[0..d_len]);
    try testing.expectEqualSlices(u8, tc.want, d_buf[0..n]);

    // Overrun: every byte from dLen onward must still hold its sentinel.
    for (d_buf[d_len..], 0..) |x, j| {
        try testing.expectEqual(overrun_base + @as(u8, @intCast((d_len + j) % overrun_len)), x);
    }
}

test "golden decode: golang/snappy TestDecode vector table" {
    // lit40 = 40 bytes 0..39, used by the length=40 literal case.
    var lit40: [40]u8 = undefined;
    for (&lit40, 0..) |*b, i| b.* = @intCast(i);

    const cases = [_]DecodeCase{
        .{
            .desc = "dLen=0; valid",
            .input = "\x00",
            .want = "",
            .want_err = false,
        },
        .{
            .desc = "dLen=3; lit 0-byte len; valid",
            .input = "\x03\x08\xff\xff\xff",
            .want = "\xff\xff\xff",
            .want_err = false,
        },
        .{
            .desc = "dLen=2; lit 0-byte len; not enough dst",
            .input = "\x02\x08\xff\xff\xff",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=3; lit 0-byte len; not enough src",
            .input = "\x03\x08\xff\xff",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=40; lit 0-byte len; valid",
            .input = blk: {
                var b: [42]u8 = undefined;
                b[0] = 0x28;
                b[1] = 0x9c;
                @memcpy(b[2..][0..lit40.len], &lit40);
                break :blk &b;
            },
            .want = &lit40,
            .want_err = false,
        },
        .{
            .desc = "dLen=1; lit 1-byte len; truncated",
            .input = "\x01\xf0",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=3; lit 1-byte len; valid",
            .input = "\x03\xf0\x02\xff\xff\xff",
            .want = "\xff\xff\xff",
            .want_err = false,
        },
        .{
            .desc = "dLen=1; lit 2-byte len; truncated",
            .input = "\x01\xf4\x00",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=3; lit 2-byte len; valid",
            .input = "\x03\xf4\x02\x00\xff\xff\xff",
            .want = "\xff\xff\xff",
            .want_err = false,
        },
        .{
            .desc = "dLen=1; lit 3-byte len; truncated",
            .input = "\x01\xf8\x00\x00",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=3; lit 3-byte len; valid",
            .input = "\x03\xf8\x02\x00\x00\xff\xff\xff",
            .want = "\xff\xff\xff",
            .want_err = false,
        },
        .{
            .desc = "dLen=1; lit 4-byte len; truncated",
            .input = "\x01\xfc\x00\x00\x00",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=1; lit 4-byte len; not enough dst",
            .input = "\x01\xfc\x02\x00\x00\x00\xff\xff\xff",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=4; lit 4-byte len; not enough src",
            .input = "\x04\xfc\x02\x00\x00\x00\xff",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=3; lit 4-byte len; valid",
            .input = "\x03\xfc\x02\x00\x00\x00\xff\xff\xff",
            .want = "\xff\xff\xff",
            .want_err = false,
        },
        .{
            .desc = "dLen=4; copy1; truncated extra",
            .input = "\x04\x01",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=4; copy2; truncated extra",
            .input = "\x04\x02\x00",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=4; copy4; truncated extra",
            .input = "\x04\x03\x00\x00\x00",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=4; lit 'abcd'; valid",
            .input = "\x04\x0cabcd",
            .want = "abcd",
            .want_err = false,
        },
        .{
            .desc = "dLen=13; lit abcd; copy1 len9 off4",
            .input = "\x0d\x0cabcd\x15\x04",
            .want = "abcdabcdabcda",
            .want_err = false,
        },
        .{
            .desc = "dLen=8; lit abcd; copy1 len4 off4",
            .input = "\x08\x0cabcd\x01\x04",
            .want = "abcdabcd",
            .want_err = false,
        },
        .{
            .desc = "dLen=8; lit abcd; copy1 len4 off2",
            .input = "\x08\x0cabcd\x01\x02",
            .want = "abcdcdcd",
            .want_err = false,
        },
        .{
            .desc = "dLen=8; lit abcd; copy1 len4 off1",
            .input = "\x08\x0cabcd\x01\x01",
            .want = "abcddddd",
            .want_err = false,
        },
        .{
            .desc = "dLen=8; lit abcd; copy1 len4 off0; zero offset",
            .input = "\x08\x0cabcd\x01\x00",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=9; lit abcd; copy1 len4 off4; bad dLen",
            .input = "\x09\x0cabcd\x01\x04",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=8; lit abcd; copy1 len4 off5; offset too large",
            .input = "\x08\x0cabcd\x01\x05",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=7; lit abcd; copy1 len4 off4; length too large",
            .input = "\x07\x0cabcd\x01\x04",
            .want = "",
            .want_err = true,
        },
        .{
            .desc = "dLen=6; lit abcd; copy2 len2 off3",
            .input = "\x06\x0cabcd\x06\x03\x00",
            .want = "abcdbc",
            .want_err = false,
        },
        .{
            .desc = "dLen=6; lit abcd; copy4 len2 off3",
            .input = "\x06\x0cabcd\x07\x03\x00\x00\x00",
            .want = "abcdbc",
            .want_err = false,
        },
        .{
            .desc = "dLen=0; copy4; msb set (0x93); go-fuzz",
            .input = "\x00\xfc000\x93",
            .want = "",
            .want_err = true,
        },
    };

    var d_buf: [100]u8 = undefined;
    for (cases) |tc| {
        checkDecodeCase(&d_buf, tc) catch |err| {
            std.debug.print("\nFAIL: {s}\n", .{tc.desc});
            return err;
        };
    }
}

test "golden decode: large copy-4 offset (golang TestDecodeCopy4)" {
    // decodedLen=65545: a 4-byte literal "pqrs", a 65536-byte literal of '.',
    // then a copy-4 of length 5 offset 65540 (back into the start). Exercises
    // the 4-byte-offset copy with a real large offset and a 64KiB literal.
    const dots_len: usize = 65536;
    const total: usize = 3 + 5 + (3 + dots_len) + 5; // varint(65545)=3, lit pqrs, lit dots, copy4
    const input = try testing.allocator.alloc(u8, total);
    defer testing.allocator.free(input);
    var p: usize = 0;
    // varint 65545 = 0x89 0x80 0x04
    input[p] = 0x89;
    input[p + 1] = 0x80;
    input[p + 2] = 0x04;
    p += 3;
    // literal "pqrs" (length 4 -> tag (4-1)<<2 = 0x0c)
    input[p] = 0x0c;
    @memcpy(input[p + 1 ..][0..4], "pqrs");
    p += 5;
    // literal 65536 '.' (length 65536 -> 2-byte extended: tag 0xf4, len-1 LE)
    input[p] = 0xf4;
    const n: u32 = @intCast(dots_len - 1);
    input[p + 1] = @truncate(n);
    input[p + 2] = @truncate(n >> 8);
    p += 3;
    @memset(input[p..][0..dots_len], '.');
    p += dots_len;
    // copy-4: length 5, offset 65540. tag = ((5-1)<<2)|0b11 = 0x13.
    // offset 65540 = 0x00010004 LE.
    input[p] = 0x13;
    input[p + 1] = 0x04;
    input[p + 2] = 0x00;
    input[p + 3] = 0x01;
    input[p + 4] = 0x00;
    p += 5;
    try testing.expectEqual(total, p);

    const d_len: usize = 65545;
    const out = try testing.allocator.alloc(u8, d_len);
    defer testing.allocator.free(out);
    const got = try decompressBlock(input, out);
    try testing.expectEqual(d_len, got);
    // want = "pqrs" + dots + "pqrs."
    try testing.expectEqualSlices(u8, "pqrs", out[0..4]);
    for (out[4..][0..dots_len]) |b| try testing.expectEqual(@as(u8, '.'), b);
    try testing.expectEqualSlices(u8, "pqrs.", out[4 + dots_len ..][0..5]);
}

test "golden decode: literal + copy2 + literal (golang TestDecodeLengthOffset)" {
    // Exhaustive sweep over length, offset, suffixLen (1..18 each) of a
    // literal(prefix) + copy2(length, offset) + literal(suffix) pattern, with
    // the overrun check. This stresses copyMatch across every small offset
    // and length combination, including overlapping (offset < length) RLE.
    const prefix = "abcdefghijklmnopqr"; // 18 bytes
    const suffix = "ABCDEFGHIJKLMNOPQR"; // 18 bytes
    var got_buf: [128]u8 = undefined;
    var want_buf: [128]u8 = undefined;
    var input_buf: [128]u8 = undefined;

    for (1..19) |length| {
        for (1..19) |offset| {
            for (0..19) |suffix_len| {
                const total_len = prefix.len + length + suffix_len;

                // Build the input block: varint(total_len) + literal(prefix)
                // + copy2(length, offset) + [literal(suffix)].
                var p: usize = 0;
                p += writeUvarint(input_buf[p..], total_len) catch unreachable;
                input_buf[p] = @as(u8, @intCast(prefix.len - 1)) << 2; // tagLiteral
                p += 1;
                @memcpy(input_buf[p..][0..prefix.len], prefix);
                p += prefix.len;
                input_buf[p] = @as(u8, @intCast(length - 1)) << 2 | 0b10; // tagCopy2
                input_buf[p + 1] = @truncate(@as(u32, @intCast(offset)));
                input_buf[p + 2] = 0x00;
                p += 3;
                if (suffix_len > 0) {
                    input_buf[p] = @as(u8, @intCast(suffix_len - 1)) << 2; // tagLiteral
                    p += 1;
                    @memcpy(input_buf[p..][0..suffix_len], suffix[0..suffix_len]);
                    p += suffix_len;
                }
                const input = input_buf[0..p];

                // Pre-fill got_buf with sentinels and decode.
                for (&got_buf, 0..) |*b, j| b.* = overrun_base +
                    @as(u8, @intCast(j % overrun_len));
                const n = decompressBlock(input, got_buf[0..total_len]) catch |err| {
                    std.debug.print("\nFAIL length={d} offset={d} suffixLen={d}: {s}\n", .{
                        length, offset, suffix_len, @errorName(err),
                    });
                    return err;
                };

                // Build the expected output: prefix + (length bytes copied
                // from offset back) + suffix.
                var w: usize = 0;
                @memcpy(want_buf[w..][0..prefix.len], prefix);
                w += prefix.len;
                for (0..length) |i| {
                    want_buf[w + i] = want_buf[w + i - offset];
                }
                w += length;
                @memcpy(want_buf[w..][0..suffix_len], suffix[0..suffix_len]);
                w += suffix_len;
                try testing.expectEqualSlices(u8, want_buf[0..w], got_buf[0..n]);

                // Overrun check: bytes past total_len must be untouched.
                for (got_buf[total_len..], 0..) |x, j| {
                    try testing.expectEqual(overrun_base + @as(u8, @intCast((total_len + j) % overrun_len)), x);
                }
            }
        }
    }
}

test "golden decode: format_description.txt hand examples" {
    // From the snappy format spec, section 2.2 (Copies):
    //   "xababab" could be encoded as <literal: "xab"> <copy: offset=2 length=4>
    // Decompressed = "xababab" (3 literal + 4 copied from offset 2 → "abab").
    //   varint(7)=0x07, literal "xab" (tag (3-1)<<2=0x08), copy1 len4 off2
    //   (tag = 01|((4-4)<<2)|((2>>8)<<5) = 0x01, off byte 0x02).
    const block = [_]u8{ 0x07, 0x08, 'x', 'a', 'b', 0x01, 0x02 };
    var out: [7]u8 = undefined;
    const n = try decompressBlock(&block, &out);
    try testing.expectEqualSlices(u8, "xababab", out[0..n]);
}

test "golden decode: varint prefix examples from the spec" {
    // Section 1 (Preamble): uncompressed length 64 -> 0x40; 2097150 (0x1FFFFE)
    // -> 0xFE 0xFF 0x7F. We only need decompressedBlockLen to read them back.
    var pos: usize = 0;
    try testing.expectEqual(@as(usize, 64), try readUvarint(&[_]u8{0x40}, &pos));
    pos = 0;
    try testing.expectEqual(@as(usize, 2097150), try readUvarint(&[_]u8{ 0xFE, 0xFF, 0x7F }, &pos));
}
