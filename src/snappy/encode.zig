//! Snappy block encoder — LZ77 with a hash-table match finder.
//!
//! This is a port of the algorithm used by klauspost/compress's
//! `encodeBlockSnappyGo64K` (itself derived from the Snappy-Go reference),
//! which is the same shape as Google's C++ `CompressFragment`: hash 6 bytes at
//! the cursor into a power-of-two `u16` position table, scan forward, and on a
//! 4-byte match extend it 8 bytes at a time using `@ctz` on the XOR to find the
//! first mismatch byte. A skip heuristic gradually strides past incompressible
//! regions. If the encoded form would exceed `src - src>>5 - 5` bytes the whole
//! block is emitted as a single literal (snappy guarantees it never expands
//! past its bound).
//!
//! Zero heap allocation: the hash table is a fixed `[1<<14]u16` stack frame
//! (32 KiB), sized for the 64 KiB max block. The caller provides the `out`
//! buffer sized via `maxCompressedLength`.
//!
//! Format: https://github.com/google/snappy/blob/main/format_description.txt
//! Reference encoder: https://github.com/klauspost/compress (s2/encode_all.go)

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const mem = std.mem;

const common = @import("common.zig");
const uvarintSize = common.uvarintSize;
const writeUvarint = common.writeUvarint;
const decode = @import("decode.zig");

/// Worst-case compressed size for a raw snappy block of `input_len` bytes:
/// the varint length prefix plus the literal-only blowup bound. The real
/// encoder bails to a single literal when it can't compress, so this is the
/// true worst case (one tag byte per 60-byte chunk + the varint). Matches only
/// ever shrink the output.
pub fn maxCompressedLength(input_len: usize) usize {
    return uvarintSize(input_len) + input_len + (input_len + 59) / 60;
}

/// Maximum raw block size. Snappy positions are stored as `u16`, so a block
/// is at most 64 KiB. The Kafka Xerial framing splits inputs into 32 KiB
/// blocks, well under this limit. Larger inputs must be split by the caller.
pub const max_block_size: usize = 65536;
/// Minimum input length below which the encoder skips match-finding and emits
/// the whole block as one literal. Matches the reference `minNonLiteralBlockSize`
/// (32): below this there is nothing to gain from the hash table.
const min_non_literal_block_size: usize = 32;

/// Number of trailing input bytes reserved so the literal fast path can
/// over-copy 16 bytes without running off the input. Matches the reference
/// `inputMargin` (8): the literal fast path loads 8 bytes; keeping 8 in margin
/// means a 1-byte literal's 8-byte load stays in bounds.
const input_margin: usize = 8;

/// Hash table size: `1 << table_bits` u16 entries = 32 KiB on the stack.
/// `table_bits = 14` is what the reference uses for <=64K snappy blocks; it
/// indexes the high-quality bits of the 6-byte hash. Larger tables trade stack
/// for slightly fewer collisions (marginally better ratio); 14 is the sweet
/// spot the reference settled on.
const table_bits = 14;
const table_size: usize = 1 << table_bits;
const table_mask: usize = table_size - 1;

/// 6-byte hash multiplier (klauspost `prime6bytes`). Multiplying the low 48
/// bits of a u64 by this and taking the top `table_bits` bits gives a
/// well-distributed hash for the position table.
const prime6bytes: u64 = 227718039650203;

/// Hash the low 6 bytes of `u` into a table index.
inline fn hash6(u: u64) usize {
    return @intCast((u << 16) *% prime6bytes >> @intCast(64 - table_bits));
}

/// Load 8 bytes from `b[i..]` as a little-endian u64. `i + 8` must be within
/// `b.len` (the caller guarantees this via the margin / bounds checks).
inline fn load64(b: []const u8, i: usize) u64 {
    return readInt(u64, b[i..][0..8]);
}

/// Load 4 bytes from `b[i..]` as a little-endian u32.
inline fn load32(b: []const u8, i: usize) u32 {
    return readInt(u32, b[i..][0..4]);
}

/// Reads an integer from memory with bit count specified by T.
/// The bit count of T must be evenly divisible by 8.
/// This function cannot fail and cannot cause undefined behavior.
/// Forces little-endianness.
inline fn readInt(comptime T: type, buffer: *const [@divExact(@typeInfo(T).int.bits, 8)]u8) T {
    return mem.readInt(T, buffer, .little);
}

/// Writes an integer to memory, storing it in twos-complement.
/// This function always succeeds, has defined behavior for all inputs, but
/// the integer bit width must be divisible by 8.
/// Forces little-endianness.
inline fn writeInt(comptime T: type, buffer: *[@divExact(@typeInfo(T).int.bits, 8)]u8, value: T) void {
    return mem.writeInt(T, buffer, value, .little);
}

/// Number of leading matching bytes of `a` and `b`, compared 8 bytes at a time,
/// using `@ctz` on the first differing u64 to find the exact mismatch byte.
/// Assumes `a_base <= b_base` positions into the same `src` and that neither
/// read runs off the end (the caller keeps an 8-byte margin). Returns the match
/// length starting from the current `a`/`b` offsets.
fn matchLen(src: []const u8, a: usize, b: usize, limit: usize) usize {
    var p = a;
    var q = b;
    while (q + 8 <= limit) {
        const x = load64(src, p) ^ load64(src, q);
        if (x != 0) {
            // First differing byte = number of trailing zero bits / 8.
            return p - a + (@ctz(x) >> 3);
        }
        p += 8;
        q += 8;
    }
    while (q < limit) : ({
        p += 1;
        q += 1;
    }) {
        if (src[p] != src[q]) return p - a;
    }
    return p - a;
}

/// Compress `src` into `out` as a raw snappy block (varint uncompressed length
/// + tag stream). Returns bytes written, or `error.BufferTooSmall`. Zero heap
/// allocation. The block is at most 64 KiB; larger inputs are split by the
/// Xerial framing layer in `compress.zig`.
///
/// For `src.len < min_non_literal_block_size` the block is a single literal —
/// the hash table can't help. Otherwise the match finder runs; if it can't beat
/// the bail threshold the whole block is emitted as one literal (snappy never
/// expands past its bound).
pub fn compressBlock(src: []const u8, out: []u8) error{BufferTooSmall}!usize {
    // Positions are stored as u16, so a raw block is at most 64 KiB. The Xerial
    // framing layer splits at 32 KiB, so this is never hit from the Kafka path;
    // direct callers must respect it.
    assert(src.len <= max_block_size);

    // The varint length prefix.
    const d = writeUvarint(out, src.len) catch return error.BufferTooSmall;

    if (src.len < min_non_literal_block_size) {
        return d + try emitLiteral(out[d..], src);
    }

    const n = encodeBlock(out[d..], src) catch return error.BufferTooSmall;
    if (n == 0) {
        // Not compressible within the bail threshold: emit the whole input as
        // one literal. Re-check the bound since emitLiteral may need more room
        // than the match finder's partial output did.
        return d + try emitLiteral(out[d..], src);
    }
    return d + n;
}

/// Core match-finding encoder. Writes the tag stream (no varint prefix) into
/// `dst` and returns the byte count, or 0 if the result would exceed the bail
/// threshold (caller emits a single literal instead). `dst` must be at least
/// `maxCompressedLength(src.len) - uvarintSize(src.len)` bytes.
fn encodeBlock(dst: []u8, src: []const u8) error{BufferTooSmall}!usize {
    var table: [table_size]u16 = @splat(0);

    // Stop match-finding this far from the end so the 8-byte literal/copy fast
    // paths stay in bounds.
    const s_limit: usize = src.len - input_margin;
    // Bail threshold: if the encoded form exceeds this, give up and emit a
    // literal. `src - src>>5 - 5` is the reference's "must compress to at least
    // this" bound — it leaves headroom for the literal fallback to fit.
    const dst_limit: usize = src.len - (src.len >> 5) - 5;

    var d: usize = 0;
    var next_emit: usize = 0;
    // The stream cannot start with a copy, so begin scanning one byte in.
    var s: usize = 1;
    var cv = load64(src, s);
    // Last copy offset, used for the repeat-offset check. Initialized so the
    // first check (against src[1-repeat]) reads src[0], which is harmless.
    var repeat: usize = 1;

    outer: while (true) {
        var candidate: usize = 0;
        scan: while (true) {
            // Skip stride: move further ahead the longer we go without a match,
            // so incompressible regions are skipped quickly.
            const next_s = s + ((s - next_emit) >> 5) + 4;
            if (next_s > s_limit) break :outer;

            const h0 = hash6(cv);
            const h1 = hash6(cv >> 8);
            candidate = table[h0];
            const candidate2 = table[h1];
            table[h0] = @intCast(s);
            table[h1] = @intCast(s + 1);
            const h2 = hash6(cv >> 16);

            // Repeat-offset check: the 4 bytes one past the cursor may match the
            // 4 bytes at the last copy's offset. This catches RLE-style input
            // (e.g. runs of the same record) without a fresh hash lookup.
            const check_rep: usize = 1;
            if (load32(src, s - repeat + check_rep) == @as(u32, @truncate(cv >> (check_rep * 8)))) {
                var base = s + check_rep;
                // Extend the match backwards over any already-emitted bytes.
                while (base > next_emit and base - repeat > 0 and
                    src[base - repeat - 1] == src[base - 1])
                {
                    base -= 1;
                }
                if (d + (base - next_emit) > dst_limit) return 0;
                d += try emitLiteral(dst[d..], src[next_emit..base]);

                // Extend forwards.
                var cand = s - repeat + 4 + check_rep;
                s += 4 + check_rep;
                while (s <= s_limit) {
                    const diff = load64(src, s) ^ load64(src, cand);
                    if (diff != 0) {
                        s += @ctz(diff) >> 3;
                        break;
                    }
                    s += 8;
                    cand += 8;
                }
                if (d + emitCopySize(repeat, s - base) > dst.len) return error.BufferTooSmall;
                d += emitCopy(dst[d..], repeat, s - base);
                next_emit = s;
                if (s >= s_limit) break :outer;
                cv = load64(src, s);
                continue :scan;
            }

            if (load32(src, candidate) == @as(u32, @truncate(cv))) break :scan;
            candidate = table[h2];
            if (load32(src, candidate2) == @as(u32, @truncate(cv >> 8))) {
                table[h2] = @intCast(s + 2);
                candidate = candidate2;
                s += 1;
                break :scan;
            }
            table[h2] = @intCast(s + 2);
            if (load32(src, candidate) == @as(u32, @truncate(cv >> 16))) {
                s += 2;
                break :scan;
            }

            cv = load64(src, next_s);
            s = next_s;
        }

        // Extend a found 4-byte match backwards over emitted literals.
        while (candidate > 0 and s > next_emit and src[candidate - 1] == src[s - 1]) {
            candidate -= 1;
            s -= 1;
        }
        if (d + (s - next_emit) > dst_limit) return 0;
        d += try emitLiteral(dst[d..], src[next_emit..s]);

        // Emit copies for as long as a match starts right where the last one
        // ended (the "emitCopy then re-check immediately" inner loop).
        while (true) {
            const base = s;
            repeat = base - candidate;

            // Extend the 4-byte match forward 8 bytes at a time.
            s += 4;
            candidate += 4;
            while (s + 8 <= src.len) {
                const diff = load64(src, s) ^ load64(src, candidate);
                if (diff != 0) {
                    s += @ctz(diff) >> 3;
                    break;
                }
                s += 8;
                candidate += 8;
            }
            // Clamp to the input end: the 8-byte loop may have run to the very
            // end without a mismatch (a match that consumes the tail).
            if (s > src.len) s = src.len;

            if (d + emitCopySize(repeat, s - base) > dst.len) return error.BufferTooSmall;
            d += emitCopy(dst[d..], repeat, s - base);

            next_emit = s;
            if (s >= s_limit) break :outer;
            if (d > dst_limit) return 0;

            // Re-check for a match at the new cursor using the bytes just before
            // it (s-2), updating two table slots. This catches repeated patterns
            // without reloading cv from scratch.
            const x = load64(src, s - 2);
            const m2_hash = hash6(x);
            const curr_hash = hash6(x >> 16);
            candidate = table[curr_hash];
            table[m2_hash] = @intCast(s - 2);
            table[curr_hash] = @intCast(s);
            if (load32(src, candidate) != @as(u32, @truncate(x >> 16))) {
                cv = load64(src, s + 1);
                s += 1;
                break;
            }
        }
    }

    // Emit the trailing bytes as a literal.
    if (next_emit < src.len) {
        if (d + (src.len - next_emit) > dst_limit) return 0;
        d += try emitLiteral(dst[d..], src[next_emit..]);
    }
    return d;
}

/// Write a literal tag + the literal bytes into `dst`. Returns bytes written.
/// `error.BufferTooSmall` when `dst` cannot hold the encoding.
fn emitLiteral(dst: []u8, lit: []const u8) error{BufferTooSmall}!usize {
    if (lit.len == 0) return 0;
    const n = lit.len - 1;
    var i: usize = 0;
    if (n < 60) {
        if (dst.len < 1 + lit.len) return error.BufferTooSmall;
        dst[0] = @as(u8, @intCast(n)) << 2;
        i = 1;
    } else if (n < 256) {
        if (dst.len < 2 + lit.len) return error.BufferTooSmall;
        dst[0] = 60 << 2;
        dst[1] = @intCast(n);
        i = 2;
    } else if (n < 65536) {
        if (dst.len < 3 + lit.len) return error.BufferTooSmall;
        dst[0] = 61 << 2;
        dst[1] = @truncate(@as(u32, @intCast(n)));
        dst[2] = @truncate(@as(u32, @intCast(n)) >> 8);
        i = 3;
    } else if (n < 16777216) {
        if (dst.len < 4 + lit.len) return error.BufferTooSmall;
        dst[0] = 62 << 2;
        const v: u32 = @intCast(n);
        dst[1] = @truncate(v);
        dst[2] = @truncate(v >> 8);
        dst[3] = @truncate(v >> 16);
        i = 4;
    } else {
        if (dst.len < 5 + lit.len) return error.BufferTooSmall;
        dst[0] = 63 << 2;
        const v: u32 = @intCast(n);
        dst[1] = @truncate(v);
        dst[2] = @truncate(v >> 8);
        dst[3] = @truncate(v >> 16);
        dst[4] = @truncate(v >> 24);
        i = 5;
    }
    @memcpy(dst[i..][0..lit.len], lit);
    return i + lit.len;
}

/// Upper bound on the bytes `emitCopy` will write for one (offset, length)
/// copy, so the caller can bounds-check `dst` before emitting. A copy is at
/// most 5 bytes (copy-4) per 64-byte chunk for large offsets, or 3 bytes
/// (copy-2) per 60-byte chunk for smaller offsets, plus a final chunk (<= 5).
fn emitCopySize(offset: usize, length: usize) usize {
    if (offset >= 65536) {
        // 5 bytes per 64-byte chunk + a final chunk (<= 5 bytes).
        return 5 * ((length + 63) / 64);
    }
    // 3 bytes per 60-byte chunk + a final chunk (<= 3 bytes).
    return 3 * ((length + 59) / 60);
}

/// Write a copy tag for (offset, length) into `dst`. Returns bytes written.
/// Assumes `dst` is large enough (check via `emitCopySize` first). Splits long
/// copies into chunks: copy-4 (5 bytes, 64-byte chunks) for offsets >= 65536,
/// and copy-2 (3 bytes, 60-byte chunks) for smaller offsets. The 60-byte chunk
/// size (not 64) is deliberate — it guarantees the final remainder is >= 4, so
/// the last chunk can always use a copy-1 (length 4..11) or copy-2.
fn emitCopy(dst: []u8, offset: usize, length_in: usize) usize {
    var length = length_in;
    var i: usize = 0;

    if (offset >= 65536) {
        // 4-byte offset: chew through 64-byte chunks, then a final copy-4.
        while (length > 64) {
            dst[i] = 63 << 2 | 0b11; // length 64
            writeInt(u32, dst[i + 1 ..][0..4], @intCast(offset));
            i += 5;
            length -= 64;
        }
        dst[i] = @as(u8, @intCast(length - 1)) << 2 | 0b11;
        writeInt(u32, dst[i + 1 ..][0..4], @intCast(offset));
        return i + 5;
    }

    // Offset fits in 2 bytes. Emit 60-byte copy-2 chunks until the remainder
    // is <= 64 (and, because we used 60 not 64, >= 4 when we entered this
    // branch with length > 64).
    while (length > 64) {
        dst[i] = 59 << 2 | 0b10; // length 60
        dst[i + 1] = @truncate(@as(u32, @intCast(offset)));
        dst[i + 2] = @truncate(@as(u32, @intCast(offset)) >> 8);
        i += 3;
        length -= 60;
    }

    if (length >= 12 or offset >= 2048) {
        dst[i] = @as(u8, @intCast(length - 1)) << 2 | 0b10;
        dst[i + 1] = @truncate(@as(u32, @intCast(offset)));
        dst[i + 2] = @truncate(@as(u32, @intCast(offset)) >> 8);
        return i + 3;
    }
    // 1-byte offset copy: length 4..11, offset < 2048.
    dst[i] = @as(u8, @intCast(offset >> 8)) << 5 |
        @as(u8, @intCast(length - 4)) << 2 | 0b01;
    dst[i + 1] = @truncate(@as(u32, @intCast(offset)));
    return i + 2;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn roundTrip(input: []const u8) !void {
    const bound = maxCompressedLength(input.len);
    const comp = try testing.allocator.alloc(u8, bound);
    defer testing.allocator.free(comp);
    const clen = try compressBlock(input, comp);
    try testing.expect(clen <= bound);

    const dlen = try decode.decompressedBlockLen(comp[0..clen]);
    try testing.expectEqual(input.len, dlen);

    const back = try testing.allocator.alloc(u8, dlen);
    defer testing.allocator.free(back);
    const blen = try decode.decompressBlock(comp[0..clen], back);
    try testing.expectEqualSlices(u8, input, back[0..blen]);
}

test "encode: empty input" {
    try roundTrip("");
}

test "encode: short input (< min_non_literal_block_size) is a single literal" {
    try roundTrip("hello, kafka!");
}

test "encode: repetitive input compresses (back-references)" {
    // A repeating string should produce copy tags, so the compressed size is
    // well under the input length.
    const input = "the quick brown fox" ** 200; // 3800 bytes
    const bound = maxCompressedLength(input.len);
    const comp = try testing.allocator.alloc(u8, bound);
    defer testing.allocator.free(comp);
    const clen = try compressBlock(input, comp);
    try testing.expect(clen < input.len / 2);
    try roundTrip(input);
}

test "encode: incompressible input bails to a single literal" {
    // Random bytes shouldn't compress; the encoder emits one literal and the
    // output is slightly larger than the input (tag + length overhead).
    var input: [4096]u8 = undefined;
    var rng = std.Random.DefaultPrng.init(0xDEADBEEF);
    for (&input) |*b| b.* = rng.random().int(u8);
    const bound = maxCompressedLength(input.len);
    const comp = try testing.allocator.alloc(u8, bound);
    defer testing.allocator.free(comp);
    const clen = try compressBlock(&input, comp);
    try testing.expect(clen > input.len); // literal overhead
    try testing.expect(clen <= bound);
    try roundTrip(&input);
}

test "encode: 64K boundary round-trips" {
    var input: [65536]u8 = undefined;
    var rng = std.Random.DefaultPrng.init(0xCAFEBABE);
    for (&input, 0..) |*b, i| b.* = @truncate(rng.random().int(u8) ^ @as(u8, @truncate(i)));
    const bound = maxCompressedLength(input.len);
    const comp = try testing.allocator.alloc(u8, bound);
    defer testing.allocator.free(comp);
    const clen = try compressBlock(&input, comp);
    const dlen = try decode.decompressedBlockLen(comp[0..clen]);
    try testing.expectEqual(input.len, dlen);
    const back = try testing.allocator.alloc(u8, dlen);
    defer testing.allocator.free(back);
    const blen = try decode.decompressBlock(comp[0..clen], back);
    try testing.expectEqualSlices(u8, &input, back[0..blen]);
}

test "encode: RLE input (single-byte run)" {
    const input = [_]u8{0x41} ** 4096;
    const bound = maxCompressedLength(input.len);
    const comp = try testing.allocator.alloc(u8, bound);
    defer testing.allocator.free(comp);
    const clen = try compressBlock(&input, comp);
    // Snappy copies max at 64 bytes, so a 4096-byte run is ~one literal + many
    // 60-byte copy-2 chunks (3 bytes each): roughly 4096/60*3 ~= 205 bytes.
    // Well under the input, but not tiny.
    try testing.expect(clen < input.len / 10);
    try roundTrip(&input);
}

test "maxCompressedLength >= actual for literal-only small input" {
    inline for (.{ 0, 1, 30, 31, 60, 61, 256, 257 }) |size| {
        const input = try testing.allocator.alloc(u8, size);
        defer testing.allocator.free(input);
        @memset(input, 0xAB);
        const bound = maxCompressedLength(size);
        const comp = try testing.allocator.alloc(u8, bound);
        defer testing.allocator.free(comp);
        const clen = try compressBlock(input, comp);
        try testing.expect(bound >= clen);
    }
}

test "emitCopy: long match splits into chunks" {
    // A long match at a small offset exercises the chunked emitCopy loop
    // (many 60-byte copy-2 chunks for a single back-reference). Standard
    // snappy blocks are <= 64 KiB so offsets are always < 65536; copy-4 is
    // exercised on the decode side instead (see decode.zig).
    var input: [65536]u8 = undefined;
    @memset(&input, 0);
    // A 64-byte alphabet repeated ~1000 times: one long copy of length ~65500.
    for (0..64) |i| input[i] = @intCast('A' + i % 26);
    for (64..input.len) |i| input[i] = input[i % 64];
    const bound = maxCompressedLength(input.len);
    const comp = try testing.allocator.alloc(u8, bound);
    defer testing.allocator.free(comp);
    const clen = try compressBlock(&input, comp);
    // A 64-byte period over 64 KiB: ~65536/60 copy-2 chunks * 3 bytes ~= 3.3K.
    // Highly compressible but bounded by snappy's 64-byte copy limit.
    try testing.expect(clen < input.len / 10);
    try roundTrip(&input);
}
