//! Kafka wire-protocol primitive readers and writers.
//!
//! Sans-I/O and zero-allocation: no heap parameter appears anywhere in this file.
//! The `Reader` is a cursor over a caller-owned `[]const u8`; every read is
//! bounds-checked. The write helpers take a `*std.Io.Writer` the caller
//! provides (fixed buffer or growable). This is the only module that knows
//! about endianness and varint encoding.
//!
//! Spec: https://kafka.apache.org/protocol.html — "Protocol Primitive Types".
//! Compact types (compact_string/compact_bytes/compact_array, uvarint length
//! encoded as N+1) are from KIP-482 (flexible versions), documented on the
//! same page under the flexible-version note.

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const native_endian = std.cpu.arch.endian();
const be: std.builtin.Endian = .big;

// ---------------------------------------------------------------------------
// Fixed-width big-endian integers
// ---------------------------------------------------------------------------

/// Read a big-endian integer of type `T` from `reader`.
pub fn readInt(reader: *Reader, comptime T: type) error{EndOfStream}!T {
    const n = comptime @divExact(@typeInfo(T).int.bits, 8);
    // Overflow-safe: `buf.len - pos` cannot underflow because `pos <= buf.len`
    // is a maintained invariant of the Reader.
    assert(reader.pos <= reader.buf.len);
    if (n > reader.buf.len - reader.pos) return error.EndOfStream;
    const bytes: *const [n]u8 = @ptrCast(reader.buf[reader.pos..][0..n]);
    reader.pos += n;
    return std.mem.readInt(T, bytes, be);
}

/// Write a big-endian integer of type `T` to `writer`.
pub fn writeInt(writer: *std.Io.Writer, comptime T: type, value: T) std.Io.Writer.Error!void {
    try writer.writeInt(T, value, be);
}

pub fn readI8(r: *Reader) error{EndOfStream}!i8 {
    return @bitCast(try readU8(r));
}
pub fn readU8(r: *Reader) error{EndOfStream}!u8 {
    if (r.pos >= r.buf.len) return error.EndOfStream;
    const v = r.buf[r.pos];
    r.pos += 1;
    return v;
}
pub fn writeI8(w: *std.Io.Writer, v: i8) std.Io.Writer.Error!void {
    try w.writeByte(@bitCast(v));
}
pub fn writeU8(w: *std.Io.Writer, v: u8) std.Io.Writer.Error!void {
    try w.writeByte(v);
}

pub fn readI16(r: *Reader) error{EndOfStream}!i16 {
    return @bitCast(try readInt(r, u16));
}
pub fn readU16(r: *Reader) error{EndOfStream}!u16 {
    return readInt(r, u16);
}
pub fn writeI16(w: *std.Io.Writer, v: i16) std.Io.Writer.Error!void {
    try writeInt(w, u16, @bitCast(v));
}
pub fn writeU16(w: *std.Io.Writer, v: u16) std.Io.Writer.Error!void {
    try writeInt(w, u16, v);
}

pub fn readI32(r: *Reader) error{EndOfStream}!i32 {
    return @bitCast(try readInt(r, u32));
}
pub fn readU32(r: *Reader) error{EndOfStream}!u32 {
    return readInt(r, u32);
}
pub fn writeI32(w: *std.Io.Writer, v: i32) std.Io.Writer.Error!void {
    try writeInt(w, u32, @bitCast(v));
}
pub fn writeU32(w: *std.Io.Writer, v: u32) std.Io.Writer.Error!void {
    try writeInt(w, u32, v);
}

pub fn readI64(r: *Reader) error{EndOfStream}!i64 {
    return @bitCast(try readInt(r, u64));
}
pub fn writeI64(w: *std.Io.Writer, v: i64) std.Io.Writer.Error!void {
    try writeInt(w, u64, @bitCast(v));
}

// ---------------------------------------------------------------------------
// Boolean (i8 0/1)
// ---------------------------------------------------------------------------

/// Kafka `BOOLEAN`: i8, 0 = false, any non-zero value = true. Spec:
/// https://kafka.apache.org/protocol.html → "Protocol Primitive Types":
/// BOOLEAN — "boolean value (0 = false, anything else = true)."
pub fn readBool(r: *Reader) error{ EndOfStream, Malformed }!bool {
    const raw = try readI8(r);
    return raw != 0;
}

pub fn writeBool(w: *std.Io.Writer, v: bool) std.Io.Writer.Error!void {
    try writeI8(w, if (v) 1 else 0);
}

// ---------------------------------------------------------------------------
// Zigzag varint / varlong (i32 / i64)
// ---------------------------------------------------------------------------

/// Zigzag-encode an i32 for varint. `(n << 1) ^ (n >> 31)`.
inline fn zigzag32(n: i32) u32 {
    return @bitCast((n << 1) ^ (n >> 31));
}

/// Zigzag-encode an i64 for varlong. `(n << 1) ^ (n >> 63)`.
inline fn zigzag64(n: i64) u64 {
    return @bitCast((n << 1) ^ (n >> 63));
}

/// Reverse zigzag on a u32 to recover the i32.
inline fn unzigzag32(n: u32) i32 {
    return @bitCast((n >> 1) ^ (~(n & 1) +% 1));
}

/// Reverse zigzag on a u64 to recover the i64.
inline fn unzigzag64(n: u64) i64 {
    return @bitCast((n >> 1) ^ (~(n & 1) +% 1));
}

/// Kafka `VARINT`: zigzag-encoded i32 as LEB128. Spec: "Protocol Primitive
/// Types" → VARINT.
pub fn readVarint(r: *Reader) error{ EndOfStream, Malformed }!i32 {
    const raw = try readUvarintBounded(r, 5);
    if (raw > std.math.maxInt(u32)) return error.Malformed;
    return unzigzag32(@intCast(raw));
}

/// Kafka `VARLONG`: zigzag-encoded i64 as LEB128.
pub fn readVarlong(r: *Reader) error{ EndOfStream, Malformed }!i64 {
    const raw = try readUvarintBounded(r, 10);
    return unzigzag64(raw);
}

pub fn writeVarint(w: *std.Io.Writer, v: i32) std.Io.Writer.Error!void {
    try writeUvarint(w, zigzag32(v));
}

pub fn writeVarlong(w: *std.Io.Writer, v: i64) std.Io.Writer.Error!void {
    try writeUvarint(w, zigzag64(v));
}

// ---------------------------------------------------------------------------
// Unsigned LEB128 varint (no zigzag) — compact lengths
// ---------------------------------------------------------------------------

/// Read an unsigned LEB128 varint, enforcing a maximum of `max_bytes` so a
/// truncated or over-long sequence can't loop or overflow. Returns the value
/// as a u64; callers that expect a smaller width must range-check.
///
/// For the final byte in a max-length sequence, the payload bits that would
/// shift past the integer width are rejected as `error.Malformed` rather than
/// silently truncated. For a 10-byte u64 varint the 10th byte's payload (low
/// 7 bits) must be <= 0x01 (1 bit of headroom after 9×7=63 bits). For a 5-byte
/// u32 varint the 5th byte's payload must be <= 0x0f (4 bits of headroom after
/// 4×7=28 bits). Without this, a malformed length could truncate to a small
/// value (including 0, colliding with the compact null sentinel).
fn readUvarintBounded(r: *Reader, max_bytes: usize) error{ EndOfStream, Malformed }!u64 {
    var result: u64 = 0;
    var shift: u7 = 0;
    var i: usize = 0;
    while (i < max_bytes) : (i += 1) {
        if (r.pos >= r.buf.len) return error.EndOfStream;
        const byte = r.buf[r.pos];
        r.pos += 1;
        const part: u64 = @intCast(byte & 0x7f);
        if (shift >= 64) return error.Malformed;
        result |= part << @intCast(shift);
        if (byte & 0x80 == 0) {
            // Final byte. On the max-length byte, reject payload bits that
            // would shift past the target integer width. Without this, a
            // 10-byte varint with a 10th-byte payload of 0x02 would silently
            // truncate to 0 (bits shift out of u64), colliding with the
            // compact null sentinel. For a 10-byte u64 varint the 10th byte
            // has 1 bit of headroom (9×7=63 bits consumed), so payload <= 1.
            // For a 5-byte u32 varint the 5th byte has 4 bits of headroom
            // (4×7=28 bits consumed), so payload <= 0x0f.
            if (i == max_bytes - 1) {
                const max_payload: u64 = switch (max_bytes) {
                    10 => 0x01,
                    5 => 0x0f,
                    else => 0x7f,
                };
                if (part > max_payload) return error.Malformed;
            }
            return result;
        }
        shift += 7;
    }
    // Ran out of bytes without a terminator.
    return error.Malformed;
}

/// Kafka `UNSIGNED_VARINT`: LEB128, no zigzag. Used for compact-string /
/// compact-array / compact-bytes lengths (encoded as N+1). Spec: KIP-482.
pub fn readUvarint(r: *Reader) error{ EndOfStream, Malformed }!u64 {
    return readUvarintBounded(r, 10);
}

pub fn writeUvarint(w: *std.Io.Writer, value: u64) std.Io.Writer.Error!void {
    var v = value;
    while (v >= 0x80) {
        try w.writeByte(@intCast((v & 0x7f) | 0x80));
        v >>= 7;
    }
    try w.writeByte(@intCast(v));
}

// ---------------------------------------------------------------------------
// String types
// ---------------------------------------------------------------------------

/// Kafka `STRING`: i16 length + UTF-8 bytes. Non-null: negative length is
/// `error.Malformed`. Use `readNullableString` for the nullable variant.
/// Spec: https://kafka.apache.org/protocol.html → "Protocol Primitive Types":
/// STRING — "string with length encoded as int16; null is not a valid value."
pub fn readString(r: *Reader) error{ EndOfStream, Malformed }![]const u8 {
    const len = try readI16(r);
    if (len < 0) return error.Malformed;
    return try r.readSlice(@intCast(len));
}

/// Like `readString` but distinguishes null from empty. Returns `null` only
/// for length exactly -1; any other negative length is `error.Malformed`.
/// Spec: NULLABLE_STRING — "string with length encoded as int16 where -1
/// indicates null."
pub fn readNullableString(r: *Reader) error{ EndOfStream, Malformed }!?[]const u8 {
    const len = try readI16(r);
    if (len == -1) return null;
    if (len < 0) return error.Malformed;
    return try r.readSlice(@intCast(len));
}

pub fn writeString(w: *std.Io.Writer, s: []const u8) std.Io.Writer.Error!void {
    assert(s.len <= std.math.maxInt(i16));
    try writeI16(w, @intCast(s.len));
    try w.writeAll(s);
}

pub fn writeNullableString(w: *std.Io.Writer, s: ?[]const u8) std.Io.Writer.Error!void {
    if (s) |actual| {
        try writeString(w, actual);
    } else {
        try writeI16(w, -1);
    }
}

/// Kafka `COMPACT_STRING`: uvarint (len+1) + UTF-8 bytes. Non-null: the `0`
/// sentinel belongs to the nullable variant and is `error.Malformed` here.
/// `1` = empty, `2` = one byte, etc. Spec: KIP-482 flexible versions —
/// https://kafka.apache.org/protocol.html → "Protocol Primitive Types":
/// COMPACT_STRING — "string with length encoded as uvarint + 1; null is not
/// a valid value."
pub fn readCompactString(r: *Reader) error{ EndOfStream, Malformed }![]const u8 {
    const len_plus_one = try readUvarint(r);
    if (len_plus_one == 0) return error.Malformed;
    const len: usize = @intCast(len_plus_one - 1);
    return try r.readSlice(len);
}

/// Nullable compact string. Returns `null` only for `len_plus_one == 0`;
/// any other invalid value is caught by the uvarint/bounds checks.
/// Spec: COMPACT_NULLABLE_STRING — "uvarint length+1 where 0 indicates null."
pub fn readNullableCompactString(r: *Reader) error{ EndOfStream, Malformed }!?[]const u8 {
    const len_plus_one = try readUvarint(r);
    if (len_plus_one == 0) return null;
    const len: usize = @intCast(len_plus_one - 1);
    return try r.readSlice(len);
}

pub fn writeCompactString(w: *std.Io.Writer, s: []const u8) std.Io.Writer.Error!void {
    try writeUvarint(w, @as(u64, s.len) + 1);
    try w.writeAll(s);
}

pub fn writeNullableCompactString(w: *std.Io.Writer, s: ?[]const u8) std.Io.Writer.Error!void {
    if (s) |actual| {
        try writeCompactString(w, actual);
    } else {
        try writeUvarint(w, 0);
    }
}

// ---------------------------------------------------------------------------
// Bytes types
// ---------------------------------------------------------------------------

/// Kafka `BYTES`: i32 length + raw bytes. Non-null: negative length is
/// `error.Malformed`. Use `readNullableBytes` for the nullable variant.
/// Spec: https://kafka.apache.org/protocol.html → "Protocol Primitive Types":
/// BYTES — "bytes with length encoded as int32; null is not a valid value."
pub fn readBytes(r: *Reader) error{ EndOfStream, Malformed }![]const u8 {
    const len = try readI32(r);
    if (len < 0) return error.Malformed;
    return try r.readSlice(@intCast(len));
}

/// Nullable bytes. Returns `null` only for length exactly -1; any other
/// negative length is `error.Malformed`.
/// Spec: NULLABLE_BYTES — "bytes with length encoded as int32 where -1
/// indicates null."
pub fn readNullableBytes(r: *Reader) error{ EndOfStream, Malformed }!?[]const u8 {
    const len = try readI32(r);
    if (len == -1) return null;
    if (len < 0) return error.Malformed;
    return try r.readSlice(@intCast(len));
}

pub fn writeBytes(w: *std.Io.Writer, b: []const u8) std.Io.Writer.Error!void {
    assert(b.len <= std.math.maxInt(i32));
    try writeI32(w, @intCast(b.len));
    try w.writeAll(b);
}

pub fn writeNullableBytes(w: *std.Io.Writer, b: ?[]const u8) std.Io.Writer.Error!void {
    if (b) |actual| {
        try writeBytes(w, actual);
    } else {
        try writeI32(w, -1);
    }
}

/// Kafka `COMPACT_BYTES`: uvarint (len+1) + raw bytes. Non-null: the `0`
/// sentinel belongs to the nullable variant and is `error.Malformed` here.
/// Spec: KIP-482 — https://kafka.apache.org/protocol.html → COMPACT_BYTES:
/// "bytes with length encoded as uvarint + 1; null is not a valid value."
pub fn readCompactBytes(r: *Reader) error{ EndOfStream, Malformed }![]const u8 {
    const len_plus_one = try readUvarint(r);
    if (len_plus_one == 0) return error.Malformed;
    const len: usize = @intCast(len_plus_one - 1);
    return try r.readSlice(len);
}

/// Nullable compact bytes. Returns `null` only for `len_plus_one == 0`.
/// Spec: COMPACT_NULLABLE_BYTES — "uvarint length+1 where 0 indicates null."
pub fn readNullableCompactBytes(r: *Reader) error{ EndOfStream, Malformed }!?[]const u8 {
    const len_plus_one = try readUvarint(r);
    if (len_plus_one == 0) return null;
    const len: usize = @intCast(len_plus_one - 1);
    return try r.readSlice(len);
}

pub fn writeCompactBytes(w: *std.Io.Writer, b: []const u8) std.Io.Writer.Error!void {
    try writeUvarint(w, @as(u64, b.len) + 1);
    try w.writeAll(b);
}

pub fn writeNullableCompactBytes(w: *std.Io.Writer, b: ?[]const u8) std.Io.Writer.Error!void {
    if (b) |actual| {
        try writeCompactBytes(w, actual);
    } else {
        try writeUvarint(w, 0);
    }
}

// ---------------------------------------------------------------------------
// Array count readers/writers
// ---------------------------------------------------------------------------

/// Kafka `ARRAY`: i32 count prefix. -1 = null. The element encoding is the
/// caller's job — these helpers only read/write the count. Returns `null`
/// when the count is the null sentinel.
pub fn readArrayCount(r: *Reader) error{ EndOfStream, Malformed }!?usize {
    const count = try readI32(r);
    if (count == -1) return null;
    if (count < 0) return error.Malformed;
    return @intCast(count);
}

pub fn writeArrayCount(w: *std.Io.Writer, count: usize) std.Io.Writer.Error!void {
    assert(count <= std.math.maxInt(i32));
    try writeI32(w, @intCast(count));
}

pub fn writeNullableArrayCount(w: *std.Io.Writer, count: ?usize) std.Io.Writer.Error!void {
    if (count) |actual| {
        try writeArrayCount(w, actual);
    } else {
        try writeI32(w, -1);
    }
}

/// Kafka `COMPACT_ARRAY`: uvarint (count+1) prefix. 0 = null. Element
/// encoding is the caller's job.
pub fn readCompactArrayCount(r: *Reader) error{ EndOfStream, Malformed }!?usize {
    const count_plus_one = try readUvarint(r);
    if (count_plus_one == 0) return null;
    return @intCast(count_plus_one - 1);
}

pub fn writeCompactArrayCount(w: *std.Io.Writer, count: usize) std.Io.Writer.Error!void {
    try writeUvarint(w, @as(u64, count) + 1);
}

pub fn writeNullableCompactArrayCount(w: *std.Io.Writer, count: ?usize) std.Io.Writer.Error!void {
    if (count) |actual| {
        try writeCompactArrayCount(w, actual);
    } else {
        try writeUvarint(w, 0);
    }
}

// ---------------------------------------------------------------------------
// Tagged fields (flexible-version empty buffer)
// ---------------------------------------------------------------------------

/// A single tagged field: a tag ID and its raw data bytes. The data slice
/// is borrowed from the reader's buffer — no allocation. Used by
/// `readTaggedFields` to extract fields that the protocol spec marks with
/// `<tag: N>` (flexible-version optional fields encoded in the TAG_BUFFER
/// rather than inline). Spec: KIP-482.
pub const TaggedField = struct {
    tag: u64,
    data: []const u8,
};

/// Kafka `TAG_BUFFER`: uvarint count of tagged fields followed by the
/// fields. For now we only need the empty case (count = 0 → `0x00`), which
/// is what a non-tagged struct emits. Spec: KIP-482.
pub fn writeEmptyTagBuffer(w: *std.Io.Writer) std.Io.Writer.Error!void {
    try w.writeByte(0x00);
}

/// Write a tag buffer containing the given tagged fields. Each field is
/// encoded as uvarint(tag), uvarint(data.len), then the raw data bytes.
///
/// **KIP-482 invariant:** tagged fields MUST be serialized in strictly
/// ascending tag-ID order. This function enforces the invariant with a debug
/// assertion — callers must pass `fields` sorted ascending by `tag`. The
/// assertion catches malformed-frame bugs in Debug builds without adding
/// runtime sorting overhead on the encode path.
///
/// Spec: KIP-482 — tagged fields in the TAG_BUFFER.
pub fn writeTagBuffer(w: *std.Io.Writer, fields: []const TaggedField) std.Io.Writer.Error!void {
    try writeUvarint(w, fields.len);
    var prev_tag: u64 = 0;
    var first = true;
    for (fields) |f| {
        if (!first) assert(f.tag > prev_tag);
        prev_tag = f.tag;
        first = false;
        try writeUvarint(w, f.tag);
        try writeUvarint(w, f.data.len);
        try w.writeAll(f.data);
    }
}

pub fn readTagBuffer(r: *Reader) error{ EndOfStream, Malformed }!void {
    // Read and skip all tagged fields. Each field is: uvarint tag, uvarint
    // length, then `length` bytes. The count itself is a uvarint.
    const count = try readUvarint(r);
    var i: u64 = 0;
    while (i < count) : (i += 1) {
        _ = try readUvarint(r); // tag id
        const len = try readUvarint(r);
        // Guard against a huge length that would overflow usize or claim more
        // bytes than remain. `Reader.skip` bounds-checks, but capping here
        // avoids a pointless @intCast trap on 32-bit targets.
        if (len > std.math.maxInt(usize)) return error.Malformed;
        try r.skip(@intCast(len));
    }
}

/// Read tagged fields, filling known tags and skipping unknown ones.
///
/// `expected_tags` is the list of tag IDs the caller knows how to handle.
/// `out` is a parallel array of `?[]const u8` (one slot per expected tag)
/// that the caller MUST initialize to `null`. For each tagged field on the
/// wire: if its tag ID matches `expected_tags[i]`, `out[i]` is set to the
/// field's data bytes (borrowed from the reader's buffer). Unknown tag IDs
/// (not in `expected_tags`) are skipped — their bytes are consumed and
/// discarded. This is the KIP-482 forward-compatibility invariant: receivers
/// MUST skip unknown tagged fields so brokers can add tagged fields without
/// bumping the protocol version.
///
/// **KIP-482 ascending-order invariant:** tagged fields on the wire MUST
/// appear in strictly ascending tag-ID order. A violation (out-of-order or
/// duplicate tag) returns `error.Malformed`.
///
/// `out.len` must equal `expected_tags.len`. No allocation — the caller owns
/// both arrays. The `data` slices in `out` point into the reader's buffer.
///
/// Spec: KIP-482 — tagged fields.
pub fn readTaggedFields(
    r: *Reader,
    expected_tags: []const u64,
    out: []?[]const u8,
) error{ EndOfStream, Malformed }!void {
    assert(out.len == expected_tags.len);
    const count = try readUvarint(r);
    var prev_tag: ?u64 = null;
    var i: u64 = 0;
    while (i < count) : (i += 1) {
        const tag = try readUvarint(r);
        const len = try readUvarint(r);
        if (len > std.math.maxInt(usize)) return error.Malformed;
        const data = try r.readSlice(@intCast(len));
        // KIP-482: tags must be strictly ascending on the wire.
        if (prev_tag) |prev| {
            if (tag <= prev) return error.Malformed;
        }
        prev_tag = tag;
        // Match against the caller's expected tags. Linear search is fine —
        // expected_tags is small (typically ≤4 entries).
        for (expected_tags, 0..) |et, j| {
            if (tag == et) {
                out[j] = data;
                break;
            }
        }
        // If no match, the tag is unknown — skip (bytes already consumed).
    }
}

// ---------------------------------------------------------------------------
// Reader cursor
// ---------------------------------------------------------------------------

/// A forward-only, bounds-checked cursor over a caller-owned byte slice.
/// No allocation. Returns `error.EndOfStream` on truncation and
/// `error.Malformed` on structurally invalid input.
pub const Reader = struct {
    buf: []const u8,
    pos: usize = 0,

    pub fn init(buf: []const u8) Reader {
        return .{ .buf = buf };
    }

    /// Bytes not yet consumed.
    pub fn remaining(self: *const Reader) []const u8 {
        return self.buf[self.pos..];
    }

    /// Skip `n` bytes.
    pub fn skip(self: *Reader, n: usize) error{EndOfStream}!void {
        assert(self.pos <= self.buf.len);
        if (n > self.buf.len - self.pos) return error.EndOfStream;
        self.pos += n;
    }

    /// Borrowed slice of `n` bytes, advancing the cursor.
    pub fn readSlice(self: *Reader, n: usize) error{EndOfStream}![]const u8 {
        assert(self.pos <= self.buf.len);
        if (n > self.buf.len - self.pos) return error.EndOfStream;
        const s = self.buf[self.pos..][0..n];
        self.pos += n;
        return s;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn roundtripInt(comptime T: type, value: T) !void {
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeInt(&w, T, value);
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(value, try readInt(&r, T));
}

test "fixed ints round-trip" {
    try roundtripInt(u8, 0);
    try roundtripInt(u8, 0xff);
    try roundtripInt(i8, -1);
    try roundtripInt(i8, 127);
    try roundtripInt(u16, 0x0102);
    try roundtripInt(i16, -1);
    try roundtripInt(i16, -32768);
    try roundtripInt(u32, 0x01020304);
    try roundtripInt(i32, -1);
    try roundtripInt(i32, std.math.maxInt(i32));
    try roundtripInt(i64, -1);
    try roundtripInt(i64, std.math.maxInt(i64));
}

test "writeI16 big-endian bytes" {
    var buf: [2]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeI16(&w, 0x0102);
    try testing.expectEqualSlices(u8, &.{ 0x01, 0x02 }, w.buffered());
}

test "writeI32 big-endian bytes" {
    var buf: [4]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeI32(&w, 0x01020304);
    try testing.expectEqualSlices(u8, &.{ 0x01, 0x02, 0x03, 0x04 }, w.buffered());
}

test "writeI64 big-endian bytes" {
    var buf: [8]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeI64(&w, 0x0102030405060708);
    try testing.expectEqualSlices(u8, &.{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 }, w.buffered());
}

test "readInt end of stream" {
    var r: Reader = .init(&.{ 0x01, 0x02 });
    _ = try readU16(&r);
    try testing.expectError(error.EndOfStream, readU16(&r));
}

test "bool round-trip" {
    var buf: [2]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeBool(&w, true);
    try writeBool(&w, false);
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(true, try readBool(&r));
    try testing.expectEqual(false, try readBool(&r));
}

test "bool accepts any non-zero byte" {
    // Kafka BOOLEAN: 0 = false, any non-zero = true.
    var r: Reader = .init(&.{0x05});
    try testing.expectEqual(true, try readBool(&r));
    var r2: Reader = .init(&.{0x01});
    try testing.expectEqual(true, try readBool(&r2));
    var r3: Reader = .init(&.{0x00});
    try testing.expectEqual(false, try readBool(&r3));
}

// --- varint / varlong ------------------------------------------------------

fn roundtripVarint(value: i32) !void {
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeVarint(&w, value);
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(value, try readVarint(&r));
}

fn roundtripVarlong(value: i64) !void {
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeVarlong(&w, value);
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(value, try readVarlong(&r));
}

test "varint round-trip edge values" {
    try roundtripVarint(0);
    try roundtripVarint(-1);
    try roundtripVarint(1);
    try roundtripVarint(std.math.maxInt(i32));
    try roundtripVarint(std.math.minInt(i32));
    try roundtripVarint(63);
    try roundtripVarint(64);
    try roundtripVarint(8191);
    try roundtripVarint(8192);
}

test "varlong round-trip edge values" {
    try roundtripVarlong(0);
    try roundtripVarlong(-1);
    try roundtripVarlong(1);
    try roundtripVarlong(std.math.maxInt(i64));
    try roundtripVarlong(std.math.minInt(i64));
}

test "varint known encoding: 0 → 0x00" {
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeVarint(&w, 0);
    try testing.expectEqualSlices(u8, &.{0x00}, w.buffered());
}

test "varint known encoding: -1 → 0x01" {
    // zigzag(-1) = 1.
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeVarint(&w, -1);
    try testing.expectEqualSlices(u8, &.{0x01}, w.buffered());
}

test "varint known encoding: 300 → 0xd8 0x04" {
    // zigzag(300) = 600 = 0x258 → LEB128: 0xd8 0x04.
    // 600 = 0b10_01011000 → low 7 = 0b1011000 = 0x58, with continuation = 0xd8,
    // high = 0b100 = 0x04. So 0xd8 0x04.
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeVarint(&w, 300);
    try testing.expectEqualSlices(u8, &.{ 0xd8, 0x04 }, w.buffered());
}

// --- unsigned varint (LEB128) ----------------------------------------------

fn roundtripUvarint(value: u64) !void {
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeUvarint(&w, value);
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(value, try readUvarint(&r));
}

test "uvarint round-trip" {
    try roundtripUvarint(0);
    try roundtripUvarint(1);
    try roundtripUvarint(127);
    try roundtripUvarint(128);
    try roundtripUvarint(16383);
    try roundtripUvarint(16384);
    try roundtripUvarint(std.math.maxInt(u32));
    try roundtripUvarint(std.math.maxInt(u64));
}

test "uvarint known encodings (LEB128)" {
    var buf: [16]u8 = undefined;

    var w0: std.Io.Writer = .fixed(&buf);
    try writeUvarint(&w0, 0);
    try testing.expectEqualSlices(u8, &.{0x00}, w0.buffered());

    var w1: std.Io.Writer = .fixed(&buf);
    try writeUvarint(&w1, 1);
    try testing.expectEqualSlices(u8, &.{0x01}, w1.buffered());

    var w127: std.Io.Writer = .fixed(&buf);
    try writeUvarint(&w127, 127);
    try testing.expectEqualSlices(u8, &.{0x7f}, w127.buffered());

    var w128: std.Io.Writer = .fixed(&buf);
    try writeUvarint(&w128, 128);
    try testing.expectEqualSlices(u8, &.{ 0x80, 0x01 }, w128.buffered());
}

test "uvarint malformed: no terminator in 10 bytes" {
    const data = [_]u8{0x80} ** 10 ++ [_]u8{0x01};
    var r: Reader = .init(&data);
    // The 11th byte (index 10) has the terminator, but readUvarintBounded(10)
    // stops after 10 continuation bytes and returns Malformed.
    try testing.expectError(error.Malformed, readUvarint(&r));
}

// --- string ----------------------------------------------------------------

test "string round-trip" {
    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeString(&w, "hello");
    var r: Reader = .init(w.buffered());
    try testing.expectEqualStrings("hello", try readString(&r));
}

test "nullable string null" {
    var buf: [4]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeNullableString(&w, null);
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(@as(?[]const u8, null), try readNullableString(&r));
}

test "nullable string empty vs null" {
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeNullableString(&w, "");
    var r: Reader = .init(w.buffered());
    try testing.expectEqualStrings("", (try readNullableString(&r)).?);
}

test "string empty" {
    var buf: [4]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeString(&w, "");
    var r: Reader = .init(w.buffered());
    try testing.expectEqualStrings("", try readString(&r));
}

// --- compact string --------------------------------------------------------

test "compact string null → 0x00" {
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeNullableCompactString(&w, null);
    try testing.expectEqualSlices(u8, &.{0x00}, w.buffered());
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(@as(?[]const u8, null), try readNullableCompactString(&r));
}

test "compact string empty → 0x01" {
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeCompactString(&w, "");
    try testing.expectEqualSlices(u8, &.{0x01}, w.buffered());
    var r: Reader = .init(w.buffered());
    try testing.expectEqualStrings("", try readCompactString(&r));
}

test "compact string 'ab' → 0x03 0x61 0x62" {
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeCompactString(&w, "ab");
    try testing.expectEqualSlices(u8, &.{ 0x03, 0x61, 0x62 }, w.buffered());
    var r: Reader = .init(w.buffered());
    try testing.expectEqualStrings("ab", try readCompactString(&r));
}

// --- bytes -----------------------------------------------------------------

test "bytes round-trip" {
    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeBytes(&w, &.{ 0xde, 0xad, 0xbe, 0xef });
    var r: Reader = .init(w.buffered());
    try testing.expectEqualSlices(u8, &.{ 0xde, 0xad, 0xbe, 0xef }, try readBytes(&r));
}

test "nullable bytes null" {
    var buf: [8]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeNullableBytes(&w, null);
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(@as(?[]const u8, null), try readNullableBytes(&r));
}

test "compact bytes round-trip" {
    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeCompactBytes(&w, &.{ 0x01, 0x02 });
    var r: Reader = .init(w.buffered());
    try testing.expectEqualSlices(u8, &.{ 0x01, 0x02 }, try readCompactBytes(&r));
}

test "compact bytes null → 0x00" {
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeNullableCompactBytes(&w, null);
    try testing.expectEqualSlices(u8, &.{0x00}, w.buffered());
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(@as(?[]const u8, null), try readNullableCompactBytes(&r));
}

// --- array counts ----------------------------------------------------------

test "array count round-trip" {
    var buf: [8]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeArrayCount(&w, 42);
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(@as(?usize, 42), try readArrayCount(&r));
}

test "nullable array count null" {
    var buf: [8]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeNullableArrayCount(&w, null);
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(@as(?usize, null), try readArrayCount(&r));
}

test "nullable array count malformed" {
    // -2 and below are malformed, not null (only -1 is the null sentinel).
    var buf: [4]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeI32(&w, -2);
    var r: Reader = .init(w.buffered());
    try testing.expectError(error.Malformed, readArrayCount(&r));
}

test "array count zero" {
    var buf: [4]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeArrayCount(&w, 0);
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(@as(?usize, 0), try readArrayCount(&r));
}

test "compact array count round-trip" {
    var buf: [8]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeCompactArrayCount(&w, 3);
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(@as(?usize, 3), try readCompactArrayCount(&r));
}

test "compact array count null → 0x00" {
    var buf: [8]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeNullableCompactArrayCount(&w, null);
    try testing.expectEqualSlices(u8, &.{0x00}, w.buffered());
    var r: Reader = .init(w.buffered());
    try testing.expectEqual(@as(?usize, null), try readCompactArrayCount(&r));
}

// --- tagged fields ---------------------------------------------------------

test "empty tag buffer is 0x00" {
    var buf: [4]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeEmptyTagBuffer(&w);
    try testing.expectEqualSlices(u8, &.{0x00}, w.buffered());
    var r: Reader = .init(w.buffered());
    try readTagBuffer(&r);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "writeTagBuffer with fields round-trips via readTaggedFields" {
    // Two tagged fields: tag 0 with data [0x01, 0x02], tag 3 with data [0xff].
    // Tags 1 and 2 are expected but absent on the wire → out slots stay null.
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    const fields = [_]TaggedField{
        .{ .tag = 0, .data = &.{ 0x01, 0x02 } },
        .{ .tag = 3, .data = &.{0xff} },
    };
    try writeTagBuffer(&w, &fields);

    var r: Reader = .init(w.buffered());
    const expected = [_]u64{ 0, 1, 2, 3 };
    var out: [4]?[]const u8 = .{ null, null, null, null };
    try readTaggedFields(&r, &expected, &out);
    try testing.expectEqualSlices(u8, &.{ 0x01, 0x02 }, out[0].?);
    try testing.expectEqual(@as(?[]const u8, null), out[1]);
    try testing.expectEqual(@as(?[]const u8, null), out[2]);
    try testing.expectEqualSlices(u8, &.{0xff}, out[3].?);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "readTaggedFields skips unknown tags and fills knowns" {
    // KIP-482: receivers MUST skip unknown tagged fields. A tag section with
    // known tags 0 and 3 plus an unknown tag 99 (ascending: 0, 3, 99) must
    // decode successfully — known slots filled, unknown skipped, decoding
    // continues past the unknown tag's bytes.
    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    const fields = [_]TaggedField{
        .{ .tag = 0, .data = &.{0x01} },
        .{ .tag = 3, .data = &.{0xff} },
        .{ .tag = 99, .data = &.{ 0xde, 0xad, 0xbe, 0xef } },
    };
    try writeTagBuffer(&w, &fields);

    var r: Reader = .init(w.buffered());
    const expected = [_]u64{ 0, 1, 2, 3 };
    var out: [4]?[]const u8 = .{ null, null, null, null };
    try readTaggedFields(&r, &expected, &out);
    try testing.expectEqualSlices(u8, &.{0x01}, out[0].?);
    try testing.expectEqual(@as(?[]const u8, null), out[1]);
    try testing.expectEqual(@as(?[]const u8, null), out[2]);
    try testing.expectEqualSlices(u8, &.{0xff}, out[3].?);
    // The unknown tag's bytes were consumed — nothing remains.
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "readTaggedFields rejects out-of-order tags" {
    // KIP-482 invariant: tags must be in strictly ascending order on the wire.
    // Tag 3 followed by tag 1 is malformed.
    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    // Manually write out-of-order tags (writeTagBuffer would assert).
    try writeUvarint(&w, 2); // count=2
    try writeUvarint(&w, 3); // tag 3
    try writeUvarint(&w, 1); // size=1
    try w.writeByte(0xff);
    try writeUvarint(&w, 1); // tag 1 (out of order!)
    try writeUvarint(&w, 1); // size=1
    try w.writeByte(0x01);

    var r: Reader = .init(w.buffered());
    const expected = [_]u64{ 0, 1, 2, 3 };
    var out: [4]?[]const u8 = .{ null, null, null, null };
    try testing.expectError(error.Malformed, readTaggedFields(&r, &expected, &out));
}

test "readTaggedFields rejects duplicate tags" {
    // KIP-482: strictly ascending means no duplicates. Tag 0 twice is malformed.
    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeUvarint(&w, 2); // count=2
    try writeUvarint(&w, 0); // tag 0
    try writeUvarint(&w, 1); // size=1
    try w.writeByte(0xaa);
    try writeUvarint(&w, 0); // tag 0 again (duplicate!)
    try writeUvarint(&w, 1); // size=1
    try w.writeByte(0xbb);

    var r: Reader = .init(w.buffered());
    const expected = [_]u64{ 0, 1, 2, 3 };
    var out: [4]?[]const u8 = .{ null, null, null, null };
    try testing.expectError(error.Malformed, readTaggedFields(&r, &expected, &out));
}

test "readTaggedFields rejects field_size exceeding remaining buffer" {
    // Tag claims 10 bytes of data but only 1 byte follows → EndOfStream.
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try writeUvarint(&w, 1); // count=1
    try writeUvarint(&w, 0); // tag 0
    try writeUvarint(&w, 10); // size=10 (but only 1 byte follows)
    try w.writeByte(0x01);

    var r: Reader = .init(w.buffered());
    const expected = [_]u64{0};
    var out: [1]?[]const u8 = .{null};
    try testing.expectError(error.EndOfStream, readTaggedFields(&r, &expected, &out));
}

test "readTaggedFields with empty tag buffer leaves all slots null" {
    var r: Reader = .init(&.{0x00});
    const expected = [_]u64{ 0, 1, 2, 3 };
    var out: [4]?[]const u8 = .{ null, null, null, null };
    try readTaggedFields(&r, &expected, &out);
    try testing.expectEqual(@as(?[]const u8, null), out[0]);
    try testing.expectEqual(@as(?[]const u8, null), out[1]);
    try testing.expectEqual(@as(?[]const u8, null), out[2]);
    try testing.expectEqual(@as(?[]const u8, null), out[3]);
}

test "readTaggedFields with no expected tags skips all wire tags" {
    // Caller expects zero tags but the wire has 2 unknown tags — must skip
    // both and succeed (KIP-482 forward-compat).
    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    const fields = [_]TaggedField{
        .{ .tag = 5, .data = &.{0xaa} },
        .{ .tag = 10, .data = &.{0xbb} },
    };
    try writeTagBuffer(&w, &fields);

    var r: Reader = .init(w.buffered());
    var out: [0]?[]const u8 = .{};
    try readTaggedFields(&r, &.{}, &out);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "writeTagBuffer accepts strictly ascending tags" {
    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    const fields = [_]TaggedField{
        .{ .tag = 0, .data = &.{0x01} },
        .{ .tag = 1, .data = &.{0x02} },
        .{ .tag = 5, .data = &.{0x03} },
    };
    try writeTagBuffer(&w, &fields);
    // Verify the wire bytes: count=3, tag0+data, tag1+data, tag5+data.
    var r: Reader = .init(w.buffered());
    const expected = [_]u64{ 0, 1, 5 };
    var out: [3]?[]const u8 = .{ null, null, null };
    try readTaggedFields(&r, &expected, &out);
    try testing.expectEqualSlices(u8, &.{0x01}, out[0].?);
    try testing.expectEqualSlices(u8, &.{0x02}, out[1].?);
    try testing.expectEqualSlices(u8, &.{0x03}, out[2].?);
}

// --- reader ----------------------------------------------------------------

test "Reader bounds checking" {
    var r: Reader = .init(&.{ 0x01, 0x02, 0x03 });
    _ = try r.readSlice(2);
    _ = try r.readSlice(1);
    try testing.expectError(error.EndOfStream, r.readSlice(1));
}

test "Reader remaining" {
    var r: Reader = .init(&.{ 0x01, 0x02, 0x03 });
    _ = try r.skip(1);
    try testing.expectEqualSlices(u8, &.{ 0x02, 0x03 }, r.remaining());
}

// --- Fix 2: readI8 two's-complement -----------------------------------------

test "readI8 decodes negative bytes (two's complement)" {
    // 0xff → -1, 0x80 → -128, 0x7f → 127.
    var r1: Reader = .init(&.{0xff});
    try testing.expectEqual(@as(i8, -1), try readI8(&r1));
    var r2: Reader = .init(&.{0x80});
    try testing.expectEqual(@as(i8, -128), try readI8(&r2));
    var r3: Reader = .init(&.{0x7f});
    try testing.expectEqual(@as(i8, 127), try readI8(&r3));
}

// --- Fix 3: non-null readers reject null sentinels --------------------------

test "readString rejects negative length" {
    // Spec: https://kafka.apache.org/protocol.html → STRING is non-null;
    // -1 is the null sentinel for NULLABLE_STRING only.
    const data = [_]u8{ 0xff, 0xff }; // i16 -1
    var r: Reader = .init(&data);
    try testing.expectError(error.Malformed, readString(&r));
}

test "readNullableString: -1 → null, -2 → Malformed" {
    const null_data = [_]u8{ 0xff, 0xff }; // i16 -1
    var r1: Reader = .init(&null_data);
    try testing.expectEqual(@as(?[]const u8, null), try readNullableString(&r1));

    const bad_data = [_]u8{ 0xff, 0xfe }; // i16 -2
    var r2: Reader = .init(&bad_data);
    try testing.expectError(error.Malformed, readNullableString(&r2));
}

test "readBytes rejects negative length" {
    const data = [_]u8{ 0xff, 0xff, 0xff, 0xff }; // i32 -1
    var r: Reader = .init(&data);
    try testing.expectError(error.Malformed, readBytes(&r));
}

test "readNullableBytes: -1 → null, -2 → Malformed" {
    const null_data = [_]u8{ 0xff, 0xff, 0xff, 0xff }; // i32 -1
    var r1: Reader = .init(&null_data);
    try testing.expectEqual(@as(?[]const u8, null), try readNullableBytes(&r1));

    const bad_data = [_]u8{ 0xff, 0xff, 0xff, 0xfe }; // i32 -2
    var r2: Reader = .init(&bad_data);
    try testing.expectError(error.Malformed, readNullableBytes(&r2));
}

test "readCompactString rejects 0 sentinel" {
    // Spec: KIP-482 — COMPACT_STRING is non-null; 0 is the null sentinel
    // for COMPACT_NULLABLE_STRING only.
    var r: Reader = .init(&.{0x00});
    try testing.expectError(error.Malformed, readCompactString(&r));
}

test "readNullableCompactString: 0 → null" {
    var r: Reader = .init(&.{0x00});
    try testing.expectEqual(@as(?[]const u8, null), try readNullableCompactString(&r));
}

test "readCompactBytes rejects 0 sentinel" {
    var r: Reader = .init(&.{0x00});
    try testing.expectError(error.Malformed, readCompactBytes(&r));
}

test "readNullableCompactBytes: 0 → null" {
    var r: Reader = .init(&.{0x00});
    try testing.expectEqual(@as(?[]const u8, null), try readNullableCompactBytes(&r));
}

// --- Fix 4: uvarint final-byte overflow rejection --------------------------

test "uvarint 10-byte with 10th byte 0x02 → Malformed" {
    // 9 continuation bytes (0x80 each) + final byte 0x02. The 10th byte's
    // payload (0x02) has 2 bits, but only 1 bit of headroom remains in u64
    // after 9×7=63 bits. Without the check, 0x02 << 63 silently truncates to
    // 0, colliding with the compact null sentinel.
    const data = [_]u8{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02 };
    var r: Reader = .init(&data);
    try testing.expectError(error.Malformed, readUvarint(&r));
}

test "uvarint 10-byte with 10th byte 0x01 → ok (max u64)" {
    // 9 continuation bytes + final byte 0x01 = 2^63, the largest valid
    // 10-byte u64 varint.
    const data = [_]u8{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 };
    var r: Reader = .init(&data);
    try testing.expectEqual(@as(u64, 1 << 63), try readUvarint(&r));
}

test "varint 5-byte with 5th byte 0x10 → Malformed" {
    // 4 continuation bytes (0x80 each) + final byte 0x10. The 5th byte's
    // payload (0x10 = 0b10000) has 5 bits, but only 4 bits of headroom remain
    // in u32 after 4×7=28 bits. Without the check, this would decode to a
    // value > maxInt(u32).
    const data = [_]u8{ 0x80, 0x80, 0x80, 0x80, 0x10 };
    var r: Reader = .init(&data);
    try testing.expectError(error.Malformed, readVarint(&r));
}

// --- Fix 5: overflow-safe bounds checks -----------------------------------

test "readSlice with length near usize max is EndOfStream, not overflow" {
    // On any target, a length near usize.max should hit the bounds check
    // (n > buf.len - pos) without overflowing pos + n.
    var r: Reader = .init(&.{ 0x01, 0x02, 0x03 });
    try testing.expectError(error.EndOfStream, r.readSlice(std.math.maxInt(usize)));
}

test "skip with length near usize max is EndOfStream, not overflow" {
    var r: Reader = .init(&.{ 0x01, 0x02, 0x03 });
    try testing.expectError(error.EndOfStream, r.skip(std.math.maxInt(usize)));
}
