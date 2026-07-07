//! Kafka response framing: turns a byte stream into whole response bodies.
//!
//! Kafka responses are prefixed by a 4-byte big-endian `i32` length: the body
//! size that follows. Responses split and coalesce arbitrarily on the wire, so
//! this buffer accumulates transport bytes and yields one complete body at a
//! time. Sans-I/O and zero-allocation: the caller owns the storage and the
//! transport.
//!
//! Modeled on ztls's `RecordBuffer` discipline, but the frame is an `i32`
//! length + body (Kafka response framing), not TLS record framing.
//!
//! Spec: https://kafka.apache.org/protocol.html — "Protocol Basics":
//! "The request/response format is [...] len: int32, [...] payload". The
//! length is the size of the payload in bytes (positive).
//!
//! Usage:
//!     var storage: [4096]u8 = undefined;
//!     var rb: ResponseBuffer = .init(&storage);
//!     const n = try stream.read(rb.writable());
//!     rb.advance(n);
//!     while (try rb.next()) |body| {
//!         // body is a borrowed slice into storage, valid until the next
//!         // writable()/next() call. Decode the response here.
//!     }

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const be: std.builtin.Endian = .big;

const ResponseBuffer = @This();

/// Length of the length prefix (i32 = 4 bytes).
pub const header_len: usize = 4;

storage: []u8,
/// Start of unconsumed data.
pos: usize = 0,
/// End of valid data in storage.
filled: usize = 0,

pub fn init(storage: []u8) ResponseBuffer {
    return .{ .storage = storage };
}

/// Free space to read transport bytes into. Compacts first so the returned
/// slice is the largest contiguous region. Call `advance` with the number of
/// bytes written. Invalidates any body slice from `next`.
pub fn writable(self: *ResponseBuffer) []u8 {
    self.compact();
    return self.storage[self.filled..];
}

/// Report `n` bytes written into the slice from `writable`.
pub fn advance(self: *ResponseBuffer, n: usize) void {
    assert(self.filled + n <= self.storage.len);
    self.filled += n;
}

/// Return the next complete response body as a borrowed slice into storage,
/// or `null` if a full body isn't buffered yet. The length prefix is consumed
/// but not included in the returned slice. The slice stays valid until the
/// next `writable()` or `next()` call.
pub fn next(self: *ResponseBuffer) error{Malformed}!?[]const u8 {
    const avail = self.storage[self.pos..self.filled];
    if (avail.len < header_len) return null;

    var len_bytes: [4]u8 = undefined;
    @memcpy(&len_bytes, avail[0..4]);
    const signed_len = std.mem.readInt(i32, &len_bytes, be);

    // Kafka lengths are strictly positive. Zero or negative is a protocol
    // violation — the connection is desynced.
    if (signed_len <= 0) return error.Malformed;
    const body_len: usize = @intCast(signed_len);

    const total = header_len + body_len;
    if (avail.len < total) return null;

    self.pos += total;
    return avail[header_len..total];
}

/// Move unconsumed bytes to the front so `writable()` is maximally contiguous.
fn compact(self: *ResponseBuffer) void {
    if (self.pos == 0) return;
    const unconsumed = self.storage[self.pos..self.filled];
    @memmove(self.storage[0..unconsumed.len], unconsumed);
    self.filled -= self.pos;
    self.pos = 0;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "next: complete body in one fill" {
    var storage: [64]u8 = undefined;
    var rb: ResponseBuffer = .init(&storage);

    // Length prefix = 5, then 5 body bytes.
    const data = [_]u8{ 0x00, 0x00, 0x00, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05 };
    @memcpy(rb.writable()[0..data.len], &data);
    rb.advance(data.len);

    const body = (try rb.next()).?;
    try testing.expectEqual(@as(usize, 5), body.len);
    try testing.expectEqualSlices(u8, &.{ 0x01, 0x02, 0x03, 0x04, 0x05 }, body);
    try testing.expectEqual(@as(?[]const u8, null), try rb.next());
}

test "next: partial length prefix (2 bytes then 2 bytes)" {
    var storage: [64]u8 = undefined;
    var rb: ResponseBuffer = .init(&storage);

    // First 2 bytes of the length prefix.
    const part1 = [_]u8{ 0x00, 0x00 };
    @memcpy(rb.writable()[0..part1.len], &part1);
    rb.advance(part1.len);
    try testing.expectEqual(@as(?[]const u8, null), try rb.next());

    // Remaining 2 bytes of the length prefix + body.
    const part2 = [_]u8{ 0x00, 0x03, 0xaa, 0xbb, 0xcc };
    @memcpy(rb.writable()[0..part2.len], &part2);
    rb.advance(part2.len);

    const body = (try rb.next()).?;
    try testing.expectEqualSlices(u8, &.{ 0xaa, 0xbb, 0xcc }, body);
}

test "next: partial body across two fills" {
    var storage: [64]u8 = undefined;
    var rb: ResponseBuffer = .init(&storage);

    // Length prefix = 4, then only 2 of 4 body bytes.
    const part1 = [_]u8{ 0x00, 0x00, 0x00, 0x04, 0x01, 0x02 };
    @memcpy(rb.writable()[0..part1.len], &part1);
    rb.advance(part1.len);
    try testing.expectEqual(@as(?[]const u8, null), try rb.next());

    // Remaining 2 body bytes.
    const part2 = [_]u8{ 0x03, 0x04 };
    @memcpy(rb.writable()[0..part2.len], &part2);
    rb.advance(part2.len);

    const body = (try rb.next()).?;
    try testing.expectEqualSlices(u8, &.{ 0x01, 0x02, 0x03, 0x04 }, body);
}

test "next: two frames coalesced in one fill" {
    var storage: [64]u8 = undefined;
    var rb: ResponseBuffer = .init(&storage);

    const data = [_]u8{
        0x00, 0x00, 0x00, 0x02, 0xaa, 0xbb,
        0x00, 0x00, 0x00, 0x01, 0xcc,
    };
    @memcpy(rb.writable()[0..data.len], &data);
    rb.advance(data.len);

    const b1 = (try rb.next()).?;
    try testing.expectEqualSlices(u8, &.{ 0xaa, 0xbb }, b1);

    const b2 = (try rb.next()).?;
    try testing.expectEqualSlices(u8, &.{0xcc}, b2);

    try testing.expectEqual(@as(?[]const u8, null), try rb.next());
}

test "next: truncated length returns null" {
    var storage: [64]u8 = undefined;
    var rb: ResponseBuffer = .init(&storage);
    const data = [_]u8{ 0x00, 0x00, 0x00 };
    @memcpy(rb.writable()[0..data.len], &data);
    rb.advance(data.len);
    try testing.expectEqual(@as(?[]const u8, null), try rb.next());
}

test "next: length > remaining returns null" {
    var storage: [64]u8 = undefined;
    var rb: ResponseBuffer = .init(&storage);
    // Length = 10, but only 2 body bytes present.
    const data = [_]u8{ 0x00, 0x00, 0x00, 0x0a, 0x01, 0x02 };
    @memcpy(rb.writable()[0..data.len], &data);
    rb.advance(data.len);
    try testing.expectEqual(@as(?[]const u8, null), try rb.next());
}

test "next: zero length is malformed" {
    var storage: [64]u8 = undefined;
    var rb: ResponseBuffer = .init(&storage);
    const data = [_]u8{ 0x00, 0x00, 0x00, 0x00 };
    @memcpy(rb.writable()[0..data.len], &data);
    rb.advance(data.len);
    try testing.expectError(error.Malformed, rb.next());
}

test "next: negative length is malformed" {
    var storage: [64]u8 = undefined;
    var rb: ResponseBuffer = .init(&storage);
    const data = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    @memcpy(rb.writable()[0..data.len], &data);
    rb.advance(data.len);
    try testing.expectError(error.Malformed, rb.next());
}

test "next: subsequent frame works after first" {
    var storage: [64]u8 = undefined;
    var rb: ResponseBuffer = .init(&storage);

    // First frame: length 1, body 0xaa.
    const frame1 = [_]u8{ 0x00, 0x00, 0x00, 0x01, 0xaa };
    @memcpy(rb.writable()[0..frame1.len], &frame1);
    rb.advance(frame1.len);
    const b1 = (try rb.next()).?;
    try testing.expectEqualSlices(u8, &.{0xaa}, b1);

    // Second frame written after consuming the first.
    const frame2 = [_]u8{ 0x00, 0x00, 0x00, 0x01, 0xbb };
    @memcpy(rb.writable()[0..frame2.len], &frame2);
    rb.advance(frame2.len);
    const b2 = (try rb.next()).?;
    try testing.expectEqualSlices(u8, &.{0xbb}, b2);
}

// ---------------------------------------------------------------------------
// Fuzz target
// ---------------------------------------------------------------------------

const response_buffer_corpus: []const []const u8 = &.{
    &.{}, // empty
    &.{ 0x00, 0x00, 0x00, 0x00 }, // zero length → malformed
    &.{ 0xff, 0xff, 0xff, 0xff }, // negative length → malformed
    &.{ 0x00, 0x00, 0x00, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05 }, // valid frame
    &.{ 0x00, 0x00, 0x00, 0x05, 0x01, 0x02 }, // truncated body
    &.{ 0x00, 0x00, 0x00, 0x01, 0xaa, 0x00, 0x00, 0x00, 0x01, 0xbb }, // two frames
    &.{ 0x7f, 0xff, 0xff, 0xff }, // max i32 length (huge)
    &.{ 0x00, 0x00, 0x00, 0x01, 0xaa }, // valid single frame
    &([_]u8{0xff} ** 64), // all ones
    &([_]u8{0x00} ** 64), // all zeros
};

test "fuzz ResponseBuffer framing" {
    try std.testing.fuzz(std.testing.allocator, fuzzResponseBuffer, .{ .corpus = response_buffer_corpus });
}

fn fuzzResponseBuffer(_: std.mem.Allocator, input: []const u8) !void {
    var storage: [8192]u8 = undefined;
    var rb: ResponseBuffer = .init(&storage);

    var pos: usize = 0;
    while (pos < input.len) {
        const desired: usize = 1 + (input[pos] % 7);
        const free = rb.writable();
        const chunk_size = @min(desired, free.len, input.len - pos);
        if (chunk_size == 0) return;
        const chunk = input[pos .. pos + chunk_size];
        @memcpy(free[0..chunk.len], chunk);
        rb.advance(chunk.len);
        pos += chunk_size;

        while (true) {
            const maybe = rb.next() catch |err| switch (err) {
                error.Malformed => return,
                else => return err,
            };
            if (maybe == null) break;
        }
    }
    while (true) {
        const maybe = rb.next() catch |err| switch (err) {
            error.Malformed => return,
            else => return err,
        };
        if (maybe == null) break;
    }
}
