//! Kafka request framing: length prefix + request header + body.
//!
//! Sans-I/O and zero-allocation. A Kafka request on the wire is:
//!
//!     length: INT32  (size of everything after this field)
//!     <request header vN>
//!     <request body>
//!
//! The length prefix is the size of the payload that follows, so it can only
//! be written once the header+body length is known. Rather than allocate a
//! growable writer, `frameRequest` writes the header+body into a caller-owned
//! fixed buffer (`buf[4..]`), then back-patches the `i32` length into
//! `buf[0..4]` and returns the framed slice. Overflow of the fixed buffer is
//! `error.RequestBufferTooSmall`, never a heap fallback — the caller must size
//! `buf` to the largest request it will frame (for Produce, that is
//! `max_batch_bytes` + framing overhead).
//!
//! Request header versions (spec:
//! https://kafka.apache.org/protocol.html — "Request Header"):
//!   v0: api_key INT16, api_version INT16, correlation_id INT32
//!   v1: v0 + client_id NULLABLE_STRING (i16 length)
//!   v2: api_key INT16, api_version INT16, correlation_id INT32,
//!       client_id COMPACT_NULLABLE_STRING, TAG_BUFFER
//!
//! Note the header ALWAYS carries api_key + api_version + correlation_id; the
//! version only changes how client_id and the trailing tag buffer are encoded.
//! We only ever emit v1 (non-flexible APIs) or v2 (flexible APIs); the header
//! version is resolved from `api_keys.headerVersion`.

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const primitives = @import("primitives.zig");
const api_keys = @import("api/api_keys.zig");
const ApiKey = api_keys.ApiKey;

const be: std.builtin.Endian = .big;

/// Length prefix width (i32).
pub const length_prefix_len: usize = 4;

/// Frame a full Kafka request into `buf` and return the framed slice.
///
/// `body_ctx` is any value exposing `write(self, *std.Io.Writer) Error!void`
/// that encodes the request body (the per-API body encoders in `api/` fit
/// behind a one-line wrapper). Zero-alloc: everything is written into `buf`,
/// which the caller owns and sizes. Returns `error.RequestBufferTooSmall` if
/// `buf` cannot hold the framed request.
pub fn frameRequest(
    buf: []u8,
    api_key: ApiKey,
    api_version: u16,
    correlation_id: i32,
    client_id: ?[]const u8,
    body_ctx: anytype,
) error{RequestBufferTooSmall}![]const u8 {
    assert(buf.len > length_prefix_len);
    const hv = api_keys.headerVersion(api_key, api_version);

    var w: std.Io.Writer = .fixed(buf[length_prefix_len..]);

    // A fixed writer's only failure mode is running out of room, so map every
    // header/body write overflow to the honest `RequestBufferTooSmall`.
    encodeHeaderAndBody(
        &w,
        hv.request,
        api_key,
        api_version,
        correlation_id,
        client_id,
        body_ctx,
    ) catch |err| switch (err) {
        error.WriteFailed => return error.RequestBufferTooSmall,
    };

    const payload_len = w.buffered().len;
    assert(payload_len <= std.math.maxInt(i32));
    std.mem.writeInt(i32, buf[0..4], @intCast(payload_len), be);
    return buf[0 .. length_prefix_len + payload_len];
}

/// Write the request header (per `header_version`) followed by the body. Split
/// out so `frameRequest` can remap the fixed-writer overflow in one place.
fn encodeHeaderAndBody(
    w: *std.Io.Writer,
    header_version: u8,
    api_key: ApiKey,
    api_version: u16,
    correlation_id: i32,
    client_id: ?[]const u8,
    body_ctx: anytype,
) std.Io.Writer.Error!void {
    // Request header (all versions carry these three).
    try primitives.writeI16(w, @intCast(api_key.toInt()));
    try primitives.writeI16(w, @intCast(api_version));
    try primitives.writeI32(w, correlation_id);
    switch (header_version) {
        0 => {},
        1 => try primitives.writeNullableString(w, client_id),
        // Header v2 (flexible) adds a trailing tag buffer, but the client_id
        // is STILL a nullable_string (i16 length + bytes), NOT a compact_string.
        // The RequestHeader.json spec marks ClientId as flexibleVersions: "none" —
        // the old-style two-byte length prefix is retained so older brokers can
        // read the header for any ApiVersionsRequest regardless of version.
        2 => {
            try primitives.writeNullableString(w, client_id);
            try primitives.writeEmptyTagBuffer(w);
        },
        else => unreachable, // no request header version above 2 exists
    }

    // Body.
    try body_ctx.write(w);
}

/// A body context that writes nothing — for requests with an empty body
/// (e.g. the pre-auth ApiVersions v0 probe).
pub const EmptyBody = struct {
    pub fn write(_: EmptyBody, _: *std.Io.Writer) std.Io.Writer.Error!void {}
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const RawBody = struct {
    bytes: []const u8,
    // Non-pub: only used by same-file tests through `frameRequest`.
    fn write(self: RawBody, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try w.writeAll(self.bytes);
    }
};

test "frameRequest: v1 header (SaslHandshake), empty body" {
    // SaslHandshake v1 → request header v1. api_key=17, version=1.
    // client_id = "kz". correlation_id = 7. body = empty.
    //
    //   length: INT32 = 2+2+4 + (2+2) = 12
    //   api_key: INT16 = 17        → 00 11
    //   api_version: INT16 = 1     → 00 01
    //   correlation_id: INT32 = 7  → 00 00 00 07
    //   client_id: STRING "kz"     → 00 02 6b 7a
    var buf: [64]u8 = undefined;
    const body: RawBody = .{ .bytes = "" };
    const framed = try frameRequest(&buf, .sasl_handshake, 1, 7, "kz", body);
    const expected = [_]u8{
        0x00, 0x00, 0x00, 0x0c, // length = 12
        0x00, 0x11, // api_key = 17
        0x00, 0x01, // api_version = 1
        0x00, 0x00, 0x00, 0x07, // correlation_id = 7
        0x00, 0x02, 0x6b, 0x7a, // client_id = "kz"
    };
    try testing.expectEqualSlices(u8, &expected, framed);
}

test "frameRequest: v1 header with null client_id" {
    // client_id = null → i16 -1 (ff ff).
    var buf: [64]u8 = undefined;
    const body: RawBody = .{ .bytes = &.{0xaa} };
    const framed = try frameRequest(&buf, .sasl_authenticate, 1, 1, null, body);
    const expected = [_]u8{
        0x00, 0x00, 0x00, 0x09, // length = 2+2+4+2+1 = 11? no: 2+2+4 + 2 (null str) + 1 body = 11
        0x00, 0x24, // api_key = 36
        0x00, 0x01, // api_version = 1
        0x00, 0x00, 0x00, 0x01, // correlation_id = 1
        0xff, 0xff, // client_id = null
        0xaa, // body
    };
    // length = 2+2+4+2+1 = 11 = 0x0b
    _ = expected;
    try testing.expectEqual(@as(usize, 4 + 11), framed.len);
    try testing.expectEqual(@as(i32, 11), std.mem.readInt(i32, framed[0..4], be));
    const want = [_]u8{ 0x00, 0x24, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xaa };
    try testing.expectEqualSlices(u8, &want, framed[4..]);
}

test "frameRequest: v2 header (Metadata), nullable_string client_id + tag buffer" {
    // Metadata v12 → request header v2. api_key=3, version=12.
    // client_id = "kz" (NULLABLE_STRING = i16 length 2, then bytes),
    // trailing TAG_BUFFER = 0x00. body = empty.
    //
    // The request header's client_id is ALWAYS a nullable_string (i16 length),
    // even in flexible header v2 — see RequestHeader.json: flexibleVersions: "none"
    // for the ClientId field. The v2 header only adds a trailing tag buffer.
    var buf: [64]u8 = undefined;
    const body: RawBody = .{ .bytes = "" };
    const framed = try frameRequest(&buf, .metadata, 12, 42, "kz", body);
    const expected = [_]u8{
        0x00, 0x00, 0x00, 0x0d, // length = 2+2+4 + (2+2) + 1 = 13
        0x00, 0x03, // api_key = 3
        0x00, 0x0c, // api_version = 12
        0x00, 0x00, 0x00, 0x2a, // correlation_id = 42
        0x00, 0x02, 0x6b, 0x7a, // client_id = "kz" (nullable_string: i16 len + bytes)
        0x00, // tag buffer
    };
    try testing.expectEqualSlices(u8, &expected, framed);
}

test "frameRequest: EmptyBody produces header only" {
    var buf: [64]u8 = undefined;
    const body: EmptyBody = .{};
    const framed = try frameRequest(&buf, .api_versions, 0, 99, "c", body);
    // header v1: api_key 18, version 0, corr 99, client_id "c"
    const expected = [_]u8{
        0x00, 0x00, 0x00, 0x0b, // length = 2+2+4 + (2+1) = 11
        0x00, 0x12, // api_key = 18
        0x00, 0x00, // api_version = 0
        0x00, 0x00, 0x00, 0x63, // correlation_id = 99
        0x00, 0x01, 0x63, // client_id = "c"
    };
    try testing.expectEqualSlices(u8, &expected, framed);
}

test "frameRequest: buffer too small returns RequestBufferTooSmall" {
    var buf: [8]u8 = undefined; // 4 length + only 4 for header (need >= 8)
    const body: RawBody = .{ .bytes = "" };
    try testing.expectError(
        error.RequestBufferTooSmall,
        frameRequest(&buf, .sasl_handshake, 1, 1, "very-long-client-id", body),
    );
}
