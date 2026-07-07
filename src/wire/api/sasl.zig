//! SASL APIs (keys 17 + 36, v1) — request and response codecs.
//!
//! Spec: https://kafka.apache.org/43/design/protocol#the_messages_saslhandshake
//! — "SaslHandshake API (Key: 17)".
//! Spec: https://kafka.apache.org/43/design/protocol#the_messages_saslauthenticate
//! — "SaslAuthenticate API (Key: 36)".
//!
//! Both v1 are NON-FLEXIBLE: int16-length strings, int32-count arrays,
//! request header v1, response header v0, no tagged-field buffers.
//!
//! SaslHandshake (key 17, v1):
//!   Request:  mechanism: STRING
//!   Response: error_code: INT16, mechanisms: ARRAY of STRING
//!   Header: {request 1, response 0}
//!
//! SaslAuthenticate (key 36, v1):
//!   Request:  auth_bytes: BYTES
//!   Response: error_code: INT16, error_message: NULLABLE_STRING,
//!             auth_bytes: BYTES, session_lifetime_ms: INT64
//!   Header: {request 1, response 0}
//!
//! `auth_bytes` is opaque `[]const u8` — the SCRAM module fills it in later.
//! This slice just round-trips bytes.
//!
//! This module encodes/decodes the **body only**. The fixture tests assemble
//! header+body inline to verify the boundary.
//!
//! Allocator policy: the request encode paths are zero-alloc. The response
//! decode paths take an `std.mem.Allocator` for nested arrays and duplicated
//! strings. Call `deinit` on decoded responses to free owned data.

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const primitives = @import("../primitives.zig");
const Reader = primitives.Reader;

// ===========================================================================
// SaslHandshake (key 17, v1)
// ===========================================================================

/// SaslHandshake v1 request body.
///
/// Spec: "SaslHandshake Request (Version: 1) => { mechanism }"
///   mechanism => STRING (non-compact, i16 length)
pub const HandshakeRequest = struct {
    mechanism: []const u8,
};

/// SaslHandshake v1 response body.
///
/// Spec: "SaslHandshake Response (Version: 1) => { error_code [mechanisms] }"
///   error_code => INT16
///   mechanisms => STRING (non-compact, i32-count ARRAY)
///
/// Owned data: the mechanisms strings are duplicated. Call `deinit`.
pub const HandshakeResponse = struct {
    error_code: i16,
    mechanisms: [][]const u8,

    pub fn deinit(self: *HandshakeResponse, allocator: std.mem.Allocator) void {
        for (self.mechanisms) |m| allocator.free(m);
        allocator.free(self.mechanisms);
        self.* = undefined;
    }
};

/// Encode a SaslHandshake v1 request body to `writer`. Zero-alloc.
pub fn encodeHandshakeRequest(writer: *std.Io.Writer, req: HandshakeRequest) std.Io.Writer.Error!void {
    try primitives.writeString(writer, req.mechanism);
}

/// Decode a SaslHandshake v1 response body from `reader`.
///
/// The returned `HandshakeResponse` owns the mechanisms strings (duplicated
/// from the response buffer). Call `deinit` to free.
pub fn decodeHandshakeResponse(allocator: std.mem.Allocator, reader: *Reader) !HandshakeResponse {
    const error_code = try primitives.readI16(reader);

    // mechanisms: non-flexible ARRAY (i32 count) of STRING.
    const count = try primitives.readArrayCount(reader);
    const n = count orelse return error.Malformed;
    const mechanisms = try allocator.alloc([]const u8, n);
    var len: usize = 0;
    errdefer {
        for (mechanisms[0..len]) |m| allocator.free(m);
        allocator.free(mechanisms);
    }
    while (len < n) : (len += 1) {
        const raw = try primitives.readString(reader);
        mechanisms[len] = try allocator.dupe(u8, raw);
    }

    return .{
        .error_code = error_code,
        .mechanisms = mechanisms,
    };
}

/// Encode a SaslHandshake v1 response body to `writer`. For round-trip tests.
pub fn encodeHandshakeResponse(writer: *std.Io.Writer, resp: HandshakeResponse) std.Io.Writer.Error!void {
    try primitives.writeI16(writer, resp.error_code);
    try primitives.writeArrayCount(writer, resp.mechanisms.len);
    for (resp.mechanisms) |m| {
        try primitives.writeString(writer, m);
    }
}

// ===========================================================================
// SaslAuthenticate (key 36, v1)
// ===========================================================================

/// SaslAuthenticate v1 request body.
///
/// Spec: "SaslAuthenticate Request (Version: 1) => { auth_bytes }"
///   auth_bytes => BYTES (non-compact, i32 length)
///
/// v1 does NOT have `session_lifetime_ms` in the request — that's a response
/// field only. The request body is just `auth_bytes`.
pub const AuthenticateRequest = struct {
    auth_bytes: []const u8,
};

/// SaslAuthenticate v1 response body.
///
/// Spec: "SaslAuthenticate Response (Version: 1) => { error_code error_message auth_bytes session_lifetime_ms }"
///   error_code => INT16
///   error_message => NULLABLE_STRING (non-compact, i16 length)
///   auth_bytes => BYTES (non-compact, i32 length)
///   session_lifetime_ms => INT64
///
/// v1 adds `session_lifetime_ms` compared to v0. Confirmed against the spec.
///
/// Owned data: `error_message` and `auth_bytes` are duplicated. Call `deinit`.
pub const AuthenticateResponse = struct {
    error_code: i16,
    error_message: ?[]const u8,
    auth_bytes: []const u8,
    session_lifetime_ms: i64,

    pub fn deinit(self: *AuthenticateResponse, allocator: std.mem.Allocator) void {
        if (self.error_message) |em| allocator.free(em);
        allocator.free(self.auth_bytes);
        self.* = undefined;
    }
};

/// Encode a SaslAuthenticate v1 request body to `writer`. Zero-alloc.
pub fn encodeAuthenticateRequest(writer: *std.Io.Writer, req: AuthenticateRequest) std.Io.Writer.Error!void {
    try primitives.writeBytes(writer, req.auth_bytes);
}

/// Decode a SaslAuthenticate v1 response body from `reader`.
///
/// The returned `AuthenticateResponse` owns `error_message` and `auth_bytes`
/// (duplicated from the response buffer). Call `deinit` to free.
pub fn decodeAuthenticateResponse(allocator: std.mem.Allocator, reader: *Reader) !AuthenticateResponse {
    const error_code = try primitives.readI16(reader);

    const error_message_raw = try primitives.readNullableString(reader);
    const error_message: ?[]const u8 = if (error_message_raw) |em| try allocator.dupe(u8, em) else null;
    errdefer if (error_message) |em| allocator.free(em);

    const auth_bytes_raw = try primitives.readBytes(reader);
    const auth_bytes = try allocator.dupe(u8, auth_bytes_raw);
    errdefer allocator.free(auth_bytes);

    const session_lifetime_ms = try primitives.readI64(reader);

    return .{
        .error_code = error_code,
        .error_message = error_message,
        .auth_bytes = auth_bytes,
        .session_lifetime_ms = session_lifetime_ms,
    };
}

/// Encode a SaslAuthenticate v1 response body to `writer`. For round-trip tests.
pub fn encodeAuthenticateResponse(writer: *std.Io.Writer, resp: AuthenticateResponse) std.Io.Writer.Error!void {
    try primitives.writeI16(writer, resp.error_code);
    try primitives.writeNullableString(writer, resp.error_message);
    try primitives.writeBytes(writer, resp.auth_bytes);
    try primitives.writeI64(writer, resp.session_lifetime_ms);
}

// ===========================================================================
// Tests — SaslHandshake
// ===========================================================================

test "encode handshake request: mechanism SCRAM-SHA-512" {
    // Hand-constructed from the v1 spec. Non-flexible, so STRING = i16 len + bytes.
    //
    //   mechanism: STRING "SCRAM-SHA-512" (13 chars)
    //     → 0x00 0x0d 53 43 52 41 4d 2d 53 48 41 2d 35 31 32
    const expected = [_]u8{
        0x00, 0x0d, // i16 length = 13
        0x53, 0x43, 0x52, 0x41, 0x4d, 0x2d, 0x53, 0x48, 0x41, 0x2d, 0x35, 0x31, 0x32, // "SCRAM-SHA-512"
    };

    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeHandshakeRequest(&w, .{ .mechanism = "SCRAM-SHA-512" });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "decode handshake response: two mechanisms" {
    // Hand-constructed from the v1 spec. Non-flexible: i32 array count, i16 strings.
    //
    //   error_code: INT16 = 0 → 00 00
    //   mechanisms: ARRAY count=2 → 00 00 00 02
    //     "SCRAM-SHA-512" → 00 0d 53 43 52 41 4d 2d 53 48 41 2d 35 31 32
    //     "SCRAM-SHA-256" → 00 0d 53 43 52 41 4d 2d 53 48 41 2d 32 35 36
    const fixture = [_]u8{
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x02, // mechanisms: count=2
        0x00, 0x0d, 0x53, 0x43, 0x52, 0x41, 0x4d, 0x2d, 0x53, 0x48, 0x41, 0x2d, 0x35, 0x31, 0x32, // "SCRAM-SHA-512"
        0x00, 0x0d, 0x53, 0x43, 0x52, 0x41, 0x4d, 0x2d, 0x53, 0x48, 0x41, 0x2d, 0x32, 0x35, 0x36, // "SCRAM-SHA-256"
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeHandshakeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expectEqual(@as(usize, 2), resp.mechanisms.len);
    try testing.expectEqualStrings("SCRAM-SHA-512", resp.mechanisms[0]);
    try testing.expectEqualStrings("SCRAM-SHA-256", resp.mechanisms[1]);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "round-trip: decode then re-encode handshake response" {
    const fixture = [_]u8{
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x02,
        0x00, 0x0d,
        0x53, 0x43,
        0x52, 0x41,
        0x4d, 0x2d,
        0x53, 0x48,
        0x41, 0x2d,
        0x35, 0x31,
        0x32, 0x00,
        0x0d, 0x53,
        0x43, 0x52,
        0x41, 0x4d,
        0x2d, 0x53,
        0x48, 0x41,
        0x2d, 0x32,
        0x35, 0x36,
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeHandshakeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    var aw: std.Io.Writer.Allocating = .init(testing.allocator);
    defer aw.deinit();
    try encodeHandshakeResponse(&aw.writer, resp);
    const reencoded = try aw.toOwnedSlice();
    defer testing.allocator.free(reencoded);

    try testing.expectEqualSlices(u8, &fixture, reencoded);
}

test "decode handshake response: empty mechanisms array" {
    //   error_code=0, mechanisms: count=0 → 00 00 00 00
    const fixture = [_]u8{
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, // mechanisms: count=0
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeHandshakeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expectEqual(@as(usize, 0), resp.mechanisms.len);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "decode handshake response: error_code nonzero" {
    // error_code=7 (UNSUPPORTED_SASL_MECHANISM), empty mechanisms.
    const fixture = [_]u8{
        0x00, 0x07, // error_code = 7
        0x00, 0x00, 0x00, 0x00, // mechanisms: count=0
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeHandshakeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 7), resp.error_code);
    try testing.expectEqual(@as(usize, 0), resp.mechanisms.len);
}

test "round-trip: decode then re-encode handshake response with error_code" {
    const fixture = [_]u8{
        0x00, 0x07,
        0x00, 0x00,
        0x00, 0x00,
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeHandshakeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    var aw: std.Io.Writer.Allocating = .init(testing.allocator);
    defer aw.deinit();
    try encodeHandshakeResponse(&aw.writer, resp);
    const reencoded = try aw.toOwnedSlice();
    defer testing.allocator.free(reencoded);

    try testing.expectEqualSlices(u8, &fixture, reencoded);
}

test "decode handshake response: truncated mid-mechanism leaks nothing" {
    // Two mechanisms declared; first decodes, second is truncated.
    const fixture = [_]u8{
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x02, // mechanisms: count=2
        0x00, 0x0d, 0x53, 0x43, 0x52, 0x41, 0x4d, 0x2d, 0x53, 0x48, 0x41, 0x2d, 0x35, 0x31, 0x32, // "SCRAM-SHA-512"
        0x00, 0x0d, 0x53, 0x43, 0x52, // "SCRAM-SHA-256" truncated
    };

    var r: Reader = .init(&fixture);
    try testing.expectError(
        error.EndOfStream,
        decodeHandshakeResponse(testing.allocator, &r),
    );
    // std.testing.allocator checks for leaks on test exit.
}

test "decode handshake response with full frame: length + response header v0" {
    // SaslHandshake response uses header v0: correlation_id (i32) only, no tag buffer.
    const body = [_]u8{
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x01, // mechanisms: count=1
        0x00, 0x0d, 0x53, 0x43, 0x52, 0x41, 0x4d, 0x2d, 0x53, 0x48, 0x41, 0x2d, 0x35, 0x31, 0x32, // "SCRAM-SHA-512"
    };

    const header = [_]u8{
        0x00, 0x00, 0x01, 0x00, // correlation_id = 256
    };

    const frame_len: u32 = @intCast(header.len + body.len);
    var frame: [64]u8 = undefined;
    var pos: usize = 0;
    std.mem.writeInt(u32, frame[pos..][0..4], frame_len, .big);
    pos += 4;
    @memcpy(frame[pos..][0..header.len], &header);
    pos += header.len;
    @memcpy(frame[pos..][0..body.len], &body);
    pos += body.len;

    var len_reader: Reader = .init(frame[0..pos]);
    const body_len = try primitives.readI32(&len_reader);
    try testing.expectEqual(@as(i32, @intCast(header.len + body.len)), body_len);

    const payload = len_reader.remaining();

    // Skip response header v0: correlation_id only, no tag buffer.
    var hr: Reader = .init(payload);
    const correlation_id = try primitives.readI32(&hr);
    try testing.expectEqual(@as(i32, 256), correlation_id);

    var body_reader: Reader = .init(hr.remaining());
    var resp = try decodeHandshakeResponse(testing.allocator, &body_reader);
    defer resp.deinit(testing.allocator);

    try testing.expectEqualStrings("SCRAM-SHA-512", resp.mechanisms[0]);
    try testing.expectEqual(@as(usize, 0), body_reader.remaining().len);
}

// ===========================================================================
// Tests — SaslAuthenticate
// ===========================================================================

test "encode authenticate request: fake SCRAM client-first message" {
    // Hand-constructed from the v1 spec. Non-flexible: BYTES = i32 len + raw bytes.
    //
    //   auth_bytes: BYTES "n,,n=user,r=abc123" (18 bytes)
    //     → 00 00 00 12 6e 2c 2c 6e 3d 75 73 65 72 2c 72 3d 61 62 63 31 32 33
    const expected = [_]u8{
        0x00, 0x00, 0x00, 0x12, // i32 length = 18
        0x6e, 0x2c, 0x2c, 0x6e, 0x3d, 0x75, 0x73, 0x65, 0x72, 0x2c, // "n,,n=user,"
        0x72, 0x3d, 0x61, 0x62, 0x63, 0x31, 0x32, 0x33, // "r=abc123"
    };

    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeAuthenticateRequest(&w, .{ .auth_bytes = "n,,n=user,r=abc123" });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "decode authenticate response: success with server-first and session_lifetime" {
    // Hand-constructed from the v1 spec. Non-flexible.
    //
    //   error_code: INT16 = 0 → 00 00
    //   error_message: NULLABLE_STRING null → ff ff
    //   auth_bytes: BYTES "r=abc123,s=...,i=4096" (21 bytes) → 00 00 00 15 ...
    //   session_lifetime_ms: INT64 = 3600000 → 00 00 00 00 00 36 ee 80
    const fixture = [_]u8{
        0x00, 0x00, // error_code = 0
        0xff, 0xff, // error_message = null
        0x00, 0x00, 0x00, 0x15, // auth_bytes length = 21
        0x72, 0x3d, 0x61, 0x62, 0x63, 0x31, 0x32, 0x33, 0x2c, 0x73, 0x3d, 0x2e, 0x2e, 0x2e, 0x2c, // "r=abc123,s=...,"
        0x69, 0x3d, 0x34, 0x30, 0x39, 0x36, // "i=4096"
        0x00, 0x00, 0x00, 0x00, 0x00, 0x36, 0xee, 0x80, // session_lifetime_ms = 3600000
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeAuthenticateResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expectEqual(@as(?[]const u8, null), resp.error_message);
    try testing.expectEqualStrings("r=abc123,s=...,i=4096", resp.auth_bytes);
    try testing.expectEqual(@as(i64, 3600000), resp.session_lifetime_ms);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "round-trip: decode then re-encode authenticate response" {
    const fixture = [_]u8{
        0x00, 0x00,
        0xff, 0xff,
        0x00, 0x00,
        0x00, 0x15,
        0x72, 0x3d,
        0x61, 0x62,
        0x63, 0x31,
        0x32, 0x33,
        0x2c, 0x73,
        0x3d, 0x2e,
        0x2e, 0x2e,
        0x2c, 0x69,
        0x3d, 0x34,
        0x30, 0x39,
        0x36, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x36, 0xee,
        0x80,
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeAuthenticateResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    var aw: std.Io.Writer.Allocating = .init(testing.allocator);
    defer aw.deinit();
    try encodeAuthenticateResponse(&aw.writer, resp);
    const reencoded = try aw.toOwnedSlice();
    defer testing.allocator.free(reencoded);

    try testing.expectEqualSlices(u8, &fixture, reencoded);
}

test "decode authenticate response: error with message" {
    //   error_code=58 (SASL_AUTHENTICATION_FAILED), error_message="bad creds",
    //   auth_bytes="" (empty), session_lifetime_ms=0
    const fixture = [_]u8{
        0x00, 0x3a, // error_code = 58
        0x00, 0x09, 0x62, 0x61, 0x64, 0x20, 0x63, 0x72, 0x65, 0x64, 0x73, // error_message = "bad creds"
        0x00, 0x00, 0x00, 0x00, // auth_bytes = empty (length=0)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // session_lifetime_ms = 0
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeAuthenticateResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 58), resp.error_code);
    try testing.expectEqualStrings("bad creds", resp.error_message.?);
    try testing.expectEqualStrings("", resp.auth_bytes);
    try testing.expectEqual(@as(i64, 0), resp.session_lifetime_ms);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "round-trip: decode then re-encode authenticate response with error" {
    const fixture = [_]u8{
        0x00, 0x3a,
        0x00, 0x09,
        0x62, 0x61,
        0x64, 0x20,
        0x63, 0x72,
        0x65, 0x64,
        0x73, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00,
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeAuthenticateResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    var aw: std.Io.Writer.Allocating = .init(testing.allocator);
    defer aw.deinit();
    try encodeAuthenticateResponse(&aw.writer, resp);
    const reencoded = try aw.toOwnedSlice();
    defer testing.allocator.free(reencoded);

    try testing.expectEqualSlices(u8, &fixture, reencoded);
}

test "decode authenticate response: truncated mid-auth_bytes leaks nothing" {
    // error_message decodes, but auth_bytes is truncated (claims 16 bytes, only 4 present).
    const fixture = [_]u8{
        0x00, 0x00, // error_code = 0
        0xff, 0xff, // error_message = null
        0x00, 0x00, 0x00, 0x10, // auth_bytes length = 16, but only 4 bytes follow
        0x72, 0x3d, 0x61, 0x62, // "r=ab" truncated
    };

    var r: Reader = .init(&fixture);
    try testing.expectError(
        error.EndOfStream,
        decodeAuthenticateResponse(testing.allocator, &r),
    );
    // std.testing.allocator checks for leaks on test exit.
}

test "decode authenticate response: truncated mid-error_message leaks nothing" {
    // error_message string is truncated (declared length > remaining bytes).
    const fixture = [_]u8{
        0x00, 0x3a, // error_code = 58
        0x00, 0x20, // error_message length = 32, but no bytes follow
    };

    var r: Reader = .init(&fixture);
    try testing.expectError(
        error.EndOfStream,
        decodeAuthenticateResponse(testing.allocator, &r),
    );
}

test "decode authenticate response with full frame: length + response header v0" {
    // SaslAuthenticate response uses header v0: correlation_id (i32) only.
    const body = [_]u8{
        0x00, 0x00, // error_code = 0
        0xff, 0xff, // error_message = null
        0x00, 0x00, 0x00, 0x05, // auth_bytes length = 5
        0x76, 0x3d, 0x72, 0x3d, 0x2c, // "v=r=," (fake server-first)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // session_lifetime_ms = 0
    };

    const header = [_]u8{
        0x00, 0x00, 0x02, 0x00, // correlation_id = 512
    };

    const frame_len: u32 = @intCast(header.len + body.len);
    var frame: [64]u8 = undefined;
    var pos: usize = 0;
    std.mem.writeInt(u32, frame[pos..][0..4], frame_len, .big);
    pos += 4;
    @memcpy(frame[pos..][0..header.len], &header);
    pos += header.len;
    @memcpy(frame[pos..][0..body.len], &body);
    pos += body.len;

    var len_reader: Reader = .init(frame[0..pos]);
    const body_len = try primitives.readI32(&len_reader);
    try testing.expectEqual(@as(i32, @intCast(header.len + body.len)), body_len);

    const payload = len_reader.remaining();

    // Skip response header v0: correlation_id only.
    var hr: Reader = .init(payload);
    const correlation_id = try primitives.readI32(&hr);
    try testing.expectEqual(@as(i32, 512), correlation_id);

    var body_reader: Reader = .init(hr.remaining());
    var resp = try decodeAuthenticateResponse(testing.allocator, &body_reader);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expectEqualStrings("v=r=,", resp.auth_bytes);
    try testing.expectEqual(@as(usize, 0), body_reader.remaining().len);
}

// ---------------------------------------------------------------------------
// Fuzz targets
// ---------------------------------------------------------------------------

const handshake_corpus: []const []const u8 = &.{
    &.{}, // empty
    // Two mechanisms, error_code = 0.
    &.{
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x02, // mechanisms: count=2
        0x00, 0x0d, 0x53, 0x43, 0x52, 0x41, 0x4d, 0x2d, 0x53, 0x48, 0x41, 0x2d, 0x35, 0x31, 0x32, // "SCRAM-SHA-512"
        0x00, 0x0d, 0x53, 0x43, 0x52, 0x41, 0x4d, 0x2d, 0x53, 0x48, 0x41, 0x2d, 0x32, 0x35, 0x36, // "SCRAM-SHA-256"
    },
    // Truncated mid-mechanism.
    &.{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x0d, 0x53, 0x43 },
    &([_]u8{0xff} ** 64), // all ones
    &([_]u8{0x00} ** 64), // all zeros
};

test "fuzz SaslHandshake v1 decodeResponse" {
    try std.testing.fuzz(std.testing.allocator, fuzzHandshakeResponse, .{ .corpus = handshake_corpus });
}

fn fuzzHandshakeResponse(allocator: std.mem.Allocator, input: []const u8) !void {
    var r: Reader = .init(input);
    var resp = decodeHandshakeResponse(allocator, &r) catch |err| switch (err) {
        error.EndOfStream, error.Malformed, error.OutOfMemory => return,
        else => return err,
    };
    defer resp.deinit(allocator);
}

const authenticate_corpus: []const []const u8 = &.{
    &.{}, // empty
    // Success with server-first and session lifetime.
    &.{
        0x00, 0x00, // error_code = 0
        0xff, 0xff, // error_message = null
        0x00, 0x00, 0x00, 0x15, // auth_bytes length = 21
        0x72, 0x3d, 0x61, 0x62, 0x63, 0x31, 0x32, 0x33, 0x2c, 0x73, 0x3d, 0x2e, 0x2e, 0x2e, 0x2c, // "r=abc123,s=...,"
        0x69, 0x3d, 0x34, 0x30, 0x39, 0x36, // "i=4096"
        0x00, 0x00, 0x00, 0x00, 0x00, 0x36, 0xee, 0x80, // session_lifetime_ms = 3600000
    },
    // Error with message and empty auth_bytes.
    &.{
        0x00, 0x3a, // error_code = 58
        0x00, 0x09, 0x62, 0x61, 0x64, 0x20, 0x63, 0x72, 0x65, 0x64, 0x73, // "bad creds"
        0x00, 0x00, 0x00, 0x00, // auth_bytes = empty
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // session_lifetime_ms = 0
    },
    // Truncated mid-auth_bytes.
    &.{ 0x00, 0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x10, 0x01, 0x02 },
    &([_]u8{0xff} ** 64), // all ones
    &([_]u8{0x00} ** 64), // all zeros
};

test "fuzz SaslAuthenticate v1 decodeResponse" {
    try std.testing.fuzz(std.testing.allocator, fuzzAuthenticateResponse, .{ .corpus = authenticate_corpus });
}

fn fuzzAuthenticateResponse(allocator: std.mem.Allocator, input: []const u8) !void {
    var r: Reader = .init(input);
    var resp = decodeAuthenticateResponse(allocator, &r) catch |err| switch (err) {
        error.EndOfStream, error.Malformed, error.OutOfMemory => return,
        else => return err,
    };
    defer resp.deinit(allocator);
}
