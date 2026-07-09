//! InitProducerId API (key 22, v4) — request and response codec.
//!
//! Spec: https://kafka.apache.org/43/design/protocol#the_messages_init_producer_id
//! — "InitProducerId API (Key: 22)".
//!
//! v4 is FLEXIBLE (KIP-482): compact strings, unsigned-varint lengths, request
//! header v2, response header v1, and a trailing tagged-field buffer on every
//! flexible struct. Header versions per api_keys.zig: request v2, response v1.
//!
//! v3 added `producer_id` and `producer_epoch` to the request (for transactional
//! id reuse after expiration). v4 is field-identical to v3; the version bump
//! added the `PRODUCER_FENCED` error code on the response side. v6 adds
//! `enable2_pc` / `keep_prepared_txn` (request) and `ongoing_txn_*` (response) —
//! NOT in v4.
//!
//! For a non-transactional idempotent producer, send `transactional_id = null`,
//! `producer_id = -1`, `producer_epoch = -1`. The response returns the assigned
//! `producer_id` + `producer_epoch`. The coordinator for non-transactional
//! idempotent init is ANY broker — FindCoordinator is NOT needed for
//! idempotent-only (out of scope for this slice).
//!
//! This module encodes/decodes the **body only** — the bytes after the i32
//! length prefix and the request/response header.
//!
//! Allocator policy: both request encode and response decode are **zero-alloc**.
//! The response body is entirely scalar (INT32, INT16, INT64, INT16 + TAG_BUFFER)
//! — no nested arrays or strings — so `decodeResponse` takes no allocator and
//! the `Response` struct owns nothing. This is the sans-I/O discipline: no heap
//! where none is needed.

const std = @import("std");
const testing = std.testing;

const primitives = @import("../primitives.zig");
const Reader = primitives.Reader;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// InitProducerId v4 request body.
///
/// Spec field order (v4):
///   transactional_id: COMPACT_NULLABLE_STRING
///   transaction_timeout_ms: INT32
///   producer_id: INT64
///   producer_epoch: INT16
///   TAG_BUFFER
///
/// For non-transactional idempotent init: transactional_id=null,
/// producer_id=-1, producer_epoch=-1.
pub const Request = struct {
    transactional_id: ?[]const u8,
    transaction_timeout_ms: i32,
    producer_id: i64,
    producer_epoch: i16,
};

/// InitProducerId v4 response body.
///
/// Spec field order (v4):
///   throttle_time_ms: INT32
///   error_code: INT16
///   producer_id: INT64
///   producer_epoch: INT16
///   TAG_BUFFER
///
/// All scalar — no owned data, no deinit needed.
pub const Response = struct {
    throttle_time_ms: i32,
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
};

// ---------------------------------------------------------------------------
// Encode (request body — zero-alloc, writes to caller's Writer)
// ---------------------------------------------------------------------------

/// Encode an InitProducerId v4 request body to `writer`.
///
/// Spec: https://kafka.apache.org/43/design/protocol#the_messages_init_producer_id
/// — "InitProducerId Request (Version: 4)".
pub fn encodeRequest(writer: *std.Io.Writer, req: Request) std.Io.Writer.Error!void {
    try primitives.writeNullableCompactString(writer, req.transactional_id);
    try primitives.writeI32(writer, req.transaction_timeout_ms);
    try primitives.writeI64(writer, req.producer_id);
    try primitives.writeI16(writer, req.producer_epoch);
    try primitives.writeEmptyTagBuffer(writer);
}

// ---------------------------------------------------------------------------
// Decode (response body — zero-alloc, all scalar)
// ---------------------------------------------------------------------------

/// Decode an InitProducerId v4 response body from `reader`.
///
/// Spec: https://kafka.apache.org/43/design/protocol#the_messages_init_producer_id
/// — "InitProducerId Response (Version: 4)".
///
/// The response is entirely scalar (no arrays, no strings), so this function
/// takes no allocator and the returned `Response` owns nothing — no `deinit`
/// needed.
pub fn decodeResponse(reader: *Reader) !Response {
    const throttle_time_ms = try primitives.readI32(reader);
    const error_code = try primitives.readI16(reader);
    const producer_id = try primitives.readI64(reader);
    const producer_epoch = try primitives.readI16(reader);
    try primitives.readTagBuffer(reader);
    return .{
        .throttle_time_ms = throttle_time_ms,
        .error_code = error_code,
        .producer_id = producer_id,
        .producer_epoch = producer_epoch,
    };
}

// ---------------------------------------------------------------------------
// Encode (response body — for round-trip tests)
// ---------------------------------------------------------------------------

/// Encode an InitProducerId v4 response body to `writer`.
///
/// Inverse of `decodeResponse`, used for round-trip tests. Assumes an empty
/// tag buffer (the fixture uses empty tags, so the round-trip is lossless).
pub fn encodeResponse(writer: *std.Io.Writer, resp: Response) std.Io.Writer.Error!void {
    try primitives.writeI32(writer, resp.throttle_time_ms);
    try primitives.writeI16(writer, resp.error_code);
    try primitives.writeI64(writer, resp.producer_id);
    try primitives.writeI16(writer, resp.producer_epoch);
    try primitives.writeEmptyTagBuffer(writer);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "encode request: non-transactional idempotent init" {
    // Hand-constructed from the v4 spec. Field-by-field breakdown:
    //
    //   transactional_id: COMPACT_NULLABLE_STRING null → 0x00
    //   transaction_timeout_ms: INT32 = 30000 → 00 00 75 30
    //   producer_id: INT64 = -1 → ff ff ff ff ff ff ff ff
    //   producer_epoch: INT16 = -1 → ff ff
    //   TAG_BUFFER: 0x00
    const expected = [_]u8{
        0x00, // transactional_id = null
        0x00, 0x00, 0x75, 0x30, // transaction_timeout_ms = 30000
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // producer_id = -1
        0xff, 0xff, // producer_epoch = -1
        0x00, // tag buffer
    };

    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .transactional_id = null,
        .transaction_timeout_ms = 30000,
        .producer_id = -1,
        .producer_epoch = -1,
    });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "encode request: transactional with existing producer_id/epoch" {
    //   transactional_id: COMPACT_NULLABLE_STRING "tx-1" → 0x05 74 78 2d 31
    //   transaction_timeout_ms: INT32 = 60000 → 00 00 ea 60
    //   producer_id: INT64 = 42 → 00 00 00 00 00 00 00 2a
    //   producer_epoch: INT16 = 3 → 00 03
    //   TAG_BUFFER: 0x00
    const expected = [_]u8{
        0x05, 0x74, 0x78, 0x2d, 0x31, // transactional_id = "tx-1"
        0x00, 0x00, 0xea, 0x60, // transaction_timeout_ms = 60000
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // producer_id = 42
        0x00, 0x03, // producer_epoch = 3
        0x00, // tag buffer
    };

    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .transactional_id = "tx-1",
        .transaction_timeout_ms = 60000,
        .producer_id = 42,
        .producer_epoch = 3,
    });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "decode response: successful PID assignment" {
    // Hand-constructed from the v4 spec. Field-by-field breakdown:
    //
    //   throttle_time_ms: INT32 = 0 → 00 00 00 00
    //   error_code: INT16 = 0 → 00 00
    //   producer_id: INT64 = 42 → 00 00 00 00 00 00 00 2a
    //   producer_epoch: INT16 = 0 → 00 00
    //   TAG_BUFFER: 0x00
    const fixture = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // producer_id = 42
        0x00, 0x00, // producer_epoch = 0
        0x00, // tag buffer
    };

    var r: Reader = .init(&fixture);
    const resp = try decodeResponse(&r);

    try testing.expectEqual(@as(i32, 0), resp.throttle_time_ms);
    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expectEqual(@as(i64, 42), resp.producer_id);
    try testing.expectEqual(@as(i16, 0), resp.producer_epoch);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "decode response: throttled with nonzero epoch" {
    //   throttle_time_ms: INT32 = 100 → 00 00 00 64
    //   error_code: INT16 = 0 → 00 00
    //   producer_id: INT64 = 9999 → 00 00 00 00 00 00 27 0f
    //   producer_epoch: INT16 = 7 → 00 07
    //   TAG_BUFFER: 0x00
    const fixture = [_]u8{
        0x00, 0x00, 0x00, 0x64, // throttle_time_ms = 100
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x0f, // producer_id = 9999
        0x00, 0x07, // producer_epoch = 7
        0x00, // tag buffer
    };

    var r: Reader = .init(&fixture);
    const resp = try decodeResponse(&r);

    try testing.expectEqual(@as(i32, 100), resp.throttle_time_ms);
    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expectEqual(@as(i64, 9999), resp.producer_id);
    try testing.expectEqual(@as(i16, 7), resp.producer_epoch);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "decode response: error code (PRODUCER_FENCED = 90)" {
    //   throttle_time_ms: INT32 = 0
    //   error_code: INT16 = 90 → 00 5a
    //   producer_id: INT64 = -1 → ff ff ff ff ff ff ff ff
    //   producer_epoch: INT16 = -1 → ff ff
    //   TAG_BUFFER: 0x00
    const fixture = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, 0x5a, // error_code = 90 (PRODUCER_FENCED)
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // producer_id = -1
        0xff, 0xff, // producer_epoch = -1
        0x00, // tag buffer
    };

    var r: Reader = .init(&fixture);
    const resp = try decodeResponse(&r);

    try testing.expectEqual(@as(i16, 90), resp.error_code);
    try testing.expectEqual(@as(i64, -1), resp.producer_id);
    try testing.expectEqual(@as(i16, -1), resp.producer_epoch);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "round-trip: decode then re-encode response equals fixture" {
    // Same fixture as the successful PID assignment test.
    const fixture = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // producer_id = 42
        0x00, 0x00, // producer_epoch = 0
        0x00, // tag buffer
    };

    var r: Reader = .init(&fixture);
    const resp = try decodeResponse(&r);

    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeResponse(&w, resp);
    try testing.expectEqualSlices(u8, &fixture, w.buffered());
}

test "round-trip: decode then re-encode response with throttle + nonzero epoch" {
    const fixture = [_]u8{
        0x00, 0x00, 0x00, 0x64, // throttle_time_ms = 100
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x0f, // producer_id = 9999
        0x00, 0x07, // producer_epoch = 7
        0x00, // tag buffer
    };

    var r: Reader = .init(&fixture);
    const resp = try decodeResponse(&r);

    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeResponse(&w, resp);
    try testing.expectEqualSlices(u8, &fixture, w.buffered());
}

test "decode response: truncated input returns EndOfStream" {
    // Cut off mid-producer_id (only 4 of 8 bytes present).
    const truncated = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, // producer_id (first 4 of 8 bytes)
    };

    var r: Reader = .init(&truncated);
    try testing.expectError(error.EndOfStream, decodeResponse(&r));
}

test "decode response: truncated at tag buffer returns EndOfStream" {
    // All fields present but tag buffer byte missing.
    const truncated = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // producer_id = 42
        0x00, 0x00, // producer_epoch = 0
        // ← tag buffer missing
    };

    var r: Reader = .init(&truncated);
    try testing.expectError(error.EndOfStream, decodeResponse(&r));
}

test "decode response with full frame: length prefix + response header v1" {
    // Assemble the full on-wire frame: i32 length prefix + response header v1
    // (correlation_id i32 + tag buffer) + body. Verifies the boundary between
    // header and body that this module operates on.
    //
    // Response header v1 (flexible): correlation_id: INT32, TAG_BUFFER.
    const body = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // producer_id = 42
        0x00, 0x00, // producer_epoch = 0
        0x00, // tag buffer
    };

    // Response header v1: correlation_id (i32) + tag buffer (0x00).
    const header = [_]u8{
        0x00, 0x00, 0x7b, 0x00, // correlation_id = 31488
        0x00, // tag buffer
    };

    // Full frame: i32 length prefix (header + body) + header + body.
    const frame_len: u32 = @intCast(header.len + body.len);
    var frame: [32]u8 = undefined;
    var pos: usize = 0;
    std.mem.writeInt(u32, frame[pos..][0..4], frame_len, .big);
    pos += 4;
    @memcpy(frame[pos..][0..header.len], &header);
    pos += header.len;
    @memcpy(frame[pos..][0..body.len], &body);
    pos += body.len;

    // Parse the length prefix, then skip header, decode body.
    var len_reader: Reader = .init(frame[0..pos]);
    const body_len = try primitives.readI32(&len_reader);
    try testing.expectEqual(@as(i32, @intCast(header.len + body.len)), body_len);

    const payload = len_reader.remaining();

    // Skip response header v1: correlation_id (i32) + tag buffer.
    var hr: Reader = .init(payload);
    const correlation_id = try primitives.readI32(&hr);
    try testing.expectEqual(@as(i32, 0x7b00), correlation_id);
    try primitives.readTagBuffer(&hr);

    // The rest is the body — decode it.
    var body_reader: Reader = .init(hr.remaining());
    const resp = try decodeResponse(&body_reader);

    try testing.expectEqual(@as(i64, 42), resp.producer_id);
    try testing.expectEqual(@as(usize, 0), body_reader.remaining().len);
}

test "encode request then manually decode matches struct" {
    // Encode a request, then manually decode it and verify the fields.
    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .transactional_id = "tx-1",
        .transaction_timeout_ms = 60000,
        .producer_id = 42,
        .producer_epoch = 3,
    });

    var r: Reader = .init(w.buffered());
    const tid = try primitives.readNullableCompactString(&r);
    try testing.expectEqualStrings("tx-1", tid.?);
    const timeout = try primitives.readI32(&r);
    try testing.expectEqual(@as(i32, 60000), timeout);
    const pid = try primitives.readI64(&r);
    try testing.expectEqual(@as(i64, 42), pid);
    const epoch = try primitives.readI16(&r);
    try testing.expectEqual(@as(i16, 3), epoch);
    try primitives.readTagBuffer(&r);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

// ---------------------------------------------------------------------------
// Fuzz target
// ---------------------------------------------------------------------------

const init_producer_id_response_corpus: []const []const u8 = &.{
    &.{}, // empty
    // Successful PID assignment.
    &.{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // producer_id = 42
        0x00, 0x00, // producer_epoch = 0
        0x00, // tag buffer
    },
    // Truncated mid-producer_id.
    &.{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
    &([_]u8{0xff} ** 32), // all ones
    &([_]u8{0x00} ** 32), // all zeros
};

test "fuzz InitProducerId v4 decodeResponse" {
    try std.testing.fuzz(std.testing.allocator, fuzzInitProducerIdResponse, .{
        .corpus = init_producer_id_response_corpus,
    });
}

fn fuzzInitProducerIdResponse(_: std.mem.Allocator, input: []const u8) !void {
    var r: Reader = .init(input);
    _ = decodeResponse(&r) catch |err| switch (err) {
        error.EndOfStream, error.Malformed => return,
        else => return err,
    };
}
