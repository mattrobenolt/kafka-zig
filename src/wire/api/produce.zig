//! Produce API (key 0, v9) — request and response codec.
//!
//! Spec: https://kafka.apache.org/43/design/protocol#the_messages_produce
//! — "Produce API (Key: 0)".
//!
//! v9 is FLEXIBLE (KIP-482): compact strings/arrays, unsigned-varint lengths,
//! request header v2, response header v1, and a trailing tagged-field buffer
//! on every flexible struct. Header versions per api_keys.zig: request v2,
//! response v1.
//!
//! This module encodes/decodes the **body only** — the bytes after the i32
//! length prefix and the request/response header. The fixture tests assemble
//! header+body inline to verify the boundary.
//!
//! **Produce v9 request field order** (verified against the spec):
//!   transactional_id: COMPACT_NULLABLE_STRING (null for non-transactional)
//!   acks: INT16
//!   timeout_ms: INT32
//!   topic_data: COMPACT_ARRAY of { name: COMPACT_STRING, partition_data:
//!     COMPACT_ARRAY of { index: INT32, records: COMPACT_NULLABLE_RECORDS,
//!     TAG_BUFFER }, TAG_BUFFER }
//!   TAG_BUFFER
//!
//! **Produce v9 response field order** (verified against the spec):
//!   responses: COMPACT_ARRAY of { name: COMPACT_STRING, partition_responses:
//!     COMPACT_ARRAY of { index: INT32, error_code: INT16, base_offset: INT64,
//!     log_append_time_ms: INT64, log_start_offset: INT64,
//!     record_errors: COMPACT_ARRAY of { batch_index: INT32,
//!     batch_index_error_message: COMPACT_NULLABLE_STRING, TAG_BUFFER },
//!     error_message: COMPACT_NULLABLE_STRING, TAG_BUFFER }, TAG_BUFFER }
//!   throttle_time_ms: INT32
//!   TAG_BUFFER
//!
//! **Spec notes:**
//! - `log_append_time_ms` is INT64 (NOT nullable). When the topic uses
//!   CreateTime, the broker returns -1. When LogAppendTime, it returns the
//!   broker-local time. There is no null encoding — it's a plain INT64.
//! - `record_errors` entries have `batch_index` (INT32) and
//!   `batch_index_error_message` (COMPACT_NULLABLE_STRING). There is NO
//!   `batch_index_acknowledged` field (that does not exist in any version).
//! - `current_leader` sub-struct does NOT exist in v9. It was added in v10
//!   as a tagged field (`<tag: 0>`). Do not encode or decode it for v9.
//! - `records` in the request is `COMPACT_NULLABLE_RECORDS`: encoded as
//!   COMPACT_NULLABLE_BYTES (uvarint(len+1) for non-null, 0x00 for null).
//!   The records bytes are the serialized v2 record batch(es). Exactly one
//!   record batch per partition_data entry (ProduceRequest validates this).
//!
//! Allocator policy: the request encode path is zero-alloc. The response
//! decode path takes an `std.mem.Allocator` for nested arrays (responses,
//! partition_responses, record_errors) and duplicated strings. Call `deinit`
//! to free.

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const primitives = @import("../primitives.zig");
const Reader = primitives.Reader;
const record_batch = @import("../record_batch.zig");

// ---------------------------------------------------------------------------
// Types (request)
// ---------------------------------------------------------------------------

/// One partition's data in a Produce v9 request: the partition index and the
/// record batch bytes. `records` is the raw serialized v2 record batch (the
/// bytes that go into the COMPACT_NULLABLE_RECORDS field). It may be null
/// (empty produce for partition metadata probing). Exactly one record batch
/// per entry — the broker validates this.
pub const PartitionData = struct {
    index: i32,
    records: ?[]const u8,
};

/// One topic's data in a Produce v9 request.
pub const TopicData = struct {
    name: []const u8,
    partition_data: []const PartitionData,
};

/// Produce v9 request body.
pub const Request = struct {
    transactional_id: ?[]const u8,
    acks: i16,
    timeout_ms: i32,
    topic_data: []const TopicData,
};

// ---------------------------------------------------------------------------
// Types (response)
// ---------------------------------------------------------------------------

/// A record error in a Produce v9 response partition.
pub const RecordError = struct {
    batch_index: i32,
    batch_index_error_message: ?[]const u8,
};

/// A partition response in a Produce v9 response.
pub const PartitionResponse = struct {
    index: i32,
    error_code: i16,
    base_offset: i64,
    /// INT64, not nullable. -1 when the topic uses CreateTime.
    log_append_time_ms: i64,
    log_start_offset: i64,
    record_errors: []RecordError,
    error_message: ?[]const u8,

    pub fn deinit(self: *PartitionResponse, allocator: std.mem.Allocator) void {
        for (self.record_errors) |*re| {
            if (re.batch_index_error_message) |msg| allocator.free(msg);
        }
        allocator.free(self.record_errors);
        if (self.error_message) |msg| allocator.free(msg);
        self.* = undefined;
    }
};

/// One topic's response in a Produce v9 response.
pub const TopicResponse = struct {
    name: []const u8,
    partition_responses: []PartitionResponse,

    pub fn deinit(self: *TopicResponse, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        for (self.partition_responses) |*p| p.deinit(allocator);
        allocator.free(self.partition_responses);
        self.* = undefined;
    }
};

/// Produce v9 response body.
pub const Response = struct {
    responses: []TopicResponse,
    throttle_time_ms: i32,

    pub fn deinit(self: *Response, allocator: std.mem.Allocator) void {
        for (self.responses) |*t| t.deinit(allocator);
        allocator.free(self.responses);
        self.* = undefined;
    }
};

// ---------------------------------------------------------------------------
// Encode (request body — zero-alloc)
// ---------------------------------------------------------------------------

/// Encode a Produce v9 request body to `writer`.
///
/// Spec: https://kafka.apache.org/43/design/protocol#the_messages_produce
/// — "Produce Request (Version: 9)".
pub fn encodeRequest(writer: *std.Io.Writer, req: Request) std.Io.Writer.Error!void {
    // transactional_id: COMPACT_NULLABLE_STRING (null for non-transactional).
    try primitives.writeNullableCompactString(writer, req.transactional_id);
    // acks: INT16.
    try primitives.writeI16(writer, req.acks);
    // timeout_ms: INT32.
    try primitives.writeI32(writer, req.timeout_ms);

    // topic_data: COMPACT_ARRAY.
    try primitives.writeCompactArrayCount(writer, req.topic_data.len);
    for (req.topic_data) |td| {
        // name: COMPACT_STRING.
        try primitives.writeCompactString(writer, td.name);
        // partition_data: COMPACT_ARRAY.
        try primitives.writeCompactArrayCount(writer, td.partition_data.len);
        for (td.partition_data) |pd| {
            // index: INT32.
            try primitives.writeI32(writer, pd.index);
            // records: COMPACT_NULLABLE_RECORDS (= COMPACT_NULLABLE_BYTES).
            if (pd.records) |recs| {
                try primitives.writeUvarint(writer, @as(u64, recs.len) + 1);
                try writer.writeAll(recs);
            } else {
                try primitives.writeUvarint(writer, 0); // null
            }
            // TAG_BUFFER.
            try primitives.writeEmptyTagBuffer(writer);
        }
        // TAG_BUFFER.
        try primitives.writeEmptyTagBuffer(writer);
    }

    // Trailing TAG_BUFFER.
    try primitives.writeEmptyTagBuffer(writer);
}

// ---------------------------------------------------------------------------
// Decode (response body — allocates nested arrays, cold-path)
// ---------------------------------------------------------------------------

/// Decode a Produce v9 response body from `reader`.
///
/// Spec: https://kafka.apache.org/43/design/protocol#the_messages_produce
/// — "Produce Response (Version: 9)".
///
/// The returned `Response` owns all nested data. Call `deinit` to free.
pub fn decodeResponse(allocator: std.mem.Allocator, reader: *Reader) !Response {
    // responses: COMPACT_ARRAY.
    const resp_count = try primitives.readCompactArrayCount(reader);
    const n_responses = resp_count orelse return error.Malformed;
    // Cap: every response element is ≥1 byte on the wire.
    if (n_responses > reader.remaining().len) return error.Malformed;
    const responses = try allocator.alloc(TopicResponse, n_responses);
    var responses_len: usize = 0;
    errdefer {
        for (responses[0..responses_len]) |*t| t.deinit(allocator);
        allocator.free(responses);
    }
    while (responses_len < n_responses) : (responses_len += 1) {
        responses[responses_len] = try decodeTopicResponse(allocator, reader);
    }

    const throttle_time_ms = try primitives.readI32(reader);

    // Trailing TAG_BUFFER.
    try primitives.readTagBuffer(reader);

    return .{
        .responses = responses,
        .throttle_time_ms = throttle_time_ms,
    };
}

fn decodeTopicResponse(allocator: std.mem.Allocator, reader: *Reader) !TopicResponse {
    const name_raw = try primitives.readCompactString(reader);
    const name = try allocator.dupe(u8, name_raw);
    errdefer allocator.free(name);

    const part_count = try primitives.readCompactArrayCount(reader);
    const n_parts = part_count orelse return error.Malformed;
    // Cap: every partition_response element is ≥1 byte on the wire.
    if (n_parts > reader.remaining().len) return error.Malformed;
    const partitions = try allocator.alloc(PartitionResponse, n_parts);
    var parts_len: usize = 0;
    errdefer {
        for (partitions[0..parts_len]) |*p| p.deinit(allocator);
        allocator.free(partitions);
    }
    while (parts_len < n_parts) : (parts_len += 1) {
        partitions[parts_len] = try decodePartitionResponse(allocator, reader);
    }

    try primitives.readTagBuffer(reader);

    return .{
        .name = name,
        .partition_responses = partitions,
    };
}

fn decodePartitionResponse(allocator: std.mem.Allocator, reader: *Reader) !PartitionResponse {
    const index = try primitives.readI32(reader);
    const error_code = try primitives.readI16(reader);
    const base_offset = try primitives.readI64(reader);
    const log_append_time_ms = try primitives.readI64(reader);
    const log_start_offset = try primitives.readI64(reader);

    // record_errors: COMPACT_ARRAY.
    const re_count = try primitives.readCompactArrayCount(reader);
    const n_re = re_count orelse return error.Malformed;
    // Cap: every record_error element is ≥1 byte on the wire.
    if (n_re > reader.remaining().len) return error.Malformed;
    const record_errors = try allocator.alloc(RecordError, n_re);
    var re_len: usize = 0;
    errdefer {
        for (record_errors[0..re_len]) |*re| {
            if (re.batch_index_error_message) |msg| allocator.free(msg);
        }
        allocator.free(record_errors);
    }
    while (re_len < n_re) : (re_len += 1) {
        const batch_index = try primitives.readI32(reader);
        const msg_raw = try primitives.readNullableCompactString(reader);
        const msg: ?[]const u8 = if (msg_raw) |m| try allocator.dupe(u8, m) else null;
        errdefer if (msg) |m| allocator.free(m);
        record_errors[re_len] = .{
            .batch_index = batch_index,
            .batch_index_error_message = msg,
        };
        try primitives.readTagBuffer(reader);
    }

    // error_message: COMPACT_NULLABLE_STRING.
    const err_msg_raw = try primitives.readNullableCompactString(reader);
    const error_message: ?[]const u8 = if (err_msg_raw) |m| try allocator.dupe(u8, m) else null;
    errdefer if (error_message) |m| allocator.free(m);

    try primitives.readTagBuffer(reader);

    return .{
        .index = index,
        .error_code = error_code,
        .base_offset = base_offset,
        .log_append_time_ms = log_append_time_ms,
        .log_start_offset = log_start_offset,
        .record_errors = record_errors,
        .error_message = error_message,
    };
}

// ---------------------------------------------------------------------------
// Encode (response body — for round-trip tests)
// ---------------------------------------------------------------------------

/// Encode a Produce v9 response body to `writer`. Used for round-trip tests.
pub fn encodeResponse(writer: *std.Io.Writer, resp: Response) std.Io.Writer.Error!void {
    // responses: COMPACT_ARRAY.
    try primitives.writeCompactArrayCount(writer, resp.responses.len);
    for (resp.responses) |t| {
        try primitives.writeCompactString(writer, t.name);
        try primitives.writeCompactArrayCount(writer, t.partition_responses.len);
        for (t.partition_responses) |p| {
            try primitives.writeI32(writer, p.index);
            try primitives.writeI16(writer, p.error_code);
            try primitives.writeI64(writer, p.base_offset);
            try primitives.writeI64(writer, p.log_append_time_ms);
            try primitives.writeI64(writer, p.log_start_offset);

            // record_errors: COMPACT_ARRAY.
            try primitives.writeCompactArrayCount(writer, p.record_errors.len);
            for (p.record_errors) |re| {
                try primitives.writeI32(writer, re.batch_index);
                try primitives.writeNullableCompactString(writer, re.batch_index_error_message);
                try primitives.writeEmptyTagBuffer(writer);
            }

            try primitives.writeNullableCompactString(writer, p.error_message);
            try primitives.writeEmptyTagBuffer(writer);
        }
        try primitives.writeEmptyTagBuffer(writer);
    }

    try primitives.writeI32(writer, resp.throttle_time_ms);
    try primitives.writeEmptyTagBuffer(writer);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "encode request: non-transactional, one topic, one partition, one record batch" {
    // Hand-constructed from the v9 spec. Field-by-field breakdown:
    //
    //   transactional_id: COMPACT_NULLABLE_STRING null → 0x00
    //   acks: INT16 = -1 (all ISR) → ff ff
    //   timeout_ms: INT32 = 5000 → 00 00 13 88
    //   topic_data: compact array count=1 → 0x02
    //     name: COMPACT_STRING "test" → 0x05 74 65 73 74
    //     partition_data: compact array count=1 → 0x02
    //       index: INT32 = 0 → 00 00 00 00
    //       records: COMPACT_NULLABLE_RECORDS (non-null, 4 bytes) →
    //         uvarint(4+1) = 0x05, then 4 bytes: de ad be ef
    //       TAG_BUFFER: 0x00
    //     TAG_BUFFER: 0x00
    //   TAG_BUFFER: 0x00
    const fake_records = [_]u8{ 0xde, 0xad, 0xbe, 0xef };
    const expected = [_]u8{
        0x00, // transactional_id = null
        0xff, 0xff, // acks = -1
        0x00, 0x00, 0x13, 0x88, // timeout_ms = 5000
        0x02, // topic_data: count=1
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x02, // partition_data: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x05, 0xde, 0xad, 0xbe, 0xef, // records: uvarint(5), 4 bytes
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, // trailing tag buffer
    };

    const partitions = [_]PartitionData{.{
        .index = 0,
        .records = &fake_records,
    }};
    const topics = [_]TopicData{.{
        .name = "test",
        .partition_data = &partitions,
    }};

    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .transactional_id = null,
        .acks = -1,
        .timeout_ms = 5000,
        .topic_data = &topics,
    });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "encode request: null records (partition probing)" {
    //   transactional_id: null → 0x00
    //   acks: 1 → 00 01
    //   timeout_ms: 1000 → 00 00 03 e8
    //   topic_data: count=1 → 0x02
    //     name: "t" → 0x02 74
    //     partition_data: count=1 → 0x02
    //       index: 0 → 00 00 00 00
    //       records: null → 0x00
    //       TAG_BUFFER: 0x00
    //     TAG_BUFFER: 0x00
    //   TAG_BUFFER: 0x00
    const expected = [_]u8{
        0x00, // transactional_id = null
        0x00, 0x01, // acks = 1
        0x00, 0x00, 0x03, 0xe8, // timeout_ms = 1000
        0x02, // topic_data: count=1
        0x02, 0x74, // name = "t"
        0x02, // partition_data: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, // records = null
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, // trailing tag buffer
    };

    const partitions = [_]PartitionData{.{
        .index = 0,
        .records = null,
    }};
    const topics = [_]TopicData{.{
        .name = "t",
        .partition_data = &partitions,
    }};

    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .transactional_id = null,
        .acks = 1,
        .timeout_ms = 1000,
        .topic_data = &topics,
    });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "encode request: transactional_id present" {
    //   transactional_id: "tx1" → 0x04 74 78 31
    //   acks: -1 → ff ff
    //   timeout_ms: 100 → 00 00 00 64
    //   topic_data: count=1 → 0x02
    //     name: "topic" → 0x06 74 6f 70 69 63
    //     partition_data: count=2 → 0x03
    //       p[0]: index=0, records=[0xaa], tag=0x00
    //       p[1]: index=1, records=[0xbb, 0xcc], tag=0x00
    //     TAG_BUFFER: 0x00
    //   TAG_BUFFER: 0x00
    const recs0 = [_]u8{0xaa};
    const recs1 = [_]u8{ 0xbb, 0xcc };
    const expected = [_]u8{
        0x04, 0x74, 0x78, 0x31, // transactional_id = "tx1"
        0xff, 0xff, // acks = -1
        0x00, 0x00, 0x00, 0x64, // timeout_ms = 100
        0x02, // topic_data: count=1
        0x06, 0x74, 0x6f, 0x70, 0x69, 0x63, // name = "topic"
        0x03, // partition_data: count=2
        0x00, 0x00, 0x00, 0x00, // p[0] index = 0
        0x02, 0xaa, // p[0] records: uvarint(2), 1 byte
        0x00, // p[0] tag buffer
        0x00, 0x00, 0x00, 0x01, // p[1] index = 1
        0x03, 0xbb, 0xcc, // p[1] records: uvarint(3), 2 bytes
        0x00, // p[1] tag buffer
        0x00, // topic tag buffer
        0x00, // trailing tag buffer
    };

    const partitions = [_]PartitionData{
        .{ .index = 0, .records = &recs0 },
        .{ .index = 1, .records = &recs1 },
    };
    const topics = [_]TopicData{.{
        .name = "topic",
        .partition_data = &partitions,
    }};

    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .transactional_id = "tx1",
        .acks = -1,
        .timeout_ms = 100,
        .topic_data = &topics,
    });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "encode request: multiple topics" {
    //   transactional_id: null → 0x00
    //   acks: 0 → 00 00
    //   timeout_ms: 0 → 00 00 00 00
    //   topic_data: count=2 → 0x03
    //     topic[0]: name="a" → 0x02 61, partition_data: count=1, index=0, records=[0xff], tag=0x00, tag=0x00
    //     topic[1]: name="bb" → 0x03 62 62, partition_data: count=1, index=5, records=[0x11], tag=0x00, tag=0x00
    //   TAG_BUFFER: 0x00
    const recs_a = [_]u8{0xff};
    const recs_b = [_]u8{0x11};
    const expected = [_]u8{
        0x00, // transactional_id = null
        0x00, 0x00, // acks = 0
        0x00, 0x00, 0x00, 0x00, // timeout_ms = 0
        0x03, // topic_data: count=2
        0x02, 0x61, // topic[0] name = "a"
        0x02, // topic[0] partition_data: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x02, 0xff, // records: uvarint(2), 1 byte
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x03, 0x62, 0x62, // topic[1] name = "bb"
        0x02, // topic[1] partition_data: count=1
        0x00, 0x00, 0x00, 0x05, // index = 5
        0x02, 0x11, // records: uvarint(2), 1 byte
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, // trailing tag buffer
    };

    const parts_a = [_]PartitionData{.{ .index = 0, .records = &recs_a }};
    const parts_b = [_]PartitionData{.{ .index = 5, .records = &recs_b }};
    const topics = [_]TopicData{
        .{ .name = "a", .partition_data = &parts_a },
        .{ .name = "bb", .partition_data = &parts_b },
    };

    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .transactional_id = null,
        .acks = 0,
        .timeout_ms = 0,
        .topic_data = &topics,
    });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "encode request with real record batch" {
    // Encode a request with a real v2 record batch (not fake bytes). This
    // exercises the integration between record_batch.zig and produce.zig.
    const records = [_]record_batch.Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &.{},
    }};

    // Encode the record batch.
    var batch_buf: [256]u8 = undefined;
    const batch_bytes = try record_batch.encodeBatch(&batch_buf, &records, .{});

    // Encode the Produce request with this batch.
    const partitions = [_]PartitionData{.{
        .index = 0,
        .records = batch_bytes,
    }};
    const topics = [_]TopicData{.{
        .name = "test",
        .partition_data = &partitions,
    }};

    var req_buf: [512]u8 = undefined;
    var rw: std.Io.Writer = .fixed(&req_buf);
    try encodeRequest(&rw, .{
        .transactional_id = null,
        .acks = -1,
        .timeout_ms = 5000,
        .topic_data = &topics,
    });
    const req_bytes = rw.buffered();

    // Verify we can parse the request back: check the records field contains
    // the batch bytes.
    var r: Reader = .init(req_bytes);
    // Skip to the records field.
    _ = try primitives.readNullableCompactString(&r); // transactional_id
    _ = try primitives.readI16(&r); // acks
    _ = try primitives.readI32(&r); // timeout_ms
    _ = try primitives.readCompactArrayCount(&r); // topic_data count
    _ = try primitives.readCompactString(&r); // name
    _ = try primitives.readCompactArrayCount(&r); // partition_data count
    _ = try primitives.readI32(&r); // index

    // records: COMPACT_NULLABLE_RECORDS
    const recs_len_plus_one = try primitives.readUvarint(&r);
    const recs_len: usize = @intCast(recs_len_plus_one - 1);
    try testing.expectEqual(batch_bytes.len, recs_len);
    const recs_bytes = try r.readSlice(recs_len);
    try testing.expectEqualSlices(u8, batch_bytes, recs_bytes);
}

test "decode response: one topic, one partition, success (error_code=0)" {
    // Hand-constructed from the v9 spec. Field-by-field breakdown:
    //
    //   responses: compact array count=1 → 0x02
    //     name: COMPACT_STRING "test" → 0x05 74 65 73 74
    //     partition_responses: compact array count=1 → 0x02
    //       index: INT32 = 0 → 00 00 00 00
    //       error_code: INT16 = 0 → 00 00
    //       base_offset: INT64 = 42 → 00 00 00 00 00 00 00 2a
    //       log_append_time_ms: INT64 = -1 (CreateTime) → ff ff ff ff ff ff ff ff
    //       log_start_offset: INT64 = 0 → 00 00 00 00 00 00 00 00
    //       record_errors: compact array count=0 → 0x01
    //       error_message: COMPACT_NULLABLE_STRING null → 0x00
    //       TAG_BUFFER: 0x00
    //     TAG_BUFFER: 0x00
    //   throttle_time_ms: INT32 = 0 → 00 00 00 00
    //   TAG_BUFFER: 0x00
    const fixture = [_]u8{
        0x02, // responses: count=1
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // base_offset = 42
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x01, // record_errors: count=0
        0x00, // error_message = null
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 1), resp.responses.len);
    try testing.expectEqualStrings("test", resp.responses[0].name);
    try testing.expectEqual(@as(usize, 1), resp.responses[0].partition_responses.len);

    const p = &resp.responses[0].partition_responses[0];
    try testing.expectEqual(@as(i32, 0), p.index);
    try testing.expectEqual(@as(i16, 0), p.error_code);
    try testing.expectEqual(@as(i64, 42), p.base_offset);
    try testing.expectEqual(@as(i64, -1), p.log_append_time_ms);
    try testing.expectEqual(@as(i64, 0), p.log_start_offset);
    try testing.expectEqual(@as(usize, 0), p.record_errors.len);
    try testing.expectEqual(@as(?[]const u8, null), p.error_message);
    try testing.expectEqual(@as(i32, 0), resp.throttle_time_ms);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "decode response: error_code nonzero (NOT_LEADER)" {
    //   responses: count=1
    //     name: "test"
    //     partition_responses: count=1
    //       index=0, error_code=6 (NOT_LEADER_OR_FOLLOWER), base_offset=-1,
    //       log_append_time_ms=-1, log_start_offset=0,
    //       record_errors: count=0, error_message="not leader", tag=0x00
    //     tag=0x00
    //   throttle_time_ms=0, tag=0x00
    const fixture = [_]u8{
        0x02, // responses: count=1
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x06, // error_code = 6
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // base_offset = -1
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x01, // record_errors: count=0
        0x0b, 0x6e, 0x6f, 0x74, 0x20, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, // error_message = "not leader"
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    const p = &resp.responses[0].partition_responses[0];
    try testing.expectEqual(@as(i16, 6), p.error_code);
    try testing.expectEqual(@as(i64, -1), p.base_offset);
    try testing.expectEqualStrings("not leader", p.error_message.?);
}

test "decode response: record_errors present" {
    //   responses: count=1
    //     name: "test"
    //     partition_responses: count=1
    //       index=0, error_code=1 (OFFSET_OUT_OF_ORDER),
    //       base_offset=100, log_append_time_ms=1234567890,
    //       log_start_offset=0,
    //       record_errors: count=2
    //         re[0]: batch_index=3, batch_index_error_message="too old", tag=0x00
    //         re[1]: batch_index=5, batch_index_error_message=null, tag=0x00
    //       error_message=null, tag=0x00
    //     tag=0x00
    //   throttle_time_ms=100, tag=0x00
    const fixture = [_]u8{
        0x02, // responses: count=1
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x01, // error_code = 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // base_offset = 100
        0x00, 0x00, 0x00, 0x00, 0x49, 0x96, 0x02, 0xd2, // log_append_time_ms = 1234567890
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x03, // record_errors: count=2
        0x00, 0x00, 0x00, 0x03, // batch_index = 3
        0x08, 0x74, 0x6f, 0x6f, 0x20, 0x6f, 0x6c, 0x64, // error_message = "too old" (7 chars → uvarint(8))
        0x00, // record_error tag buffer
        0x00, 0x00, 0x00, 0x05, // batch_index = 5
        0x00, // error_message = null
        0x00, // record_error tag buffer
        0x00, // error_message (partition) = null
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, 0x00, 0x00, 0x64, // throttle_time_ms = 100
        0x00, // trailing tag buffer
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    const p = &resp.responses[0].partition_responses[0];
    try testing.expectEqual(@as(i16, 1), p.error_code);
    try testing.expectEqual(@as(i64, 100), p.base_offset);
    try testing.expectEqual(@as(usize, 2), p.record_errors.len);
    try testing.expectEqual(@as(i32, 3), p.record_errors[0].batch_index);
    try testing.expectEqualStrings("too old", p.record_errors[0].batch_index_error_message.?);
    try testing.expectEqual(@as(i32, 5), p.record_errors[1].batch_index);
    try testing.expectEqual(@as(?[]const u8, null), p.record_errors[1].batch_index_error_message);
    try testing.expectEqual(@as(i32, 100), resp.throttle_time_ms);
}

test "decode response: log_append_time_ms with real timestamp" {
    // Verify log_append_time_ms is a plain INT64, not nullable. When the topic
    // uses LogAppendTime, the broker returns a real timestamp.
    const fixture = [_]u8{
        0x02, // responses: count=1
        0x02, 0x74, // name = "t"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // base_offset = 0
        0x00, 0x00, 0x00, 0xe8, 0xd4, 0xa5, 0x10, 0x00, // log_append_time_ms = 1000000000000
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x01, // record_errors: count=0
        0x00, // error_message = null
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    const p = &resp.responses[0].partition_responses[0];
    try testing.expectEqual(@as(i64, 1000000000000), p.log_append_time_ms);
}

test "decode response: multiple topics and partitions" {
    //   responses: count=2
    //     topic[0]: name="a", partition_responses: count=2
    //       p[0]: index=0, error=0, base_offset=0, lat=-1, lso=0, re=[], em=null, tag=0x00
    //       p[1]: index=1, error=0, base_offset=10, lat=-1, lso=5, re=[], em=null, tag=0x00
    //     topic[1]: name="b", partition_responses: count=1
    //       p[0]: index=0, error=0, base_offset=20, lat=-1, lso=0, re=[], em=null, tag=0x00
    //   throttle_time_ms=0, tag=0x00
    const fixture = [_]u8{
        0x03, // responses: count=2
        0x02, 0x61, // topic[0] name = "a"
        0x03, // topic[0] partition_responses: count=2
        0x00, 0x00, 0x00, 0x00, // p[0] index = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // base_offset = 0
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x01, // record_errors: count=0
        0x00, // error_message = null
        0x00, // tag buffer
        0x00, 0x00, 0x00, 0x01, // p[1] index = 1
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, // base_offset = 10
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // log_start_offset = 5
        0x01, // record_errors: count=0
        0x00, // error_message = null
        0x00, // tag buffer
        0x00, // topic[0] tag buffer
        0x02, 0x62, // topic[1] name = "b"
        0x02, // topic[1] partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // p[0] index = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, // base_offset = 20
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x01, // record_errors: count=0
        0x00, // error_message = null
        0x00, // tag buffer
        0x00, // topic[1] tag buffer
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 2), resp.responses.len);
    try testing.expectEqualStrings("a", resp.responses[0].name);
    try testing.expectEqual(@as(usize, 2), resp.responses[0].partition_responses.len);
    try testing.expectEqual(@as(i64, 10), resp.responses[0].partition_responses[1].base_offset);
    try testing.expectEqual(@as(i64, 5), resp.responses[0].partition_responses[1].log_start_offset);

    try testing.expectEqualStrings("b", resp.responses[1].name);
    try testing.expectEqual(@as(i64, 20), resp.responses[1].partition_responses[0].base_offset);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "round-trip: decode then re-encode response" {
    const fixture = [_]u8{
        0x02, // responses: count=1
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // base_offset = 42
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x01, // record_errors: count=0
        0x00, // error_message = null
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    var aw: std.Io.Writer.Allocating = .init(testing.allocator);
    defer aw.deinit();
    try encodeResponse(&aw.writer, resp);
    const reencoded = try aw.toOwnedSlice();
    defer testing.allocator.free(reencoded);

    try testing.expectEqualSlices(u8, &fixture, reencoded);
}

test "round-trip: decode then re-encode response with record_errors" {
    const fixture = [_]u8{
        0x02, // responses: count=1
        0x02, 0x74, // name = "t"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x01, // error_code = 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // base_offset = 100
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x03, // record_errors: count=2
        0x00, 0x00, 0x00, 0x03, // batch_index = 3
        0x08, 0x74, 0x6f, 0x6f, 0x20, 0x6f, 0x6c, 0x64, // error_message = "too old"
        0x00, // record_error tag buffer
        0x00, 0x00, 0x00, 0x05, // batch_index = 5
        0x00, // error_message = null
        0x00, // record_error tag buffer
        0x00, // partition error_message = null
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, 0x00, 0x00, 0x64, // throttle_time_ms = 100
        0x00, // trailing tag buffer
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    var aw: std.Io.Writer.Allocating = .init(testing.allocator);
    defer aw.deinit();
    try encodeResponse(&aw.writer, resp);
    const reencoded = try aw.toOwnedSlice();
    defer testing.allocator.free(reencoded);

    try testing.expectEqualSlices(u8, &fixture, reencoded);
}

test "decode response: empty responses array" {
    //   responses: count=0 → 0x01
    //   throttle_time_ms: 0 → 00 00 00 00
    //   TAG_BUFFER: 0x00
    const fixture = [_]u8{
        0x01, // responses: count=0
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 0), resp.responses.len);
    try testing.expectEqual(@as(i32, 0), resp.throttle_time_ms);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "decode response with full frame: length prefix + response header v1" {
    // Assemble the full on-wire frame: i32 length prefix + response header v1
    // (correlation_id i32 + tag buffer) + body. This verifies the boundary.
    //
    // Response header v1 (flexible): correlation_id: INT32, TAG_BUFFER.
    const body = [_]u8{
        0x02, // responses: count=1
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // base_offset = 42
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x01, // record_errors: count=0
        0x00, // error_message = null
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer
    };

    const header = [_]u8{
        0x00, 0x00, 0x7b, 0x00, // correlation_id = 31488
        0x00, // tag buffer
    };

    const frame_len: u32 = @intCast(header.len + body.len);
    var frame: [128]u8 = undefined;
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
    var hr: Reader = .init(payload);
    const correlation_id = try primitives.readI32(&hr);
    try testing.expectEqual(@as(i32, 0x7b00), correlation_id);
    try primitives.readTagBuffer(&hr);

    var body_reader: Reader = .init(hr.remaining());
    var resp = try decodeResponse(testing.allocator, &body_reader);
    defer resp.deinit(testing.allocator);

    try testing.expectEqualStrings("test", resp.responses[0].name);
    try testing.expectEqual(@as(i64, 42), resp.responses[0].partition_responses[0].base_offset);
    try testing.expectEqual(@as(usize, 0), body_reader.remaining().len);
}

test "decode response: truncated mid-partition returns EndOfStream" {
    // Cut off mid-partition_responses (after error_code, missing base_offset).
    const truncated = [_]u8{
        0x02, // responses: count=1
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x00, // error_code = 0
        // ← truncated: missing base_offset
    };

    var r: Reader = .init(&truncated);
    try testing.expectError(error.EndOfStream, decodeResponse(testing.allocator, &r));
}

test "decode response: truncation during topic 2 name leaks nothing" {
    // Two topics declared. Topic 1 ("a") decodes fully; topic 2's name is
    // truncated. The errdefer must free topic 1's name and partition data.
    const truncated = [_]u8{
        0x03, // responses: count=2
        0x02, 0x61, // topic[0] name = "a"
        0x02, // topic[0] partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // base_offset = 0
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x01, // record_errors: count=0
        0x00, // error_message = null
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x03, // topic[1] name length = 2, but no bytes follow
    };

    var r: Reader = .init(&truncated);
    try testing.expectError(
        error.EndOfStream,
        decodeResponse(testing.allocator, &r),
    );
    // std.testing.allocator checks for leaks on test exit.
}

test "decode response: truncation during record_errors leaks nothing" {
    // Partition decodes up to record_errors, then record_errors data is
    // truncated. The errdefer must free the topic name, partitions slice,
    // and any partially-allocated record_errors.
    const truncated = [_]u8{
        0x02, // responses: count=1
        0x02, 0x74, // name = "t"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // base_offset = 0
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x03, // record_errors: count=2
        0x00, 0x00, 0x00, 0x01, // re[0] batch_index = 1
        0x04, // re[0] error_message length = 3, but no bytes follow
    };

    var r: Reader = .init(&truncated);
    try testing.expectError(
        error.EndOfStream,
        decodeResponse(testing.allocator, &r),
    );
}

test "decode response: huge responses count with few bytes → Malformed (no huge alloc)" {
    // A frame claiming a massive responses array count but with only a few
    // bytes of data. The count cap must reject it as Malformed before any alloc.
    const huge_count: u64 = 1_000_000 + 1;
    var fixture: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&fixture);
    try primitives.writeUvarint(&w, huge_count); // responses: huge count
    const written = w.buffered();

    var r: Reader = .init(written);
    try testing.expectError(error.Malformed, decodeResponse(testing.allocator, &r));
}

test "decode response: huge partition_responses count → Malformed" {
    // Topic name decodes, then partition_responses claims a huge count.
    const huge_count: u64 = 1_000_000 + 1;
    var fixture: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&fixture);
    try primitives.writeUvarint(&w, 2); // responses: count=1
    try primitives.writeCompactString(&w, "t"); // name
    try primitives.writeUvarint(&w, huge_count); // partition_responses: huge
    const written = w.buffered();

    var r: Reader = .init(written);
    try testing.expectError(error.Malformed, decodeResponse(testing.allocator, &r));
}

test "decode response: huge record_errors count → Malformed" {
    // Partition fields decode up to record_errors, which claims a huge count.
    const huge_count: u64 = 1_000_000 + 1;
    var fixture: [128]u8 = undefined;
    var w: std.Io.Writer = .fixed(&fixture);
    try primitives.writeUvarint(&w, 2); // responses: count=1
    try primitives.writeCompactString(&w, "t"); // name
    try primitives.writeUvarint(&w, 2); // partition_responses: count=1
    try primitives.writeI32(&w, 0); // index
    try primitives.writeI16(&w, 0); // error_code
    try primitives.writeI64(&w, 0); // base_offset
    try primitives.writeI64(&w, -1); // log_append_time_ms
    try primitives.writeI64(&w, 0); // log_start_offset
    try primitives.writeUvarint(&w, huge_count); // record_errors: huge
    const written = w.buffered();

    var r: Reader = .init(written);
    try testing.expectError(error.Malformed, decodeResponse(testing.allocator, &r));
}

// ---------------------------------------------------------------------------
// Fuzz target
// ---------------------------------------------------------------------------

const produce_response_corpus: []const []const u8 = &.{
    &.{}, // empty
    // One topic, one partition, success (error_code=0, null error_message).
    &.{
        0x02, // responses: count=1
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // base_offset = 42
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x01, // record_errors: count=0
        0x00, // error_message = null
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer
    },
    // Nonzero error_code with a non-null error_message.
    &.{
        0x02, // responses: count=1
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x06, // error_code = 6 (NOT_LEADER_OR_FOLLOWER)
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // base_offset = -1
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x01, // record_errors: count=0
        0x0b, 0x6e, 0x6f, 0x74, 0x20, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, // error_message = "not leader"
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer
    },
    // Record errors present; null partition error_message.
    &.{
        0x02, // responses: count=1
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x01, // error_code = 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // base_offset = 100
        0x00, 0x00, 0x00, 0x00, 0x49, 0x96, 0x02, 0xd2, // log_append_time_ms = 1234567890
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x03, // record_errors: count=2
        0x00, 0x00, 0x00, 0x03, // batch_index = 3
        0x08, 0x74, 0x6f, 0x6f, 0x20, 0x6f, 0x6c, 0x64, // error_message = "too old"
        0x00, // record_error tag buffer
        0x00, 0x00, 0x00, 0x05, // batch_index = 5
        0x00, // error_message = null
        0x00, // record_error tag buffer
        0x00, // partition error_message = null
        0x00, // partition tag buffer
        0x00, // topic tag buffer
        0x00, 0x00, 0x00, 0x64, // throttle_time_ms = 100
        0x00, // trailing tag buffer
    },
    // Multiple topics and partitions.
    &.{
        0x03, // responses: count=2
        0x02, 0x61, // topic[0] name = "a"
        0x03, // topic[0] partition_responses: count=2
        0x00, 0x00, 0x00, 0x00, // p[0] index = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // base_offset = 0
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x01, // record_errors: count=0
        0x00, // error_message = null
        0x00, // tag buffer
        0x00, 0x00, 0x00, 0x01, // p[1] index = 1
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, // base_offset = 10
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // log_start_offset = 5
        0x01, // record_errors: count=0
        0x00, // error_message = null
        0x00, // tag buffer
        0x00, // topic[0] tag buffer
        0x02, 0x62, // topic[1] name = "b"
        0x02, // topic[1] partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // p[0] index = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, // base_offset = 20
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0x01, // record_errors: count=0
        0x00, // error_message = null
        0x00, // tag buffer
        0x00, // topic[1] tag buffer
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer
    },
    // Empty responses array.
    &.{
        0x01, // responses: count=0
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer
    },
    // Truncated mid-partition response.
    &.{
        0x02, // responses: count=1
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x00, // error_code = 0
        // truncated: missing the rest of the partition response
    },
    // All-ones and all-zeros bulk sentinels.
    &([_]u8{0xff} ** 64),
    &([_]u8{0x00} ** 64),
    // Huge array counts: the decoder's count cap must reject these as
    // Malformed before allocating. The uvarint 0xc1 0x84 0x3d encodes
    // 1_000_001 (i.e., compact-array count = 1_000_000).
    &.{
        0xc1, 0x84, 0x3d, // responses: huge count, no data
    },
    &.{
        0x02, // responses: count=1
        0x02, 0x74, // name = "t"
        0xc1, 0x84, 0x3d, // partition_responses: huge count
    },
    &.{
        0x02, // responses: count=1
        0x02, 0x74, // name = "t"
        0x02, // partition_responses: count=1
        0x00, 0x00, 0x00, 0x00, // index = 0
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // base_offset = 0
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log_append_time_ms = -1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset = 0
        0xc1, 0x84, 0x3d, // record_errors: huge count
    },
};

test "fuzz Produce v9 decodeResponse" {
    try std.testing.fuzz(std.testing.allocator, fuzzProduceResponse, .{ .corpus = produce_response_corpus });
}

fn fuzzProduceResponse(allocator: std.mem.Allocator, input: []const u8) !void {
    var r: Reader = .init(input);
    const result: error{ EndOfStream, Malformed, OutOfMemory, BufferTooSmall }!Response = decodeResponse(allocator, &r);
    var resp = result catch |err| switch (err) {
        error.EndOfStream, error.Malformed, error.OutOfMemory, error.BufferTooSmall => return,
    };
    defer resp.deinit(allocator);
}
