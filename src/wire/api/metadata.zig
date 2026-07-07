//! Metadata API (key 3, v12) — request and response codec.
//!
//! Spec: https://kafka.apache.org/43/design/protocol#the_messages_metadata
//! — "Metadata API (Key: 3)".
//!
//! v12 is FLEXIBLE (KIP-482): compact strings/arrays, unsigned-varint lengths,
//! request header v2, response header v1, and a trailing tagged-field buffer
//! on every flexible struct. Header versions per api_keys.zig: request v2,
//! response v1.
//!
//! This module encodes/decodes the **body only** — the bytes after the i32
//! length prefix and the request/response header. The header (correlation_id,
//! client_id, tagged fields) is a separate concern framed by a future
//! `request.zig`. The fixture tests assemble header+body inline to verify the
//! boundary.
//!
//! Allocator policy: the request encode path is zero-alloc (writes to a
//! caller-provided `*std.Io.Writer`). The response decode path takes an
//! `std.mem.Allocator` for the nested arrays (brokers, topics, partitions,
//! replica/isr/offline node arrays) because metadata refresh is cold-path
//! (PLAN §2.1: "metadata copy on refresh is fine"). The parsed response owns
//! its data; call `deinit` to free.

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const primitives = @import("../primitives.zig");
const Reader = primitives.Reader;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Kafka UUID: 16 raw bytes. The spec's UUID type is a 128-bit identifier.
/// Zero-filled UUID (all zeros) means "absent / queried by name."
pub const Uuid = [16]u8;

/// A topic in a Metadata v12 request. The broker resolves by topic_id when
/// name is null, or by name when topic_id is zero. One of the two should
/// always be populated.
pub const TopicRequest = struct {
    topic_id: Uuid,
    name: ?[]const u8,

    /// Convenience: build a topic-by-name request (topic_id = zeros).
    pub fn byName(name: []const u8) TopicRequest {
        return .{ .topic_id = @splat(0), .name = name };
    }
};

/// Metadata v12 request body.
///
/// Spec field order (v12):
///   topics: ?(COMPACT_ARRAY of { topic_id: UUID, name: COMPACT_NULLABLE_STRING, TAG_BUFFER })
///   allow_auto_topic_creation: BOOLEAN
///   include_topic_authorized_operations: BOOLEAN
///   TAG_BUFFER
///
/// Note: v12 dropped `include_cluster_authorized_operations` (present in v8–v10,
/// removed in v11). An empty/null topics array requests all topics.
pub const Request = struct {
    topics: ?[]const TopicRequest,
    allow_auto_topic_creation: bool,
    include_topic_authorized_operations: bool,
};

/// Metadata v12 response body.
///
/// Spec field order (v12):
///   throttle_time_ms: INT32
///   brokers: COMPACT_ARRAY of Broker
///   cluster_id: COMPACT_NULLABLE_STRING
///   controller_id: INT32
///   topics: COMPACT_ARRAY of Topic
///   TAG_BUFFER
///
/// Owned data: all nested arrays are allocated by `decode`. Call `deinit`.
pub const Response = struct {
    throttle_time_ms: i32,
    brokers: []Broker,
    cluster_id: ?[]const u8,
    controller_id: i32,
    topics: []Topic,

    pub fn deinit(self: *Response, allocator: std.mem.Allocator) void {
        for (self.brokers) |*b| b.deinit(allocator);
        allocator.free(self.brokers);
        if (self.cluster_id) |cid| allocator.free(cid);
        for (self.topics) |*t| t.deinit(allocator);
        allocator.free(self.topics);
        self.* = undefined;
    }

    pub const Broker = struct {
        node_id: i32,
        host: []const u8,
        port: i32,
        rack: ?[]const u8,

        pub fn deinit(self: *Broker, allocator: std.mem.Allocator) void {
            allocator.free(self.host);
            if (self.rack) |r| allocator.free(r);
            self.* = undefined;
        }
    };

    pub const Partition = struct {
        error_code: i16,
        partition_index: i32,
        leader_id: i32,
        leader_epoch: i32,
        replica_nodes: []i32,
        isr_nodes: []i32,
        offline_replicas: []i32,

        pub fn deinit(self: *Partition, allocator: std.mem.Allocator) void {
            allocator.free(self.replica_nodes);
            allocator.free(self.isr_nodes);
            allocator.free(self.offline_replicas);
            self.* = undefined;
        }
    };

    pub const Topic = struct {
        error_code: i16,
        name: ?[]const u8,
        topic_id: Uuid,
        is_internal: bool,
        partitions: []Partition,
        topic_authorized_operations: i32,

        pub fn deinit(self: *Topic, allocator: std.mem.Allocator) void {
            if (self.name) |n| allocator.free(n);
            for (self.partitions) |*p| p.deinit(allocator);
            allocator.free(self.partitions);
            self.* = undefined;
        }
    };
};

// ---------------------------------------------------------------------------
// Encode (request body — zero-alloc, writes to caller's Writer)
// ---------------------------------------------------------------------------

/// Encode a Metadata v12 request body to `writer`.
///
/// Spec: https://kafka.apache.org/43/design/protocol#the_messages_metadata
/// — "Metadata Request (Version: 12)".
pub fn encodeRequest(writer: *std.Io.Writer, req: Request) std.Io.Writer.Error!void {
    // topics: nullable compact array. null → 0, N elements → uvarint(N+1).
    if (req.topics) |topics| {
        try primitives.writeCompactArrayCount(writer, topics.len);
        for (topics) |t| {
            // topic_id: UUID (16 raw bytes).
            try writer.writeAll(&t.topic_id);
            // name: COMPACT_NULLABLE_STRING.
            try primitives.writeNullableCompactString(writer, t.name);
            // TAG_BUFFER (empty).
            try primitives.writeEmptyTagBuffer(writer);
        }
    } else {
        try primitives.writeNullableCompactArrayCount(writer, null);
    }

    try primitives.writeBool(writer, req.allow_auto_topic_creation);
    try primitives.writeBool(writer, req.include_topic_authorized_operations);

    // Trailing TAG_BUFFER (empty).
    try primitives.writeEmptyTagBuffer(writer);
}

// ---------------------------------------------------------------------------
// Decode (response body — allocates nested arrays, cold-path)
// ---------------------------------------------------------------------------

/// Decode a Metadata v12 response body from `reader`.
///
/// Spec: https://kafka.apache.org/43/design/protocol#the_messages_metadata
/// — "Metadata Response (Version: 12)".
///
/// The returned `Response` owns all nested data (brokers, topics, partitions,
/// node-id arrays, strings). Strings are copied from the response buffer so
/// the caller is not bound to the buffer's lifetime. Call `deinit` to free.
pub fn decodeResponse(allocator: std.mem.Allocator, reader: *Reader) !Response {
    const throttle_time_ms = try primitives.readI32(reader);

    // brokers: COMPACT_ARRAY of Broker. Track the number fully decoded so the
    // errdefer frees only the initialized prefix — a failure mid-broker leaves
    // the partial slot's strings unowned, so we never deinit past `brokers_len`.
    const broker_count = try primitives.readCompactArrayCount(reader);
    const n_brokers = broker_count orelse return error.Malformed;
    const brokers = try allocator.alloc(Response.Broker, n_brokers);
    var brokers_len: usize = 0;
    errdefer {
        for (brokers[0..brokers_len]) |*b| b.deinit(allocator);
        allocator.free(brokers);
    }
    while (brokers_len < n_brokers) : (brokers_len += 1) {
        brokers[brokers_len] = try decodeBroker(allocator, reader);
    }

    // cluster_id: COMPACT_NULLABLE_STRING.
    const cluster_id_raw = try primitives.readNullableCompactString(reader);
    const cluster_id: ?[]const u8 = if (cluster_id_raw) |cid| try allocator.dupe(u8, cid) else null;
    errdefer if (cluster_id) |cid| allocator.free(cid);

    const controller_id = try primitives.readI32(reader);

    // topics: COMPACT_ARRAY of Topic. Same running-count discipline; a failure
    // inside `decodeTopic` (including a partition-decode failure) propagates
    // here, and the errdefer frees the topics decoded so far plus the brokers.
    const topic_count = try primitives.readCompactArrayCount(reader);
    const n_topics = topic_count orelse return error.Malformed;
    const topics = try allocator.alloc(Response.Topic, n_topics);
    var topics_len: usize = 0;
    errdefer {
        for (topics[0..topics_len]) |*t| t.deinit(allocator);
        allocator.free(topics);
    }
    while (topics_len < n_topics) : (topics_len += 1) {
        topics[topics_len] = try decodeTopic(allocator, reader);
    }

    // Trailing TAG_BUFFER.
    try primitives.readTagBuffer(reader);

    return .{
        .throttle_time_ms = throttle_time_ms,
        .brokers = brokers,
        .cluster_id = cluster_id,
        .controller_id = controller_id,
        .topics = topics,
    };
}

fn decodeBroker(allocator: std.mem.Allocator, reader: *Reader) !Response.Broker {
    const node_id = try primitives.readI32(reader);
    const host_raw = try primitives.readCompactString(reader);
    const host = try allocator.dupe(u8, host_raw);
    errdefer allocator.free(host);
    const port = try primitives.readI32(reader);
    const rack_raw = try primitives.readNullableCompactString(reader);
    const rack: ?[]const u8 = if (rack_raw) |r| try allocator.dupe(u8, r) else null;
    errdefer if (rack) |r| allocator.free(r);
    try primitives.readTagBuffer(reader);
    return .{
        .node_id = node_id,
        .host = host,
        .port = port,
        .rack = rack,
    };
}

fn decodeTopic(allocator: std.mem.Allocator, reader: *Reader) !Response.Topic {
    const error_code = try primitives.readI16(reader);
    const name_raw = try primitives.readNullableCompactString(reader);
    const name: ?[]const u8 = if (name_raw) |n| try allocator.dupe(u8, n) else null;
    errdefer if (name) |n| allocator.free(n);
    const topic_id: Uuid = blk: {
        const slice = try reader.readSlice(16);
        var uuid: Uuid = undefined;
        @memcpy(&uuid, slice);
        break :blk uuid;
    };
    const is_internal = try primitives.readBool(reader);

    // partitions: COMPACT_ARRAY of Partition. Running-count errdefer frees
    // only the initialized prefix; a failure mid-partition does not deinit
    // the partial slot (its node arrays are either all owned or all unset).
    const partition_count = try primitives.readCompactArrayCount(reader);
    const n_partitions = partition_count orelse return error.Malformed;
    const partitions = try allocator.alloc(Response.Partition, n_partitions);
    var partitions_len: usize = 0;
    errdefer {
        for (partitions[0..partitions_len]) |*p| p.deinit(allocator);
        allocator.free(partitions);
    }
    while (partitions_len < n_partitions) : (partitions_len += 1) {
        partitions[partitions_len] = try decodePartition(allocator, reader);
    }

    const topic_authorized_operations = try primitives.readI32(reader);
    try primitives.readTagBuffer(reader);

    return .{
        .error_code = error_code,
        .name = name,
        .topic_id = topic_id,
        .is_internal = is_internal,
        .partitions = partitions,
        .topic_authorized_operations = topic_authorized_operations,
    };
}

fn decodePartition(allocator: std.mem.Allocator, reader: *Reader) !Response.Partition {
    const error_code = try primitives.readI16(reader);
    const partition_index = try primitives.readI32(reader);
    const leader_id = try primitives.readI32(reader);
    const leader_epoch = try primitives.readI32(reader);

    const replica_nodes = try decodeI32Array(allocator, reader);
    errdefer allocator.free(replica_nodes);
    const isr_nodes = try decodeI32Array(allocator, reader);
    errdefer allocator.free(isr_nodes);
    const offline_replicas = try decodeI32Array(allocator, reader);
    errdefer allocator.free(offline_replicas);

    try primitives.readTagBuffer(reader);

    return .{
        .error_code = error_code,
        .partition_index = partition_index,
        .leader_id = leader_id,
        .leader_epoch = leader_epoch,
        .replica_nodes = replica_nodes,
        .isr_nodes = isr_nodes,
        .offline_replicas = offline_replicas,
    };
}

/// Decode a COMPACT_ARRAY of INT32 into an owned slice.
fn decodeI32Array(allocator: std.mem.Allocator, reader: *Reader) ![]i32 {
    const count = try primitives.readCompactArrayCount(reader);
    const n = count orelse return error.Malformed;
    const arr = try allocator.alloc(i32, n);
    errdefer allocator.free(arr);
    for (arr) |*v| {
        v.* = try primitives.readI32(reader);
    }
    return arr;
}

// ---------------------------------------------------------------------------
// Encode (response body — for round-trip tests)
// ---------------------------------------------------------------------------

/// Encode a Metadata v12 response body to `writer`.
///
/// This is the inverse of `decodeResponse`, used for round-trip tests. It
/// assumes all tag buffers are empty (the fixture uses empty tags, so the
/// round-trip is lossless).
pub fn encodeResponse(writer: *std.Io.Writer, resp: Response) std.Io.Writer.Error!void {
    try primitives.writeI32(writer, resp.throttle_time_ms);

    // brokers: COMPACT_ARRAY.
    try primitives.writeCompactArrayCount(writer, resp.brokers.len);
    for (resp.brokers) |b| {
        try primitives.writeI32(writer, b.node_id);
        try primitives.writeCompactString(writer, b.host);
        try primitives.writeI32(writer, b.port);
        try primitives.writeNullableCompactString(writer, b.rack);
        try primitives.writeEmptyTagBuffer(writer);
    }

    // cluster_id: COMPACT_NULLABLE_STRING.
    try primitives.writeNullableCompactString(writer, resp.cluster_id);

    try primitives.writeI32(writer, resp.controller_id);

    // topics: COMPACT_ARRAY.
    try primitives.writeCompactArrayCount(writer, resp.topics.len);
    for (resp.topics) |t| {
        try primitives.writeI16(writer, t.error_code);
        try primitives.writeNullableCompactString(writer, t.name);
        try writer.writeAll(&t.topic_id);
        try primitives.writeBool(writer, t.is_internal);

        // partitions: COMPACT_ARRAY.
        try primitives.writeCompactArrayCount(writer, t.partitions.len);
        for (t.partitions) |p| {
            try primitives.writeI16(writer, p.error_code);
            try primitives.writeI32(writer, p.partition_index);
            try primitives.writeI32(writer, p.leader_id);
            try primitives.writeI32(writer, p.leader_epoch);
            try writeI32Array(writer, p.replica_nodes);
            try writeI32Array(writer, p.isr_nodes);
            try writeI32Array(writer, p.offline_replicas);
            try primitives.writeEmptyTagBuffer(writer);
        }

        try primitives.writeI32(writer, t.topic_authorized_operations);
        try primitives.writeEmptyTagBuffer(writer);
    }

    // Trailing TAG_BUFFER.
    try primitives.writeEmptyTagBuffer(writer);
}

fn writeI32Array(writer: *std.Io.Writer, arr: []const i32) std.Io.Writer.Error!void {
    try primitives.writeCompactArrayCount(writer, arr.len);
    for (arr) |v| {
        try primitives.writeI32(writer, v);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "encode request: one topic by name" {
    // Hand-constructed from the v12 spec. Field-by-field breakdown:
    //
    //   topics: compact array count=1 → uvarint(1+1) = 0x02
    //     topic[0]:
    //       topic_id: UUID = 16×0x00 (querying by name)
    //       name: COMPACT_NULLABLE_STRING "test" → 0x05 0x74 0x65 0x73 0x74
    //       TAG_BUFFER: 0x00
    //   allow_auto_topic_creation: BOOLEAN false → 0x00
    //   include_topic_authorized_operations: BOOLEAN false → 0x00
    //   TAG_BUFFER: 0x00
    const expected = [_]u8{
        0x02, // topics: compact array, count=1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // topic_id (UUID, 16 bytes)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //   ...continued
        0x05, 0x74, 0x65, 0x73, 0x74, // name: "test"
        0x00, // tag buffer
        0x00, // allow_auto_topic_creation: false
        0x00, // include_topic_authorized_operations: false
        0x00, // tag buffer
    };

    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .topics = &.{.{ .topic_id = @splat(0), .name = "test" }},
        .allow_auto_topic_creation = false,
        .include_topic_authorized_operations = false,
    });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "encode request: null topics (all topics)" {
    // Null topics array → uvarint(0) = 0x00.
    //   topics: null compact array → 0x00
    //   allow_auto_topic_creation: BOOLEAN true → 0x01
    //   include_topic_authorized_operations: BOOLEAN true → 0x01
    //   TAG_BUFFER: 0x00
    const expected = [_]u8{
        0x00, // topics: null
        0x01, // allow_auto_topic_creation: true
        0x01, // include_topic_authorized_operations: true
        0x00, // tag buffer
    };

    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .topics = null,
        .allow_auto_topic_creation = true,
        .include_topic_authorized_operations = true,
    });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "encode request: empty topics array (all topics, non-null)" {
    // Empty (non-null) topics array → uvarint(0+1) = 0x01.
    //   topics: compact array, count=0 → 0x01
    //   allow_auto_topic_creation: BOOLEAN false → 0x00
    //   include_topic_authorized_operations: BOOLEAN false → 0x00
    //   TAG_BUFFER: 0x00
    const expected = [_]u8{
        0x01, // topics: empty compact array
        0x00, // allow_auto_topic_creation: false
        0x00, // include_topic_authorized_operations: false
        0x00, // tag buffer
    };

    var buf: [16]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .topics = &.{},
        .allow_auto_topic_creation = false,
        .include_topic_authorized_operations = false,
    });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "encode request: two topics by name" {
    //   topics: compact array count=2 → uvarint(2+1) = 0x03
    //     topic[0]: topic_id=zeros, name="a" → 0x02 0x61, tag 0x00
    //     topic[1]: topic_id=zeros, name="bb" → 0x03 0x62 0x62, tag 0x00
    //   allow_auto_topic_creation: false → 0x00
    //   include_topic_authorized_operations: false → 0x00
    //   TAG_BUFFER: 0x00
    const expected = [_]u8{
        0x03, // topics: compact array, count=2
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // topic[0] topic_id
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
        0x02, 0x61, // topic[0] name: "a"
        0x00, // topic[0] tag buffer
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // topic[1] topic_id
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
        0x03, 0x62, 0x62, // topic[1] name: "bb"
        0x00, // topic[1] tag buffer
        0x00, // allow_auto_topic_creation: false
        0x00, // include_topic_authorized_operations: false
        0x00, // tag buffer
    };

    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .topics = &.{
            .{ .topic_id = @splat(0), .name = "a" },
            .{ .topic_id = @splat(0), .name = "bb" },
        },
        .allow_auto_topic_creation = false,
        .include_topic_authorized_operations = false,
    });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "decode response: minimal cluster (1 broker, 1 topic, 1 partition)" {
    // Hand-constructed from the v12 spec. Field-by-field breakdown:
    //
    //   throttle_time_ms: INT32 = 0 → 00 00 00 00
    //   brokers: compact array count=1 → 0x02
    //     broker[0]:
    //       node_id: INT32 = 1 → 00 00 00 01
    //       host: COMPACT_STRING "localhost" → 0x0a 6c 6f 63 61 6c 68 6f 73 74
    //       port: INT32 = 9092 → 00 00 23 84
    //       rack: COMPACT_NULLABLE_STRING null → 0x00
    //       TAG_BUFFER: 0x00
    //   cluster_id: COMPACT_NULLABLE_STRING null → 0x00
    //   controller_id: INT32 = 1 → 00 00 00 01
    //   topics: compact array count=1 → 0x02
    //     topic[0]:
    //       error_code: INT16 = 0 → 00 00
    //       name: COMPACT_NULLABLE_STRING "test" → 0x05 74 65 73 74
    //       topic_id: UUID = 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f 10
    //       is_internal: BOOLEAN false → 0x00
    //       partitions: compact array count=1 → 0x02
    //         partition[0]:
    //           error_code: INT16 = 0 → 00 00
    //           partition_index: INT32 = 0 → 00 00 00 00
    //           leader_id: INT32 = 1 → 00 00 00 01
    //           leader_epoch: INT32 = 0 → 00 00 00 00
    //           replica_nodes: compact array count=1 → 0x02, INT32 1 → 00 00 00 01
    //           isr_nodes: compact array count=1 → 0x02, INT32 1 → 00 00 00 01
    //           offline_replicas: compact array count=0 → 0x01
    //           TAG_BUFFER: 0x00
    //       topic_authorized_operations: INT32 = 0 → 00 00 00 00
    //       TAG_BUFFER: 0x00
    //   TAG_BUFFER: 0x00
    const fixture = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x02, // brokers: count=1
        0x00, 0x00, 0x00, 0x01, // broker[0] node_id = 1
        0x0a, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, // host = "localhost"
        0x00, 0x00, 0x23, 0x84, // port = 9092
        0x00, // rack = null
        0x00, // broker tag buffer
        0x00, // cluster_id = null
        0x00, 0x00, 0x00, 0x01, // controller_id = 1
        0x02, // topics: count=1
        0x00, 0x00, // topic[0] error_code = 0
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // topic_id (UUID)
        0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, //
        0x00, // is_internal = false
        0x02, // partitions: count=1
        0x00, 0x00, // partition[0] error_code = 0
        0x00, 0x00, 0x00, 0x00, // partition_index = 0
        0x00, 0x00, 0x00, 0x01, // leader_id = 1
        0x00, 0x00, 0x00, 0x00, // leader_epoch = 0
        0x02, 0x00, 0x00, 0x00, 0x01, // replica_nodes = [1]
        0x02, 0x00, 0x00, 0x00, 0x01, // isr_nodes = [1]
        0x01, // offline_replicas = []
        0x00, // partition tag buffer
        0x00, 0x00, 0x00, 0x00, // topic_authorized_operations = 0
        0x00, // topic tag buffer
        0x00, // trailing tag buffer
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i32, 0), resp.throttle_time_ms);
    try testing.expectEqual(@as(usize, 1), resp.brokers.len);
    try testing.expectEqual(@as(i32, 1), resp.brokers[0].node_id);
    try testing.expectEqualStrings("localhost", resp.brokers[0].host);
    try testing.expectEqual(@as(i32, 9092), resp.brokers[0].port);
    try testing.expectEqual(@as(?[]const u8, null), resp.brokers[0].rack);
    try testing.expectEqual(@as(?[]const u8, null), resp.cluster_id);
    try testing.expectEqual(@as(i32, 1), resp.controller_id);

    try testing.expectEqual(@as(usize, 1), resp.topics.len);
    try testing.expectEqual(@as(i16, 0), resp.topics[0].error_code);
    try testing.expectEqualStrings("test", resp.topics[0].name.?);
    const expected_topic_id: Uuid = .{
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    };
    try testing.expectEqualSlices(u8, &expected_topic_id, &resp.topics[0].topic_id);
    try testing.expectEqual(false, resp.topics[0].is_internal);

    try testing.expectEqual(@as(usize, 1), resp.topics[0].partitions.len);
    const p = &resp.topics[0].partitions[0];
    try testing.expectEqual(@as(i16, 0), p.error_code);
    try testing.expectEqual(@as(i32, 0), p.partition_index);
    try testing.expectEqual(@as(i32, 1), p.leader_id);
    try testing.expectEqual(@as(i32, 0), p.leader_epoch);
    try testing.expectEqualSlices(i32, &.{1}, p.replica_nodes);
    try testing.expectEqualSlices(i32, &.{1}, p.isr_nodes);
    try testing.expectEqualSlices(i32, &.{}, p.offline_replicas);
    try testing.expectEqual(@as(i32, 0), resp.topics[0].topic_authorized_operations);

    // All bytes consumed.
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "decode response: non-null rack and cluster_id" {
    //   throttle_time_ms: 0
    //   brokers: count=1
    //     node_id=1, host="b1", port=9092, rack="us-east-1a", tag=0x00
    //   cluster_id: "abc"
    //   controller_id: 1
    //   topics: count=0 → 0x01
    //   TAG_BUFFER: 0x00
    const fixture = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x02, // brokers: count=1
        0x00, 0x00, 0x00, 0x01, // node_id = 1
        0x03, 0x62, 0x31, // host = "b1"
        0x00, 0x00, 0x23, 0x84, // port = 9092
        0x0b, 0x75, 0x73, 0x2d, 0x65, 0x61, 0x73, 0x74, 0x2d, 0x31, 0x61, // rack = "us-east-1a"
        0x00, // broker tag buffer
        0x04, 0x61, 0x62, 0x63, // cluster_id = "abc"
        0x00, 0x00, 0x00, 0x01, // controller_id = 1
        0x01, // topics: count=0
        0x00, // trailing tag buffer
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqualStrings("us-east-1a", resp.brokers[0].rack.?);
    try testing.expectEqualStrings("abc", resp.cluster_id.?);
    try testing.expectEqual(@as(usize, 0), resp.topics.len);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "decode response: error_code nonzero on topic" {
    //   throttle_time_ms: 0
    //   brokers: count=0 → 0x01
    //   cluster_id: null → 0x00
    //   controller_id: 1
    //   topics: count=1
    //     error_code=3 (UNKNOWN_TOPIC), name=null, topic_id=zeros, is_internal=false,
    //     partitions=count=0, topic_authorized_ops=0, tag=0x00
    //   TAG_BUFFER: 0x00
    const fixture = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x01, // brokers: count=0
        0x00, // cluster_id = null
        0x00, 0x00, 0x00, 0x01, // controller_id = 1
        0x02, // topics: count=1
        0x00, 0x03, // error_code = 3 (UNKNOWN_TOPIC)
        0x00, // name = null (COMPACT_NULLABLE_STRING)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // topic_id = zeros
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
        0x00, // is_internal = false
        0x01, // partitions: count=0
        0x00, 0x00, 0x00, 0x00, // topic_authorized_operations = 0
        0x00, // topic tag buffer
        0x00, // trailing tag buffer
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 3), resp.topics[0].error_code);
    try testing.expectEqual(@as(?[]const u8, null), resp.topics[0].name);
    try testing.expectEqual(@as(usize, 0), resp.topics[0].partitions.len);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "decode response: multiple partitions" {
    //   throttle_time_ms: 0
    //   brokers: count=0
    //   cluster_id: null
    //   controller_id: 1
    //   topics: count=1
    //     error_code=0, name="t", topic_id=zeros, is_internal=false
    //     partitions: count=2
    //       p[0]: error=0, idx=0, leader=1, epoch=0, replicas=[1], isr=[1], offline=[], tag=0x00
    //       p[1]: error=0, idx=1, leader=2, epoch=3, replicas=[1,2], isr=[1], offline=[2], tag=0x00
    //     topic_authorized_ops=0, tag=0x00
    //   TAG_BUFFER: 0x00
    const fixture = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x01, // brokers: count=0
        0x00, // cluster_id = null
        0x00, 0x00, 0x00, 0x01, // controller_id = 1
        0x02, // topics: count=1
        0x00, 0x00, // error_code = 0
        0x02, 0x74, // name = "t"
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // topic_id = zeros
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
        0x00, // is_internal = false
        0x03, // partitions: count=2
        // partition[0]
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x00, // partition_index = 0
        0x00, 0x00, 0x00, 0x01, // leader_id = 1
        0x00, 0x00, 0x00, 0x00, // leader_epoch = 0
        0x02, 0x00, 0x00, 0x00, 0x01, // replica_nodes = [1]
        0x02, 0x00, 0x00, 0x00, 0x01, // isr_nodes = [1]
        0x01, // offline_replicas = []
        0x00, // tag buffer
        // partition[1]
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x01, // partition_index = 1
        0x00, 0x00, 0x00, 0x02, // leader_id = 2
        0x00, 0x00, 0x00, 0x03, // leader_epoch = 3
        0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, // replica_nodes = [1, 2]
        0x02, 0x00, 0x00, 0x00, 0x01, // isr_nodes = [1]
        0x02, 0x00, 0x00, 0x00, 0x02, // offline_replicas = [2]
        0x00, // tag buffer
        0x00, 0x00, 0x00, 0x00, // topic_authorized_operations = 0
        0x00, // topic tag buffer
        0x00, // trailing tag buffer
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 2), resp.topics[0].partitions.len);

    const p0 = &resp.topics[0].partitions[0];
    try testing.expectEqual(@as(i32, 0), p0.partition_index);
    try testing.expectEqual(@as(i32, 1), p0.leader_id);
    try testing.expectEqualSlices(i32, &.{1}, p0.replica_nodes);
    try testing.expectEqualSlices(i32, &.{}, p0.offline_replicas);

    const p1 = &resp.topics[0].partitions[1];
    try testing.expectEqual(@as(i32, 1), p1.partition_index);
    try testing.expectEqual(@as(i32, 2), p1.leader_id);
    try testing.expectEqual(@as(i32, 3), p1.leader_epoch);
    try testing.expectEqualSlices(i32, &.{ 1, 2 }, p1.replica_nodes);
    try testing.expectEqualSlices(i32, &.{1}, p1.isr_nodes);
    try testing.expectEqualSlices(i32, &.{2}, p1.offline_replicas);

    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "round-trip: decode then re-encode response equals fixture" {
    // Same fixture as the minimal cluster test. decode → encode → bytes equal.
    const fixture = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x02, // brokers: count=1
        0x00, 0x00, 0x00, 0x01, // broker[0] node_id = 1
        0x0a, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, // host = "localhost"
        0x00, 0x00, 0x23, 0x84, // port = 9092
        0x00, // rack = null
        0x00, // broker tag buffer
        0x00, // cluster_id = null
        0x00, 0x00, 0x00, 0x01, // controller_id = 1
        0x02, // topics: count=1
        0x00, 0x00, // topic[0] error_code = 0
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // topic_id (UUID)
        0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, //
        0x00, // is_internal = false
        0x02, // partitions: count=1
        0x00, 0x00, // partition[0] error_code = 0
        0x00, 0x00, 0x00, 0x00, // partition_index = 0
        0x00, 0x00, 0x00, 0x01, // leader_id = 1
        0x00, 0x00, 0x00, 0x00, // leader_epoch = 0
        0x02, 0x00, 0x00, 0x00, 0x01, // replica_nodes = [1]
        0x02, 0x00, 0x00, 0x00, 0x01, // isr_nodes = [1]
        0x01, // offline_replicas = []
        0x00, // partition tag buffer
        0x00, 0x00, 0x00, 0x00, // topic_authorized_operations = 0
        0x00, // topic tag buffer
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

test "round-trip: decode then re-encode response with rack and cluster_id" {
    // Fixture from the non-null rack and cluster_id test.
    const fixture = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x02, // brokers: count=1
        0x00, 0x00, 0x00, 0x01, // node_id = 1
        0x03, 0x62, 0x31, // host = "b1"
        0x00, 0x00, 0x23, 0x84, // port = 9092
        0x0b, 0x75, 0x73, 0x2d, 0x65, 0x61, 0x73, 0x74, 0x2d, 0x31, 0x61, // rack = "us-east-1a"
        0x00, // broker tag buffer
        0x04, 0x61, 0x62, 0x63, // cluster_id = "abc"
        0x00, 0x00, 0x00, 0x01, // controller_id = 1
        0x01, // topics: count=0
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

test "round-trip: decode then re-encode multi-partition response" {
    // Fixture from the multiple partitions test.
    const fixture = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x01, // brokers: count=0
        0x00, // cluster_id = null
        0x00, 0x00, 0x00, 0x01, // controller_id = 1
        0x02, // topics: count=1
        0x00, 0x00, // error_code = 0
        0x02, 0x74, // name = "t"
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // topic_id = zeros
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
        0x00, // is_internal = false
        0x03, // partitions: count=2
        0x00, 0x00, // p[0] error_code = 0
        0x00, 0x00, 0x00, 0x00, // p[0] partition_index = 0
        0x00, 0x00, 0x00, 0x01, // p[0] leader_id = 1
        0x00, 0x00, 0x00, 0x00, // p[0] leader_epoch = 0
        0x02, 0x00, 0x00, 0x00, 0x01, // p[0] replica_nodes = [1]
        0x02, 0x00, 0x00, 0x00, 0x01, // p[0] isr_nodes = [1]
        0x01, // p[0] offline_replicas = []
        0x00, // p[0] tag buffer
        0x00, 0x00, // p[1] error_code = 0
        0x00, 0x00, 0x00, 0x01, // p[1] partition_index = 1
        0x00, 0x00, 0x00, 0x02, // p[1] leader_id = 2
        0x00, 0x00, 0x00, 0x03, // p[1] leader_epoch = 3
        0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, // p[1] replica_nodes = [1, 2]
        0x02, 0x00, 0x00, 0x00, 0x01, // p[1] isr_nodes = [1]
        0x02, 0x00, 0x00, 0x00, 0x02, // p[1] offline_replicas = [2]
        0x00, // p[1] tag buffer
        0x00, 0x00, 0x00, 0x00, // topic_authorized_operations = 0
        0x00, // topic tag buffer
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

test "round-trip: encode request then decode matches struct" {
    // Encode a request, then manually decode it and verify the fields.
    // (Request decode is not part of the public API — we verify via the
    // encoded bytes instead, checking the request against its expected
    // encoding.)
    const topics = [_]TopicRequest{
        .byName("test"),
    };

    var buf: [64]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .topics = &topics,
        .allow_auto_topic_creation = true,
        .include_topic_authorized_operations = false,
    });

    // Manually decode the request body to verify.
    var r: Reader = .init(w.buffered());
    const count = try primitives.readCompactArrayCount(&r);
    try testing.expectEqual(@as(?usize, 1), count);

    // topic[0]
    const topic_id_slice = try r.readSlice(16);
    var topic_id: Uuid = undefined;
    @memcpy(&topic_id, topic_id_slice);
    try testing.expectEqualSlices(u8, &@as(Uuid, @splat(0)), &topic_id);
    const name = try primitives.readNullableCompactString(&r);
    try testing.expectEqualStrings("test", name.?);
    try primitives.readTagBuffer(&r);

    const allow = try primitives.readBool(&r);
    try testing.expectEqual(true, allow);
    const include = try primitives.readBool(&r);
    try testing.expectEqual(false, include);
    try primitives.readTagBuffer(&r);

    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "decode response with full frame: length prefix + response header v1" {
    // This test assembles the full on-wire frame: i32 length prefix + response
    // header v1 (correlation_id i32 + tag buffer) + body. This verifies the
    // boundary between header and body that metadata.zig operates on.
    //
    // Response header v1 (flexible): correlation_id: INT32, TAG_BUFFER.
    // The body is the same minimal cluster fixture used above.
    const body = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x02, // brokers: count=1
        0x00, 0x00, 0x00, 0x01, // broker[0] node_id = 1
        0x0a, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, // host = "localhost"
        0x00, 0x00, 0x23, 0x84, // port = 9092
        0x00, // rack = null
        0x00, // broker tag buffer
        0x00, // cluster_id = null
        0x00, 0x00, 0x00, 0x01, // controller_id = 1
        0x02, // topics: count=1
        0x00, 0x00, // topic[0] error_code = 0
        0x05, 0x74, 0x65, 0x73, 0x74, // name = "test"
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // topic_id (UUID)
        0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, //
        0x00, // is_internal = false
        0x02, // partitions: count=1
        0x00, 0x00, // partition[0] error_code = 0
        0x00, 0x00, 0x00, 0x00, // partition_index = 0
        0x00, 0x00, 0x00, 0x01, // leader_id = 1
        0x00, 0x00, 0x00, 0x00, // leader_epoch = 0
        0x02, 0x00, 0x00, 0x00, 0x01, // replica_nodes = [1]
        0x02, 0x00, 0x00, 0x00, 0x01, // isr_nodes = [1]
        0x01, // offline_replicas = []
        0x00, // partition tag buffer
        0x00, 0x00, 0x00, 0x00, // topic_authorized_operations = 0
        0x00, // topic tag buffer
        0x00, // trailing tag buffer
    };

    // Response header v1: correlation_id (i32) + tag buffer (0x00).
    const header = [_]u8{
        0x00, 0x00, 0x7b, 0x00, // correlation_id = 31488
        0x00, // tag buffer
    };

    // Full frame: i32 length prefix (header + body) + header + body.
    const frame_len: u32 = @intCast(header.len + body.len);
    var frame: [128]u8 = undefined;
    var pos: usize = 0;
    // Length prefix (big-endian u32).
    std.mem.writeInt(u32, frame[pos..][0..4], frame_len, .big);
    pos += 4;
    @memcpy(frame[pos..][0..header.len], &header);
    pos += header.len;
    @memcpy(frame[pos..][0..body.len], &body);
    pos += body.len;

    // Parse the length prefix, then skip header + body bytes (simulating
    // what the frame layer + request.zig will do in the future).
    var len_reader: Reader = .init(frame[0..pos]);
    const body_len = try primitives.readI32(&len_reader);
    try testing.expectEqual(@as(i32, @intCast(header.len + body.len)), body_len);

    // The payload after the length prefix is header + body.
    const payload = len_reader.remaining();

    // Skip the response header v1: correlation_id (i32) + tag buffer.
    var hr: Reader = .init(payload);
    const correlation_id = try primitives.readI32(&hr);
    try testing.expectEqual(@as(i32, 0x7b00), correlation_id);
    try primitives.readTagBuffer(&hr);

    // The rest is the body — decode it.
    var body_reader: Reader = .init(hr.remaining());
    var resp = try decodeResponse(testing.allocator, &body_reader);
    defer resp.deinit(testing.allocator);

    try testing.expectEqualStrings("localhost", resp.brokers[0].host);
    try testing.expectEqualStrings("test", resp.topics[0].name.?);
    try testing.expectEqual(@as(usize, 0), body_reader.remaining().len);
}

test "decode response: truncated input returns EndOfStream" {
    // A response that's cut off mid-broker.
    const truncated = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x02, // brokers: count=1
        0x00, 0x00, 0x00, 0x01, // node_id = 1
        0x0a, // host length = 9, but no bytes follow
    };

    var r: Reader = .init(&truncated);
    try testing.expectError(error.EndOfStream, decodeResponse(testing.allocator, &r));
}

test "decode response: truncation during broker 2 host leaks nothing" {
    // Two brokers declared. Broker 1 ("b1") decodes fully; broker 2's host
    // string is truncated. `decodeResponse` must return an error, and
    // `std.testing.allocator` must report zero leaks — which means the
    // errdefer freed broker 1's duplicated `host` string and the brokers
    // slice itself.
    //
    //   throttle_time_ms: 0
    //   brokers: count=2 → 0x03
    //     broker[0]: node_id=1, host="b1", port=9092, rack=null, tag=0x00
    //     broker[1]: node_id=2, host=<length=3 but no bytes> ← truncated
    const truncated = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x03, // brokers: count=2
        // broker[0] — fully decodable
        0x00, 0x00, 0x00, 0x01, // node_id = 1
        0x03, 0x62, 0x31, // host = "b1"
        0x00, 0x00, 0x23, 0x84, // port = 9092
        0x00, // rack = null
        0x00, // broker tag buffer
        // broker[1] — truncated mid-host-string
        0x00, 0x00, 0x00, 0x02, // node_id = 2
        0x04, // host length = 3, but no bytes follow
    };

    var r: Reader = .init(&truncated);
    try testing.expectError(
        error.EndOfStream,
        decodeResponse(testing.allocator, &r),
    );
    // std.testing.allocator checks for leaks on test exit; no explicit
    // assertion needed — a leak fails the test.
}

test "decode response: truncation during topic decode with brokers present leaks nothing" {
    // Brokers decode fully, then a topic's name string is truncated. The
    // errdefer must free the decoded brokers (host/rack strings + slice) in
    // addition to the topics slice. Exercises the outer-level cleanup path.
    //
    //   throttle_time_ms: 0
    //   brokers: count=1
    //     broker[0]: node_id=1, host="b1", port=9092, rack="r1", tag=0x00
    //   cluster_id: null
    //   controller_id: 1
    //   topics: count=1
    //     topic[0]: error_code=0, name=<length=4 but no bytes> ← truncated
    const truncated = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x02, // brokers: count=1
        0x00, 0x00, 0x00, 0x01, // node_id = 1
        0x03, 0x62, 0x31, // host = "b1"
        0x00, 0x00, 0x23, 0x84, // port = 9092
        0x03, 0x72, 0x31, // rack = "r1"
        0x00, // broker tag buffer
        0x00, // cluster_id = null
        0x00, 0x00, 0x00, 0x01, // controller_id = 1
        0x02, // topics: count=1
        0x00, 0x00, // topic[0] error_code = 0
        0x05, // name length = 4, but no bytes follow
    };

    var r: Reader = .init(&truncated);
    try testing.expectError(
        error.EndOfStream,
        decodeResponse(testing.allocator, &r),
    );
}

test "decode response: truncation during partition decode with brokers+topic present leaks nothing" {
    // Brokers and the topic header decode fully, then a partition's
    // replica_nodes array is truncated. Verifies that the topic's `name`
    // string and the topic's `partitions` slice are cleaned up, along with
    // the outer brokers.
    //
    //   throttle_time_ms: 0
    //   brokers: count=1
    //     broker[0]: node_id=1, host="b1", port=9092, rack=null, tag=0x00
    //   cluster_id: null
    //   controller_id: 1
    //   topics: count=1
    //     topic[0]: error_code=0, name="t", topic_id=zeros, is_internal=false
    //     partitions: count=1
    //       partition[0]: error=0, idx=0, leader=1, epoch=0,
    //         replica_nodes=count=2 but only 1 int follows ← truncated
    const truncated = [_]u8{
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x02, // brokers: count=1
        0x00, 0x00, 0x00, 0x01, // node_id = 1
        0x03, 0x62, 0x31, // host = "b1"
        0x00, 0x00, 0x23, 0x84, // port = 9092
        0x00, // rack = null
        0x00, // broker tag buffer
        0x00, // cluster_id = null
        0x00, 0x00, 0x00, 0x01, // controller_id = 1
        0x02, // topics: count=1
        0x00, 0x00, // error_code = 0
        0x02, 0x74, // name = "t"
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // topic_id = zeros
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
        0x00, // is_internal = false
        0x02, // partitions: count=1
        0x00, 0x00, // partition[0] error_code = 0
        0x00, 0x00, 0x00, 0x00, // partition_index = 0
        0x00, 0x00, 0x00, 0x01, // leader_id = 1
        0x00, 0x00, 0x00, 0x00, // leader_epoch = 0
        0x03, // replica_nodes: count=2, but only 1 i32 follows
        0x00, 0x00, 0x00, 0x01, // replica[0] = 1
        // ← truncated here: replica[1] missing
    };

    var r: Reader = .init(&truncated);
    try testing.expectError(
        error.EndOfStream,
        decodeResponse(testing.allocator, &r),
    );
}

test "TopicRequest.byName produces zero topic_id" {
    const t = TopicRequest.byName("events");
    try testing.expectEqualSlices(u8, &@as(Uuid, @splat(0)), &t.topic_id);
    try testing.expectEqualStrings("events", t.name.?);
}
