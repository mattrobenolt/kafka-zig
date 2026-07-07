//! ApiVersions API (key 18, v3) — request and response codec.
//!
//! Spec: https://kafka.apache.org/43/design/protocol#the_messages_api_versions
//! — "ApiVersions API (Key: 18)".
//!
//! v3 is FLEXIBLE (KIP-482): compact strings/arrays, unsigned-varint lengths,
//! request header v2. But the **response uses header v0** (the classic gotcha
//! — the broker parses the response before knowing the client's flexibility).
//! The api_keys.headerVersion table encodes this as {request 2, response 0}.
//!
//! **v3 tagged fields:** The spec marks `supported_features`, `finalized_features_epoch`,
//! `finalized_features`, and `zk_migration_ready` with `<tag: 0>` through
//! `<tag: 3>`, meaning they are encoded as tagged fields in the response's
//! trailing TAG_BUFFER — NOT inline. The inline response body is just
//! `error_code`, `api_keys`, `throttle_time_ms`; the v3 feature data lives in
//! the tag buffer. A response from a broker that doesn't support these features
//! has an empty tag buffer (0x00).
//!
//! There is NO `kvp_supported` field in v3 (or v4). The task guide was wrong
//! about this.
//!
//! This module encodes/decodes the **body only** — the bytes after the i32
//! length prefix and the request/response header. The fixture tests assemble
//! header+body inline to verify the boundary.
//!
//! Allocator policy: the request encode path is zero-alloc (writes to a
//! caller-provided `*std.Io.Writer`). The response decode path takes an
//! `std.mem.Allocator` for the nested arrays (api_keys, supported_features,
//! finalized_features) and duplicated strings. The response encode path is
//! zero-alloc (computes tagged-field data sizes without allocating). Call
//! `deinit` on the decoded response to free owned data.

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const primitives = @import("../primitives.zig");
const Reader = primitives.Reader;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// An entry in the `api_keys` array of an ApiVersions response.
pub const ApiKeyEntry = struct {
    api_key: i16,
    min_version: i16,
    max_version: i16,
};

/// A supported feature in the v3 response's tag buffer (tag 0).
pub const SupportedFeature = struct {
    name: []const u8,
    min_version: i16,
    max_version: i16,
};

/// A finalized feature in the v3 response's tag buffer (tag 2).
pub const FinalizedFeature = struct {
    name: []const u8,
    max_version_level: i16,
    min_version_level: i16,
};

/// ApiVersions v3 request body.
///
/// Spec field order (v3):
///   client_software_name: COMPACT_STRING
///   client_software_version: COMPACT_STRING
///   TAG_BUFFER
pub const Request = struct {
    client_software_name: []const u8,
    client_software_version: []const u8,
};

/// ApiVersions v3 response body.
///
/// Spec field order (v3):
///   error_code: INT16
///   api_keys: nullable COMPACT_ARRAY of { api_key: INT16, min_version: INT16, max_version: INT16, TAG_BUFFER }
///   throttle_time_ms: INT32
///   TAG_BUFFER — contains v3 tagged fields:
///     tag 0: supported_features (COMPACT_ARRAY of { name: COMPACT_STRING, min_version: INT16, max_version: INT16, TAG_BUFFER })
///     tag 1: finalized_features_epoch (INT64)
///     tag 2: finalized_features (COMPACT_ARRAY of { name: COMPACT_STRING, max_version_level: INT16, min_version_level: INT16, TAG_BUFFER })
///     tag 3: zk_migration_ready (BOOLEAN)
///
/// The v3 tagged fields are null when absent from the tag buffer (a broker
/// that doesn't support features sends an empty tag buffer).
pub const Response = struct {
    error_code: i16,
    api_keys: []ApiKeyEntry,
    throttle_time_ms: i32,
    // v3 tagged fields — null when the tag is absent from the tag buffer.
    supported_features: ?[]SupportedFeature,
    finalized_features_epoch: ?i64,
    finalized_features: ?[]FinalizedFeature,
    zk_migration_ready: ?bool,

    pub fn deinit(self: *Response, allocator: std.mem.Allocator) void {
        allocator.free(self.api_keys);
        if (self.supported_features) |sf| {
            for (sf) |f| allocator.free(f.name);
            allocator.free(sf);
        }
        if (self.finalized_features) |ff| {
            for (ff) |f| allocator.free(f.name);
            allocator.free(ff);
        }
    }
};

// ---------------------------------------------------------------------------
// Encode (request body — zero-alloc)
// ---------------------------------------------------------------------------

/// Encode an ApiVersions v3 request body to `writer`.
///
/// Spec: https://kafka.apache.org/43/design/protocol#the_messages_api_versions
/// — "ApiVersions Request (Version: 3)".
pub fn encodeRequest(writer: *std.Io.Writer, req: Request) std.Io.Writer.Error!void {
    try primitives.writeCompactString(writer, req.client_software_name);
    try primitives.writeCompactString(writer, req.client_software_version);
    try primitives.writeEmptyTagBuffer(writer);
}

// ---------------------------------------------------------------------------
// Decode (response body — allocates nested arrays, cold-path)
// ---------------------------------------------------------------------------

/// Decode an ApiVersions v3 response body from `reader`.
///
/// Spec: https://kafka.apache.org/43/design/protocol#the_messages_api_versions
/// — "ApiVersions Response (Version: 3)".
///
/// The returned `Response` owns all nested data (api_keys, supported_features,
/// finalized_features, duplicated strings). Call `deinit` to free.
pub fn decodeResponse(allocator: std.mem.Allocator, reader: *Reader) !Response {
    const error_code = try primitives.readI16(reader);

    // api_keys: nullable compact array. Each element has a trailing TAG_BUFFER.
    const api_keys_count = try primitives.readCompactArrayCount(reader);
    const n_api_keys = api_keys_count orelse return error.Malformed;
    const api_keys = try allocator.alloc(ApiKeyEntry, n_api_keys);
    var api_keys_len: usize = 0;
    errdefer allocator.free(api_keys);
    while (api_keys_len < n_api_keys) : (api_keys_len += 1) {
        api_keys[api_keys_len] = .{
            .api_key = try primitives.readI16(reader),
            .min_version = try primitives.readI16(reader),
            .max_version = try primitives.readI16(reader),
        };
        try primitives.readTagBuffer(reader);
    }

    const throttle_time_ms = try primitives.readI32(reader);

    // Read the trailing tag buffer and extract v3 tagged fields. At most 4
    // tagged fields (tags 0-3), so a 4-element stack array suffices.
    var tagged_buf: [4]primitives.TaggedField = undefined;
    const n_tagged = try primitives.readTaggedFields(reader, &tagged_buf);
    const tagged = tagged_buf[0..n_tagged];

    // Parse tagged fields. Each tag's data is a borrowed slice into the
    // reader's buffer; we parse it with a sub-Reader and copy owned data.
    var supported_features: ?[]SupportedFeature = null;
    errdefer if (supported_features) |sf| {
        for (sf) |f| allocator.free(f.name);
        allocator.free(sf);
    };

    var finalized_features: ?[]FinalizedFeature = null;
    errdefer if (finalized_features) |ff| {
        for (ff) |f| allocator.free(f.name);
        allocator.free(ff);
    };

    var finalized_features_epoch: ?i64 = null;
    var zk_migration_ready: ?bool = null;

    for (tagged) |tf| {
        var sub: Reader = .init(tf.data);
        switch (tf.tag) {
            0 => supported_features = try decodeSupportedFeatures(allocator, &sub),
            1 => finalized_features_epoch = try primitives.readI64(&sub),
            2 => finalized_features = try decodeFinalizedFeatures(allocator, &sub),
            3 => zk_migration_ready = try primitives.readBool(&sub),
            else => {}, // Skip unknown tagged fields.
        }
    }

    return .{
        .error_code = error_code,
        .api_keys = api_keys,
        .throttle_time_ms = throttle_time_ms,
        .supported_features = supported_features,
        .finalized_features_epoch = finalized_features_epoch,
        .finalized_features = finalized_features,
        .zk_migration_ready = zk_migration_ready,
    };
}

fn decodeSupportedFeatures(allocator: std.mem.Allocator, reader: *Reader) ![]SupportedFeature {
    const count = try primitives.readCompactArrayCount(reader);
    const n = count orelse return error.Malformed;
    const arr = try allocator.alloc(SupportedFeature, n);
    var len: usize = 0;
    errdefer {
        for (arr[0..len]) |*f| allocator.free(f.name);
        allocator.free(arr);
    }
    while (len < n) : (len += 1) {
        const name_raw = try primitives.readCompactString(reader);
        const name = try allocator.dupe(u8, name_raw);
        errdefer allocator.free(name);
        arr[len] = .{
            .name = name,
            .min_version = try primitives.readI16(reader),
            .max_version = try primitives.readI16(reader),
        };
        try primitives.readTagBuffer(reader);
    }
    return arr;
}

fn decodeFinalizedFeatures(allocator: std.mem.Allocator, reader: *Reader) ![]FinalizedFeature {
    const count = try primitives.readCompactArrayCount(reader);
    const n = count orelse return error.Malformed;
    const arr = try allocator.alloc(FinalizedFeature, n);
    var len: usize = 0;
    errdefer {
        for (arr[0..len]) |*f| allocator.free(f.name);
        allocator.free(arr);
    }
    while (len < n) : (len += 1) {
        const name_raw = try primitives.readCompactString(reader);
        const name = try allocator.dupe(u8, name_raw);
        errdefer allocator.free(name);
        arr[len] = .{
            .name = name,
            .max_version_level = try primitives.readI16(reader),
            .min_version_level = try primitives.readI16(reader),
        };
        try primitives.readTagBuffer(reader);
    }
    return arr;
}

// ---------------------------------------------------------------------------
// Encode (response body — zero-alloc, computes tagged-field sizes)
// ---------------------------------------------------------------------------

/// Encode an ApiVersions v3 response body to `writer`.
///
/// This is the inverse of `decodeResponse`, used for round-trip tests. It
/// serializes the v3 tagged fields (supported_features, finalized_features_epoch,
/// finalized_features, zk_migration_ready) into the trailing TAG_BUFFER.
/// Zero-alloc: tagged-field data sizes are computed without allocating, then
/// the data is written directly to the writer.
pub fn encodeResponse(writer: *std.Io.Writer, resp: Response) std.Io.Writer.Error!void {
    try primitives.writeI16(writer, resp.error_code);

    // api_keys: nullable compact array.
    try primitives.writeCompactArrayCount(writer, resp.api_keys.len);
    for (resp.api_keys) |k| {
        try primitives.writeI16(writer, k.api_key);
        try primitives.writeI16(writer, k.min_version);
        try primitives.writeI16(writer, k.max_version);
        try primitives.writeEmptyTagBuffer(writer);
    }

    try primitives.writeI32(writer, resp.throttle_time_ms);

    // Build the trailing TAG_BUFFER with present v3 tagged fields.
    // Count the number of present tagged fields.
    var n_tags: usize = 0;
    if (resp.supported_features != null) n_tags += 1;
    if (resp.finalized_features_epoch != null) n_tags += 1;
    if (resp.finalized_features != null) n_tags += 1;
    if (resp.zk_migration_ready != null) n_tags += 1;

    try primitives.writeUvarint(writer, n_tags);

    // Tag 0: supported_features.
    if (resp.supported_features) |sf| {
        try primitives.writeUvarint(writer, 0);
        try primitives.writeUvarint(writer, supportedFeaturesDataSize(sf));
        try writeSupportedFeaturesData(writer, sf);
    }

    // Tag 1: finalized_features_epoch.
    if (resp.finalized_features_epoch) |epoch| {
        try primitives.writeUvarint(writer, 1);
        try primitives.writeUvarint(writer, 8);
        try primitives.writeI64(writer, epoch);
    }

    // Tag 2: finalized_features.
    if (resp.finalized_features) |ff| {
        try primitives.writeUvarint(writer, 2);
        try primitives.writeUvarint(writer, finalizedFeaturesDataSize(ff));
        try writeFinalizedFeaturesData(writer, ff);
    }

    // Tag 3: zk_migration_ready.
    if (resp.zk_migration_ready) |zmr| {
        try primitives.writeUvarint(writer, 3);
        try primitives.writeUvarint(writer, 1);
        try primitives.writeBool(writer, zmr);
    }
}

/// Compute the number of bytes a uvarint will occupy.
fn uvarintSize(value: u64) usize {
    var v = value;
    var size: usize = 1;
    while (v >= 0x80) : (v >>= 7) size += 1;
    return size;
}

/// Compute the serialized size of a compact string (uvarint(len+1) + bytes).
fn compactStringSize(s: []const u8) usize {
    return uvarintSize(@as(u64, s.len) + 1) + s.len;
}

/// Compute the serialized byte size of the supported_features tagged field data
/// (a compact array of { name, min_version, max_version, TAG_BUFFER }).
fn supportedFeaturesDataSize(features: []const SupportedFeature) usize {
    var size: usize = uvarintSize(@as(u64, features.len) + 1);
    for (features) |f| {
        size += compactStringSize(f.name);
        size += 2 + 2 + 1; // min_version + max_version + empty tag buffer
    }
    return size;
}

/// Compute the serialized byte size of the finalized_features tagged field data
/// (a compact array of { name, max_version_level, min_version_level, TAG_BUFFER }).
fn finalizedFeaturesDataSize(features: []const FinalizedFeature) usize {
    var size: usize = uvarintSize(@as(u64, features.len) + 1);
    for (features) |f| {
        size += compactStringSize(f.name);
        size += 2 + 2 + 1; // max_version_level + min_version_level + empty tag buffer
    }
    return size;
}

/// Write the supported_features tagged field data (compact array).
fn writeSupportedFeaturesData(writer: *std.Io.Writer, features: []const SupportedFeature) std.Io.Writer.Error!void {
    try primitives.writeCompactArrayCount(writer, features.len);
    for (features) |f| {
        try primitives.writeCompactString(writer, f.name);
        try primitives.writeI16(writer, f.min_version);
        try primitives.writeI16(writer, f.max_version);
        try primitives.writeEmptyTagBuffer(writer);
    }
}

/// Write the finalized_features tagged field data (compact array).
fn writeFinalizedFeaturesData(writer: *std.Io.Writer, features: []const FinalizedFeature) std.Io.Writer.Error!void {
    try primitives.writeCompactArrayCount(writer, features.len);
    for (features) |f| {
        try primitives.writeCompactString(writer, f.name);
        try primitives.writeI16(writer, f.max_version_level);
        try primitives.writeI16(writer, f.min_version_level);
        try primitives.writeEmptyTagBuffer(writer);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "encode request v3: client_software_name and version" {
    // Hand-constructed from the v3 spec. Field-by-field breakdown:
    //
    //   client_software_name: COMPACT_STRING "kafka-zig" → 0x0a 6b 61 66 6b 61 2d 7a 69 67
    //   client_software_version: COMPACT_STRING "0.1.0" → 0x06 30 2e 31 2e 30
    //   TAG_BUFFER: 0x00
    const expected = [_]u8{
        0x0a, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x7a, 0x69, 0x67, // "kafka-zig"
        0x06, 0x30, 0x2e, 0x31, 0x2e, 0x30, // "0.1.0"
        0x00, // tag buffer
    };

    var buf: [32]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .client_software_name = "kafka-zig",
        .client_software_version = "0.1.0",
    });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "encode request v3: empty strings" {
    //   client_software_name: COMPACT_STRING "" → 0x01
    //   client_software_version: COMPACT_STRING "" → 0x01
    //   TAG_BUFFER: 0x00
    const expected = [_]u8{ 0x01, 0x01, 0x00 };

    var buf: [8]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try encodeRequest(&w, .{
        .client_software_name = "",
        .client_software_version = "",
    });
    try testing.expectEqualSlices(u8, &expected, w.buffered());
}

test "decode response v3: two api_keys, no v3 features (empty tag buffer)" {
    // Hand-constructed from the v3 spec. Field-by-field breakdown:
    //
    //   error_code: INT16 = 0 → 00 00
    //   api_keys: compact array count=2 → uvarint(2+1) = 0x03
    //     api_key[0]: Produce key=0, min=0, max=9, TAG_BUFFER=0x00
    //     api_key[1]: Metadata key=3, min=0, max=12, TAG_BUFFER=0x00
    //   throttle_time_ms: INT32 = 0 → 00 00 00 00
    //   TAG_BUFFER: 0x00 (no v3 features)
    const fixture = [_]u8{
        0x00, 0x00, // error_code = 0
        0x03, // api_keys: compact array, count=2
        0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, // Produce: key=0, min=0, max=9, tag=0x00
        0x00, 0x03, 0x00, 0x00, 0x00, 0x0c, 0x00, // Metadata: key=3, min=0, max=12, tag=0x00
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer (empty — no v3 features)
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expectEqual(@as(usize, 2), resp.api_keys.len);
    try testing.expectEqual(@as(i16, 0), resp.api_keys[0].api_key);
    try testing.expectEqual(@as(i16, 0), resp.api_keys[0].min_version);
    try testing.expectEqual(@as(i16, 9), resp.api_keys[0].max_version);
    try testing.expectEqual(@as(i16, 3), resp.api_keys[1].api_key);
    try testing.expectEqual(@as(i16, 0), resp.api_keys[1].min_version);
    try testing.expectEqual(@as(i16, 12), resp.api_keys[1].max_version);
    try testing.expectEqual(@as(i32, 0), resp.throttle_time_ms);
    // No v3 features present.
    try testing.expectEqual(@as(?[]SupportedFeature, null), resp.supported_features);
    try testing.expectEqual(@as(?i64, null), resp.finalized_features_epoch);
    try testing.expectEqual(@as(?[]FinalizedFeature, null), resp.finalized_features);
    try testing.expectEqual(@as(?bool, null), resp.zk_migration_ready);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "round-trip: decode then re-encode response with empty tag buffer" {
    const fixture = [_]u8{
        0x00, 0x00, // error_code = 0
        0x03, // api_keys: compact array, count=2
        0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, // Produce: key=0, min=0, max=9, tag=0x00
        0x00, 0x03, 0x00, 0x00, 0x00, 0x0c, 0x00, // Metadata: key=3, min=0, max=12, tag=0x00
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer (empty)
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

test "decode response v3: with v3 features in tag buffer" {
    // Hand-constructed from the v3 spec. The tag buffer contains tags 0-3.
    //
    //   error_code: INT16 = 0
    //   api_keys: compact array count=1 → 0x02
    //     api_key[0]: key=18 (ApiVersions), min=0, max=3, TAG_BUFFER=0x00
    //   throttle_time_ms: INT32 = 0
    //   TAG_BUFFER: count=4 → 0x04
    //     tag 0 (supported_features):
    //       tag=0, data_size=..., data = compact array count=1
    //         { name="kraft.version" COMPACT_STRING, min=0, max=1, TAG_BUFFER=0x00 }
    //     tag 1 (finalized_features_epoch):
    //       tag=1, data_size=8, data = INT64 = 42
    //     tag 2 (finalized_features):
    //       tag=2, data_size=..., data = compact array count=1
    //         { name="kraft.version" COMPACT_STRING, max=1, min=0, TAG_BUFFER=0x00 }
    //     tag 3 (zk_migration_ready):
    //       tag=3, data_size=1, data = BOOLEAN true → 0x01
    //
    // Let me build this byte by byte.
    //
    // supported_features data:
    //   compact array count=1 → 0x02
    //   name: COMPACT_STRING "kraft.version" (13 chars) → 0x0e 6b 72 61 66 74 2e 76 65 72 73 69 6f 6e
    //   min_version: INT16 = 0 → 00 00
    //   max_version: INT16 = 1 → 00 01
    //   TAG_BUFFER: 0x00
    // Total: 1 + 1+13 + 2 + 2 + 1 = 20 bytes
    //
    // finalized_features data:
    //   compact array count=1 → 0x02
    //   name: COMPACT_STRING "kraft.version" → 0x0e 6b 72 61 66 74 2e 76 65 72 73 69 6f 6e
    //   max_version_level: INT16 = 1 → 00 01
    //   min_version_level: INT16 = 0 → 00 00
    //   TAG_BUFFER: 0x00
    // Total: 1 + 1+13 + 2 + 2 + 1 = 20 bytes

    const supported_features_data = [_]u8{
        0x02, // compact array count=1
        0x0e, 0x6b, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, // "kraft.version"
        0x00, 0x00, // min_version = 0
        0x00, 0x01, // max_version = 1
        0x00, // tag buffer
    };
    // 20 bytes

    const finalized_features_data = [_]u8{
        0x02, // compact array count=1
        0x0e, 0x6b, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, // "kraft.version"
        0x00, 0x01, // max_version_level = 1
        0x00, 0x00, // min_version_level = 0
        0x00, // tag buffer
    };
    // 20 bytes

    const fixture = [_]u8{
        0x00, 0x00, // error_code = 0
        0x02, // api_keys: compact array, count=1
        0x00, 0x12, 0x00, 0x00, 0x00, 0x03, 0x00, // key=18, min=0, max=3, tag=0x00
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        // TAG_BUFFER: count=4
        0x04,
        // tag 0: supported_features
        0x00, // tag id = 0
        0x14, // data size = 20
    } ++ supported_features_data ++ [_]u8{
        // tag 1: finalized_features_epoch
        0x01, // tag id = 1
        0x08, // data size = 8
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // INT64 = 42
        // tag 2: finalized_features
        0x02, // tag id = 2
        0x14, // data size = 20
    } ++ finalized_features_data ++ [_]u8{
        // tag 3: zk_migration_ready
        0x03, // tag id = 3
        0x01, // data size = 1
        0x01, // BOOLEAN true
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expectEqual(@as(usize, 1), resp.api_keys.len);
    try testing.expectEqual(@as(i16, 18), resp.api_keys[0].api_key);
    try testing.expectEqual(@as(i16, 0), resp.api_keys[0].min_version);
    try testing.expectEqual(@as(i16, 3), resp.api_keys[0].max_version);

    // v3 features.
    const sf = resp.supported_features.?;
    try testing.expectEqual(@as(usize, 1), sf.len);
    try testing.expectEqualStrings("kraft.version", sf[0].name);
    try testing.expectEqual(@as(i16, 0), sf[0].min_version);
    try testing.expectEqual(@as(i16, 1), sf[0].max_version);

    try testing.expectEqual(@as(i64, 42), resp.finalized_features_epoch.?);

    const ff = resp.finalized_features.?;
    try testing.expectEqual(@as(usize, 1), ff.len);
    try testing.expectEqualStrings("kraft.version", ff[0].name);
    try testing.expectEqual(@as(i16, 1), ff[0].max_version_level);
    try testing.expectEqual(@as(i16, 0), ff[0].min_version_level);

    try testing.expectEqual(true, resp.zk_migration_ready.?);

    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "round-trip: decode then re-encode response with v3 features" {
    // Same fixture as above.
    const supported_features_data = [_]u8{
        0x02,
        0x0e,
        0x6b,
        0x72,
        0x61,
        0x66,
        0x74,
        0x2e,
        0x76,
        0x65,
        0x72,
        0x73,
        0x69,
        0x6f,
        0x6e,
        0x00,
        0x00,
        0x00,
        0x01,
        0x00,
    };
    const finalized_features_data = [_]u8{
        0x02,
        0x0e,
        0x6b,
        0x72,
        0x61,
        0x66,
        0x74,
        0x2e,
        0x76,
        0x65,
        0x72,
        0x73,
        0x69,
        0x6f,
        0x6e,
        0x00,
        0x01,
        0x00,
        0x00,
        0x00,
    };
    const fixture = [_]u8{
        0x00, 0x00,
        0x02, 0x00,
        0x12, 0x00,
        0x00, 0x00,
        0x03, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x04, 0x00,
        0x14,
    } ++ supported_features_data ++ [_]u8{
        0x01, 0x08,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x2a,
        0x02, 0x14,
    } ++ finalized_features_data ++ [_]u8{
        0x03, 0x01, 0x01,
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

test "decode response v3: error_code nonzero" {
    // error_code=35 (UNSUPPORTED_VERSION), empty api_keys, throttle=0, empty tag buffer.
    const fixture = [_]u8{
        0x00, 0x23, // error_code = 35
        0x01, // api_keys: compact array, count=0
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer (empty)
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 35), resp.error_code);
    try testing.expectEqual(@as(usize, 0), resp.api_keys.len);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "round-trip: decode then re-encode response with error_code and empty api_keys" {
    const fixture = [_]u8{
        0x00, 0x23, // error_code = 35
        0x01, // api_keys: compact array, count=0
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer (empty)
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

test "decode response v3: partial v3 features (only zk_migration_ready)" {
    // A response where only tag 3 (zk_migration_ready) is present.
    //   error_code=0, api_keys=[1 entry], throttle=0
    //   TAG_BUFFER: count=1, tag=3, data_size=1, data=0x01 (true)
    const fixture = [_]u8{
        0x00, 0x00, // error_code = 0
        0x02, // api_keys: compact array, count=1
        0x00, 0x12, 0x00, 0x00, 0x00, 0x03, 0x00, // key=18, min=0, max=3, tag=0x00
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x01, // tag buffer: count=1
        0x03, // tag id = 3
        0x01, // data size = 1
        0x01, // BOOLEAN true
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(?[]SupportedFeature, null), resp.supported_features);
    try testing.expectEqual(@as(?i64, null), resp.finalized_features_epoch);
    try testing.expectEqual(@as(?[]FinalizedFeature, null), resp.finalized_features);
    try testing.expectEqual(true, resp.zk_migration_ready.?);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "round-trip: decode then re-encode with only zk_migration_ready" {
    const fixture = [_]u8{
        0x00, 0x00,
        0x02, 0x00,
        0x12, 0x00,
        0x00, 0x00,
        0x03, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x01, 0x03,
        0x01, 0x01,
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

test "decode response with full frame: length prefix + response header v0" {
    // ApiVersions response uses header v0 even at v3 (the gotcha). Header v0
    // is just correlation_id: INT32 — no tag buffer.
    //
    // Full frame: i32 length prefix + response header v0 (correlation_id i32)
    // + body. This verifies the boundary between header and body.
    const body = [_]u8{
        0x00, 0x00, // error_code = 0
        0x03, // api_keys: compact array, count=2
        0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, // Produce: key=0, min=0, max=9, tag=0x00
        0x00, 0x03, 0x00, 0x00, 0x00, 0x0c, 0x00, // Metadata: key=3, min=0, max=12, tag=0x00
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x00, // trailing tag buffer (empty)
    };

    // Response header v0: correlation_id (i32) only, no tag buffer.
    const header = [_]u8{
        0x00, 0x00, 0x7b, 0x00, // correlation_id = 31488
    };

    // Full frame: i32 length prefix (header + body) + header + body.
    const frame_len: u32 = @intCast(header.len + body.len);
    var frame: [128]u8 = undefined;
    var pos: usize = 0;
    std.mem.writeInt(u32, frame[pos..][0..4], frame_len, .big);
    pos += 4;
    @memcpy(frame[pos..][0..header.len], &header);
    pos += header.len;
    @memcpy(frame[pos..][0..body.len], &body);
    pos += body.len;

    // Parse the length prefix.
    var len_reader: Reader = .init(frame[0..pos]);
    const body_len = try primitives.readI32(&len_reader);
    try testing.expectEqual(@as(i32, @intCast(header.len + body.len)), body_len);

    const payload = len_reader.remaining();

    // Skip the response header v0: correlation_id (i32) only — NO tag buffer.
    var hr: Reader = .init(payload);
    const correlation_id = try primitives.readI32(&hr);
    try testing.expectEqual(@as(i32, 0x7b00), correlation_id);

    // The rest is the body — decode it.
    var body_reader: Reader = .init(hr.remaining());
    var resp = try decodeResponse(testing.allocator, &body_reader);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 2), resp.api_keys.len);
    try testing.expectEqual(@as(i16, 0), resp.api_keys[0].api_key);
    try testing.expectEqual(@as(i16, 3), resp.api_keys[1].api_key);
    try testing.expectEqual(@as(usize, 0), body_reader.remaining().len);
}

test "decode response: truncated mid-api-keys returns EndOfStream" {
    // A response cut off mid-api_keys array.
    const truncated = [_]u8{
        0x00, 0x00, // error_code = 0
        0x03, // api_keys: compact array, count=2
        0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, // api_key[0] complete
        0x00, 0x03, 0x00, 0x00, // api_key[1] truncated (missing max_version + tag)
    };

    var r: Reader = .init(&truncated);
    try testing.expectError(error.EndOfStream, decodeResponse(testing.allocator, &r));
}

test "decode response: truncated mid-supported-features leaks nothing" {
    // A response with supported_features in the tag buffer, but the
    // supported_features data is truncated mid-element. The errdefer must
    // free the api_keys array and the partially-allocated supported_features.
    //
    //   error_code=0, api_keys=[1 entry], throttle=0
    //   TAG_BUFFER: count=1, tag=0 (supported_features)
    //     data: compact array count=1, name="ab" (0x03 0x61 0x62), then truncated
    //     (data claims 5 bytes but only 4 are present — reader hits EndOfStream)
    const fixture = [_]u8{
        0x00, 0x00, // error_code = 0
        0x02, // api_keys: compact array, count=1
        0x00, 0x12, 0x00, 0x00, 0x00, 0x03, 0x00, // key=18, min=0, max=3, tag=0x00
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x01, // tag buffer: count=1
        0x00, // tag id = 0
        0x05, // data size = 5 (claims 5 bytes)
        0x02, // compact array count=1
        0x03, 0x61, 0x62, // name = "ab"
        0x00, // min_version high byte only — 4 bytes present, 1 short
    };

    var r: Reader = .init(&fixture);
    try testing.expectError(
        error.EndOfStream,
        decodeResponse(testing.allocator, &r),
    );
    // std.testing.allocator checks for leaks on test exit.
}

test "decode response: truncated mid-finalized-features with supported present leaks nothing" {
    // supported_features decodes fully, then finalized_features is truncated.
    // The errdefer must free both supported_features and finalized_features
    // (the partial one) plus api_keys.
    //
    // supported_features data (9 bytes):
    //   0x02, 0x03 0x61 0x62, 0x00 0x00, 0x00 0x01, 0x00
    //   = count=1, name="ab", min=0, max=1, tag=0x00
    // finalized_features data claims 4 bytes but only has 3:
    //   0x02, 0x03 0x61 0x62 = count=1, name="ab", then truncated
    //   Actually 4 bytes present, but missing max_version_level (i16) — 2 bytes short
    const fixture = [_]u8{
        0x00, 0x00, // error_code = 0
        0x02, // api_keys: count=1
        0x00, 0x12, 0x00, 0x00, 0x00, 0x03, 0x00, // key=18, min=0, max=3, tag=0x00
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x02, // tag buffer: count=2
        0x00, 0x09, // tag 0, size=9
        0x02, 0x03, 0x61, 0x62, 0x00, 0x00, 0x00, 0x01, 0x00, // supported_features data (9 bytes)
        0x02, 0x04, // tag 2, size=4
        0x02, 0x03, 0x61, 0x62, // finalized_features data (4 bytes, truncated)
    };

    var r: Reader = .init(&fixture);
    try testing.expectError(
        error.EndOfStream,
        decodeResponse(testing.allocator, &r),
    );
}

test "decode response: finalized_features_epoch = -1 (unknown epoch)" {
    // The spec says -1 is a special value representing unknown epoch.
    //   error_code=0, api_keys=[], throttle=0
    //   TAG_BUFFER: count=1, tag=1 (finalized_features_epoch), data=INT64=-1
    const fixture = [_]u8{
        0x00, 0x00, // error_code = 0
        0x01, // api_keys: compact array, count=0
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x01, // tag buffer: count=1
        0x01, // tag id = 1
        0x08, // data size = 8
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // INT64 = -1
    };

    var r: Reader = .init(&fixture);
    var resp = try decodeResponse(testing.allocator, &r);
    defer resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, -1), resp.finalized_features_epoch.?);
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}
