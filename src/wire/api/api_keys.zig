//! Kafka API key enum, pinned version table, and header-version resolver.
//!
//! Spec: https://kafka.apache.org/protocol.html — "The ApiKeys" table, and
//! "Request Header" / "Response Header" version notes. The header version
//! used for a given request/response depends on the API key and its version,
//! and there is one classic gotcha: the **ApiVersions response uses header
//! version 0 even at v3**, because the broker must parse the response before
//! it knows whether the client supports flexible versions. Get this wrong and
//! the connection desyncs right after connect.
//!
//! Flexible versions (KIP-482): flexible APIs use request header v2 (with a
//! trailing tagged-field buffer) and compact string/array/bytes types. The
//! response header version is NOT determined by flexibility alone — it is
//! per-API-per-version per the protocol page's per-API message entries.
//! Most flexible APIs use response header v1, but ApiVersions uses v0 even
//! at v3 (the gotcha below).
//!
//! Pinned versions target modern Kafka 3.x / AWS MSK.

const std = @import("std");
const testing = std.testing;

/// In-scope Kafka API keys. Only the APIs we implement are listed; the `_`
/// tag catches any other key at the boundary.
pub const ApiKey = enum(u16) {
    produce = 0,
    metadata = 3,
    sasl_handshake = 17,
    api_versions = 18,
    sasl_authenticate = 36,
    _,

    pub fn toInt(self: ApiKey) u16 {
        return @intFromEnum(self);
    }

    pub fn fromU16(value: u16) error{UnknownApiKey}!ApiKey {
        return switch (value) {
            0 => .produce,
            3 => .metadata,
            17 => .sasl_handshake,
            18 => .api_versions,
            36 => .sasl_authenticate,
            else => error.UnknownApiKey,
        };
    }
};

/// The request/response header version pair for a given API + version.
pub const HeaderVersion = struct {
    request: u8,
    response: u8,
};

/// Pinned version for each in-scope API. These are the versions we negotiate
/// and implement; see PLAN §1.
pub const pinned_version: std.EnumMap(ApiKey, u16) = .init(.{
    .produce = 9,
    .metadata = 12,
    .sasl_handshake = 1,
    .api_versions = 3,
    .sasl_authenticate = 1,
});

/// Whether a given API + version uses flexible (KIP-482) encoding: compact
/// strings/arrays, unsigned-varint lengths, header v2, tagged-field buffers.
/// Produce v9, Metadata v12, and ApiVersions v3 are flexible. SaslHandshake
/// v1 and SaslAuthenticate v1 are NOT.
pub fn isFlexible(api: ApiKey, version: u16) bool {
    return switch (api) {
        .produce => version >= 9,
        .metadata => version >= 12,
        .api_versions => version >= 3,
        .sasl_handshake => false,
        .sasl_authenticate => false,
        _ => false,
    };
}

/// Resolve the request and response header versions for a given API + version.
///
/// This is an explicit per-API-per-version table, NOT derived from the
/// `isFlexible` flag. The response header version is per-API-per-version per
/// the protocol page's per-API message entries (the "Header Version" columns
/// in each API's version table). Spec:
/// https://kafka.apache.org/43/design/protocol#request-and-response-headers
/// — "The Header version for a given request is version-specific and can
/// be found in the detailed request definitions."
///
/// Gotcha: the **ApiVersions response uses header v0 even at v3**. The broker
/// parses the ApiVersions response before it knows the client's flexibility,
/// so the response must use the legacy v0 header (no tagged fields). The
/// ApiVersions request at v3 uses v2 (flexible request header) because the
/// client sends it and the broker knows how to parse flexible requests by the
/// time ApiVersions v3 is on the wire.
///
/// Response headers only have versions v0 and v1. Flexible responses use
/// response header v1 (with a trailing tagged-field buffer), NOT v2. The
/// request header goes up to v2 (flexible request header with client_id and
/// tagged fields).
pub fn headerVersion(api: ApiKey, version: u16) HeaderVersion {
    return switch (api) {
        .produce => switch (version) {
            9 => .{ .request = 2, .response = 1 },
            else => .{ .request = 1, .response = 0 },
        },
        .metadata => switch (version) {
            12 => .{ .request = 2, .response = 1 },
            else => .{ .request = 1, .response = 0 },
        },
        .api_versions => switch (version) {
            0 => .{ .request = 1, .response = 0 },
            3 => .{ .request = 2, .response = 0 },
            else => .{ .request = 1, .response = 0 },
        },
        .sasl_handshake => .{ .request = 1, .response = 0 },
        .sasl_authenticate => .{ .request = 1, .response = 0 },
        _ => .{ .request = 1, .response = 0 },
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "ApiKey.toInt and fromU16 round-trip" {
    try testing.expectEqual(@as(u16, 0), ApiKey.produce.toInt());
    try testing.expectEqual(ApiKey.produce, try ApiKey.fromU16(0));
    try testing.expectEqual(ApiKey.metadata, try ApiKey.fromU16(3));
    try testing.expectEqual(ApiKey.sasl_handshake, try ApiKey.fromU16(17));
    try testing.expectEqual(ApiKey.api_versions, try ApiKey.fromU16(18));
    try testing.expectEqual(ApiKey.sasl_authenticate, try ApiKey.fromU16(36));
}

test "ApiKey.fromU16 rejects unknown keys" {
    try testing.expectError(error.UnknownApiKey, ApiKey.fromU16(1));
    try testing.expectError(error.UnknownApiKey, ApiKey.fromU16(999));
}

test "sasl_authenticate is key 36, not 366" {
    try testing.expectEqual(@as(u16, 36), ApiKey.sasl_authenticate.toInt());
    try testing.expectError(error.UnknownApiKey, ApiKey.fromU16(366));
}

test "pinned versions" {
    try testing.expectEqual(@as(u16, 9), pinned_version.get(.produce).?);
    try testing.expectEqual(@as(u16, 12), pinned_version.get(.metadata).?);
    try testing.expectEqual(@as(u16, 1), pinned_version.get(.sasl_handshake).?);
    try testing.expectEqual(@as(u16, 3), pinned_version.get(.api_versions).?);
    try testing.expectEqual(@as(u16, 1), pinned_version.get(.sasl_authenticate).?);
}

test "isFlexible" {
    try testing.expect(isFlexible(.produce, 9));
    try testing.expect(isFlexible(.metadata, 12));
    try testing.expect(isFlexible(.api_versions, 3));
    try testing.expect(!isFlexible(.sasl_handshake, 1));
    try testing.expect(!isFlexible(.sasl_authenticate, 1));
}

test "headerVersion: metadata v12 → {2, 1}" {
    // Spec: https://kafka.apache.org/43/design/protocol#request-and-response-headers
    // — flexible responses use response header v1 (with tagged fields), NOT v2.
    const hv = headerVersion(.metadata, 12);
    try testing.expectEqual(@as(u8, 2), hv.request);
    try testing.expectEqual(@as(u8, 1), hv.response);
}

test "headerVersion: sasl_handshake v1 → {1, 0}" {
    const hv = headerVersion(.sasl_handshake, 1);
    try testing.expectEqual(@as(u8, 1), hv.request);
    try testing.expectEqual(@as(u8, 0), hv.response);
}

test "headerVersion: sasl_authenticate v1 → {1, 0}" {
    const hv = headerVersion(.sasl_authenticate, 1);
    try testing.expectEqual(@as(u8, 1), hv.request);
    try testing.expectEqual(@as(u8, 0), hv.response);
}

test "headerVersion: api_versions v3 → {2, 0} (the gotcha)" {
    // The ApiVersions response uses header v0 even at v3. The broker parses
    // it before knowing the client's flexibility. See:
    // https://kafka.apache.org/43/design/protocol#request-and-response-headers
    // — "ApiVersions response always uses header version 0."
    const hv = headerVersion(.api_versions, 3);
    try testing.expectEqual(@as(u8, 2), hv.request);
    try testing.expectEqual(@as(u8, 0), hv.response);
}

test "headerVersion: api_versions v0 (pre-auth) → {1, 0}" {
    // Pre-auth ApiVersions v0: non-flexible request header v1, response v0.
    const hv = headerVersion(.api_versions, 0);
    try testing.expectEqual(@as(u8, 1), hv.request);
    try testing.expectEqual(@as(u8, 0), hv.response);
}

test "headerVersion: produce v9 → {2, 1}" {
    // Spec: https://kafka.apache.org/43/design/protocol#request-and-response-headers
    // — flexible responses use response header v1 (with tagged fields), NOT v2.
    const hv = headerVersion(.produce, 9);
    try testing.expectEqual(@as(u8, 2), hv.request);
    try testing.expectEqual(@as(u8, 1), hv.response);
}
