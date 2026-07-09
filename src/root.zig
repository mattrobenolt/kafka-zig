//! kafka-zig — native Zig Kafka producer. Pre-alpha.
const std = @import("std");

pub const version = "0.0.0";

pub const wire = @import("wire/root.zig");
pub const Ring = @import("Ring.zig");
pub const Connection = @import("Connection.zig");
pub const Client = @import("Client.zig");
pub const Stats = Client.Stats;
pub const ErrorCounts = Client.ErrorCounts;

/// In-process mock broker, exposed for the benchmark harness (issue #9).
/// Test-only infrastructure; not intended for production use.
pub const mock_broker = @import("testing/mock_broker.zig");

test {
    std.testing.refAllDecls(@This());
}
