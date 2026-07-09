//! kafka-zig — native Zig Kafka producer. Pre-alpha.
const std = @import("std");

pub const version = "0.0.0";

pub const wire = @import("wire/root.zig");
pub const Ring = @import("Ring.zig");
pub const Connection = @import("Connection.zig");
pub const Client = @import("Client.zig");
pub const Stats = Client.Stats;
pub const ErrorCounts = Client.ErrorCounts;

test {
    std.testing.refAllDecls(@This());
}
