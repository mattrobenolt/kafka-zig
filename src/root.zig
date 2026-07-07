//! kafka-zig — native Zig Kafka producer. Pre-alpha.
const std = @import("std");

pub const version = "0.0.0";

pub const wire = @import("wire/root.zig");

test {
    std.testing.refAllDecls(@This());
}
