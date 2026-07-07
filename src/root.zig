//! kafka-zig — native Zig Kafka producer. Pre-alpha.
const std = @import("std");

pub const version = "0.0.0";

pub fn add(a: i32, b: i32) i32 {
    return a + b;
}

test "basic add functionality" {
    try std.testing.expect(add(3, 7) == 10);
}
