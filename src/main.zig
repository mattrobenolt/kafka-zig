const std = @import("std");
const kafka = @import("kafka");

pub fn main() !void {
    var buf: [256]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    defer writer.interface.flush() catch {};
    try writer.interface.print("kafka-zig {s}\n", .{kafka.version});
}
