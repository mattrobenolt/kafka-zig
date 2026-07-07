//! `wire` module root — sans-I/O Kafka protocol framing and codec.
//!
//! Re-exports the primitive readers/writers, response frame buffer, and the
//! API key/version table. Per-API request/response encoders live under
//! `api/` and are added in later phases.

const std = @import("std");

pub const primitives = @import("primitives.zig");
pub const frame = @import("frame.zig");
pub const api_keys = @import("api/api_keys.zig");

test {
    std.testing.refAllDecls(@This());
}
