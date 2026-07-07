//! `wire` module root — sans-I/O Kafka protocol framing and codec.
//!
//! Re-exports the primitive readers/writers, response frame buffer, and the
//! API key/version table. Per-API request/response encoders live under
//! `api/` and are added in later phases.

const std = @import("std");

pub const primitives = @import("primitives.zig");
pub const ResponseBuffer = @import("ResponseBuffer.zig");
pub const record_batch = @import("record_batch.zig");
pub const compress = @import("compress.zig");
pub const api_keys = @import("api/api_keys.zig");
pub const metadata = @import("api/metadata.zig");
pub const api_versions = @import("api/api_versions.zig");
pub const sasl = @import("api/sasl.zig");
pub const produce = @import("api/produce.zig");

test {
    std.testing.refAllDecls(@This());
}
