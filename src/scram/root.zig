//! Standalone SCRAM-SHA-256/512 client (RFC 5802 + RFC 7677). No Kafka imports.
//!
//! Exposed as its own build module (`scram`) so it can be lifted to a separate
//! repo unchanged. Imports only `std`.
const std = @import("std");

const scram = @import("scram.zig");

/// Incremental PBKDF2 state machine, generic over the HMAC type.
pub const Pbkdf2 = @import("pbkdf2.zig").Pbkdf2;

/// Generic SCRAM client constructor: `Scram(Hmac, Sha, mechanism_name)`.
pub const Scram = scram.Scram;
/// SCRAM-SHA-256 (RFC 7677).
pub const ScramSha256 = scram.ScramSha256;
/// SCRAM-SHA-512 (RFC 5802 construction over SHA-512; Kafka-supported).
pub const ScramSha512 = scram.ScramSha512;
pub const Error = scram.Error;

test {
    _ = scram;
    _ = @import("pbkdf2.zig");
    std.testing.refAllDecls(@This());
}
