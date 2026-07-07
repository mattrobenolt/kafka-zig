//! Incremental PBKDF2 for non-blocking key derivation, generic over the HMAC.
//!
//! Restructured from `std.crypto.pwhash.pbkdf2` as a resumable state machine:
//! each `step()` performs a bounded number of HMAC iterations so the caller can
//! yield between chunks (SCRAM's `Hi()` runs thousands of iterations). No heap;
//! the password is borrowed and must outlive the state machine.
//!
//! Specialized for SCRAM: a single output block, so the derived key length
//! equals the HMAC output length (32 bytes for SHA-256, 64 for SHA-512). This
//! is exactly what `SaltedPassword = Hi(password, salt, i)` needs.

const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;

/// Resumable single-block PBKDF2, parameterized over the HMAC type.
///
/// Usage:
///   var kdf: Pbkdf2(HmacSha512) = .init(password, salt, rounds);
///   while (!kdf.step(256)) { /* yield */ }
///   const salted_password = kdf.result();
pub fn Pbkdf2(comptime Hmac: type) type {
    return struct {
        const Self = @This();

        /// Output block length in bytes (= HMAC output length).
        pub const hash_len = Hmac.mac_length;
        pub const Hash = [hash_len]u8;
        const HashVec = @Vector(hash_len, u8);

        /// XOR accumulator: T = U_1 ^ U_2 ^ ... ^ U_c.
        dk: Hash,
        /// Previous PRF output: U_{i-1}.
        prev: Hash,
        /// HMAC key (password). Borrowed — caller must keep it alive.
        password: []const u8,
        /// Current iteration (starts at 1 since U_1 is computed in init).
        iteration: u32,
        /// Total iterations required.
        rounds: u32,

        /// Compute U_1 = PRF(password, salt || INT_32_BE(1)) and seed the
        /// accumulator. This is the only step that touches the salt, so the
        /// salt slice need only be valid for the duration of this call.
        pub fn init(password: []const u8, salt: []const u8, rounds: u32) Self {
            assert(rounds > 0);

            var ctx: Hmac = .init(password);
            ctx.update(salt);
            ctx.update(&mem.toBytes(mem.nativeToBig(u32, 1)));
            var first: Hash = undefined;
            ctx.final(&first);

            return .{
                .dk = first,
                .prev = first,
                .password = password,
                .iteration = 1,
                .rounds = rounds,
            };
        }

        /// Perform up to `chunk_size` iterations. Returns true when complete.
        pub fn step(self: *Self, chunk_size: u32) bool {
            assert(chunk_size > 0);
            const target = @min(self.iteration + chunk_size, self.rounds);

            while (self.iteration < target) : (self.iteration += 1) {
                // U_i = PRF(password, U_{i-1})
                var next: Hash = undefined;
                Hmac.create(&next, &self.prev, self.password);
                self.prev = next;

                // dk ^= U_i
                const dk_vec: HashVec = self.dk;
                const next_vec: HashVec = next;
                self.dk = dk_vec ^ next_vec;
            }

            return self.iteration >= self.rounds;
        }

        /// The derived salted password. Only valid after `step()` returns true.
        pub fn result(self: *const Self) Hash {
            assert(self.iteration >= self.rounds);
            return self.dk;
        }
    };
}

// ---------------------------------------------------------------------------
// Tests — cross-check the incremental machine against std.crypto.pwhash.pbkdf2.
// ---------------------------------------------------------------------------

const testing = std.testing;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const HmacSha512 = std.crypto.auth.hmac.sha2.HmacSha512;

fn expectMatchesStd(comptime Hmac: type, password: []const u8, salt: []const u8, rounds: u32, chunk: u32) !void {
    var expected: [Hmac.mac_length]u8 = undefined;
    try std.crypto.pwhash.pbkdf2(&expected, password, salt, rounds, Hmac);

    var kdf: Pbkdf2(Hmac) = .init(password, salt, rounds);
    while (!kdf.step(chunk)) {}
    try testing.expectEqualSlices(u8, &expected, &kdf.result());
}

test "sha256: incremental matches std pbkdf2 (1 iteration)" {
    try expectMatchesStd(HmacSha256, "password", "salt", 1, 256);
}

test "sha256: incremental matches std pbkdf2 (4096, small chunks)" {
    try expectMatchesStd(HmacSha256, "password", "salt", 4096, 100);
}

test "sha256: incremental matches std pbkdf2 (4096, single step)" {
    try expectMatchesStd(HmacSha256, "password", "salt", 4096, 4096);
}

test "sha512: incremental matches std pbkdf2 (4096, small chunks)" {
    try expectMatchesStd(HmacSha512, "password", "salt", 4096, 100);
}

test "sha512: incremental matches std pbkdf2 (odd chunk, longer inputs)" {
    try expectMatchesStd(
        HmacSha512,
        "passwordPASSWORDpassword",
        "saltSALTsaltSALTsaltSALTsaltSALTsalt",
        4096,
        37,
    );
}

test "output length tracks the HMAC" {
    try testing.expectEqual(32, Pbkdf2(HmacSha256).hash_len);
    try testing.expectEqual(64, Pbkdf2(HmacSha512).hash_len);
}
