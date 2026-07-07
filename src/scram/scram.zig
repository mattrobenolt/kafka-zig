//! SCRAM client (RFC 5802), generic over the hash pair.
//!
//! Instantiated as `ScramSha256` (RFC 7677) and `ScramSha512`. SCRAM-SHA-512 is
//! the same construction over SHA-512/HMAC-SHA-512 — Kafka supports it per the
//! Kafka security docs; it is NOT an IANA-registered SCRAM family mechanism and
//! is NOT RFC 7804 (that is HTTP SCRAM).
//!
//! Standalone: imports only `std`. No Kafka / wire / TLS coupling. Produces and
//! consumes bare SCRAM messages as byte slices; the SASL framing
//! (SaslHandshake / SaslAuthenticate) lives in the connection layer.
//!
//! No heap and no I/O on any path. Messages are built into fixed-capacity
//! buffers owned by the returned value structs; if a username / server message
//! exceeds the compiled bounds the call returns `error.BufferOverflow` rather
//! than allocating.
//!
//! ## SASLprep (RFC 4013) — deliberately minimal for v1
//!
//! A full SASLprep (stringprep with the SASLprep profile: Unicode mapping,
//! NFKC normalization, prohibited-code-point checks, bidi checks) is out of
//! scope for v1. We do the two things that are mandatory for the wire format to
//! parse correctly and that RFC 5802 §5.1 calls out explicitly: escape `,` as
//! `=2C` and `=` as `=3D` in the username. We do NOT normalize Unicode or the
//! password. This is safe for ASCII usernames/passwords, which is what Kafka
//! credentials from AWS Secrets Manager / MSK are in practice. If a deployment
//! uses non-ASCII SCRAM credentials that were SASLprep-normalized server-side,
//! callers must normalize before calling in. This limitation is documented and
//! intentional; a real SASLprep can be layered on later without changing the
//! message-building code.

const std = @import("std");
const mem = std.mem;
const base64 = std.base64;
const assert = std.debug.assert;

const pbkdf2_mod = @import("pbkdf2.zig");
const encoder = base64.standard.Encoder;
const decoder = base64.standard.Decoder;

pub const Error = error{
    /// Server-first / server-final message was malformed or unparseable.
    InvalidServerMessage,
    /// Server nonce did not begin with our client nonce (RFC 5802 §5.1).
    NonceMismatch,
    /// Server signature (v=) did not match the expected ServerSignature.
    ServerSignatureMismatch,
    /// A message did not fit the compiled fixed-capacity buffers.
    BufferOverflow,
};

/// SCRAM client for the given HMAC/hash pair and mechanism name.
///
/// The hash and HMAC must agree in output length (they do for the SHA-2
/// family used here). All key/proof math follows RFC 5802 §3:
///   SaltedPassword  = Hi(Normalize(password), salt, i)
///   ClientKey       = HMAC(SaltedPassword, "Client Key")
///   StoredKey       = H(ClientKey)
///   ClientSignature = HMAC(StoredKey, AuthMessage)
///   ClientProof     = ClientKey XOR ClientSignature
///   ServerKey       = HMAC(SaltedPassword, "Server Key")
///   ServerSignature = HMAC(ServerKey, AuthMessage)
/// where AuthMessage = client-first-bare + "," + server-first + "," +
/// client-final-without-proof.
pub fn Scram(comptime Hmac: type, comptime Sha: type, comptime mechanism_name: []const u8) type {
    return struct {
        const Self = @This();

        comptime {
            assert(Sha.digest_length == Hmac.mac_length);
        }

        /// SASL mechanism name (e.g. "SCRAM-SHA-256").
        pub const mechanism = mechanism_name;

        /// Incremental PBKDF2 state machine for this hash.
        pub const Pbkdf2 = pbkdf2_mod.Pbkdf2(Hmac);

        /// Digest length in bytes (32 for SHA-256, 64 for SHA-512).
        pub const hash_len = Sha.digest_length;
        pub const Hash = [hash_len]u8;
        const HashVec = @Vector(hash_len, u8);

        const base64_hash_len = encoder.calcSize(hash_len);

        // --- Bounds (fixed buffers; overflow → error, never heap) ---
        // Raw random nonce; base64-encoded into the client-first message.
        const raw_nonce_len = 18;
        const base64_nonce_len = encoder.calcSize(raw_nonce_len);
        /// Max raw username length accepted before `,`/`=` escaping.
        pub const max_username_len = 256;
        const max_escaped_username_len = max_username_len * 3;
        // A nonce as carried in a message we build/parse (client or server).
        const max_message_nonce_len = 256;
        const max_salt_len = 128;
        const max_server_first_len = 512;

        const client_first_cap = "n,,n=".len + max_escaped_username_len +
            ",r=".len + max_message_nonce_len;
        const client_final_cap = "c=biws,r=".len + max_message_nonce_len +
            ",p=".len + base64_hash_len;
        const auth_message_cap = client_first_cap + 1 + max_server_first_len +
            ",c=biws,r=".len + max_message_nonce_len;

        /// The client-first message plus the offsets needed later to derive the
        /// client-first-bare and the client nonce. Held by value; no heap.
        pub const ClientFirstMessage = struct {
            buf: [client_first_cap]u8,
            len: usize,
            /// Offset where the base64 client nonce begins.
            nonce_off: usize,

            /// The full `n,,n=<user>,r=<nonce>` message.
            pub fn bytes(self: *const ClientFirstMessage) []const u8 {
                return self.buf[0..self.len];
            }
            /// client-first-bare: everything after the `n,,` GS2 header.
            pub fn bare(self: *const ClientFirstMessage) []const u8 {
                return self.buf["n,,".len..self.len];
            }
            /// The client nonce (used to validate the server nonce prefix).
            pub fn clientNonce(self: *const ClientFirstMessage) []const u8 {
                return self.buf[self.nonce_off..self.len];
            }
        };

        /// The client-final message, held by value; no heap.
        pub const ClientFinalMessage = struct {
            buf: [client_final_cap]u8,
            len: usize,

            pub fn bytes(self: *const ClientFinalMessage) []const u8 {
                return self.buf[0..self.len];
            }
        };

        /// Result of processing server-first: the message to send next and the
        /// signature to expect in the server-final message.
        pub const ServerFirstResult = struct {
            client_final_message: ClientFinalMessage,
            expected_server_signature: Hash,
        };

        /// Server-first parsed for incremental derivation. `pbkdf2` borrows the
        /// caller's `password`; keep it alive until `result()`.
        pub const ParsedServerFirst = struct {
            pbkdf2: Pbkdf2,
        };

        /// Generate a client-first message with a fresh random nonce.
        /// Builds `n,,n=<escaped-username>,r=<base64-nonce>`.
        pub fn generateClientFirstMessage(username: []const u8) Error!ClientFirstMessage {
            var raw: [raw_nonce_len]u8 = undefined;
            std.crypto.random.bytes(&raw);
            var nonce: [base64_nonce_len]u8 = undefined;
            _ = encoder.encode(&nonce, &raw);
            return clientFirstMessageWithNonce(username, &nonce);
        }

        /// Build a client-first message with a caller-supplied nonce. Exposed
        /// for deterministic tests and known-answer vectors. The nonce is used
        /// verbatim (must be printable, no `,`).
        pub fn clientFirstMessageWithNonce(username: []const u8, nonce: []const u8) Error!ClientFirstMessage {
            assert(nonce.len > 0);
            var msg: ClientFirstMessage = .{ .buf = undefined, .len = 0, .nonce_off = 0 };

            var w = try put(&msg.buf, 0, "n,,n=");
            w = try putEscaped(&msg.buf, w, username);
            w = try put(&msg.buf, w, ",r=");
            msg.nonce_off = w;
            w = try put(&msg.buf, w, nonce);
            msg.len = w;
            return msg;
        }

        /// Parse server-first and start incremental PBKDF2. Drive with
        /// `result.pbkdf2.step()`, then call `completeServerFirst()`.
        pub fn parseServerFirst(
            server_first: []const u8,
            client_first: *const ClientFirstMessage,
            password: []const u8,
        ) Error!ParsedServerFirst {
            const fields = try parseServerFirstFields(server_first, client_first);
            var salt_buf: [max_salt_len]u8 = undefined;
            const salt = try decodeSalt(&salt_buf, fields.salt_b64);
            return .{ .pbkdf2 = .init(password, salt, fields.iterations) };
        }

        /// Blocking convenience: parse server-first, derive keys via std PBKDF2,
        /// and build the client-final message + expected server signature.
        pub fn processServerFirstWithPassword(
            server_first: []const u8,
            client_first: *const ClientFirstMessage,
            password: []const u8,
        ) Error!ServerFirstResult {
            const fields = try parseServerFirstFields(server_first, client_first);
            var salt_buf: [max_salt_len]u8 = undefined;
            const salt = try decodeSalt(&salt_buf, fields.salt_b64);

            var salted: Hash = undefined;
            std.crypto.pwhash.pbkdf2(&salted, password, salt, fields.iterations, Hmac) catch
                return error.InvalidServerMessage;

            return completeServerFirst(&salted, server_first, client_first);
        }

        /// Complete the exchange from a pre-derived salted password (the output
        /// of the incremental PBKDF2). Re-extracts the server nonce (cheap).
        pub fn completeServerFirst(
            salted_password: *const Hash,
            server_first: []const u8,
            client_first: *const ClientFirstMessage,
        ) Error!ServerFirstResult {
            const after_r = cutPrefix(server_first, "r=") orelse return error.InvalidServerMessage;
            const server_nonce, _ = cutScalar(after_r, ',') orelse return error.InvalidServerMessage;

            // ClientKey / ServerKey = HMAC(SaltedPassword, "...").
            var client_key: Hash = undefined;
            Hmac.create(&client_key, "Client Key", salted_password);
            var server_key: Hash = undefined;
            Hmac.create(&server_key, "Server Key", salted_password);

            // AuthMessage = client-first-bare "," server-first "," client-final-without-proof.
            var auth: Buf(auth_message_cap) = .{};
            try auth.append(client_first.bare());
            try auth.append(",");
            try auth.append(server_first);
            try auth.append(",c=biws,r=");
            try auth.append(server_nonce);
            const auth_message = auth.slice();

            // StoredKey = H(ClientKey); ClientSignature = HMAC(StoredKey, AuthMessage).
            var stored_key: Hash = undefined;
            Sha.hash(&client_key, &stored_key, .{});
            var client_sig: Hash = undefined;
            Hmac.create(&client_sig, auth_message, &stored_key);

            // ClientProof = ClientKey XOR ClientSignature.
            const key_vec: HashVec = client_key;
            const sig_vec: HashVec = client_sig;
            const proof: Hash = key_vec ^ sig_vec;

            // client-final = "c=biws,r=" server_nonce ",p=" base64(proof).
            var final: ClientFinalMessage = .{ .buf = undefined, .len = 0 };
            var w = try put(&final.buf, 0, "c=biws,r=");
            w = try put(&final.buf, w, server_nonce);
            w = try put(&final.buf, w, ",p=");
            if (w + base64_hash_len > final.buf.len) return error.BufferOverflow;
            _ = encoder.encode(final.buf[w..][0..base64_hash_len], &proof);
            final.len = w + base64_hash_len;

            // ServerSignature = HMAC(ServerKey, AuthMessage).
            var server_sig: Hash = undefined;
            Hmac.create(&server_sig, auth_message, &server_key);

            return .{
                .client_final_message = final,
                .expected_server_signature = server_sig,
            };
        }

        /// Verify the server-final message (`v=<base64-signature>`) against the
        /// expected ServerSignature. Timing-safe.
        pub fn verifyServerFinal(sasl_final: []const u8, expected: *const Hash) Error!void {
            const after_v = cutPrefix(sasl_final, "v=") orelse return error.InvalidServerMessage;
            // Tolerate trailing SCRAM extensions after the signature.
            const sig_b64 = if (mem.indexOfScalar(u8, after_v, ',')) |i| after_v[0..i] else after_v;

            const decoded_len = decoder.calcSizeForSlice(sig_b64) catch return error.InvalidServerMessage;
            if (decoded_len != hash_len) return error.InvalidServerMessage;
            var received: Hash = undefined;
            decoder.decode(&received, sig_b64) catch return error.InvalidServerMessage;

            const received_vec: HashVec = received;
            const expected_vec: HashVec = expected.*;
            if (!std.crypto.timing_safe.eql(HashVec, received_vec, expected_vec))
                return error.ServerSignatureMismatch;
        }

        // --- internal helpers ---

        const ServerFirstFields = struct {
            server_nonce: []const u8,
            salt_b64: []const u8,
            iterations: u32,
        };

        fn parseServerFirstFields(
            server_first: []const u8,
            client_first: *const ClientFirstMessage,
        ) Error!ServerFirstFields {
            const after_r = cutPrefix(server_first, "r=") orelse return error.InvalidServerMessage;
            const server_nonce, const after_nonce = cutScalar(after_r, ',') orelse
                return error.InvalidServerMessage;

            // Server nonce MUST start with the client nonce (RFC 5802 §5.1).
            if (!mem.startsWith(u8, server_nonce, client_first.clientNonce()))
                return error.NonceMismatch;

            const after_s = cutPrefix(after_nonce, "s=") orelse return error.InvalidServerMessage;
            const salt_b64, const after_salt = cutScalar(after_s, ',') orelse
                return error.InvalidServerMessage;

            const iter_str = cutPrefix(after_salt, "i=") orelse return error.InvalidServerMessage;
            // Tolerate trailing extensions after the iteration count.
            const iter_only = if (mem.indexOfScalar(u8, iter_str, ',')) |i| iter_str[0..i] else iter_str;
            const iterations = std.fmt.parseInt(u32, iter_only, 10) catch return error.InvalidServerMessage;
            if (iterations == 0) return error.InvalidServerMessage;

            return .{ .server_nonce = server_nonce, .salt_b64 = salt_b64, .iterations = iterations };
        }

        fn decodeSalt(out: []u8, salt_b64: []const u8) Error![]const u8 {
            const n = decoder.calcSizeForSlice(salt_b64) catch return error.InvalidServerMessage;
            if (n > out.len) return error.InvalidServerMessage;
            decoder.decode(out[0..n], salt_b64) catch return error.InvalidServerMessage;
            return out[0..n];
        }
    };
}

/// SCRAM-SHA-256 (RFC 7677).
pub const ScramSha256 = Scram(
    std.crypto.auth.hmac.sha2.HmacSha256,
    std.crypto.hash.sha2.Sha256,
    "SCRAM-SHA-256",
);

/// SCRAM-SHA-512 (RFC 5802 construction over SHA-512; supported by Kafka).
pub const ScramSha512 = Scram(
    std.crypto.auth.hmac.sha2.HmacSha512,
    std.crypto.hash.sha2.Sha512,
    "SCRAM-SHA-512",
);

// --- shared byte-building helpers (no heap) ---

/// Append `src` into `buf` at offset `w`, returning the new offset. Overflow is
/// an error, never a silent truncation or a heap allocation.
fn put(buf: []u8, w: usize, src: []const u8) Error!usize {
    if (w + src.len > buf.len) return error.BufferOverflow;
    @memcpy(buf[w..][0..src.len], src);
    return w + src.len;
}

/// Append `src` with SCRAM username escaping (`,`→`=2C`, `=`→`=3D`).
fn putEscaped(buf: []u8, start: usize, src: []const u8) Error!usize {
    var w = start;
    for (src) |c| {
        switch (c) {
            ',' => w = try put(buf, w, "=2C"),
            '=' => w = try put(buf, w, "=3D"),
            else => {
                if (w + 1 > buf.len) return error.BufferOverflow;
                buf[w] = c;
                w += 1;
            },
        }
    }
    return w;
}

/// Fixed-capacity byte accumulator. Append overflow → error.BufferOverflow.
fn Buf(comptime cap: usize) type {
    return struct {
        const Self = @This();
        bytes: [cap]u8 = undefined,
        len: usize = 0,

        fn append(self: *Self, src: []const u8) Error!void {
            self.len = try put(&self.bytes, self.len, src);
        }
        fn slice(self: *const Self) []const u8 {
            return self.bytes[0..self.len];
        }
    };
}

fn cutPrefix(s: []const u8, prefix: []const u8) ?[]const u8 {
    return if (mem.startsWith(u8, s, prefix)) s[prefix.len..] else null;
}

fn cutScalar(s: []const u8, delim: u8) ?struct { []const u8, []const u8 } {
    const i = mem.indexOfScalar(u8, s, delim) orelse return null;
    return .{ s[0..i], s[i + 1 ..] };
}

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;

test "client-first includes the username (Kafka, not the empty-user Postgres form)" {
    const msg = try ScramSha256.clientFirstMessageWithNonce("alice", "NONCE0123456789");
    try testing.expectEqualStrings("n,,n=alice,r=NONCE0123456789", msg.bytes());
    try testing.expectEqualStrings("n=alice,r=NONCE0123456789", msg.bare());
    try testing.expectEqualStrings("NONCE0123456789", msg.clientNonce());
}

test "username escaping: ',' -> =2C and '=' -> =3D (RFC 5802 §5.1)" {
    const msg = try ScramSha256.clientFirstMessageWithNonce("a,b=c", "N");
    try testing.expectEqualStrings("n,,n=a=2Cb=3Dc,r=N", msg.bytes());
}

test "generateClientFirstMessage produces a well-formed random-nonce message" {
    const msg = try ScramSha256.generateClientFirstMessage("user");
    try testing.expect(mem.startsWith(u8, msg.bytes(), "n,,n=user,r="));
    try testing.expectEqual(@as(usize, 24), msg.clientNonce().len); // base64(18 bytes)
    try testing.expectEqual(null, mem.indexOfScalar(u8, msg.bytes(), 0));
}

test "mechanism names" {
    try testing.expectEqualStrings("SCRAM-SHA-256", ScramSha256.mechanism);
    try testing.expectEqualStrings("SCRAM-SHA-512", ScramSha512.mechanism);
}

// RFC 7677 §5 SHA-256 known-answer vector.
//   username=user password=pencil, client nonce rOprNGfwEbeRWgbNEkqO
//   Verified independently with Python hashlib/hmac.
const rfc7677_client_nonce = "rOprNGfwEbeRWgbNEkqO";
const rfc7677_server_first =
    "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
const rfc7677_client_final =
    "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," ++
    "p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
const rfc7677_server_final = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";

test "RFC 7677 §5 SHA-256 known-answer vector (proof + server signature)" {
    const cf = try ScramSha256.clientFirstMessageWithNonce("user", rfc7677_client_nonce);
    try testing.expectEqualStrings("n,,n=user,r=rOprNGfwEbeRWgbNEkqO", cf.bytes());

    const res = try ScramSha256.processServerFirstWithPassword(rfc7677_server_first, &cf, "pencil");
    try testing.expectEqualStrings(rfc7677_client_final, res.client_final_message.bytes());

    try ScramSha256.verifyServerFinal(rfc7677_server_final, &res.expected_server_signature);
}

// SHA-512 deterministic vector: same inputs as RFC 7677 §5 but over
// SHA-512/HMAC-SHA-512. Expected proof/signature computed independently with
// Python hashlib.pbkdf2_hmac('sha512', ...) + hmac.
const sha512_client_final =
    "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," ++
    "p=gMGXRcevScNtxZ6/8lQYpGtnsNAc3mGcmNomv+xnoOMw+3R2xNJdMNnzMlTN8PPC6wdp6dybEmDYXYTxwnYPJQ==";
const sha512_server_final =
    "v=ZQnYEgWQMFmmsM8aQMF0nDDCy/AgCzkwk8CmMZYcMg0vSVlKDanekLtifDSeVGT4+5ZxXnJq199RVG2rR7N7Zw==";

test "SCRAM-SHA-512 deterministic vector (proof + server signature)" {
    const cf = try ScramSha512.clientFirstMessageWithNonce("user", rfc7677_client_nonce);
    const res = try ScramSha512.processServerFirstWithPassword(rfc7677_server_first, &cf, "pencil");
    try testing.expectEqualStrings(sha512_client_final, res.client_final_message.bytes());
    try ScramSha512.verifyServerFinal(sha512_server_final, &res.expected_server_signature);
}

test "server nonce not starting with client nonce is rejected" {
    const cf = try ScramSha256.clientFirstMessageWithNonce("user", "clientnonce");
    const bad = "r=totallyDifferentNonce,s=c2FsdA==,i=4096";
    try testing.expectError(error.NonceMismatch, ScramSha256.processServerFirstWithPassword(bad, &cf, "pw"));
}

test "verifyServerFinal: wrong signature is rejected, correct is accepted" {
    var expected: ScramSha256.Hash = @splat(0x11);
    var same_b64: [encoder.calcSize(32)]u8 = undefined;
    _ = encoder.encode(&same_b64, &expected);
    var msg_buf: [128]u8 = undefined;
    const good = std.fmt.bufPrint(&msg_buf, "v={s}", .{same_b64}) catch unreachable;
    try ScramSha256.verifyServerFinal(good, &expected);

    // Flip one byte of the expected signature → mismatch.
    var wrong = expected;
    wrong[0] ^= 0xFF;
    try testing.expectError(error.ServerSignatureMismatch, ScramSha256.verifyServerFinal(good, &wrong));
}

fn expectIncrementalMatchesBlocking(comptime S: type) !void {
    const cf = try S.clientFirstMessageWithNonce("user", "clientnonce123456789");
    const server_first = "r=clientnonce123456789servernonce,s=c2FsdHNhbHQ=,i=4096";
    const password = "testpassword";

    const blocking = try S.processServerFirstWithPassword(server_first, &cf, password);

    var parsed = try S.parseServerFirst(server_first, &cf, password);
    while (!parsed.pbkdf2.step(256)) {}
    const salted = parsed.pbkdf2.result();
    const incremental = try S.completeServerFirst(&salted, server_first, &cf);

    try testing.expectEqualSlices(
        u8,
        blocking.client_final_message.bytes(),
        incremental.client_final_message.bytes(),
    );
    try testing.expectEqualSlices(
        u8,
        &blocking.expected_server_signature,
        &incremental.expected_server_signature,
    );
}

test "incremental path matches blocking path (SHA-256)" {
    try expectIncrementalMatchesBlocking(ScramSha256);
}

test "incremental path matches blocking path (SHA-512)" {
    try expectIncrementalMatchesBlocking(ScramSha512);
}

test "fuzz parseServerFirst: arbitrary bytes never panic" {
    try testing.fuzz({}, struct {
        fn run(_: void, input: []const u8) anyerror!void {
            const cf = ScramSha256.clientFirstMessageWithNonce("user", "clientnonce") catch return;
            _ = ScramSha256.parseServerFirst(input, &cf, "password") catch return;
            _ = ScramSha256.processServerFirstWithPassword(input, &cf, "password") catch return;
        }
    }.run, .{
        .corpus = &.{
            "r=clientnonceXYZ,s=c2FsdA==,i=4096",
            "r=wrongnonce,s=c2FsdA==,i=4096",
            "r=,s=,i=0",
            "s=c2FsdA==,i=4096",
            "garbage",
            "",
        },
    });
}

test "fuzz verifyServerFinal: arbitrary bytes never panic" {
    try testing.fuzz({}, struct {
        fn run(_: void, input: []const u8) anyerror!void {
            var dummy: ScramSha256.Hash = @splat(0x42);
            ScramSha256.verifyServerFinal(input, &dummy) catch return;
        }
    }.run, .{
        .corpus = &.{
            "v=dGhlIHNhbXBsZSBub25jZQ==AAAAAAAAAAAAAA==",
            "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=",
            "v=",
            "v=!!!invalid!!!",
            "e=other-error",
            "",
        },
    });
}
