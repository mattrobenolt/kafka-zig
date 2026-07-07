//! TCP + TLS 1.3 (ztls) + SASL/SCRAM connection to a Kafka broker.
//!
//! This is the first I/O slice (PLAN §2.5, phase 5). It composes three
//! already-built pieces: `ztls` (TLS 1.3 client over a blocking
//! `std.net.Stream`), `scram` (SCRAM-SHA-256/512 client), and `wire` (request
//! framing + SASL body codecs). A dialed `Connection` is authenticated and
//! ready for Metadata/Produce request/response exchange (phase 6).
//!
//! ## Buffer / allocation discipline
//!
//! A `Connection` is heap-allocated once at `dial` (the ztls `RecordBuffer`
//! and Kafka `ResponseBuffer` hold pointers into inline storage arrays, so the
//! object must not move). All per-connection buffers are fixed-size inline
//! fields sized up front; there is **no per-request heap allocation** on the
//! `sendRequest` / `readResponse` path. The only allocator use is:
//!   - `dial` allocates the `Connection` itself (once), and
//!   - SASL auth (cold path, one-time) decodes SASL response bodies with the
//!     allocator — the `wire` SASL decoders own nested strings. Every such
//!     response is `deinit`'d before auth returns.
//!
//! The request/response data path copies decrypted TLS application-data into
//! the `ResponseBuffer` accumulator. That copy is inherent to TLS (ztls hands
//! back plaintext in its own record buffer, which is reused on the next
//! record) and is distinct from the single produce-path copy discussed in
//! PLAN §8. No heap is involved.
//!
//! ## SASL flow (KIP-152, KIP-84)
//!
//! connect TCP → ztls handshake → ApiVersions v0 pre-auth probe (Java-client
//! robustness) → SaslHandshake(mechanism) v1 → SCRAM exchange wrapped in
//! SaslAuthenticate v1 (client-first → server-first → client-final →
//! server-final verify) → ready.

const std = @import("std");
const assert = std.debug.assert;

const ztls = @import("ztls");
const scram = @import("scram");

const wire = @import("wire/root.zig");
const request = wire.request;
const sasl = wire.sasl;
const primitives = wire.primitives;
const api_keys = wire.api_keys;
const ApiKey = api_keys.ApiKey;
const Reader = primitives.Reader;
const ResponseBuffer = wire.ResponseBuffer;

/// SASL/SCRAM mechanism. MSK is SHA-512 only; SHA-256 is kept for other
/// brokers (PLAN §9).
pub const Mechanism = enum { scram_sha256, scram_sha512 };

/// SCRAM credentials + mechanism selection.
pub const ScramConfig = struct {
    mechanism: Mechanism,
    username: []const u8,
    password: []const u8,
};

/// Connection configuration. `ca_bundle` is caller-owned and must outlive the
/// connection (ztls borrows it for the connection lifetime). For a self-signed
/// test/dev broker, set `insecure_skip_verify = true` instead of supplying a
/// bundle.
pub const Config = struct {
    host: []const u8,
    port: u16,
    /// SNI hostname; must match the broker leaf certificate SAN/CN.
    sni: []const u8,
    ca_bundle: ?*const std.crypto.Certificate.Bundle = null,
    /// Test/dev opt-out from chain-anchor verification (ztls
    /// `insecure_no_chain_anchor`). Still verifies CertificateVerify key
    /// possession and the hostname. Never set this against a real broker.
    insecure_skip_verify: bool = false,
    scram: ScramConfig,
    client_id: ?[]const u8 = null,
};

pub const Error = error{
    /// The broker rejected the SASL mechanism in SaslHandshake.
    UnsupportedSaslMechanism,
    /// The broker returned a nonzero error code during SaslAuthenticate.
    SaslAuthenticationFailed,
    /// The TLS peer closed the connection.
    ConnectionClosed,
    /// A response body did not fit the fixed response buffer.
    ResponseTooLarge,
    /// A malformed response frame (bad length prefix).
    MalformedResponse,
};

/// Fixed per-connection buffer sizes. Requests and responses for the auth path
/// and Metadata are comfortably within these; Produce sizing is a phase-6
/// concern (the produce path writes directly into the ring, not here).
const request_buffer_len = 64 * 1024;
const response_buffer_len = 256 * 1024;

/// Max plaintext per TLS record (RFC 8446 §5.1). Larger requests are chunked
/// across records.
const max_tls_plaintext = 1 << 14;

const Connection = @This();

allocator: std.mem.Allocator,
stream: std.net.Stream,
keypair: ztls.x25519.KeyPair,
hs: ztls.ClientHandshake,
out: ztls.ClientHandshake.OutBuffer,
/// Inbound TLS record framing storage.
tls_storage: ztls.RecordBuffer.Storage,
rb: ztls.RecordBuffer,
/// Kafka response framing over decrypted application data.
resp_storage: [response_buffer_len]u8,
resp_buf: ResponseBuffer,
/// Request framing scratch.
req_scratch: [request_buffer_len]u8,
correlation_id: i32,
/// client_id captured at dial time (stored so it survives the Config's
/// lifetime; Config stays caller-owned).
client_id_storage: ?[]const u8 = null,

/// Dial, TLS-handshake, and SASL-authenticate a broker connection.
///
/// On success the returned `*Connection` is owned by the caller; call
/// `close` (or `deinit`) to release it. On any failure the TCP stream and the
/// heap allocation are released before returning.
pub fn dial(allocator: std.mem.Allocator, config: Config) !*Connection {
    const self = try allocator.create(Connection);
    errdefer allocator.destroy(self);

    const stream = try std.net.tcpConnectToHost(allocator, config.host, config.port);
    errdefer stream.close();

    self.* = .{
        .allocator = allocator,
        .stream = stream,
        .keypair = .generate(),
        .hs = undefined,
        .out = .empty,
        .tls_storage = .empty,
        .rb = undefined,
        .resp_storage = undefined,
        .resp_buf = undefined,
        .req_scratch = undefined,
        .correlation_id = 0,
        .client_id_storage = null,
    };

    var random: ztls.Random = undefined;
    std.crypto.random.bytes(&random.data);
    self.hs = .init(.{
        .keypairs = .init(self.keypair),
        .host_name = config.sni,
        .now_sec = std.time.timestamp(),
        .random = random,
        .bundle = config.ca_bundle,
        .insecure_no_chain_anchor = config.insecure_skip_verify,
    });
    errdefer self.hs.deinit();

    self.rb = .init(&self.tls_storage.buffer);
    self.resp_buf = .init(&self.resp_storage);

    try self.driveHandshake();
    try self.authenticate(config);

    return self;
}

/// Release the connection: send a `close_notify` alert (best effort), close
/// the TCP stream, and free the heap allocation.
pub fn close(self: *Connection) void {
    // Best-effort clean close; ignore errors, the socket is going away.
    if (self.hs.sendAlert(.close_notify, &self.out.buffer)) |rec| {
        self.stream.writeAll(rec) catch {}; // ziglint-ignore: Z026 -- best-effort close_notify on teardown
        self.hs.completeWrite();
    } else |_| {}
    self.deinit();
}

/// Release without the close_notify handshake (e.g. after a fatal error).
pub fn deinit(self: *Connection) void { // ziglint-ignore: Z030 -- self is heap-owned, destroyed here
    self.hs.deinit();
    self.stream.close();
    const allocator = self.allocator;
    allocator.destroy(self);
}

// ---------------------------------------------------------------------------
// Public request/response primitives (phase 6 drives these)
// ---------------------------------------------------------------------------

/// Frame and send a request. `body_ctx` exposes
/// `write(self, *std.Io.Writer) !void` (the per-API body encoders fit behind a
/// one-line wrapper). Returns the correlation id assigned to the request so
/// the caller can match the response. Zero-alloc.
pub fn sendRequest(
    self: *Connection,
    api_key: ApiKey,
    api_version: u16,
    body_ctx: anytype,
) !i32 {
    const corr = self.nextCorrelationId();
    const framed = try request.frameRequest(
        &self.req_scratch,
        api_key,
        api_version,
        corr,
        self.clientId(),
        body_ctx,
    );
    try self.sendApplicationData(framed);
    return corr;
}

/// Read the next complete Kafka response body (everything after the i32 length
/// prefix, i.e. the response header + payload). The returned slice borrows the
/// connection's response buffer and is valid only until the next
/// `readResponse` / `sendRequest` call. The caller strips the response header
/// (per `api_keys.headerVersion`) and decodes the payload.
pub fn readResponse(self: *Connection) ![]const u8 {
    while (true) {
        if (self.resp_buf.next() catch return Error.MalformedResponse) |body| return body;
        try self.pump();
    }
}

fn nextCorrelationId(self: *Connection) i32 {
    const corr = self.correlation_id;
    self.correlation_id +%= 1;
    return corr;
}

fn clientId(self: *Connection) ?[]const u8 {
    return self.client_id_storage;
}

// ---------------------------------------------------------------------------
// TLS drive loop
// ---------------------------------------------------------------------------

fn driveHandshake(self: *Connection) !void {
    try self.writeAll(try self.hs.start(&self.out.buffer));
    self.hs.completeWrite();

    while (!self.hs.isConnected()) {
        const n = try self.read(self.rb.writable());
        if (n == 0) return Error.ConnectionClosed;
        self.rb.advance(n);
        while (try self.rb.next()) |record| switch (try self.hs.handleRecord(record, &self.out.buffer)) {
            .write => |w| {
                try self.writeAll(w);
                self.hs.completeWrite();
            },
            .none, .new_session_ticket => {},
            .key_update => |ku| try self.handleKeyUpdate(ku),
            .application_data, .closed => return Error.ConnectionClosed,
        };
    }
}

/// Encrypt and send `plaintext` as one or more TLS application-data records.
fn sendApplicationData(self: *Connection, plaintext: []const u8) !void {
    var off: usize = 0;
    while (off < plaintext.len) {
        const chunk_len = @min(plaintext.len - off, max_tls_plaintext);
        const rec = try self.hs.sendApplicationData(plaintext[off..][0..chunk_len], &self.out.buffer);
        try self.writeAll(rec);
        self.hs.completeWrite();
        off += chunk_len;
    }
}

/// Read one transport chunk, decrypt records, and accumulate decrypted
/// application data into the Kafka response buffer.
fn pump(self: *Connection) !void {
    const n = try self.read(self.rb.writable());
    if (n == 0) return Error.ConnectionClosed;
    self.rb.advance(n);
    while (try self.rb.next()) |record| switch (try self.hs.handleRecord(record, &self.out.buffer)) {
        .application_data => |data| {
            // Copy decrypted plaintext into the response accumulator: the ztls
            // slice is only valid until the next record is processed.
            const dst = self.resp_buf.writable();
            if (data.len > dst.len) return Error.ResponseTooLarge;
            @memcpy(dst[0..data.len], data);
            self.resp_buf.advance(data.len);
        },
        .write => |w| {
            try self.writeAll(w);
            self.hs.completeWrite();
        },
        .none, .new_session_ticket => {},
        .key_update => |ku| try self.handleKeyUpdate(ku),
        .closed => return Error.ConnectionClosed,
    };
}

fn handleKeyUpdate(self: *Connection, ku: ztls.ClientHandshake.KeyUpdateEvent) !void {
    if (ku.response) |w| {
        try self.writeAll(w);
        self.hs.completeWrite();
    }
}

/// `stream.read`, normalizing "peer went away" transport errors into
/// `Error.ConnectionClosed` so a broker that vanishes mid-exchange yields one
/// deterministic error regardless of whether the OS reports EOF or a reset.
fn read(self: *Connection, buf: []u8) !usize {
    return self.stream.read(buf) catch |err| switch (err) {
        error.ConnectionResetByPeer, error.BrokenPipe, error.ConnectionTimedOut => Error.ConnectionClosed,
        else => err,
    };
}

/// `stream.writeAll`, normalizing peer-gone errors like `read`.
fn writeAll(self: *Connection, bytes: []const u8) !void {
    self.stream.writeAll(bytes) catch |err| switch (err) {
        error.ConnectionResetByPeer, error.BrokenPipe => return Error.ConnectionClosed,
        else => return err,
    };
}

// ---------------------------------------------------------------------------
// SASL / SCRAM authentication (cold path, one-time)
// ---------------------------------------------------------------------------

fn authenticate(self: *Connection, config: Config) !void {
    self.client_id_storage = config.client_id;

    try self.apiVersionsProbe();
    try self.saslHandshake(config.scram.mechanism);

    switch (config.scram.mechanism) {
        .scram_sha256 => try self.scramExchange(scram.ScramSha256, config.scram),
        .scram_sha512 => try self.scramExchange(scram.ScramSha512, config.scram),
    }
}

/// ApiVersions v0 pre-auth probe (PLAN §2.1): brokers historically treat a
/// schema exception in the first request as a GSSAPI token, so the Java client
/// sends v0 as cheap insurance. We send it, read the response, and discard it.
fn apiVersionsProbe(self: *Connection) !void {
    const body: request.EmptyBody = .{};
    _ = try self.sendRequest(.api_versions, 0, body);
    _ = try self.readResponse();
}

fn saslHandshake(self: *Connection, mechanism: Mechanism) !void {
    const name = mechanismName(mechanism);
    const req_body: HandshakeBody = .{ .mechanism = name };
    _ = try self.sendRequest(.sasl_handshake, 1, req_body);

    const body = try self.readResponse();
    var payload = try stripResponseHeaderV0(body);
    var resp = try sasl.decodeHandshakeResponse(self.allocator, &payload);
    defer resp.deinit(self.allocator);

    if (resp.error_code != 0) return Error.UnsupportedSaslMechanism;
}

/// Drive the SCRAM message exchange over SaslAuthenticate v1. Generic over the
/// SCRAM instantiation (`scram.ScramSha256` / `scram.ScramSha512`).
fn scramExchange(self: *Connection, comptime S: type, cfg: ScramConfig) !void {
    const client_first = try S.generateClientFirstMessage(cfg.username);

    // client-first → server-first
    var sf = try self.saslAuthenticate(client_first.bytes());
    // Copy server-first out of the (owned) response before freeing it, and
    // before the next network round-trip reuses the response buffer.
    var server_first_buf: [1024]u8 = undefined;
    if (sf.auth_bytes.len > server_first_buf.len) {
        sf.deinit(self.allocator);
        return Error.SaslAuthenticationFailed;
    }
    @memcpy(server_first_buf[0..sf.auth_bytes.len], sf.auth_bytes);
    const server_first = server_first_buf[0..sf.auth_bytes.len];
    const sf_err = sf.error_code;
    sf.deinit(self.allocator);
    if (sf_err != 0) return Error.SaslAuthenticationFailed;

    const result = try S.processServerFirstWithPassword(server_first, &client_first, cfg.password);

    // client-final → server-final
    var ff = try self.saslAuthenticate(result.client_final_message.bytes());
    var server_final_buf: [1024]u8 = undefined;
    if (ff.auth_bytes.len > server_final_buf.len) {
        ff.deinit(self.allocator);
        return Error.SaslAuthenticationFailed;
    }
    @memcpy(server_final_buf[0..ff.auth_bytes.len], ff.auth_bytes);
    const server_final = server_final_buf[0..ff.auth_bytes.len];
    const ff_err = ff.error_code;
    ff.deinit(self.allocator);
    if (ff_err != 0) return Error.SaslAuthenticationFailed;

    try S.verifyServerFinal(server_final, &result.expected_server_signature);
}

/// Send one SaslAuthenticate v1 request carrying `token` and return the decoded
/// response. Caller owns the returned response and must `deinit` it.
fn saslAuthenticate(self: *Connection, token: []const u8) !sasl.AuthenticateResponse {
    const req_body: AuthBody = .{ .auth_bytes = token };
    _ = try self.sendRequest(.sasl_authenticate, 1, req_body);
    const body = try self.readResponse();
    var payload = try stripResponseHeaderV0(body);
    return sasl.decodeAuthenticateResponse(self.allocator, &payload);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn mechanismName(mechanism: Mechanism) []const u8 {
    return switch (mechanism) {
        .scram_sha256 => scram.ScramSha256.mechanism,
        .scram_sha512 => scram.ScramSha512.mechanism,
    };
}

/// Strip a response header v0 (correlation_id INT32 only) and return a reader
/// positioned at the response body. All SASL and ApiVersions-v0 responses use
/// header v0.
fn stripResponseHeaderV0(body: []const u8) !Reader {
    var r: Reader = .init(body);
    _ = primitives.readI32(&r) catch return Error.MalformedResponse;
    return Reader.init(r.remaining());
}

/// Body context: SaslHandshake v1 request. Public because `frameRequest`
/// (another module) invokes `write` through its duck-typed `anytype` param.
pub const HandshakeBody = struct {
    mechanism: []const u8,
    pub fn write(self: HandshakeBody, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try sasl.encodeHandshakeRequest(w, .{ .mechanism = self.mechanism });
    }
};

/// Body context: SaslAuthenticate v1 request. Public for the same reason as
/// `HandshakeBody`.
pub const AuthBody = struct {
    auth_bytes: []const u8,
    pub fn write(self: AuthBody, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try sasl.encodeAuthenticateRequest(w, .{ .auth_bytes = self.auth_bytes });
    }
};

// ===========================================================================
// Tests — mock broker (auth path) over ztls server + real loopback TCP
// ===========================================================================

const testing = std.testing;

test {
    testing.refAllDecls(@This());
}

test "dial: TLS handshake + ApiVersions probe + SCRAM-SHA-512 auth succeeds" {
    try runAuthTest(.{ .mode = .ok, .password = mock_password });
}

test "dial: SaslHandshake mechanism rejected → UnsupportedSaslMechanism" {
    try testing.expectError(
        Error.UnsupportedSaslMechanism,
        runAuthTest(.{ .mode = .bad_mechanism, .password = mock_password }),
    );
}

test "dial: wrong SCRAM password → broker rejects → SaslAuthenticationFailed" {
    try testing.expectError(
        Error.SaslAuthenticationFailed,
        runAuthTest(.{ .mode = .ok, .password = "wrong-password" }),
    );
}

test "dial: broker closes mid-handshake → ConnectionClosed" {
    try testing.expectError(
        Error.ConnectionClosed,
        runAuthTest(.{ .mode = .close_after_tls, .password = mock_password }),
    );
}

// --- test harness ---------------------------------------------------------

const mock_username = "kafka-zig-test";
const mock_password = "correct-horse-battery-staple";

const MockMode = enum { ok, bad_mechanism, reject_proof, close_after_tls };

const RunArgs = struct {
    mode: MockMode,
    password: []const u8,
};

const MockCtx = struct {
    server: *std.net.Server,
    mode: MockMode,
    result: ?anyerror = null,
};

fn runAuthTest(args: RunArgs) !void {
    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try addr.listen(.{ .reuse_address = true });
    defer server.deinit();
    const port = server.listen_address.getPort();

    var ctx: MockCtx = .{ .server = &server, .mode = args.mode };
    const thread = try std.Thread.spawn(.{}, mockBrokerRun, .{&ctx});
    defer thread.join();

    const dial_result = dial(testing.allocator, .{
        .host = "127.0.0.1",
        .port = port,
        .sni = mock_server_name,
        .insecure_skip_verify = true,
        .scram = .{
            .mechanism = .scram_sha512,
            .username = mock_username,
            .password = args.password,
        },
        .client_id = "kafka-zig",
    });

    if (dial_result) |conn| {
        defer conn.close();
        // Post-auth: exercise the generic request/response primitives with a
        // Metadata request. The mock replies with a canned framed body; we
        // only assert the round-trip and correlation id, not the decode.
        const meta_body: request.EmptyBody = .{};
        const corr = try conn.sendRequest(.metadata, 12, meta_body);
        const body = try conn.readResponse();
        try testing.expect(body.len >= 4);
        var r: Reader = .init(body);
        try testing.expectEqual(corr, try primitives.readI32(&r));
    } else |err| {
        return err;
    }
}

// --- mock broker (ztls server + SCRAM server side) ------------------------

// Self-signed ECDSA P-256 fixture (copied from ztls tests/fixtures; SAN =
// "ztls.server.test"). Fixture data, not a production credential.
const mock_server_name = "ztls.server.test";

const mock_cert_der = fixtureDecode(
    "MIIByzCCAXKgAwIBAgIUbmarzBd+vWR/mRLY6OMXZSPvVQQwCgYIKoZIzj0EAwIwGzEZMBcGA1UE" ++
        "AwwQenRscy5zZXJ2ZXIudGVzdDAeFw0yNjA2MDEwMTA3MzFaFw0zNjA1MjkwMTA3MzFaMBsxGTAX" ++
        "BgNVBAMMEHp0bHMuc2VydmVyLnRlc3QwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAATKaYKPBZrI" ++
        "1VlFanyJm97M16XUgwbBJkpHVern9TQuuQmG35VDsVMFA0FvUT6cigFaiHvB6NSZWczKYURvdTIl" ++
        "o4GTMIGQMB0GA1UdDgQWBBQVmks0H0iMK08RcQYrOzH1jLnV0jAfBgNVHSMEGDAWgBQVmks0H0iM" ++
        "K08RcQYrOzH1jLnV0jAMBgNVHRMBAf8EAjAAMA4GA1UdDwEB/wQEAwIHgDATBgNVHSUEDDAKBggr" ++
        "BgEFBQcDATAbBgNVHREEFDASghB6dGxzLnNlcnZlci50ZXN0MAoGCCqGSM49BAMCA0cAMEQCIE7r" ++
        "vZ4pcB7M69DXnXztJ3RKJzHRMZg/jvjL7Ad2t9wZAiB7s3wziFsMpfnXGN05V/q29wgFLilNG8YQ" ++
        "X6ssYxwWog==",
);
const mock_ecdsa_scalar = fixtureDecode("139HGdxmRe2N5F69cAY4IgK8B4ybwx0hgPE0siIOaeY=");

fn fixtureDecode(comptime b64: []const u8) [std.base64.standard.Decoder.calcSizeForSlice(b64) catch unreachable]u8 {
    const len = std.base64.standard.Decoder.calcSizeForSlice(b64) catch unreachable;
    var out: [len]u8 = undefined;
    std.base64.standard.Decoder.decode(&out, b64) catch unreachable;
    return out;
}

fn mockBrokerRun(ctx: *MockCtx) void {
    mockBrokerServe(ctx) catch |err| {
        ctx.result = err;
    };
}

fn mockBrokerServe(ctx: *MockCtx) !void {
    const conn = try ctx.server.accept();
    defer conn.stream.close();
    const stream = conn.stream;

    const server_keypair: ztls.x25519.KeyPair = .generate();
    var random: ztls.Random = undefined;
    std.crypto.random.bytes(&random.data);
    var hs: ztls.ServerHandshake = .init(.{
        .keypairs = .init(server_keypair),
        .random = random,
    });
    defer hs.deinit();

    const cert_der: []const u8 = &mock_cert_der;
    var signer: ztls.signature.PrivateKey = try .fromP256Scalar(mock_ecdsa_scalar[0..32]);
    defer signer.deinit();
    hs.setCredentials(&.{cert_der}, signer.signer());

    var storage: ztls.RecordBuffer.Storage = .empty;
    var rb: ztls.RecordBuffer = .init(&storage.buffer);
    var out: ztls.ServerHandshake.OutBuffer = .empty;
    var flight: ztls.ServerHandshake.FlightBuffer = .empty;

    var srv: MockServer = .{ .stream = stream, .hs = &hs, .out = &out };
    var req_storage: [64 * 1024]u8 = undefined;
    var req_buf: ResponseBuffer = .init(&req_storage);
    var scram_state: MockScramState = .{};

    // Unified loop: the same read path drives the TLS handshake and, once
    // connected, buffers decrypted application data into `req_buf`. TCP
    // coalescing routinely delivers the client Finished and its first
    // application-data record (the ApiVersions probe) in one read, so the
    // handshake and request phases cannot be split into separate loops.
    while (true) {
        const n = stream.read(rb.writable()) catch return;
        if (n == 0) return;
        rb.advance(n);
        while (try rb.next()) |record| switch (try hs.handleRecord(record, &out.buffer)) {
            .write => |w| {
                try stream.writeAll(w);
                hs.completeWrite();
                if (try hs.sendServerFlightBuffered(&flight)) |fb| {
                    try stream.writeAll(fb);
                    hs.completeWrite();
                }
            },
            .application_data => |data| {
                const dst = req_buf.writable();
                if (data.len > dst.len) return error.RequestTooLarge;
                @memcpy(dst[0..data.len], data);
                req_buf.advance(data.len);
            },
            .none => {},
            .closed => return,
            .key_update => {},
        };
        if (hs.isConnected() and ctx.mode == .close_after_tls) return;
        if (hs.isConnected()) {
            while (req_buf.next() catch return error.MalformedRequest) |frame| {
                try srv.dispatch(frame, ctx.mode, &scram_state);
            }
        }
    }
}

const MockScramState = struct {
    /// Full SCRAM nonce (client + server) advertised in server-first.
    full_nonce: [64]u8 = undefined,
    full_nonce_len: usize = 0,
    /// server-first message we sent (needed for AuthMessage on verify).
    server_first: [256]u8 = undefined,
    server_first_len: usize = 0,
    /// client-first-bare (n=...,r=...) captured for AuthMessage.
    client_first_bare: [256]u8 = undefined,
    client_first_bare_len: usize = 0,
};

const mock_salt = "kafka-zig-salt00"; // 16 bytes, fixed for determinism
const mock_iterations: u32 = 4096;

const MockServer = struct {
    stream: std.net.Stream,
    hs: *ztls.ServerHandshake,
    out: *ztls.ServerHandshake.OutBuffer,

    fn dispatch(self: *MockServer, frame: []const u8, mode: MockMode, st: *MockScramState) !void {
        var r: Reader = .init(frame);
        const api_key_raw = try primitives.readI16(&r);
        const api_version: u16 = @intCast(try primitives.readI16(&r));
        const correlation_id = try primitives.readI32(&r);
        const api_key = try ApiKey.fromU16(@intCast(api_key_raw));
        const hv = api_keys.headerVersion(api_key, api_version);
        // Skip request-header client_id + tag buffer.
        switch (hv.request) {
            1 => _ = try primitives.readNullableString(&r),
            2 => {
                _ = try primitives.readNullableCompactString(&r);
                try primitives.readTagBuffer(&r);
            },
            else => {},
        }
        const req_body = r.remaining();

        switch (api_key) {
            .api_versions => try self.sendApiVersionsV0(correlation_id),
            .sasl_handshake => try self.sendSaslHandshake(correlation_id, mode),
            .sasl_authenticate => try self.sendSaslAuthenticate(correlation_id, req_body, mode, st),
            .metadata => try self.sendMetadata(correlation_id),
            else => return error.UnexpectedApiKey,
        }
    }

    /// Build a response frame (length + response header + body) and encrypt it.
    fn respond(self: *MockServer, correlation_id: i32, response_header_version: u8, body: []const u8) !void {
        var buf: [64 * 1024]u8 = undefined;
        var w: std.Io.Writer = .fixed(buf[4..]);
        try primitives.writeI32(&w, correlation_id);
        if (response_header_version == 1) try primitives.writeEmptyTagBuffer(&w);
        try w.writeAll(body);
        const payload_len = w.buffered().len;
        std.mem.writeInt(i32, buf[0..4], @intCast(payload_len), .big);
        const frame = buf[0 .. 4 + payload_len];

        var off: usize = 0;
        while (off < frame.len) {
            const chunk = @min(frame.len - off, max_tls_plaintext);
            const rec = try self.hs.sendApplicationData(frame[off..][0..chunk], &self.out.buffer);
            try self.stream.writeAll(rec);
            self.hs.completeWrite();
            off += chunk;
        }
    }

    fn sendApiVersionsV0(self: *MockServer, correlation_id: i32) !void {
        // ApiVersions v0 response body: error_code INT16, api_keys ARRAY of
        // { api_key INT16, min_version INT16, max_version INT16 }. No throttle
        // in v0. Header v0.
        var body: [64]u8 = undefined;
        var w: std.Io.Writer = .fixed(&body);
        try primitives.writeI16(&w, 0); // error_code
        try primitives.writeArrayCount(&w, 2);
        try primitives.writeI16(&w, 18); // api_versions
        try primitives.writeI16(&w, 0);
        try primitives.writeI16(&w, 3);
        try primitives.writeI16(&w, 3); // metadata
        try primitives.writeI16(&w, 0);
        try primitives.writeI16(&w, 12);
        try self.respond(correlation_id, 0, w.buffered());
    }

    fn sendSaslHandshake(self: *MockServer, correlation_id: i32, mode: MockMode) !void {
        var body: [64]u8 = undefined;
        var w: std.Io.Writer = .fixed(&body);
        if (mode == .bad_mechanism) {
            // error_code 33 (UNSUPPORTED_SASL_MECHANISM), advertise SHA-256 only.
            try sasl.encodeHandshakeResponse(&w, .{
                .error_code = 33,
                .mechanisms = @constCast(&[_][]const u8{"SCRAM-SHA-256"}),
            });
        } else {
            try sasl.encodeHandshakeResponse(&w, .{
                .error_code = 0,
                .mechanisms = @constCast(&[_][]const u8{"SCRAM-SHA-512"}),
            });
        }
        try self.respond(correlation_id, 0, w.buffered());
    }

    fn sendSaslAuthenticate(
        self: *MockServer,
        correlation_id: i32,
        req_body: []const u8,
        mode: MockMode,
        st: *MockScramState,
    ) !void {
        // req_body = SaslAuthenticate v1 request: auth_bytes BYTES.
        var br: Reader = .init(req_body);
        const token = try primitives.readBytes(&br);

        if (st.full_nonce_len == 0) {
            try self.handleClientFirst(correlation_id, token, st);
        } else {
            try self.handleClientFinal(correlation_id, token, mode, st);
        }
    }

    fn handleClientFirst(self: *MockServer, correlation_id: i32, token: []const u8, st: *MockScramState) !void {
        // token = "n,,n=<user>,r=<cnonce>". bare = after "n,,".
        if (!std.mem.startsWith(u8, token, "n,,")) return error.MalformedScram;
        const bare = token["n,,".len..];
        @memcpy(st.client_first_bare[0..bare.len], bare);
        st.client_first_bare_len = bare.len;

        const cnonce = scramField(bare, "r=") orelse return error.MalformedScram;

        // full_nonce = cnonce ++ server_random(base64).
        var srv_rand: [12]u8 = undefined;
        std.crypto.random.bytes(&srv_rand);
        var srv_rand_b64: [16]u8 = undefined;
        _ = std.base64.standard.Encoder.encode(&srv_rand_b64, &srv_rand);
        @memcpy(st.full_nonce[0..cnonce.len], cnonce);
        @memcpy(st.full_nonce[cnonce.len..][0..srv_rand_b64.len], &srv_rand_b64);
        st.full_nonce_len = cnonce.len + srv_rand_b64.len;

        var salt_b64: [32]u8 = undefined;
        const salt_b64_len = std.base64.standard.Encoder.encode(&salt_b64, mock_salt).len;

        // server-first = "r=<full_nonce>,s=<salt_b64>,i=<iter>".
        var sfw: std.Io.Writer = .fixed(&st.server_first);
        try sfw.writeAll("r=");
        try sfw.writeAll(st.full_nonce[0..st.full_nonce_len]);
        try sfw.writeAll(",s=");
        try sfw.writeAll(salt_b64[0..salt_b64_len]);
        try sfw.print(",i={d}", .{mock_iterations});
        st.server_first_len = sfw.buffered().len;

        try self.sendSaslAuthResponse(correlation_id, 0, st.server_first[0..st.server_first_len]);
    }

    fn handleClientFinal(
        self: *MockServer,
        correlation_id: i32,
        token: []const u8,
        mode: MockMode,
        st: *MockScramState,
    ) !void {
        // token = "c=biws,r=<full_nonce>,p=<proof_b64>".
        const proof_idx = std.mem.indexOf(u8, token, ",p=") orelse return error.MalformedScram;
        const client_final_without_proof = token[0..proof_idx];
        const proof_b64 = token[proof_idx + ",p=".len ..];

        const ok = verifyClientProof(
            mock_password,
            st.client_first_bare[0..st.client_first_bare_len],
            st.server_first[0..st.server_first_len],
            client_final_without_proof,
            proof_b64,
        ) catch false;

        if (mode == .reject_proof or !ok) {
            // SASL_AUTHENTICATION_FAILED (58).
            try self.sendSaslAuthResponseWithError(correlation_id, 58, "authentication failed");
            return;
        }

        // server-final = "v=<base64 ServerSignature>".
        var server_sig: [64]u8 = undefined;
        computeServerSignature(
            mock_password,
            st.client_first_bare[0..st.client_first_bare_len],
            st.server_first[0..st.server_first_len],
            client_final_without_proof,
            &server_sig,
        );
        var v_msg: [128]u8 = undefined;
        var vw: std.Io.Writer = .fixed(&v_msg);
        try vw.writeAll("v=");
        var sig_b64: [88]u8 = undefined;
        const nb = std.base64.standard.Encoder.encode(&sig_b64, &server_sig).len;
        try vw.writeAll(sig_b64[0..nb]);
        try self.sendSaslAuthResponse(correlation_id, 0, vw.buffered());
    }

    fn sendSaslAuthResponse(self: *MockServer, correlation_id: i32, error_code: i16, auth_bytes: []const u8) !void {
        var body: [512]u8 = undefined;
        var w: std.Io.Writer = .fixed(&body);
        try sasl.encodeAuthenticateResponse(&w, .{
            .error_code = error_code,
            .error_message = null,
            .auth_bytes = auth_bytes,
            .session_lifetime_ms = 0,
        });
        try self.respond(correlation_id, 0, w.buffered());
    }

    fn sendSaslAuthResponseWithError(self: *MockServer, correlation_id: i32, error_code: i16, msg: []const u8) !void {
        var body: [256]u8 = undefined;
        var w: std.Io.Writer = .fixed(&body);
        try sasl.encodeAuthenticateResponse(&w, .{
            .error_code = error_code,
            .error_message = msg,
            .auth_bytes = "",
            .session_lifetime_ms = 0,
        });
        try self.respond(correlation_id, 0, w.buffered());
    }

    fn sendMetadata(self: *MockServer, correlation_id: i32) !void {
        // Canned, minimal body. The dial test does not decode it — it only
        // proves the post-auth request/response round-trip and correlation id.
        // Metadata v12 response uses header v1 (correlation_id + tag buffer).
        const body = [_]u8{ 0x00, 0x00, 0x00, 0x00 }; // throttle_time_ms = 0 (rest omitted)
        try self.respond(correlation_id, 1, &body);
    }
};

/// Extract the value of a SCRAM field `key=` up to the next `,` (or end).
fn scramField(msg: []const u8, key: []const u8) ?[]const u8 {
    const start = std.mem.indexOf(u8, msg, key) orelse return null;
    const after = msg[start + key.len ..];
    const end = std.mem.indexOfScalar(u8, after, ',') orelse after.len;
    return after[0..end];
}

const HmacSha512 = std.crypto.auth.hmac.sha2.HmacSha512;
const Sha512 = std.crypto.hash.sha2.Sha512;

/// SCRAM server-side proof check (RFC 5802 §3): recover ClientKey from the
/// proof and confirm H(ClientKey) == StoredKey.
fn verifyClientProof(
    password: []const u8,
    client_first_bare: []const u8,
    server_first: []const u8,
    client_final_without_proof: []const u8,
    proof_b64: []const u8,
) !bool {
    var salted: [64]u8 = undefined;
    try std.crypto.pwhash.pbkdf2(&salted, password, mock_salt, mock_iterations, HmacSha512);

    var client_key: [64]u8 = undefined;
    HmacSha512.create(&client_key, "Client Key", &salted);
    var stored_key: [64]u8 = undefined;
    Sha512.hash(&client_key, &stored_key, .{});

    var auth_message: [1024]u8 = undefined;
    const am = buildAuthMessage(&auth_message, client_first_bare, server_first, client_final_without_proof);

    var client_sig: [64]u8 = undefined;
    HmacSha512.create(&client_sig, am, &stored_key);

    // ClientProof = ClientKey XOR ClientSignature → ClientKey = proof XOR sig.
    var proof: [64]u8 = undefined;
    const dec_len = std.base64.standard.Decoder.calcSizeForSlice(proof_b64) catch return false;
    if (dec_len != 64) return false;
    std.base64.standard.Decoder.decode(&proof, proof_b64) catch return false;

    var recovered_client_key: [64]u8 = undefined;
    for (0..64) |i| recovered_client_key[i] = proof[i] ^ client_sig[i];

    var recovered_stored_key: [64]u8 = undefined;
    Sha512.hash(&recovered_client_key, &recovered_stored_key, .{});

    return std.crypto.timing_safe.eql([64]u8, recovered_stored_key, stored_key);
}

fn computeServerSignature(
    password: []const u8,
    client_first_bare: []const u8,
    server_first: []const u8,
    client_final_without_proof: []const u8,
    out: *[64]u8,
) void {
    var salted: [64]u8 = undefined;
    std.crypto.pwhash.pbkdf2(&salted, password, mock_salt, mock_iterations, HmacSha512) catch unreachable;
    var server_key: [64]u8 = undefined;
    HmacSha512.create(&server_key, "Server Key", &salted);
    var auth_message: [1024]u8 = undefined;
    const am = buildAuthMessage(&auth_message, client_first_bare, server_first, client_final_without_proof);
    HmacSha512.create(out, am, &server_key);
}

fn buildAuthMessage(
    buf: []u8,
    client_first_bare: []const u8,
    server_first: []const u8,
    client_final_without_proof: []const u8,
) []const u8 {
    var w: std.Io.Writer = .fixed(buf);
    w.writeAll(client_first_bare) catch unreachable;
    w.writeAll(",") catch unreachable;
    w.writeAll(server_first) catch unreachable;
    w.writeAll(",") catch unreachable;
    w.writeAll(client_final_without_proof) catch unreachable;
    return w.buffered();
}
