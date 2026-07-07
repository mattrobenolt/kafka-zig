//! In-process mock Kafka broker for the phase-6 producer integration tests
//! (PLAN §5.2). Speaks the protocol with our own `wire` codec (dogfooded) over
//! a real ztls TLS 1.3 server on loopback:
//!
//!   - ApiVersions v0 pre-auth probe → advertise our target versions.
//!   - SaslHandshake v1 + SaslAuthenticate v1 → real SCRAM-SHA-512 server side.
//!   - Metadata v12 → a fixed 1-broker cluster (itself), N partitions,
//!     all led by node 1.
//!   - Produce v9 → ack the batches, optionally injecting a retriable error on
//!     the first Produce per partition, or a fatal error, per `Mode`.
//!
//! Deterministic, single connection, sequential request/response. The TLS +
//! SCRAM server code is adapted from the phase-5 `Connection.zig` mock.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ztls = @import("ztls");
const wire = @import("../wire/root.zig");
const primitives = wire.primitives;
const api_keys = wire.api_keys;
const sasl = wire.sasl;
const metadata = wire.metadata;
const produce = wire.produce;
const ApiKey = api_keys.ApiKey;
const Reader = primitives.Reader;
const ResponseBuffer = wire.ResponseBuffer;

pub const username = "kafka-zig-test";
pub const password = "correct-horse-battery-staple";
pub const server_name = "ztls.server.test";

const topic_name = "events";
const max_partitions = 64;
const max_tls_plaintext = 1 << 14;

pub const Mode = enum {
    /// Ack every Produce.
    ok,
    /// Return NOT_LEADER_OR_FOLLOWER (6) on the first Produce per partition,
    /// then ack subsequent attempts.
    retriable_once,
    /// Return INVALID_RECORD (87) for every Produce (non-retriable).
    fatal,
};

pub const Options = struct {
    mode: Mode = .ok,
    num_partitions: u32 = 4,
};

pub const Broker = struct {
    allocator: Allocator,
    server: std.net.Server,
    port: u16,
    options: Options,
    thread: std.Thread = undefined,
    /// Records the broker acked (error_code 0). The test asserts no loss.
    produced: std.atomic.Value(u32) = .init(0),
    /// Metadata requests served (the retry test asserts a refresh happened).
    metadata_requests: std.atomic.Value(u32) = .init(0),
    /// Per-partition "already injected a retriable error" flags (mock thread
    /// only; no synchronization needed).
    injected: [max_partitions]bool = @splat(false),
    /// Set by `stop()` to signal the run loop to exit after the next accept.
    shutdown: std.atomic.Value(bool) = .init(false),

    pub fn start(allocator: Allocator, options: Options) !*Broker {
        std.debug.assert(options.num_partitions <= max_partitions);
        const addr = try std.net.Address.parseIp("127.0.0.1", 0);
        const self = try allocator.create(Broker);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .server = try addr.listen(.{ .reuse_address = true }),
            .port = 0,
            .options = options,
        };
        self.port = self.server.listen_address.getPort();
        self.thread = try std.Thread.spawn(.{}, run, .{self});
        return self;
    }

    pub fn stop(self: *Broker) void {
        // Signal the broker thread to exit, then unblock its `accept()` by
        // connecting to ourselves. The run loop checks `shutdown` between
        // accept attempts and exits when set. We join BEFORE deinit to avoid
        // closing the listening fd while the thread is blocked in `accept()` —
        // on macOS that causes `accept` to return EBADF, which the stdlib
        // treats as `unreachable`.
        self.shutdown.store(true, .release);
        var wake_stream = std.net.tcpConnectToAddress(self.server.listen_address) catch null;
        if (wake_stream) |*ws| ws.close();
        self.thread.join();
        self.server.deinit();
        self.allocator.destroy(self);
    }

    fn run(self: *Broker) void {
        while (!self.shutdown.load(.acquire)) {
            const conn = self.server.accept() catch |err| switch (err) {
                error.SocketNotListening, error.ConnectionAborted => continue,
                else => continue,
            };
            if (self.shutdown.load(.acquire)) {
                conn.stream.close();
                return;
            }
            self.serve(conn.stream) catch |err| {
                std.debug.print("mock broker serve error: {s}\n", .{@errorName(err)});
            };
            conn.stream.close();
        }
    }

    fn serve(self: *Broker, stream: std.net.Stream) !void {
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

        var sess: Session = .{ .broker = self, .stream = stream, .hs = &hs, .out = &out };
        var req_storage: [128 * 1024]u8 = undefined;
        var req_buf: ResponseBuffer = .init(&req_storage);
        var scram_state: ScramState = .{};

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
            if (hs.isConnected()) {
                while (req_buf.next() catch return error.MalformedRequest) |frame| {
                    try sess.dispatch(frame, &scram_state);
                }
            }
        }
    }
};

const Session = struct {
    broker: *Broker,
    stream: std.net.Stream,
    hs: *ztls.ServerHandshake,
    out: *ztls.ServerHandshake.OutBuffer,

    fn dispatch(self: *Session, frame: []const u8, st: *ScramState) !void {
        var r: Reader = .init(frame);
        const api_key_raw = try primitives.readI16(&r);
        const api_version: u16 = @intCast(try primitives.readI16(&r));
        const correlation_id = try primitives.readI32(&r);
        const api_key = try ApiKey.fromU16(@intCast(api_key_raw));
        const hv = api_keys.headerVersion(api_key, api_version);
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
            .sasl_handshake => try self.sendSaslHandshake(correlation_id),
            .sasl_authenticate => try self.sendSaslAuthenticate(correlation_id, req_body, st),
            .metadata => try self.sendMetadata(correlation_id),
            .produce => try self.sendProduce(correlation_id, req_body),
            else => return error.UnexpectedApiKey,
        }
    }

    /// Build a response frame (length + response header + body) and encrypt it.
    fn respond(self: *Session, correlation_id: i32, response_header_version: u8, body: []const u8) !void {
        var buf: [128 * 1024]u8 = undefined;
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

    fn sendApiVersionsV0(self: *Session, correlation_id: i32) !void {
        var body: [64]u8 = undefined;
        var w: std.Io.Writer = .fixed(&body);
        try primitives.writeI16(&w, 0); // error_code
        try primitives.writeArrayCount(&w, 3);
        try primitives.writeI16(&w, 0); // produce
        try primitives.writeI16(&w, 0);
        try primitives.writeI16(&w, 9);
        try primitives.writeI16(&w, 3); // metadata
        try primitives.writeI16(&w, 0);
        try primitives.writeI16(&w, 12);
        try primitives.writeI16(&w, 18); // api_versions
        try primitives.writeI16(&w, 0);
        try primitives.writeI16(&w, 3);
        try self.respond(correlation_id, 0, w.buffered());
    }

    fn sendSaslHandshake(self: *Session, correlation_id: i32) !void {
        var body: [64]u8 = undefined;
        var w: std.Io.Writer = .fixed(&body);
        try sasl.encodeHandshakeResponse(&w, .{
            .error_code = 0,
            .mechanisms = @constCast(&[_][]const u8{"SCRAM-SHA-512"}),
        });
        try self.respond(correlation_id, 0, w.buffered());
    }

    fn sendMetadata(self: *Session, correlation_id: i32) !void {
        _ = self.broker.metadata_requests.fetchAdd(1, .acq_rel);

        var brokers = [_]metadata.Response.Broker{.{
            .node_id = 1,
            .host = "127.0.0.1",
            .port = @intCast(self.broker.port),
            .rack = null,
        }};
        var partitions: [max_partitions]metadata.Response.Partition = undefined;
        var one = [_]i32{1};
        const np = self.broker.options.num_partitions;
        for (0..np) |i| {
            partitions[i] = .{
                .error_code = 0,
                .partition_index = @intCast(i),
                .leader_id = 1,
                .leader_epoch = 0,
                .replica_nodes = &one,
                .isr_nodes = &one,
                .offline_replicas = &.{},
            };
        }
        var topics = [_]metadata.Response.Topic{.{
            .error_code = 0,
            .name = topic_name,
            .topic_id = @splat(0),
            .is_internal = false,
            .partitions = partitions[0..np],
            .topic_authorized_operations = 0,
        }};

        var body: [8 * 1024]u8 = undefined;
        var w: std.Io.Writer = .fixed(&body);
        try metadata.encodeResponse(&w, .{
            .throttle_time_ms = 0,
            .brokers = &brokers,
            .cluster_id = null,
            .controller_id = 1,
            .topics = &topics,
        });
        try self.respond(correlation_id, 1, w.buffered());
    }

    fn sendProduce(self: *Session, correlation_id: i32, req_body: []const u8) !void {
        var r: Reader = .init(req_body);
        _ = try primitives.readNullableCompactString(&r); // transactional_id
        _ = try primitives.readI16(&r); // acks
        _ = try primitives.readI32(&r); // timeout_ms

        const topic_count = (try primitives.readCompactArrayCount(&r)) orelse return error.Malformed;

        var pr_scratch: [max_partitions]produce.PartitionResponse = undefined;
        var tr_scratch: [8]produce.TopicResponse = undefined;
        var tr_len: usize = 0;

        for (0..topic_count) |_| {
            const name = try primitives.readCompactString(&r);
            const part_count = (try primitives.readCompactArrayCount(&r)) orelse return error.Malformed;
            var pr_len: usize = 0;
            for (0..part_count) |_| {
                const index = try primitives.readI32(&r);
                const records = (try primitives.readNullableCompactBytes(&r)) orelse &.{};
                try primitives.readTagBuffer(&r); // partition tag buffer

                const record_count = recordsCount(records);
                const err = self.produceError(index);
                if (err == 0) _ = self.broker.produced.fetchAdd(record_count, .acq_rel);

                pr_scratch[pr_len] = .{
                    .index = index,
                    .error_code = err,
                    .base_offset = 0,
                    .log_append_time_ms = -1,
                    .log_start_offset = 0,
                    .record_errors = &.{},
                    .error_message = if (err == 0) null else "mock injected",
                };
                pr_len += 1;
            }
            try primitives.readTagBuffer(&r); // topic tag buffer
            tr_scratch[tr_len] = .{ .name = name, .partition_responses = pr_scratch[0..pr_len] };
            tr_len += 1;
        }

        var body: [16 * 1024]u8 = undefined;
        var w: std.Io.Writer = .fixed(&body);
        try produce.encodeResponse(&w, .{ .responses = tr_scratch[0..tr_len], .throttle_time_ms = 0 });
        try self.respond(correlation_id, 1, w.buffered());
    }

    /// The error code to return for a partition under the broker's mode.
    fn produceError(self: *Session, partition: i32) i16 {
        const idx: usize = @intCast(partition);
        return switch (self.broker.options.mode) {
            .ok => 0,
            .fatal => 87, // INVALID_RECORD
            .retriable_once => blk: {
                if (idx < max_partitions and !self.broker.injected[idx]) {
                    self.broker.injected[idx] = true;
                    break :blk 6; // NOT_LEADER_OR_FOLLOWER
                }
                break :blk 0;
            },
        };
    }

    // --- SCRAM-SHA-512 server side (adapted from Connection.zig mock) ------

    fn sendSaslAuthenticate(self: *Session, correlation_id: i32, req_body: []const u8, st: *ScramState) !void {
        var br: Reader = .init(req_body);
        const token = try primitives.readBytes(&br);
        if (st.full_nonce_len == 0) {
            try self.handleClientFirst(correlation_id, token, st);
        } else {
            try self.handleClientFinal(correlation_id, token, st);
        }
    }

    fn handleClientFirst(self: *Session, correlation_id: i32, token: []const u8, st: *ScramState) !void {
        if (!std.mem.startsWith(u8, token, "n,,")) return error.MalformedScram;
        const bare = token["n,,".len..];
        @memcpy(st.client_first_bare[0..bare.len], bare);
        st.client_first_bare_len = bare.len;

        const cnonce = scramField(bare, "r=") orelse return error.MalformedScram;
        var srv_rand: [12]u8 = undefined;
        std.crypto.random.bytes(&srv_rand);
        var srv_rand_b64: [16]u8 = undefined;
        _ = std.base64.standard.Encoder.encode(&srv_rand_b64, &srv_rand);
        @memcpy(st.full_nonce[0..cnonce.len], cnonce);
        @memcpy(st.full_nonce[cnonce.len..][0..srv_rand_b64.len], &srv_rand_b64);
        st.full_nonce_len = cnonce.len + srv_rand_b64.len;

        var salt_b64: [32]u8 = undefined;
        const salt_b64_len = std.base64.standard.Encoder.encode(&salt_b64, mock_salt).len;

        var sfw: std.Io.Writer = .fixed(&st.server_first);
        try sfw.writeAll("r=");
        try sfw.writeAll(st.full_nonce[0..st.full_nonce_len]);
        try sfw.writeAll(",s=");
        try sfw.writeAll(salt_b64[0..salt_b64_len]);
        try sfw.print(",i={d}", .{mock_iterations});
        st.server_first_len = sfw.buffered().len;

        try self.sendSaslAuthResponse(correlation_id, 0, st.server_first[0..st.server_first_len]);
    }

    fn handleClientFinal(self: *Session, correlation_id: i32, token: []const u8, st: *ScramState) !void {
        const proof_idx = std.mem.indexOf(u8, token, ",p=") orelse return error.MalformedScram;
        const client_final_without_proof = token[0..proof_idx];
        const proof_b64 = token[proof_idx + ",p=".len ..];

        const ok = verifyClientProof(
            st.client_first_bare[0..st.client_first_bare_len],
            st.server_first[0..st.server_first_len],
            client_final_without_proof,
            proof_b64,
        ) catch false;
        if (!ok) {
            try self.sendSaslAuthResponseWithError(correlation_id, 58, "authentication failed");
            return;
        }

        var server_sig: [64]u8 = undefined;
        computeServerSignature(
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

    fn sendSaslAuthResponse(self: *Session, correlation_id: i32, error_code: i16, auth_bytes: []const u8) !void {
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

    fn sendSaslAuthResponseWithError(self: *Session, correlation_id: i32, error_code: i16, msg: []const u8) !void {
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
};

const ScramState = struct {
    full_nonce: [64]u8 = undefined,
    full_nonce_len: usize = 0,
    server_first: [256]u8 = undefined,
    server_first_len: usize = 0,
    client_first_bare: [256]u8 = undefined,
    client_first_bare_len: usize = 0,
};

const mock_salt = "kafka-zig-salt00"; // 16 bytes, fixed for determinism
const mock_iterations: u32 = 4096;

/// Read the v2 record batch `recordsCount` field (offset 57). Returns 0 for a
/// truncated / empty batch.
fn recordsCount(records: []const u8) u32 {
    if (records.len < 61) return 0;
    const c = std.mem.readInt(i32, records[57..][0..4], .big);
    return if (c < 0) 0 else @intCast(c);
}

fn scramField(msg: []const u8, key: []const u8) ?[]const u8 {
    const start = std.mem.indexOf(u8, msg, key) orelse return null;
    const after = msg[start + key.len ..];
    const end = std.mem.indexOfScalar(u8, after, ',') orelse after.len;
    return after[0..end];
}

const HmacSha512 = std.crypto.auth.hmac.sha2.HmacSha512;
const Sha512 = std.crypto.hash.sha2.Sha512;

fn verifyClientProof(
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

// Self-signed ECDSA P-256 fixture (SAN = "ztls.server.test"), copied from the
// phase-5 Connection.zig mock. Test fixture, not a production credential.
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
