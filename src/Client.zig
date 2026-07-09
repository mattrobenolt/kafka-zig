//! The public producer API (PLAN §3).
//!
//! `Client.init` spawns the network thread (a `Producer` draining the `Ring`)
//! and returns a handle. Submission is slot-first: `acquire` a `Message`, write
//! directly into its inline buffers, `commit`, then `await` the broker ack.
//! The ring owns the payload, so there is no borrowed-buffer lifetime for the
//! caller to manage — once `commit` returns, the slot is the network thread's
//! problem.
//!
//! ```
//! var client = try Client.init(allocator, .{ .bootstrap_brokers = ..., ... });
//! defer client.deinit();
//! var m = try client.acquire();     // blocks when the ring is full (backpressure)
//! try m.setTopic("events");
//! m.setPartition(null);             // null → partitioner decides at batch time
//! const dst = m.value();
//! @memcpy(dst[0..payload.len], payload);
//! try m.commit(payload.len);
//! try m.await();                    // blocks until ack; error on failure
//! ```
//!
//! Config string fields (`bootstrap_brokers` hosts, `tls.sni`, credentials,
//! `client_id`) are copied into client-owned storage at `init`; the caller may
//! free them the moment `init` returns. `tls.ca_bundle`, when set, is borrowed
//! for the client's lifetime (ztls holds it per connection).

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const Ring = @import("Ring.zig");
const Connection = @import("Connection.zig");
const Producer = @import("Producer.zig");
const partitioner = @import("partitioner.zig");
const record_batch = @import("wire/record_batch.zig");
const build_options = @import("build_options");

/// Record-batch compression for produced batches. `.zstd` requires building
/// with `-Dzstd=true`; requesting it otherwise is rejected at `init` with
/// `error.CompressionUnavailable`.
pub const Compression = record_batch.Compression;

const Client = @This();

/// A producer message handle: the ring's `Message`, re-exported.
pub const Message = Ring.Message;
pub const Error = Ring.Error;
pub const Strategy = partitioner.Strategy;

/// A bootstrap broker endpoint.
pub const Broker = struct {
    host: []const u8,
    port: u16,
};

/// `acks` semantics (Produce request `acks` field).
pub const Acks = enum(i16) {
    /// Fire-and-forget: the broker sends no response; `await` returns as soon
    /// as the request is on the wire.
    none = 0,
    /// The leader acks after writing to its local log.
    leader = 1,
    /// The leader acks after all in-sync replicas have the record (default).
    all = -1,
};

pub const Credentials = struct {
    username: []const u8,
    password: []const u8,
};

/// SASL/SCRAM mechanism + credentials. MSK is SHA-512 only (PLAN §9).
pub const Sasl = union(enum) {
    scram_sha256: Credentials,
    scram_sha512: Credentials,
};

pub const Tls = struct {
    /// SNI hostname sent on every connection (must match the broker leaf cert).
    /// When null, each connection uses its own broker host as SNI.
    sni: ?[]const u8 = null,
    /// CA bundle for chain verification. Caller-owned; must outlive the client.
    ca_bundle: ?*const std.crypto.Certificate.Bundle = null,
    /// Test/dev only: skip chain-anchor verification (still verifies the
    /// CertificateVerify signature and hostname). Never set against a real broker.
    insecure_skip_verify: bool = false,
};

pub const Config = struct {
    bootstrap_brokers: []const Broker,
    tls: Tls,
    sasl: Sasl,
    acks: Acks = .all,
    /// Backoff (ms) between drains that made no forward progress. (linger_ms in
    /// PLAN §3; the current drain sends eagerly, so this is a retry backoff
    /// rather than a batch-accumulation timer — see the follow-up note in the
    /// slice report.)
    linger_ms: u32 = 50,
    max_batch_bytes: u32 = 16 * 1024,
    max_message_size: u32 = 16 * 1024,
    max_key_len: u16 = 256,
    max_topic_len: u8 = 128,
    /// Ring slot count (rounded up to a power of two). Reserved memory is
    /// `ring_slots × (max_topic_len + max_key_len + max_message_size)`.
    ring_slots: u32 = 8192,
    partitioner: Strategy = .default,
    client_id: ?[]const u8 = null,
    /// Produce request timeout (ms) sent to the broker.
    request_timeout_ms: i32 = 30_000,
    /// Socket I/O timeout (ms) for broker reads/writes (SO_RCVTIMEO +
    /// SO_SNDTIMEO). A stalled broker hits this and triggers reconnect.
    io_timeout_ms: u32 = 30_000,
    max_retries: u8 = 8,
    /// Record-batch compression. `.none` (default) sends plaintext batches;
    /// `.zstd` compresses each batch (requires `-Dzstd=true` at build time).
    compression: Compression = .none,
    /// When true (production default), the producer acquires a PID/epoch at
    /// startup via InitProducerId v4, tracks per-partition sequence numbers,
    /// and sends real producer_id/producer_epoch/base_sequence in record
    /// batches. When false, uses the -1 sentinels (non-idempotent mode).
    enable_idempotency: bool = true,
};

allocator: Allocator,
/// Arena for client-owned copies of config strings.
arena: std.heap.ArenaAllocator,
ring: Ring,
producer: Producer,
thread: std.Thread,

/// Initialize the client and spawn the network thread. Copies all config
/// strings into client-owned storage; the caller may free the config's slices
/// as soon as this returns.
pub fn init(allocator: Allocator, config: Config) !*Client {
    assert(config.bootstrap_brokers.len > 0);

    const self = try allocator.create(Client);
    errdefer allocator.destroy(self);

    self.allocator = allocator;
    self.arena = .init(allocator);
    errdefer self.arena.deinit();
    const arena = self.arena.allocator();

    // Copy config strings into arena-owned storage.
    const bootstrap = try arena.alloc(Producer.Broker, config.bootstrap_brokers.len);
    for (config.bootstrap_brokers, bootstrap) |src, *dst| {
        dst.* = .{ .host = try arena.dupe(u8, src.host), .port = src.port };
    }
    const sni: ?[]const u8 = if (config.tls.sni) |s| try arena.dupe(u8, s) else null;
    const client_id: ?[]const u8 = if (config.client_id) |c| try arena.dupe(u8, c) else null;
    const scram = try dupScram(arena, config.sasl);

    self.ring = try Ring.init(allocator, .{
        .max_message_size = config.max_message_size,
        .max_key_len = config.max_key_len,
        .max_topic_len = config.max_topic_len,
        .num_slots = config.ring_slots,
    });
    errdefer self.ring.deinit(allocator);

    // zstd compression requires the library to be built with -Dzstd=true.
    // Reject at init rather than failing per-batch at runtime.
    if (config.compression == .zstd and
        !build_options.zstd_enabled)
    {
        return error.CompressionUnavailable;
    }

    self.producer = try Producer.init(allocator, &self.ring, .{
        .bootstrap = bootstrap,
        .sni = sni,
        .ca_bundle = config.tls.ca_bundle,
        .insecure_skip_verify = config.tls.insecure_skip_verify,
        .scram = scram,
        .client_id = client_id,
        .acks = @intFromEnum(config.acks),
        .timeout_ms = config.request_timeout_ms,
        .max_batch_bytes = config.max_batch_bytes,
        .max_message_size = config.max_message_size,
        .strategy = config.partitioner,
        .max_retries = config.max_retries,
        .retry_backoff_ns = @as(u64, config.linger_ms) * std.time.ns_per_ms,
        .io_timeout_ms = config.io_timeout_ms,
        .compression = config.compression,
        .enable_idempotency = config.enable_idempotency,
    });
    errdefer self.producer.deinit();

    self.thread = try std.Thread.spawn(.{}, Producer.run, .{&self.producer});
    return self;
}

/// Shut down: signal the ring, join the network thread, close connections, and
/// free everything. Any `Message` still in flight is abandoned (its `await`/// returns `error.Shutdown`).
pub fn deinit(self: *Client) void { // ziglint-ignore: Z030 -- self is heap-owned, destroyed here
    self.ring.requestShutdown();
    self.thread.join();
    // Zeroize the SCRAM credentials (username/password) that were duped into
    // the arena, so they don't linger in freed heap for the process lifetime.
    // Must run BEFORE producer.deinit (which sets self.* = undefined, wiping
    // the scram slice pointers) and before arena.deinit (which frees the
    // backing memory the slices point into).
    const scram = self.producer.options.scram;
    // Cast away const: the arena owns mutable memory; the slices are `[]const
    // u8` only because ScramConfig declares them so. secureZero needs
    // `[]volatile u8` to prevent the store from being optimized out.
    std.crypto.secureZero(u8, @as([*]u8, @ptrCast(@constCast(scram.username.ptr)))[0..scram.username.len]);
    std.crypto.secureZero(u8, @as([*]u8, @ptrCast(@constCast(scram.password.ptr)))[0..scram.password.len]);
    self.producer.deinit();
    self.ring.deinit(self.allocator);
    self.arena.deinit();
    const allocator = self.allocator;
    allocator.destroy(self);
}

/// Acquire a slot, blocking (futex) while the ring is full — the backpressure
/// path. Returns `error.Shutdown` if the client is tearing down.
pub fn acquire(self: *Client) Error!Message {
    return self.ring.acquire();
}

/// Non-blocking `acquire`: `error.WouldBlock` when the ring is full.
pub fn tryAcquire(self: *Client) Error!Message {
    return self.ring.tryAcquire();
}

fn dupScram(arena: Allocator, sasl: Sasl) !Connection.ScramConfig {
    return switch (sasl) {
        .scram_sha256 => |c| .{
            .mechanism = .scram_sha256,
            .username = try arena.dupe(u8, c.username),
            .password = try arena.dupe(u8, c.password),
        },
        .scram_sha512 => |c| .{
            .mechanism = .scram_sha512,
            .username = try arena.dupe(u8, c.username),
            .password = try arena.dupe(u8, c.password),
        },
    };
}

// ===========================================================================
// Tests — in-process mock broker (ztls + SCRAM + Metadata v12 + Produce v9)
// ===========================================================================

test {
    _ = @import("testing/mock_broker.zig");
    std.testing.refAllDecls(@This());
}

const mock = @import("testing/mock_broker.zig");
const testing = std.testing;

fn testConfig(bootstrap: []const Broker) Config {
    return .{
        .bootstrap_brokers = bootstrap,
        .tls = .{ .sni = mock.server_name, .insecure_skip_verify = true },
        .sasl = .{ .scram_sha512 = .{ .username = mock.username, .password = mock.password } },
        .acks = .all,
        .linger_ms = 2,
        .max_message_size = 1024,
        .max_key_len = 64,
        .max_topic_len = 64,
        .ring_slots = 64,
        .partitioner = .default,
        .client_id = "kafka-zig-test",
        .enable_idempotency = true,
    };
}

test "produce N messages: all ack, no loss" {
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 4 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var client = try Client.init(testing.allocator, testConfig(&bootstrap));
    defer client.deinit();

    const n = 50;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(null);
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "k{d}", .{i});
        try m.setKey(key);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "msg-{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&handles) |*m| try m.await();

    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
}

test "retriable error on first produce, then ack after retry + metadata refresh" {
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .retriable_once,
        .num_partitions = 4,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var client = try Client.init(testing.allocator, testConfig(&bootstrap));
    defer client.deinit();

    const n = 20;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        // Explicit partition so every partition sees the inject-once path.
        m.setPartition(@intCast(i % 4));
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "m{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    // All must eventually ack despite the first Produce per partition failing
    // with NOT_LEADER_OR_FOLLOWER.
    for (&handles) |*m| try m.await();

    try testing.expect(broker.metadata_requests.load(.acquire) >= 2); // initial + refresh
}

test "non-retriable error surfaces error.SendFailed" {
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .fatal,
        .num_partitions = 1,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var client = try Client.init(testing.allocator, testConfig(&bootstrap));
    defer client.deinit();

    var m = try client.acquire();
    try m.setTopic("events");
    m.setPartition(0);
    const dst = m.value();
    @memcpy(dst[0..3], "bad");
    try m.commit(3);

    try testing.expectError(error.SendFailed, m.await());
    try testing.expectEqual(@as(u32, 87), m.failureCode()); // INVALID_RECORD
}

// ---------------------------------------------------------------------------
// No-hot-path-alloc: a counting allocator wrapper to verify the produce path
// doesn't allocate from the general allocator after cold-path setup is done.
// ---------------------------------------------------------------------------

const CountingAllocator = struct {
    backing: std.mem.Allocator,
    alloc_count: u64 = 0,
    resize_count: u64 = 0,
    free_count: u64 = 0,

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn alloc(
        ctx: *anyopaque,
        len: usize,
        alignment: std.mem.Alignment,
        ra: usize,
    ) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.alloc_count += 1;
        return self.backing.rawAlloc(len, alignment, ra);
    }

    fn resize(
        ctx: *anyopaque,
        buf: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ra: usize,
    ) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.resize_count += 1;
        return self.backing.rawResize(buf, alignment, new_len, ra);
    }

    fn remap(
        ctx: *anyopaque,
        buf: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ra: usize,
    ) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        return self.backing.rawRemap(buf, alignment, new_len, ra);
    }

    fn free(
        ctx: *anyopaque,
        buf: []u8,
        alignment: std.mem.Alignment,
        ra: usize,
    ) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.free_count += 1;
        self.backing.rawFree(buf, alignment, ra);
    }
};

test "produce hot path: zero general-allocator allocs after cold-path setup" {
    // The first produce triggers cold-path allocations: metadata refresh (cache
    // rebuild), connection dial (once per broker), and the per-topic round-robin
    // key (once per topic). After that, subsequent produces to the same broker
    // should allocate zero bytes from the general allocator — the produce
    // response decode is backed by the FBA scratch buffer, not self.allocator.
    var counter: CountingAllocator = .{ .backing = testing.allocator };
    const alloc = counter.allocator();

    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 4 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var client = try Client.init(alloc, testConfig(&bootstrap));
    defer client.deinit();

    // First batch: triggers metadata + dial + RR key (cold path).
    var m1 = try client.acquire();
    try m1.setTopic("events");
    m1.setPartition(0);
    const dst1 = m1.value();
    @memcpy(dst1[0..4], "cold");
    try m1.commit(4);
    try m1.await();

    const allocs_after_cold = counter.alloc_count;

    // Second batch: same broker, same topic — should be zero general-allocator
    // allocs. The FBA absorbs the produce response decode.
    var m2 = try client.acquire();
    try m2.setTopic("events");
    m2.setPartition(0);
    const dst2 = m2.value();
    @memcpy(dst2[0..4], "hot0");
    try m2.commit(4);
    try m2.await();

    try testing.expectEqual(allocs_after_cold, counter.alloc_count);

    // Third batch to be sure.
    var m3 = try client.acquire();
    try m3.setTopic("events");
    m3.setPartition(0);
    const dst3 = m3.value();
    @memcpy(dst3[0..4], "hot1");
    try m3.commit(4);
    try m3.await();

    try testing.expectEqual(allocs_after_cold, counter.alloc_count);
}

test "max_batch_bytes splits batches across drains: all eventually ack" {
    // Set max_batch_bytes to a value that fits one small record batch (~71 B
    // for a 1-byte key + 2-byte value) but not two. Messages go to two
    // partitions with the same leader, so they form two separate batches.
    // The first drain must send one batch (<= max_batch_bytes) and leave the
    // rest pending; a subsequent drain sends them. All eventually ack.
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 4 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.max_batch_bytes = 100; // fits one ~71 B batch, not two (~142 B)
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    // Produce to two partitions (same leader) so each forms its own batch.
    const n = 4;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(@intCast(i % 2)); // partitions 0 and 1
        try m.setKey("k");
        const dst = m.value();
        @memcpy(dst[0..2], "m0");
        try m.commit(2);
    }
    for (&handles) |*m| try m.await();

    // Every message must have been acked (no loss despite the capacity split).
    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
    // And no single Produce request exceeded max_batch_bytes: the broker
    // records the max total records bytes it saw, which must stay <= the cap.
    // (Without this, the test would pass even if both batches shipped in one
    // over-cap request.)
    try testing.expect(broker.max_produce_records_bytes.load(.acquire) <= cfg.max_batch_bytes);
}

test "single batch exceeding max_batch_bytes fails MESSAGE_TOO_LARGE" {
    // A single message whose record batch exceeds max_batch_bytes must fail
    // with error.SendFailed and failureCode 10 (MESSAGE_TOO_LARGE), not be
    // silently sent as an oversized request.
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 1 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.max_batch_bytes = 100; // batch with a 200-byte value is ~261 B > 100
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    var m = try client.acquire();
    try m.setTopic("events");
    m.setPartition(0);
    const dst = m.value();
    @memset(dst[0..200], 'x');
    try m.commit(200);

    try testing.expectError(error.SendFailed, m.await());
    try testing.expectEqual(@as(u32, 10), m.failureCode()); // MESSAGE_TOO_LARGE
}

// ---------------------------------------------------------------------------
// Idempotent producer tests (1b, #13)
// ---------------------------------------------------------------------------

test "idempotency ON: batches carry real PID, epoch, and advancing base_sequence" {
    // Produce N messages to a single partition. The mock broker captures each
    // batch's producer_id / producer_epoch / base_sequence. Assert:
    //   - producer_id == mock.mock_producer_id (not -1)
    //   - producer_epoch == mock.mock_producer_epoch
    //   - base_sequence starts at 0 and advances by the record count per batch
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 1 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.enable_idempotency = true;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    // Produce 5 messages, all to partition 0. They may be batched into one or
    // more batches depending on timing; each batch's base_sequence should
    // advance by the number of records in the preceding batch.
    const n = 5;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "msg-{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&handles) |*m| try m.await();

    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));

    // Assert at least one batch was captured, and all carry the mock PID/epoch.
    try testing.expect(broker.captured_batches_len > 0);
    var expected_seq: i32 = 0;
    for (broker.captured_batches[0..broker.captured_batches_len]) |b| {
        try testing.expectEqual(mock.mock_producer_id, b.producer_id);
        try testing.expectEqual(mock.mock_producer_epoch, b.producer_epoch);
        // base_sequence should match expected_seq, then advance by record_count.
        try testing.expectEqual(expected_seq, b.base_sequence);
        try testing.expect(b.record_count > 0);
        expected_seq += @intCast(b.record_count);
    }
    // The total records across all batches should equal n.
    try testing.expectEqual(@as(i32, @intCast(n)), expected_seq);
}

test "idempotency ON: per-partition sequences are independent" {
    // Produce messages to two different partitions in separate batches (by
    // awaiting between groups). Each partition's base_sequence should start
    // at 0 and advance independently.
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 2 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.enable_idempotency = true;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    // First batch: 3 messages to partition 0.
    var h1: [3]Message = undefined;
    for (&h1, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "a{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&h1) |*m| try m.await();

    // Second batch: 2 messages to partition 1.
    var h2: [2]Message = undefined;
    for (&h2, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(1);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "b{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&h2) |*m| try m.await();

    // Third batch: 2 more messages to partition 0.
    var h3: [2]Message = undefined;
    for (&h3, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "c{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&h3) |*m| try m.await();

    try testing.expectEqual(@as(u32, 7), broker.produced.load(.acquire));

    // Collect per-partition base_sequences.
    var p0_seqs: [16]i32 = undefined;
    var p0_len: usize = 0;
    var p1_seqs: [16]i32 = undefined;
    var p1_len: usize = 0;
    for (broker.captured_batches[0..broker.captured_batches_len]) |b| {
        if (b.partition == 0) {
            p0_seqs[p0_len] = b.base_sequence;
            p0_len += 1;
        } else if (b.partition == 1) {
            p1_seqs[p1_len] = b.base_sequence;
            p1_len += 1;
        }
    }

    // Partition 0 had two batches: first with 3 records (seq 0), second with
    // 2 records (seq 3). Partition 1 had one batch with 2 records (seq 0).
    try testing.expect(p0_len >= 2);
    try testing.expectEqual(@as(i32, 0), p0_seqs[0]);
    try testing.expectEqual(@as(i32, 3), p0_seqs[1]);

    try testing.expect(p1_len >= 1);
    try testing.expectEqual(@as(i32, 0), p1_seqs[0]);
}

test "idempotency ON: retry reuses the same base_sequence" {
    // With retriable_once mode, the first Produce per partition fails with
    // NOT_LEADER_OR_FOLLOWER. The retry must REUSE the same base_sequence
    // (not re-advance), so the broker dedup window is preserved.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .retriable_once,
        .num_partitions = 1,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.enable_idempotency = true;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    const n = 3;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "r{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&handles) |*m| try m.await();

    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));

    // The broker should have captured at least 2 batches for partition 0:
    // the first (failed) and the retry (succeeded). Both must have the SAME
    // base_sequence (0), because retry reuses the assigned sequence.
    try testing.expect(broker.captured_batches_len >= 2);
    const first = broker.captured_batches[0];
    const retry = broker.captured_batches[1];
    try testing.expectEqual(@as(i32, 0), first.partition);
    try testing.expectEqual(@as(i32, 0), retry.partition);
    try testing.expectEqual(first.base_sequence, retry.base_sequence);
    try testing.expectEqual(first.record_count, retry.record_count);
    try testing.expectEqual(mock.mock_producer_id, first.producer_id);
    try testing.expectEqual(mock.mock_producer_id, retry.producer_id);
}

test "idempotency OFF: batches carry -1 sentinels, no PID" {
    // Non-idempotent mode regression: producer_id=-1, producer_epoch=-1,
    // base_sequence=-1. No InitProducerId is sent.
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 1 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    var m = try client.acquire();
    try m.setTopic("events");
    m.setPartition(0);
    const dst = m.value();
    @memcpy(dst[0..4], "test");
    try m.commit(4);
    try m.await();

    try testing.expectEqual(@as(u32, 1), broker.produced.load(.acquire));
    try testing.expect(broker.captured_batches_len >= 1);
    const b = broker.captured_batches[0];
    try testing.expectEqual(@as(i64, -1), b.producer_id);
    try testing.expectEqual(@as(i16, -1), b.producer_epoch);
    try testing.expectEqual(@as(i32, -1), b.base_sequence);
}
