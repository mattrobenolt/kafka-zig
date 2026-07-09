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
/// `error.CompressionUnavailable`. `.snappy` is always available (pure-Zig,
/// no build flag).
pub const Compression = record_batch.Compression;

/// A snapshot of producer statistics (issue #7). Pull-based: the caller
/// fetches it via `Client.stats()`. All values are a relaxed-consistency
/// snapshot -- torn or stale reads are acceptable (it's metrics, not
/// accounting). No locks, no allocation.
pub const Stats = struct {
    /// Pending slots in the ring: `write_head - read_head`. Slots that have
    /// been committed but not yet reclaimed (acked/failed + physically
    /// recycled by the network thread).
    queue_depth: u64,
    /// Messages currently in flight. In v1 this equals `queue_depth` -- every
    /// committed-but-not-reclaimed slot is "in flight." A finer split
    /// (sent-but-not-acked vs. waiting-to-send) is a follow-up.
    in_flight: u64,
    /// Total messages committed by producer threads (atomic, multi-threaded).
    messages_produced: u64,
    /// Total bytes committed by producer threads (atomic, multi-threaded).
    bytes_produced: u64,
    /// Total Produce requests sent to brokers (network thread).
    batches_sent: u64,
    /// Total messages acked by the broker (network thread).
    messages_acked: u64,
    /// Total messages permanently failed (network thread).
    messages_failed: u64,
    /// Error counts by class (network thread).
    errors: ErrorCounts,
};

/// Error counts by class, for the `Stats` snapshot.
pub const ErrorCounts = struct {
    /// Retriable errors encountered (per slot per retry attempt).
    retriable: u64,
    /// Permanent (non-retriable) failures, including retry-budget exhaustion.
    fatal: u64,
    /// Connection drops during Produce sends.
    connection_drops: u64,
};

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
    /// Linger time (ms): after the first pending record arrives, the network
    /// thread waits up to this long for more records before draining, to form
    /// larger batches (better throughput + compression ratio). 0 = eager
    /// (drain immediately when data is available). Default 5ms.
    linger_ms: u32 = 5,
    /// Backoff (ms) between drains that made no forward progress (only retries
    /// / transient connection failures), so a persistently-unhealthy cluster
    /// does not spin the CPU.
    retry_backoff_ms: u32 = 5,
    /// Metadata refresh interval (ms): refresh metadata periodically even
    /// without errors, to catch silent leader changes / new brokers / new
    /// partitions. 0 = only refresh on error/invalidation. Default 300000
    /// (5 min, matching Kafka's `metadata.max.age.ms`).
    metadata_max_age_ms: u32 = 300_000,
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
    /// `.snappy` compresses each batch (always available, pure-Zig);
    /// `.zstd` compresses each batch (requires `-Dzstd=true` at build time).
    compression: Compression = .none,
    /// When true (production default), the producer acquires a PID/epoch at
    /// startup via InitProducerId v4, tracks per-partition sequence numbers,
    /// and sends real producer_id/producer_epoch/base_sequence in record
    /// batches. When false, uses the -1 sentinels (non-idempotent mode).
    enable_idempotency: bool = true,
    /// Graceful shutdown drain timeout (ms). When `deinit` is called, the
    /// network thread continues draining in-flight Produce round-trips up to
    /// this duration before closing connections. Slots still pending after
    /// the timeout surface `error.Shutdown`. Default 10s.
    ///
    /// This bounds the TOTAL drain, checked between blocking I/O operations,
    /// not mid-syscall: a single in-flight Produce response read may overshoot
    /// it by up to `io_timeout_ms` (the socket read timeout). See
    /// `Producer.Options.drain_timeout_ns` for the full contract.
    drain_timeout_ms: u32 = 10_000,
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
        .retry_backoff_ns = @as(u64, config.retry_backoff_ms) * std.time.ns_per_ms,
        .linger_ns = @as(u64, config.linger_ms) * std.time.ns_per_ms,
        .metadata_max_age_ms = config.metadata_max_age_ms,
        .io_timeout_ms = config.io_timeout_ms,
        .compression = config.compression,
        .enable_idempotency = config.enable_idempotency,
        .drain_timeout_ns = @as(u64, config.drain_timeout_ms) * std.time.ns_per_ms,
    });
    errdefer self.producer.deinit();

    self.thread = try std.Thread.spawn(.{}, Producer.run, .{&self.producer});
    return self;
}

/// Shut down: two-phase graceful drain. Phase 1: stop accepting new messages
/// (`requestDrain`), let the network thread finish in-flight Produce
/// round-trips up to `drain_timeout_ms`, then exit. Phase 2: full shutdown
/// (`requestShutdown`) to wake any remaining `await` waiters with
/// `error.Shutdown`, close connections, and free everything.
///
/// CONTRACT: all outstanding `Message`s must be awaited (or dropped without
/// touching them again) BEFORE calling `deinit`. `deinit` is NOT safe to call
/// concurrently with `await`/`tryAwait`/`acquire` on the same client: phase 2
/// frees the ring, and a concurrent `await` would use it after free. This
/// matches real Kafka clients (librdkafka `rd_kafka_flush` before
/// `rd_kafka_destroy`; the Java client's `close()` blocks in-flight sends
/// first). Await-then-deinit, single-threaded from the caller's side.
pub fn deinit(self: *Client) void { // ziglint-ignore: Z030 -- self is heap-owned, destroyed here
    // Phase 1: stop accepting new acquires, wake the network thread to drain.
    self.ring.requestDrain();
    // Join the network thread: it drains in-flight acks (bounded by
    // drain_timeout_ns) and exits on its own.
    self.thread.join();
    // Phase 2: full shutdown — wake any remaining awaiters (slots still
    // pending after the drain timeout) with error.Shutdown.
    self.ring.requestShutdown();
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

/// Fetch a relaxed-consistency snapshot of producer statistics (issue #7).
/// No locks, no allocation -- returns a value struct. Torn/stale reads are
/// acceptable. `queue_depth` and `in_flight` are read from `Ring.depth()`
/// (which loads `read_head` before `write_head` to avoid underflow); the
/// producer-thread counters (`messages_produced`, `bytes_produced`) are
/// atomic; the network-thread counters are read with `.monotonic` loads
/// (single-writer, relaxed cross-thread reads).
pub fn stats(self: *const Client) Stats {
    const depth = self.ring.depth();
    return .{
        .queue_depth = depth,
        .in_flight = depth,
        .messages_produced = self.ring.messages_produced.load(.monotonic),
        .bytes_produced = self.ring.bytes_produced.load(.monotonic),
        .batches_sent = self.producer.batches_sent.load(.monotonic),
        .messages_acked = self.producer.messages_acked.load(.monotonic),
        .messages_failed = self.producer.messages_failed.load(.monotonic),
        .errors = .{
            .retriable = self.producer.errors_retriable.load(.monotonic),
            .fatal = self.producer.errors_fatal.load(.monotonic),
            .connection_drops = self.producer.errors_connection_drops.load(.monotonic),
        },
    };
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
        .linger_ms = 0,
        .retry_backoff_ms = 2,
        .metadata_max_age_ms = 300_000,
        .max_message_size = 1024,
        .max_key_len = 64,
        .max_topic_len = 64,
        .ring_slots = 64,
        .partitioner = .default,
        .client_id = "kafka-zig-test",
        .enable_idempotency = true,
    };
}

/// Poll `stats().queue_depth` until it reaches zero, up to 1s. The network
/// thread wakes completion waiters BEFORE reclaiming (`wakeCompletions` then
/// `reclaim` in `drainOnce`), so `await()` can return before `read_head`
/// advances. An immediate `queue_depth == 0` assertion races that window;
/// this poll waits for the reclaim to land.
fn expectQueueDepthZero(client: *const Client) !void {
    const start: u64 = @intCast(@as(i64, @truncate(std.time.nanoTimestamp())));
    const deadline_ns: u64 = start + std.time.ns_per_s;
    while (true) {
        if (client.stats().queue_depth == 0) return;
        const now: u64 = @intCast(@as(i64, @truncate(std.time.nanoTimestamp())));
        if (now >= deadline_ns) return error.Timeout;
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }
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

test "idempotency ON: retry does not merge fresh records into the retried batch" {
    // Fix 2 regression: a retriable batch (slots 0..2, base_sequence=0) fails;
    // BEFORE the retry, fresh slots 3..4 are committed to the SAME partition.
    // The retry must NOT absorb the fresh records — the retried batch stays
    // (base_sequence=0, 3 records) and the fresh records get their own batch
    // (base_sequence=3, 2 records). A merge would break broker dedup (the
    // broker would see a different batch with an already-used base_sequence).
    //
    // `gate_first_produce` makes this deterministic: the broker blocks before
    // responding to the first Produce, so the test can commit the fresh
    // records while the producer is stuck awaiting the (failing) response,
    // guaranteeing the retried and fresh slots are pending together on the
    // next drain.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .retriable_once,
        .num_partitions = 1,
        .gate_first_produce = true,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.enable_idempotency = true;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    // Three records to partition 0 — do NOT await yet.
    var h1: [3]Message = undefined;
    for (&h1, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "x{d}", .{i});
        try m.commit(@intCast(payload.len));
    }

    // Wait until the producer has sent the first Produce (now blocked in the
    // gate), then commit two MORE records to the same partition. They land in
    // the ring while the producer is awaiting the failing response, so the
    // next drain sees the retried h1 slots and the fresh h2 slots together.
    broker.waitFirstProduce();
    var h2: [2]Message = undefined;
    for (&h2, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "y{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    // Release the gate: the first Produce fails (NOT_LEADER_OR_FOLLOWER), the
    // h1 slots retry, and the h2 slots are fresh in the same drain.
    broker.openGate();

    for (&h1) |*m| try m.await();
    for (&h2) |*m| try m.await();

    try testing.expectEqual(@as(u32, 5), broker.produced.load(.acquire));

    // Group captured batches by base_sequence. A retry re-sends a byte-
    // identical batch, so every batch sharing a base_sequence MUST have the
    // same record count — the merge bug produced a base_sequence=0 retry with
    // a LARGER count than its first send. The distinct base_sequences must
    // also tile [0, 5) exactly, proving the fresh records got their own
    // sequence rather than being folded into the retried batch. (Assertions
    // are robust to the producer batching h1 as one batch or, under a rare
    // drain-timing split, several: the invariant holds either way.)
    try testing.expect(broker.captured_batches_len >= 2);
    var seqs: [8]i32 = undefined;
    var counts: [8]u32 = undefined;
    var distinct: usize = 0;
    for (broker.captured_batches[0..broker.captured_batches_len]) |b| {
        try testing.expectEqual(@as(i32, 0), b.partition);
        try testing.expectEqual(mock.mock_producer_id, b.producer_id);
        var found = false;
        for (seqs[0..distinct], counts[0..distinct]) |s, c| {
            if (s == b.base_sequence) {
                try testing.expectEqual(c, b.record_count); // identical retry
                found = true;
                break;
            }
        }
        if (!found) {
            seqs[distinct] = b.base_sequence;
            counts[distinct] = b.record_count;
            distinct += 1;
        }
    }
    // At least two distinct sequences: the retried set and the fresh set. A
    // single distinct sequence would mean everything merged into base_seq 0.
    try testing.expect(distinct >= 2);
    // Insertion-sort the distinct sequences ascending, then verify tiling.
    for (1..distinct) |i| {
        var j = i;
        while (j > 0 and seqs[j - 1] > seqs[j]) : (j -= 1) {
            std.mem.swap(i32, &seqs[j - 1], &seqs[j]);
            std.mem.swap(u32, &counts[j - 1], &counts[j]);
        }
    }
    var expected: i32 = 0;
    for (seqs[0..distinct], counts[0..distinct]) |s, c| {
        try testing.expectEqual(expected, s);
        expected += @intCast(c);
    }
    try testing.expectEqual(@as(i32, 5), expected);
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

// ---------------------------------------------------------------------------
// 1c: idempotent re-ack error handling (OUT_OF_ORDER_SEQUENCE=45,
// UNKNOWN_PRODUCER_ID=73, DUPLICATE_SEQUENCE_NUMBER=37). See PLAN §-idempotent
// and issue #1. Codes verified against the Kafka error-code table
// (https://kafka.apache.org/protocol.html#protocol_error_codes); recovery
// semantics against KIP-360 (PID reset / sequence reset) and KIP-98 (dedup).
// ---------------------------------------------------------------------------

test "idempotency ON: OUT_OF_ORDER_SEQUENCE re-inits PID, resets sequence, retries" {
    // Broker returns OUT_OF_ORDER_SEQUENCE (45) on the first Produce, then acks.
    // The producer must re-init the PID (fresh PID from the 2nd InitProducerId),
    // reset the per-partition sequence to 0, and retry — landing on the ack.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .out_of_order_once,
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
        const payload = try std.fmt.bufPrint(dst, "o{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    // Every message must eventually ack despite the first Produce failing 45.
    for (&handles) |*m| try m.await();

    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
    // The re-init happened: startup (call 1) + recovery (call 2).
    try testing.expect(broker.init_producer_id_requests.load(.acquire) >= 2);

    // The first (failed) batch carried the startup PID; the retry batch carried
    // the fresh PID (mock_producer_id + 1). Scope the base_sequence == 0 check
    // to the NEW-PID (recovery) batches: the fresh PID makes the broker forget
    // all prior sequence state, so recovery batches always restart at 0. The
    // old-PID batches may carry any base_sequence — the producer drains eagerly,
    // so which records make the first failed batch is timing-dependent (asserting
    // 0 on all batches was a CI flake). This is the structural guarantee.
    try testing.expect(broker.captured_batches_len >= 2);
    var saw_old_pid = false;
    var saw_new_pid = false;
    for (broker.captured_batches[0..broker.captured_batches_len]) |b| {
        try testing.expectEqual(@as(i32, 0), b.partition);
        if (b.producer_id == mock.mock_producer_id) saw_old_pid = true;
        if (b.producer_id == mock.mock_producer_id + 1) {
            saw_new_pid = true;
            try testing.expectEqual(@as(i32, 0), b.base_sequence);
        }
    }
    try testing.expect(saw_old_pid);
    try testing.expect(saw_new_pid);
}

test "idempotency ON: UNKNOWN_PRODUCER_ID re-inits PID, resets sequence, retries" {
    // KIP-360: on UNKNOWN_PRODUCER_ID (73) the broker lost producer state; the
    // idempotent producer re-inits the PID, resets sequences, and retries. Same
    // recovery path as OUT_OF_ORDER_SEQUENCE.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .unknown_pid_once,
        .num_partitions = 1,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.enable_idempotency = true;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    const n = 4;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "u{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&handles) |*m| try m.await();

    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
    try testing.expect(broker.init_producer_id_requests.load(.acquire) >= 2);

    // Scope base_sequence == 0 to the NEW-PID (recovery) batches — the fresh PID
    // resets all broker sequence state, so recovery batches restart at 0.
    // Pre-recovery (old-PID) batches may carry any base_sequence (timing).
    try testing.expect(broker.captured_batches_len >= 2);
    var saw_new_pid = false;
    for (broker.captured_batches[0..broker.captured_batches_len]) |b| {
        if (b.producer_id == mock.mock_producer_id + 1) {
            saw_new_pid = true;
            try testing.expectEqual(@as(i32, 0), b.base_sequence);
        }
    }
    try testing.expect(saw_new_pid);
}

test "idempotency ON: DUPLICATE_SEQUENCE_NUMBER is treated as ack, not failure" {
    // KIP-98: the broker returns DUPLICATE_SEQUENCE_NUMBER (37) when a batch's
    // sequence was already appended (a lost-ack retry). The data is durable, so
    // the producer must complete the slot as ACKED (await succeeds), NOT fail
    // it, and must NOT re-init the PID (37 is not a reset trigger).
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .duplicate_seq,
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
        const payload = try std.fmt.bufPrint(dst, "d{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    // await() succeeding proves the slots were ACKED, not failed.
    for (&handles) |*m| try m.await();

    // The mock counts the duplicate write as durable exactly once.
    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
    // No PID reset: only the startup InitProducerId.
    try testing.expectEqual(@as(u32, 1), broker.init_producer_id_requests.load(.acquire));
}

test "idempotency OFF: OUT_OF_ORDER_SEQUENCE fails (not silently recovered)" {
    // Without a PID, code 45 is unexpected and non-retriable: the slot must
    // fail with error.SendFailed carrying code 45. No InitProducerId is sent.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .out_of_order_once,
        .num_partitions = 1,
    });
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
    @memcpy(dst[0..3], "oos");
    try m.commit(3);

    try testing.expectError(error.SendFailed, m.await());
    try testing.expectEqual(@as(u32, 45), m.failureCode()); // OUT_OF_ORDER_SEQUENCE
    try testing.expectEqual(@as(u32, 0), broker.init_producer_id_requests.load(.acquire));
}

test "idempotency OFF: DUPLICATE_SEQUENCE_NUMBER fails (not treated as ack)" {
    // The 37-as-ack shortcut is idempotent-only. Without a PID, code 37 is
    // unexpected and must fail — never silently succeed.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .duplicate_seq,
        .num_partitions = 1,
    });
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
    @memcpy(dst[0..3], "dup");
    try m.commit(3);

    try testing.expectError(error.SendFailed, m.await());
    try testing.expectEqual(@as(u32, 37), m.failureCode()); // DUPLICATE_SEQUENCE_NUMBER
}

// ---------------------------------------------------------------------------
// #2: HA bootstrap failover + reconnect-on-drop + backoff
// ---------------------------------------------------------------------------

test "#2 bootstrap failover: 3 endpoints, only 2nd listening — produce succeeds" {
    // Configure 3 bootstrap endpoints; only the 2nd is the mock broker. The
    // producer must fail over from the 1st (dead port) to the 2nd (live) and
    // successfully fetch metadata + produce.
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 4 });
    defer broker.stop();

    // Port 1 and port 2 are privileged ports that will refuse on a dev box.
    // The TCP connect fails immediately with ConnectionRefused — no hang.
    const bootstrap = [_]Broker{
        .{ .host = "127.0.0.1", .port = 1 },
        .{ .host = "127.0.0.1", .port = broker.port },
        .{ .host = "127.0.0.1", .port = 2 },
    };
    var cfg = testConfig(&bootstrap);
    cfg.io_timeout_ms = 1000; // fail fast on dead ports
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    const n = 10;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(null);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "msg-{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&handles) |*m| try m.await();

    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
}

test "#2 all bootstrap down: init fails gracefully, no hang" {
    // No mock broker — both endpoints are dead. Client.init spawns the network
    // thread, which tries to refresh metadata and fails. The first produce
    // should fail gracefully (not hang). We use a short io_timeout and short
    // retry budget so the failure surfaces quickly.
    const bootstrap = [_]Broker{
        .{ .host = "127.0.0.1", .port = 1 },
        .{ .host = "127.0.0.1", .port = 2 },
    };
    var cfg = testConfig(&bootstrap);
    cfg.io_timeout_ms = 500;
    cfg.max_retries = 2;
    cfg.linger_ms = 0;
    cfg.retry_backoff_ms = 10;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    var m = try client.acquire();
    try m.setTopic("events");
    m.setPartition(0);
    const dst = m.value();
    @memcpy(dst[0..3], "bad");
    try m.commit(3);

    // Should fail, not hang. The error is SendFailed (the slot is failed
    // after exhausting retries against unreachable brokers).
    try testing.expectError(error.SendFailed, m.await());
}

test "#2 reconnect-on-drop: broker drops first Produce, re-dial + retry succeeds" {
    // The mock broker closes the TLS connection after the first Produce (no
    // response). The producer must drop the dead connection, re-dial, retry
    // the slots, and the second Produce succeeds. All messages must eventually
    // ack.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .close_after_first_produce,
        .num_partitions = 1,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    // Use a longer retry budget and shorter backoff so the reconnect happens
    // quickly within the test.
    cfg.max_retries = 5;
    cfg.linger_ms = 0;
    cfg.retry_backoff_ms = 5;
    cfg.enable_idempotency = false; // simplify: no PID re-init on reconnect
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

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

    // All messages eventually acked after the reconnect.
    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
    // The broker accepted at least 2 connections: the initial (dropped) + the
    // re-dial (successful retry).
    try testing.expect(broker.connections_accepted.load(.acquire) >= 2);
}

test "#2 reconnect backoff: dead broker is not hammered" {
    // With all bootstrap brokers down, the producer should not busy-spin:
    // the drain-level backoff sleep limits the rate of connection attempts.
    // We assert the produce fails gracefully within a bounded time (not a
    // hang), proving the backoff + retry budget is engaged.
    const bootstrap = [_]Broker{
        .{ .host = "127.0.0.1", .port = 1 },
    };
    var cfg = testConfig(&bootstrap);
    cfg.io_timeout_ms = 200;
    cfg.max_retries = 3;
    cfg.linger_ms = 0;
    cfg.retry_backoff_ms = 50; // 50ms backoff between drains
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    var m = try client.acquire();
    try m.setTopic("events");
    m.setPartition(0);
    const dst = m.value();
    @memcpy(dst[0..2], "bk");
    try m.commit(2);

    // Should fail within the retry budget, not hang. The backoff sleep
    // between drains (50ms) prevents a tight CPU spin against the dead port.
    try testing.expectError(error.SendFailed, m.await());
}

// ---------------------------------------------------------------------------
// #3: linger batching + periodic metadata refresh
// ---------------------------------------------------------------------------

test "#3 linger batching: small burst coalesces into one Produce request" {
    // With linger_ms > 0, a burst of small messages committed in quick
    // succession should coalesce into a single Produce request (one batch to
    // one partition), not N separate requests. The mock broker tracks
    // `produce_requests` to assert coalescing.
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 1 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.linger_ms = 50; // 50ms linger window for coalescing
    cfg.enable_idempotency = false; // simplify: no PID overhead
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    // Commit a burst of 10 small messages to the same partition. They should
    // all land within the linger window and form ONE batch / ONE Produce.
    const n = 10;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "m{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&handles) |*m| try m.await();

    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
    // All 10 records should have been coalesced into a single Produce request.
    // Without linger (eager drain), each message could be its own Produce.
    try testing.expectEqual(@as(u32, 1), broker.produce_requests.load(.acquire));
}

test "#3 linger=0: each message drains eagerly (regression)" {
    // With linger_ms = 0, the drain is eager. Messages committed in quick
    // succession may still batch if they arrive in the same waitForData wake,
    // but the producer does NOT wait for more. We verify that with a deliberate
    // gap between commits, each message is its own Produce request.
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 1 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.linger_ms = 0; // eager drain
    cfg.retry_backoff_ms = 0;
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    // Produce 3 messages one at a time, awaiting each before committing the
    // next. With linger=0 and no overlap, each must be its own Produce.
    for (0..3) |i| {
        var m = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "e{d}", .{i});
        try m.commit(@intCast(payload.len));
        try m.await();
    }

    try testing.expectEqual(@as(u32, 3), broker.produced.load(.acquire));
    try testing.expectEqual(@as(u32, 3), broker.produce_requests.load(.acquire));
}

test "#3 no over-linger under load: full batch drains immediately" {
    // With linger_ms > 0 but a full batch worth of records pending, the drain
    // must happen immediately (no waiting for the linger timer). We set a
    // large linger (500ms) and a max_batch_bytes that fits one batch of 15
    // small records. When all 15 are committed at once, pendingBatchFull
    // detects the batch is full and skips the linger.
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 1 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.linger_ms = 500; // long linger — should be skipped for a full batch
    cfg.max_batch_bytes = 1000; // fits one batch of 15x50B records (~976B)
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    // Commit 15 records with 50-byte payloads to the same partition. The
    // estimated pending bytes (15 * 70 = 1050) exceeds max_batch_bytes, so
    // pendingBatchFull triggers immediately and the linger is skipped. The
    // actual batch (~976B) fits within max_batch_bytes.
    const n = 15;
    var handles: [n]Message = undefined;
    const start = std.time.milliTimestamp();
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        @memset(dst[0..50], 'x');
        try m.commit(50);
        _ = i;
    }
    for (&handles) |*m| try m.await();
    const elapsed: u64 = @intCast(std.time.milliTimestamp() - start);

    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
    // The produce must complete in well under the 500ms linger. If the linger
    // were NOT skipped, the drain would wait 500ms before sending. 200ms is a
    // generous bound that accounts for TLS handshake + round-trip.
    try testing.expect(elapsed < 200);
}

test "#3 periodic metadata refresh: increments without errors" {
    // With a small metadata_max_age_ms, the producer refreshes metadata
    // proactively even without errors. The mock broker tracks
    // `metadata_requests` — assert it increments over time beyond the initial
    // load.
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 4 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.metadata_max_age_ms = 100; // refresh every 100ms
    cfg.linger_ms = 0;
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    // Produce one message to trigger the initial metadata load.
    var m1 = try client.acquire();
    try m1.setTopic("events");
    m1.setPartition(0);
    const dst = m1.value();
    @memcpy(dst[0..4], "init");
    try m1.commit(4);
    try m1.await();

    const initial_requests = broker.metadata_requests.load(.acquire);
    try testing.expect(initial_requests >= 1); // at least the initial load

    // Sleep long enough for at least 2 periodic refreshes to fire.
    std.Thread.sleep(350 * std.time.ns_per_ms);

    // Produce another message to trigger a drain (which checks metadata age).
    var m2 = try client.acquire();
    try m2.setTopic("events");
    m2.setPartition(0);
    const dst2 = m2.value();
    @memcpy(dst2[0..4], "more");
    try m2.commit(4);
    try m2.await();

    // The periodic refresh must have fired at least once beyond the initial.
    try testing.expect(broker.metadata_requests.load(.acquire) > initial_requests);
}

test "#3 periodic metadata refresh: large max_age = no proactive refresh" {
    // With a large metadata_max_age_ms (default 300s), no proactive refresh
    // should happen — only the initial load and error-triggered refreshes.
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 4 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.metadata_max_age_ms = 300_000; // 5 min — no proactive refresh during test
    cfg.linger_ms = 0;
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    // Produce two messages with a gap between them. Only the initial metadata
    // load should fire (no error-triggered or periodic refresh).
    var m1 = try client.acquire();
    try m1.setTopic("events");
    m1.setPartition(0);
    const dst = m1.value();
    @memcpy(dst[0..3], "aaa");
    try m1.commit(4);
    try m1.await();

    const requests_after_first = broker.metadata_requests.load(.acquire);

    std.Thread.sleep(100 * std.time.ns_per_ms);

    var m2 = try client.acquire();
    try m2.setTopic("events");
    m2.setPartition(0);
    const dst2 = m2.value();
    @memcpy(dst2[0..3], "bbb");
    try m2.commit(4);
    try m2.await();

    // No proactive refresh should have fired in the 100ms gap.
    try testing.expectEqual(requests_after_first, broker.metadata_requests.load(.acquire));
}

test "#3 silent leader change: producer routes to new leader after refresh" {
    // A single mock broker advertises two nodes (1 and 2) both at its own
    // address (extra broker with port=0 means "same as this broker").
    // Initially leader = 1. After a periodic metadata refresh, the leader
    // changes to 2. The producer must pick up the new leader from the
    // refreshed metadata and continue producing without errors. This tests
    // the core behavior: a silent leader change (no error trigger) is
    // recovered via the periodic refresh.
    const extra = [_]mock.ExtraBroker{.{ .node_id = 2, .host = "127.0.0.1", .port = 0 }};
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .ok,
        .num_partitions = 1,
        .node_id = 1,
        .extra_brokers = &extra,
    });
    defer broker.stop();

    // Both nodes point to the same broker (port=0 resolves to broker.port).
    // Leader starts at node 1.
    broker.leader_id.store(1, .release);

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.metadata_max_age_ms = 100; // refresh every 100ms
    cfg.linger_ms = 0;
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    // Produce to node 1 (the initial leader).
    var m1 = try client.acquire();
    try m1.setTopic("events");
    m1.setPartition(0);
    const dst1 = m1.value();
    @memcpy(dst1[0..4], "old1");
    try m1.commit(4);
    try m1.await();

    const requests_before = broker.metadata_requests.load(.acquire);
    try testing.expect(requests_before >= 1); // at least the initial load

    // Silently change the leader to node 2. No error is triggered — the
    // producer must pick this up via the periodic metadata refresh.
    broker.leader_id.store(2, .release);

    // Wait for a periodic metadata refresh to fire.
    std.Thread.sleep(250 * std.time.ns_per_ms);

    // Produce again — should succeed with the new leader (node 2).
    var m2 = try client.acquire();
    try m2.setTopic("events");
    m2.setPartition(0);
    const dst2 = m2.value();
    @memcpy(dst2[0..4], "new2");
    try m2.commit(4);
    try m2.await();

    // The periodic refresh happened: metadata_requests increased.
    try testing.expect(broker.metadata_requests.load(.acquire) > requests_before);
    // Both produces succeeded (produced count = 2).
    try testing.expectEqual(@as(u32, 2), broker.produced.load(.acquire));
}

// ---------------------------------------------------------------------------
// #4: graceful shutdown drain
// ---------------------------------------------------------------------------

test "#4 graceful drain: in-flight records are all acked before shutdown" {
    // Produce N messages WITHOUT awaiting, then call deinit. Phase 1
    // (requestDrain) keeps the network thread draining until every pending
    // Produce round-trip completes (bounded by drain_timeout_ms), so all N are
    // acked by the broker before the thread exits. We verify the drain via the
    // broker's `produced` counter, NOT via `await`: deinit frees the ring in
    // phase 2, so a concurrent `await` would be a use-after-free. The contract
    // is await-then-deinit, and here we exercise the pure drain path (nothing
    // awaited) with no concurrency against deinit at all.
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 4 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.drain_timeout_ms = 5_000;
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);

    const n = 20;
    for (0..n) |i| {
        var m = try client.acquire();
        try m.setTopic("events");
        m.setPartition(@intCast(i % 4));
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "drain-{d}", .{i});
        try m.commit(@intCast(payload.len));
    }

    // deinit on the main thread: blocks in requestDrain -> thread.join while
    // the network thread flushes all in-flight acks, then requestShutdown +
    // free. No await runs concurrently with the free.
    client.deinit();

    // Every message was acked by the broker during the drain.
    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
}

test "#4 drain timeout: deinit returns in bounded time, no hang" {
    // Produce N messages to a broker that NEVER responds to Produce (drops the
    // connection after receiving the request). The slots stay pending forever,
    // so the drain can never complete on its own — the drain TIMEOUT is what
    // bounds it. We assert deinit returns within the timeout (plus generous
    // slack for the in-flight read to time out and connect/backoff churn), i.e.
    // no hang. No `await` is called: deinit frees the ring, so an await racing
    // the free would be a use-after-free. The bounded return of deinit IS the
    // "un-acked slots don't hang shutdown" guarantee.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .never_ack,
        .num_partitions = 1,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.drain_timeout_ms = 500;
    cfg.io_timeout_ms = 200;
    cfg.max_retries = 255; // high so slots stay pending until the drain timeout
    cfg.retry_backoff_ms = 10;
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);

    const n = 5;
    for (0..n) |i| {
        var m = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "to-{d}", .{i});
        try m.commit(@intCast(payload.len));
    }

    // deinit on the main thread. The drain deadline (500ms) plus one in-flight
    // read timeout (io_timeout_ms = 200ms, the single-syscall overshoot the
    // drain contract documents) bound the total; 5s is a wide no-hang ceiling.
    var timer = try std.time.Timer.start();
    client.deinit();
    const elapsed_ms = timer.read() / std.time.ns_per_ms;
    try testing.expect(elapsed_ms < 5_000);

    // The broker did receive at least one Produce request (the records were
    // sent, just never acked), so this was a real drain-timeout, not a no-op.
    try testing.expect(broker.produce_requests.load(.acquire) >= 1);
}

test "#4 no new acquires during drain: acquire returns Shutdown" {
    // After deinit starts (requestDrain), acquire must return error.Shutdown.
    // We use a gated broker so the network thread is blocked in a Produce
    // round-trip, keeping the deinit thread in thread.join() (client alive)
    // while the main thread tests acquire.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .ok,
        .num_partitions = 1,
        .gate_first_produce = true,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.drain_timeout_ms = 10_000; // long drain — won't expire during the test
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);

    // Produce one message — the broker gates on the first Produce, so the
    // network thread is blocked in readResponse. We never await it (see below),
    // so the token is const.
    const m = try client.acquire();
    try m.setTopic("events");
    m.setPartition(0);
    const dst = m.value();
    @memcpy(dst[0..3], "blk");
    try m.commit(3);

    // Wait until the broker has the first Produce (network thread is blocked).
    broker.waitFirstProduce();

    // Spawn deinit — requestDrain fires immediately, then thread.join blocks
    // (the network thread is stuck in the gated readResponse).
    const DeinitCtx = struct {
        client: *Client,
        fn run(c: *Client) void {
            c.deinit();
        }
    };
    const dt = try std.Thread.spawn(.{}, DeinitCtx.run, .{client});

    // Give requestDrain a moment to set the draining flag.
    std.Thread.sleep(50 * std.time.ns_per_ms);

    // acquire must fail with error.Shutdown (draining blocks new acquires).
    // This is safe to call while deinit is in progress because deinit is
    // parked in thread.join() (the network thread is stuck in the gated
    // readResponse), so the ring is NOT yet freed — the free only happens in
    // phase 2, after join returns, which cannot happen until we openGate below.
    try testing.expectError(error.Shutdown, client.tryAcquire());

    // Release the gate so the broker acks the Produce, the network thread
    // finishes the drain and exits, join returns, and deinit frees the ring.
    broker.openGate();

    // Join deinit BEFORE asserting anything ring-touching. We deliberately do
    // NOT await `m`: that would race deinit's phase-2 free of the ring. The
    // drain completing the gated message is verified via the broker's counter
    // (independent of the freed ring).
    dt.join();
    try testing.expectEqual(@as(u32, 1), broker.produced.load(.acquire));
}

// --- #5 sticky partitioner tests ------------------------------------------

test "#5 sticky: keyless burst coalesces to one partition with default partitioner" {
    // With the default (sticky) partitioner + linger_ms > 0, a burst of
    // keyless records should all land on ONE partition. The mock broker's
    // captured_batches track which partition each batch went to.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .ok,
        .num_partitions = 4,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.linger_ms = 50;
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    const n = 10;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(null); // let the partitioner decide → sticky
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "keyless-{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&handles) |*m| try m.await();

    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
    // All captured batches for this drain should target the same partition.
    try testing.expect(broker.captured_batches_len > 0);
    const first_part = broker.captured_batches[0].partition;
    for (broker.captured_batches[0..broker.captured_batches_len]) |b| {
        try testing.expectEqual(first_part, b.partition);
    }
}

test "#5 sticky: round_robin spreads keyless records across partitions (regression)" {
    // With round_robin partitioner, keyless records scatter across
    // partitions (the pre-sticky behavior). This is a regression guard.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .ok,
        .num_partitions = 4,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.linger_ms = 50;
    cfg.partitioner = .round_robin;
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    const n = 8;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(null);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "rr-{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&handles) |*m| try m.await();

    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
    // round-robin should produce batches on more than one partition.
    var seen: [4]bool = .{ false, false, false, false };
    for (broker.captured_batches[0..broker.captured_batches_len]) |b| {
        const idx: usize = @intCast(b.partition);
        if (idx < seen.len) seen[idx] = true;
    }
    var count: u32 = 0;
    for (seen) |s| if (s) {
        count += 1;
    };
    try testing.expect(count > 1);
}

test "#5 sticky: keyed records use key-hash with default partitioner" {
    // With the default (sticky) partitioner, keyed records route by murmur2
    // key-hash, not sticky. Assert the partition matches the key-hash mapping.
    const np: u32 = 4;
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .ok,
        .num_partitions = np,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.linger_ms = 50;
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    const key = "entity-42";
    const expected_part = partitioner.partitionForKey(key, np);

    const n = 5;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        try m.setKey(key);
        m.setPartition(null);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "keyed-{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&handles) |*m| try m.await();

    try testing.expectEqual(@as(u32, n), broker.produced.load(.acquire));
    try testing.expect(broker.captured_batches_len > 0);
    for (broker.captured_batches[0..broker.captured_batches_len]) |b| {
        try testing.expectEqual(@as(i32, @intCast(expected_part)), b.partition);
    }
}

test "#5 sticky: partition rotates between successive drains" {
    // After a drain completes (all acked), the next drain's keyless records
    // go to a DIFFERENT partition (the sticky cursor rotates). We produce
    // two bursts with a gap, awaiting all acks between them so each burst is
    // a separate drain. With 4 partitions and deterministic rotation, the
    // two drains should target different partitions.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .ok,
        .num_partitions = 4,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.linger_ms = 20;
    cfg.retry_backoff_ms = 0;
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    // First drain: 3 keyless records.
    const n1 = 3;
    var h1: [n1]Message = undefined;
    for (&h1, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(null);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "d1-{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&h1) |*m| try m.await();

    const first_drain_len = broker.captured_batches_len;
    try testing.expect(first_drain_len > 0);
    const first_part = broker.captured_batches[0].partition;

    // Second drain: 3 more keyless records. The sticky cursor has advanced,
    // so these should go to a different partition.
    const n2 = 3;
    var h2: [n2]Message = undefined;
    for (&h2, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(null);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "d2-{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&h2) |*m| try m.await();

    try testing.expect(broker.captured_batches_len > first_drain_len);
    // At least one batch in the second drain should be on a different
    // partition than the first drain.
    var found_different = false;
    for (broker.captured_batches[first_drain_len..broker.captured_batches_len]) |b| {
        if (b.partition != first_part) found_different = true;
    }
    try testing.expect(found_different);
}

// ---------------------------------------------------------------------------
// #7: pull-based metrics/stats
// ---------------------------------------------------------------------------

test "#7 stats: messages_produced, bytes_produced, messages_acked after all acks" {
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 4 });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var client = try Client.init(testing.allocator, testConfig(&bootstrap));
    defer client.deinit();

    const n: u32 = 20;
    var handles: [n]Message = undefined;
    var total_bytes: u64 = 0;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(null);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "stats-msg-{d}", .{i});
        try m.commit(@intCast(payload.len));
        total_bytes += payload.len;
    }
    // Wait for all acks before reading stats so the values are stable
    // (relaxed consistency — the test waits for the drain to complete).
    for (&handles) |*m| try m.await();

    // After all acks + reclaim, the queue should be drained. The network
    // thread wakes completion waiters before reclaiming, so poll for the
    // reclaim to land rather than asserting immediately.
    try expectQueueDepthZero(client);
    const s = client.stats();
    try testing.expectEqual(@as(u64, n), s.messages_produced);
    try testing.expectEqual(total_bytes, s.bytes_produced);
    try testing.expectEqual(@as(u64, n), s.messages_acked);
    try testing.expectEqual(@as(u64, 0), s.messages_failed);
    try testing.expect(s.batches_sent > 0);
}

test "#7 stats: fatal errors counted on non-retriable failure" {
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

    const s = client.stats();
    try testing.expectEqual(@as(u64, 1), s.messages_produced);
    try testing.expectEqual(@as(u64, 0), s.messages_acked);
    try testing.expectEqual(@as(u64, 1), s.messages_failed);
    try testing.expect(s.errors.fatal > 0);
}

test "#7 stats: queue_depth reflects pending slots before drain" {
    // Produce N messages to a gated broker without awaiting. The broker
    // blocks before responding to the first Produce, so the network thread
    // is stuck and the ring still has pending slots. Assert queue_depth == N.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .ok,
        .num_partitions = 1,
        .gate_first_produce = true,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.linger_ms = 0;
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    const n: u32 = 5;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "q{d}", .{i});
        try m.commit(@intCast(payload.len));
    }

    // Wait until the producer has sent the first Produce (now blocked in the
    // gate). The remaining slots are still pending in the ring.
    broker.waitFirstProduce();

    const s = client.stats();
    try testing.expectEqual(@as(u64, n), s.messages_produced);
    try testing.expect(s.queue_depth > 0);
    try testing.expect(s.queue_depth <= @as(u64, n));

    // Release the gate and await all so the ring drains cleanly.
    broker.openGate();
    for (&handles) |*m| try m.await();

    // The network thread wakes completion waiters before reclaiming, so poll
    // for the reclaim to land rather than asserting immediately.
    try expectQueueDepthZero(client);
    const s2 = client.stats();
    try testing.expectEqual(@as(u64, n), s2.messages_acked);
}

test "#7 stats: retriable errors and connection_drops counted" {
    // close_after_first_produce mode: the broker drops the first Produce
    // (connection drop), then the producer reconnects and retries. Assert
    // connection_drops > 0 and errors.retriable > 0.
    var broker = try mock.Broker.start(testing.allocator, .{
        .mode = .close_after_first_produce,
        .num_partitions = 1,
    });
    defer broker.stop();

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var cfg = testConfig(&bootstrap);
    cfg.max_retries = 5;
    cfg.linger_ms = 0;
    cfg.retry_backoff_ms = 5;
    cfg.enable_idempotency = false;
    var client = try Client.init(testing.allocator, cfg);
    defer client.deinit();

    const n: u32 = 3;
    var handles: [n]Message = undefined;
    for (&handles, 0..) |*m, i| {
        m.* = try client.acquire();
        try m.setTopic("events");
        m.setPartition(0);
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "cd{d}", .{i});
        try m.commit(@intCast(payload.len));
    }
    for (&handles) |*m| try m.await();

    const s = client.stats();
    try testing.expectEqual(@as(u64, n), s.messages_acked);
    try testing.expect(s.errors.connection_drops > 0);
    try testing.expect(s.errors.retriable > 0);
    try testing.expectEqual(@as(u64, 0), s.errors.fatal);
}
