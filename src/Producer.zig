//! The producer network thread: the single consumer of the `Ring`.
//!
//! One thread owns everything I/O: the broker connections, the metadata cache,
//! the ring drain loop, batching, and ack/retry signalling. Nothing here is
//! touched by producer (submitter) threads, so there are no locks — the ring's
//! atomics are the only cross-thread synchronization.
//!
//! ## Drain loop
//!
//! The loop is fully synchronous per broker: build a Produce request, send it,
//! block reading its response, apply acks, repeat. This synchronicity is what
//! makes in-place retry safe: a slot is only ever "in the
//! air" for the duration of one blocking round-trip, so there is never an
//! older Produce for a slot that "may still complete" while we consider a
//! retry. A pending slot observed at the top of a drain is therefore always
//! either brand-new or one that came back with a retriable error and needs
//! re-sending — re-collecting and re-sending pending slots *is* the retry.
//!
//!   1. `waitForData(read_head)` — block until a producer commits (or shutdown).
//!   2. Ensure metadata is fresh (refresh if empty/invalidated).
//!   3. Scan `[read_head, write_head)`, collect pending slots, assign a
//!      partition to any left unassigned, resolve each slot's leader.
//!   4. Sort collected slots by (leader, topic, partition, ring position);
//!      each contiguous (topic, partition) run becomes at most one v2 record
//!      batch per drain (the broker requires one batch per partition_data
//!      entry). A partition run is split by base_sequence identity so a retry
//!      batch never merges in fresh records; only the first sub-run is sent
//!      this drain, the rest stay pending. Group batches per leader into one
//!      Produce request.
//!   5. Send each leader's request, read the response, and per partition:
//!      ack (error 0), retry-in-place (retriable), or fail (non-retriable).
//!      Leadership errors additionally invalidate the metadata cache.
//!   6. `wakeCompletions()` once, then `reclaim()` to advance the low-water
//!      mark and wake backpressured producers.
//!
//! ## Allocation discipline
//!
//! No heap allocation on the produce hot path. All per-drain working memory
//! (collected-slot array, record scratch, batch encode buffer, per-slot retry
//! counters, and the Produce response decode scratch) is allocated once in
//! `init` and reused every drain. The response decode is backed by a
//! `FixedBufferAllocator` over an init-time scratch buffer, reset per drain —
//! `self.allocator` is never touched on the produce path. The remaining
//! allocations are cold-path: dialing a connection (once per broker), the
//! metadata-cache rebuild on refresh, and the first-use-per-topic round-robin
//! counter key (allocated once per topic, with a safe fallback if it fails).
//! The one copy on the produce path is slot payload -> record-batch buffer,
//! inside `record_batch.encodeBatch`.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.kafka_producer);

const Ring = @import("Ring.zig");
const Connection = @import("Connection.zig");
const partitioner = @import("partitioner.zig");
const wire = @import("wire/root.zig");
const primitives = wire.primitives;
const produce = wire.produce;
const metadata = wire.metadata;
const record_batch = wire.record_batch;
const Reader = primitives.Reader;

const Producer = @This();

/// A bootstrap broker endpoint.
pub const Broker = struct {
    host: []const u8,
    port: u16,
};

/// Producer configuration. String slices (`sni`, scram credentials,
/// `client_id`, bootstrap hosts) are borrowed and must outlive the producer —
/// `Client` owns the copies. `ca_bundle`, when set, is likewise caller-owned.
pub const Options = struct {
    bootstrap: []const Broker,
    sni: ?[]const u8,
    ca_bundle: ?*const std.crypto.Certificate.Bundle,
    insecure_skip_verify: bool,
    scram: Connection.ScramConfig,
    client_id: ?[]const u8,
    acks: i16,
    timeout_ms: i32,
    max_batch_bytes: u32,
    max_message_size: u32,
    strategy: partitioner.Strategy,
    /// Record-batch compression applied to every batch. `.snappy` is always
    /// available (pure-Zig, no build flag). `.zstd` requires the library to be
    /// built with `-Dzstd=true`; requesting it without that is rejected at
    /// `Client.init` with `error.CompressionUnavailable`. `.gzip` and `.lz4`
    /// are wire-format constants but are rejected at `Client.init` with
    /// `error.CompressionNotImplemented`.
    compression: record_batch.Compression = .none,
    /// Max in-place retries before a slot is failed with its last error code.
    max_retries: u8 = 8,
    /// Socket I/O timeout in ms (SO_RCVTIMEO + SO_SNDTIMEO), passed through to
    /// each `Connection.dial`. 0 = no timeout.
    io_timeout_ms: u32 = 30_000,
    /// Backoff between drains that made no forward progress (only retries /
    /// transient connection failures), so a persistently-unhealthy cluster
    /// does not spin the CPU.
    retry_backoff_ns: u64 = 5 * std.time.ns_per_ms,
    /// Linger time in ns: after the first pending record arrives, the network
    /// thread waits up to this long for more records before draining, to form
    /// larger batches (better throughput + compression ratio). 0 = eager
    /// (drain immediately when data is available, the previous behavior).
    /// The linger is a TIMER on the waitForData path, not a sleep: if a batch
    /// fills (`max_batch_bytes` worth of pending data) before the timer
    /// expires, the drain happens immediately — no latency regression under
    /// load.
    linger_ns: u64 = 0,
    /// Max age in ms before a proactive metadata refresh. The network thread
    /// refreshes metadata periodically even without errors, to catch silent
    /// leader changes / new brokers / new partitions. 0 = never refresh
    /// proactively (only on error/invalidation). Default 300000 (5 min,
    /// matching Kafka's `metadata.max.age.ms`).
    metadata_max_age_ms: u32 = 300_000,
    /// When true, the producer acquires a PID/epoch at startup (via
    /// InitProducerId v4 to any bootstrap broker), tracks per-partition
    /// sequence numbers, and sends real producer_id/producer_epoch/base_sequence
    /// in record batches. When false, the -1 sentinels are used (non-idempotent
    /// mode, the v1 default behavior). Idempotency ON is the production default.
    enable_idempotency: bool = true,
    /// Graceful shutdown drain timeout in ns. When `requestDrain` is called,
    /// the network thread continues draining in-flight Produce round-trips
    /// up to this duration before exiting. Slots still pending after the
    /// timeout surface `error.Shutdown` (via `requestShutdown`). 0 = no
    /// drain (the network thread exits immediately on drain). Default 10s.
    ///
    /// Contract (not a hard real-time bound): the deadline is checked BETWEEN
    /// blocking I/O operations, at the top of each drain iteration — not
    /// mid-syscall. A single in-flight Produce response read is bounded only by
    /// the socket read timeout (`io_timeout_ms`), so total drain time can
    /// overshoot `drain_timeout_ns` by up to `io_timeout_ms` when the thread is
    /// parked in `conn.readResponse()` at the moment the deadline passes. Size
    /// `io_timeout_ms` accordingly if a tight shutdown bound matters (the
    /// default 30s I/O timeout dominates a sub-second drain timeout). Capping
    /// the per-read timeout by the remaining drain time is a deliberate
    /// follow-up, not done here.
    drain_timeout_ns: u64 = 10 * std.time.ns_per_s,
};

/// A retriable error code that reflects a (possibly) stale leader — retrying
/// blindly would just hit the same dead leader, so these also invalidate the
/// metadata cache and force a refresh before the next send.
fn isLeadershipError(code: i16) bool {
    return switch (code) {
        3, // UNKNOWN_TOPIC_OR_PARTITION
        5, // LEADER_NOT_AVAILABLE
        6, // NOT_LEADER_OR_FOLLOWER
        9, // REPLICA_NOT_AVAILABLE
        => true,
        else => false,
    };
}

/// Whether a Produce error code is retriable, per the "Retriable" column of
/// https://kafka.apache.org/protocol.html#protocol_error_codes . This is the
/// produce-path-relevant subset of Kafka's `Errors` retriable set; codes not
/// listed (e.g. 17 INVALID_TOPIC_EXCEPTION, 18 RECORD_LIST_TOO_LARGE,
/// 87 INVALID_RECORD, 10 MESSAGE_TOO_LARGE, auth errors) are terminal.
pub fn isRetriable(code: i16) bool {
    if (isLeadershipError(code)) return true;
    return switch (code) {
        7, // REQUEST_TIMED_OUT
        13, // NETWORK_EXCEPTION
        14, // COORDINATOR_LOAD_IN_PROGRESS
        15, // COORDINATOR_NOT_AVAILABLE
        19, // NOT_ENOUGH_REPLICAS
        20, // NOT_ENOUGH_REPLICAS_AFTER_APPEND
        56, // KAFKA_STORAGE_ERROR
        75, // OFFSET_NOT_AVAILABLE
        => true,
        else => false,
    };
}

// ---------------------------------------------------------------------------
// Reconnect backoff (per host:port, exponential with jitter)
// ---------------------------------------------------------------------------

/// Base backoff before re-dialing a recently-disconnected broker.
const reconnect_base_ns: u64 = 100 * std.time.ns_per_ms;
/// Maximum backoff cap.
const reconnect_max_ns: u64 = 10 * std.time.ns_per_s;

/// Compute the backoff delay for a given consecutive failure count.
/// Exponential: base * 2^(failures-1), capped at max, with up to 50% jitter.
fn backoffFor(failures: u32) u64 {
    if (failures == 0) return 0;
    const exp: u6 = @intCast(@min(failures - 1, 20)); // saturate at 2^20
    const raw = reconnect_base_ns << exp;
    const capped = @min(raw, reconnect_max_ns);
    // Jitter: [0.5×, 1.0×) — deterministic per-call via a simple LCG over the
    // monotonic clock so tests are bounded but not perfectly predictable.
    var prng: std.Random.DefaultPrng = .init(@bitCast(@as(i64, @truncate(std.time.nanoTimestamp()))));
    const jitter_factor: u64 = prng.random().uintLessThan(u64, capped / 2 + 1);
    return capped / 2 + jitter_factor;
}

/// Record a connection failure for `host:port` and store the next-eligible
/// reconnect time. Called when a connection is dropped or a dial fails.
/// The backoff grows exponentially with each consecutive failure (capped at
/// `reconnect_max_ns`). A successful connection clears the entry (see
/// `clearBackoff`), so the count resets on success. Cold path.
fn recordBackoff(self: *Producer, host: []const u8, port: u16) void {
    var key_buf: [280]u8 = undefined;
    const key = std.fmt.bufPrint(&key_buf, "{s}:{d}", .{ host, port }) catch return;
    const now = @as(i64, @truncate(std.time.nanoTimestamp()));
    const gop = self.reconnect_backoff.getOrPut(self.allocator, key) catch return;
    if (!gop.found_existing) {
        // New entry: own the key. If the dup fails, remove the entry to avoid
        // a dangling key.
        const owned = self.allocator.dupe(u8, key) catch {
            _ = self.reconnect_backoff.remove(key);
            return;
        };
        gop.key_ptr.* = owned;
        // Store the next-eligible time: now + base backoff (1st failure).
        gop.value_ptr.* = now + @as(i64, @intCast(backoffFor(1)));
    } else {
        // Existing entry: the stored value is the previous next-eligible time.
        // Compute the new failure count from the elapsed time since the stored
        // time (approximate exponential: each call doubles the delay). This
        // avoids storing a separate counter — the timestamp itself encodes the
        // backoff progression.
        const prev = gop.value_ptr.*;
        const elapsed = now - prev; // time since last eligible-reconnect
        // If the previous backoff has expired, restart from base. Otherwise,
        // double the remaining delay (exponential growth).
        const new_delay: u64 = if (elapsed >= 0) backoffFor(1) else blk: {
            const remaining: u64 = @intCast(-elapsed);
            break :blk @min(remaining * 2, reconnect_max_ns);
        };
        gop.value_ptr.* = now + @as(i64, @intCast(new_delay));
    }
}

/// Check whether `host:port` is eligible for a reconnect (backoff expired or
/// no prior failure). Returns the remaining wait in ns (0 = eligible now).
fn backoffRemaining(self: *Producer, host: []const u8, port: u16) u64 {
    var key_buf: [280]u8 = undefined;
    const key = std.fmt.bufPrint(&key_buf, "{s}:{d}", .{ host, port }) catch return 0;
    const entry = self.reconnect_backoff.get(key) orelse return 0;
    const now = @as(i64, @truncate(std.time.nanoTimestamp()));
    if (entry <= now) return 0;
    return @intCast(entry - now);
}

/// Clear the backoff for `host:port` — called on a successful connection.
fn clearBackoff(self: *Producer, host: []const u8, port: u16) void {
    var key_buf: [280]u8 = undefined;
    const key = std.fmt.bufPrint(&key_buf, "{s}:{d}", .{ host, port }) catch return;
    if (self.reconnect_backoff.fetchRemove(key)) |kv| {
        self.allocator.free(kv.key);
    }
}

// ---------------------------------------------------------------------------
// Metadata cache (cold-path allocated, single-threaded, no locks)
// ---------------------------------------------------------------------------

const BrokerInfo = struct {
    node_id: i32,
    host: []u8,
    port: u16,
};

const PartitionInfo = struct {
    /// -1 when the partition has no live leader (e.g. election in progress).
    leader_id: i32,
    leader_epoch: i32,
};

const TopicInfo = struct {
    name: []u8,
    /// Indexed by partition index. Partitions absent from the response are
    /// left as `leader_id = -1`.
    partitions: []PartitionInfo,
};

/// One collected pending slot, ready to be grouped into a batch.
const Collected = struct {
    pos: u64,
    generation: u32,
    leader: i32,
    partition: i32,
    topic: []const u8,
};

// --- owned config (borrowed slices; Client owns backing memory) -----------
allocator: Allocator,
ring: *Ring,
options: Options,

// --- metadata cache -------------------------------------------------------
brokers: []BrokerInfo = &.{},
topics: []TopicInfo = &.{},
metadata_loaded: bool = false,
metadata_stale: bool = false,
/// Monotonic timestamp (ms) of the last successful metadata refresh. Used by
/// the periodic refresh check in `drainOnce`: if `now - last_metadata_refresh_ms
/// > metadata_max_age_ms`, refresh proactively. 0 = never refreshed.
last_metadata_refresh_ms: i64 = 0,

// --- connections keyed by "host:port" (owned keys) ------------------------
connections: std.StringHashMapUnmanaged(*Connection) = .empty,

// --- reconnect backoff: per host:port, exponential with jitter ------------
/// Maps owned "host:port" key → next-eligible reconnect time (mono ns).
/// A broker that was just disconnected gets an exponentially growing backoff
/// so a dead endpoint is not hammered on every drain. Cleared on successful
/// connection (a working connection resets the backoff for that endpoint).
/// Cold-path only: populated on disconnect, consulted on dial.
reconnect_backoff: std.StringHashMapUnmanaged(i64) = .empty,

// --- bootstrap failover: rotating start index for metadata fetches --------
/// Incremented (mod bootstrap.len) on every `metadataConnection` call so
/// successive metadata refreshes start from a different bootstrap broker.
/// Single-threaded (network thread only), no atomics needed.
bootstrap_offset: usize = 0,
/// When true, the next `refreshMetadata` uses the bootstrap list exclusively
/// (ignoring cached broker endpoints from the last metadata). Set when all
/// known brokers are unreachable; cleared on a successful metadata refresh.
/// This is Kafka's `metadata.recovery.strategy=rebootstrap` equivalent.
force_bootstrap: bool = false,

// --- round-robin counters, persistent across metadata refreshes -----------
round_robin: std.StringHashMapUnmanaged(partitioner.RoundRobin) = .empty,
// --- sticky partition state, per topic, persistent across metadata refreshes ---
sticky: std.StringHashMapUnmanaged(partitioner.Sticky) = .empty,

// --- network-thread stats counters (read by stats() from another thread) ---
// Single-writer (network thread) / multi-reader (stats). Monotonic stores
// from the network thread, monotonic loads from stats() -- relaxed consistency,
// torn/stale reads are acceptable (it's metrics, not accounting).
batches_sent: std.atomic.Value(u64) = .init(0),
messages_acked: std.atomic.Value(u64) = .init(0),
messages_failed: std.atomic.Value(u64) = .init(0),
errors_retriable: std.atomic.Value(u64) = .init(0),
errors_fatal: std.atomic.Value(u64) = .init(0),
errors_connection_drops: std.atomic.Value(u64) = .init(0),

// --- preallocated drain scratch (reused every drain, never on hot path) ---
collected: []Collected,
records_scratch: []record_batch.Record,
topic_data_scratch: []produce.TopicData,
partition_data_scratch: []produce.PartitionData,
group_scratch: []Group,
encode_buf: []u8,
/// Produce response decode scratch, backed by a FixedBufferAllocator reset
/// per drain. Sized to fit a max Produce response (see `init`). Never touched
/// by `self.allocator` — the whole produce response path is FBA-backed.
resp_fba_buf: []u8,
/// Scratch for the compressed records region (sized to compressBound(max_batch_bytes));
/// empty when compression is .none.
compress_scratch: []u8,
/// Per physical slot: how many in-place retries so far, bound to a generation
/// so a recycled slot resets its counter.
attempts: []u8,
attempt_gen: []u32,

// --- drain cursor ---------------------------------------------------------
read_head: u64 = 0,
/// Shared round-robin counter used only if allocating a per-topic key fails.
rr_fallback: partitioner.RoundRobin = .{},
/// Shared sticky state used only if allocating a per-topic key fails.
sticky_fallback: partitioner.Sticky = .{},

// --- idempotent producer state --------------------------------------------
/// PID and epoch assigned by the broker via InitProducerId v4. -1 = not yet
/// acquired (or idempotency is off). When `enable_idempotency` is true, these
/// are populated at startup and passed to every record batch's `EncodeOptions`.
producer_id: i64 = -1,
producer_epoch: i16 = -1,
pid_acquired: bool = false,
/// Per-(topic, partition) next sequence number. Key is a packed u64:
/// high 32 bits = topic hash, low 32 bits = partition index. The value is the
/// next base sequence to assign for the NEXT first-send batch on that
/// (topic, partition). On retry, the slot's stored `base_sequence` is reused
/// — this counter is NOT advanced on retry (the broker dedupes by sequence).
/// Sequence numbers wrap at i32 max; the wraparound full-reset is a follow-up
/// and requires a full producer-ID and sequence reset.
sequences: std.AutoHashMapUnmanaged(SeqKey, i32) = .empty,
/// Set by `applyResponse` when a partition returns OUT_OF_ORDER_SEQUENCE (45)
/// or UNKNOWN_PRODUCER_ID (73): the PID's sequence state is unrecoverable, so
/// the next drain must re-init the PID (fresh epoch), reset ALL per-partition
/// counters to 0, and clear the `base_sequence` stamp on every pending slot so
/// each re-sends fresh from 0 under the new PID (see `recoverProducerId`).
pid_reset_pending: bool = false,
/// Set by `run()` when the startup `initProducerId` fails while
/// `enable_idempotency` is true. Gates Produce exactly like `pid_reset_pending`:
/// the next drain retries acquisition BEFORE building any batch and does not
/// send until a PID is in hand, so the default idempotency contract is never
/// silently downgraded to non-idempotent (sentinel -1) batches. Distinct from
/// `pid_reset_pending` (mid-flight loss after a 45/73 error) only in WHEN it is
/// set — both block Produce until the PID is acquired. Persistent acquisition
/// failure is bounded via `chargeRecoveryFailure` so in-flight slots eventually
/// fail rather than hang.
pid_pending: bool = false,

/// A (topic, partition) run in the sorted `collected` array: `[start, end)`
/// map to one record batch / one partition_response.
const Group = struct {
    topic: []const u8,
    partition: i32,
    start: usize,
    end: usize,
};

/// Key for the per-(topic, partition) sequence map. Using a u64 hash of the
/// topic name + partition avoids owning topic string copies in the map (the
/// metadata cache's topic names can be freed on refresh). A hash collision
/// would be CATASTROPHIC for sequence correctness — two partitions sharing a
/// counter would emit overlapping/gapped sequences and wedge both. We accept
/// that risk only because it is genuinely negligible: with a good 64-bit
/// mixer (wyhash) the birthday-bound collision probability across a realistic
/// topic set (~10^4 (topic, partition) pairs) is ~10^-13. Owned string keys
/// would remove the risk entirely at the cost of cold-path allocation; the
/// probability is low enough that the hash is the pragmatic choice, but the
/// tradeoff is a real (if remote) correctness gamble, not a free lunch.
const SeqKey = u64;

pub fn init(allocator: Allocator, ring: *Ring, options: Options) !Producer {
    const num_slots = ring.numSlots();
    // Encode buffer holds one leader's worth of batches. Bounded by
    // `max_batch_bytes` (the per-request cap the caller configured) plus slack
    // for the Produce request framing overhead (transactional_id, acks,
    // timeout, compact-array counts, tag buffers). A 64 KiB floor keeps
    // multi-partition fan-out cheap when max_batch_bytes is small.
    const encode_buf_len: usize = @max(
        @as(usize, 64 * 1024),
        @as(usize, options.max_batch_bytes) + 4096,
    );

    // Produce response decode scratch. A single Produce response carries one
    // TopicResponse per topic, one PartitionResponse per partition, each with
    // a few fixed fields, a small record_errors array, and an optional
    // error_message string. 64 KiB fits realistic responses (hundreds of
    // partitions × ~100 bytes each). If a response exceeds this the FBA
    // returns error.OutOfMemory and the slots are failed with a transport
    // error — see `sendLeader`.
    const resp_fba_len: usize = 64 * 1024;

    const collected = try allocator.alloc(Collected, num_slots);
    errdefer allocator.free(collected);
    const records_scratch = try allocator.alloc(record_batch.Record, num_slots);
    errdefer allocator.free(records_scratch);
    const topic_data_scratch = try allocator.alloc(produce.TopicData, num_slots);
    errdefer allocator.free(topic_data_scratch);
    const partition_data_scratch = try allocator.alloc(produce.PartitionData, num_slots);
    errdefer allocator.free(partition_data_scratch);
    const group_scratch = try allocator.alloc(Group, num_slots);
    errdefer allocator.free(group_scratch);
    const encode_buf = try allocator.alloc(u8, encode_buf_len);
    errdefer allocator.free(encode_buf);
    const resp_fba_buf = try allocator.alloc(u8, resp_fba_len);
    errdefer allocator.free(resp_fba_buf);
    // Scratch for the compressed records region, sized once to the worst-case
    // bound of one max_batch_bytes batch. Only allocated when compression is
    // enabled; empty slice otherwise (encodeBatch ignores scratch for .none).
    const compress_scratch = if (options.compression != .none)
        try allocator.alloc(u8, wire.compress.compressBound(
            @enumFromInt(@intFromEnum(options.compression)),
            options.max_batch_bytes,
        ))
    else
        try allocator.alloc(u8, 0);
    errdefer allocator.free(compress_scratch);
    const attempts = try allocator.alloc(u8, num_slots);
    errdefer allocator.free(attempts);
    const attempt_gen = try allocator.alloc(u32, num_slots);
    errdefer allocator.free(attempt_gen);
    @memset(attempts, 0);
    @memset(attempt_gen, 0);

    return .{
        .allocator = allocator,
        .ring = ring,
        .options = options,
        .collected = collected,
        .records_scratch = records_scratch,
        .topic_data_scratch = topic_data_scratch,
        .partition_data_scratch = partition_data_scratch,
        .group_scratch = group_scratch,
        .encode_buf = encode_buf,
        .resp_fba_buf = resp_fba_buf,
        .compress_scratch = compress_scratch,
        .attempts = attempts,
        .attempt_gen = attempt_gen,
    };
}

pub fn deinit(self: *Producer) void {
    self.freeMetadata();
    var it = self.connections.iterator();
    while (it.next()) |entry| {
        entry.value_ptr.*.close();
        self.allocator.free(entry.key_ptr.*);
    }
    self.connections.deinit(self.allocator);

    var rr_it = self.round_robin.iterator();
    while (rr_it.next()) |entry| self.allocator.free(entry.key_ptr.*);
    self.round_robin.deinit(self.allocator);

    var st_it = self.sticky.iterator();
    while (st_it.next()) |entry| self.allocator.free(entry.key_ptr.*);
    self.sticky.deinit(self.allocator);

    var rb_it = self.reconnect_backoff.iterator();
    while (rb_it.next()) |entry| self.allocator.free(entry.key_ptr.*);
    self.reconnect_backoff.deinit(self.allocator);

    self.allocator.free(self.collected);
    self.allocator.free(self.records_scratch);
    self.allocator.free(self.topic_data_scratch);
    self.allocator.free(self.partition_data_scratch);
    self.allocator.free(self.group_scratch);
    self.allocator.free(self.encode_buf);
    self.allocator.free(self.resp_fba_buf);
    self.allocator.free(self.compress_scratch);
    self.allocator.free(self.attempts);
    self.allocator.free(self.attempt_gen);
    self.sequences.deinit(self.allocator);
    self.* = undefined;
}

// ---------------------------------------------------------------------------
// Main loop (runs on the network thread)
// ---------------------------------------------------------------------------

/// The network-thread entry point. Blocks in `waitForData` between drains and
/// returns when the ring is shut down or the graceful drain completes/times
/// out.
pub fn run(self: *Producer) void {
    // Acquire a PID/epoch before the first drain when idempotency is enabled.
    // This is a blocking call to a bootstrap broker; if it fails we set
    // `pid_pending` so the next drain retries acquisition BEFORE sending any
    // Produce. We must NOT silently proceed with sentinel -1 batches — that
    // would downgrade the caller's default no-duplicates contract without
    // their knowledge. `drainOnce` gates Produce on `pid_pending`; mid-flight
    // PID recovery on sequence errors is handled by `recoverProducerId`.
    if (self.options.enable_idempotency) {
        self.initProducerId() catch |err| {
            log.warn("PID acquisition failed, deferring send until acquired: {s}", .{@errorName(err)});
            self.pid_pending = true;
        };
    }
    self.read_head = self.ring.readHead();
    // Drain deadline (nanoTimestamp): set when draining is first detected.
    // -1 = not yet draining. Once set, the loop exits if `now >= deadline`,
    // bounding the total drain time so a stuck broker cannot hang shutdown.
    var drain_deadline: i64 = -1;
    while (!self.ring.isShutdown()) {
        if (!self.ring.waitForData(self.read_head)) break; // shutdown or drain complete

        if (self.ring.isDraining()) {
            // First drain detection: record the deadline.
            if (drain_deadline < 0) {
                const now: i64 = @truncate(std.time.nanoTimestamp());
                drain_deadline = now + @as(i64, @intCast(self.options.drain_timeout_ns));
            }
            // Drain timeout expired: stop draining, exit the loop.
            const now: i64 = @truncate(std.time.nanoTimestamp());
            if (now >= drain_deadline) break;
            // Skip linger during drain — flush in-flight acks as fast as
            // possible, no batching benefit during shutdown.
        } else {
            // Linger: after the first pending record arrives, wait up to
            // `linger_ns` for more records to accumulate before draining, to
            // form larger batches. Skipped when `linger_ns == 0` (eager
            // drain). If a batch fills (`max_batch_bytes` worth of pending
            // data) before the timer expires, the linger exits early — no
            // latency regression under load. This is a TIMER on the
            // waitForData path, not a sleep: it only runs when data is
            // already available and we're choosing to wait for more. The
            // retry backoff (below) is a SEPARATE concern — it sleeps when a
            // drain made no forward progress.
            self.linger();
        }

        const progressed = self.drainOnce() catch |err| brk: {
            log.warn("drain failed: {s}", .{@errorName(err)});
            break :brk false;
        };
        // Advance the wait cursor to the reclaim low-water mark. When slots
        // remain pending (in-flight retries / a stuck partition), read_head
        // stays below write_head, so `waitForData` returns immediately; the
        // backoff below stops that from becoming a busy-spin.
        self.read_head = self.ring.readHead();
        if (!progressed) {
            if (self.ring.isDraining()) {
                // Bounded backoff during drain: don't sleep past the deadline.
                const now: i64 = @truncate(std.time.nanoTimestamp());
                if (now >= drain_deadline) break;
                const remaining: u64 = @intCast(drain_deadline - now);
                const sleep_ns = @min(self.options.retry_backoff_ns, remaining);
                std.Thread.sleep(sleep_ns);
            } else {
                std.Thread.sleep(self.options.retry_backoff_ns);
            }
        }
    }
}

/// Linger: after the first pending record arrives, wait up to `linger_ns`
/// for more records to accumulate before draining. Exits early if a batch
/// fills (`max_batch_bytes` worth of pending data) or on shutdown. No-op when
/// `linger_ns == 0`.
fn linger(self: *Producer) void {
    const linger_ns = self.options.linger_ns;
    if (linger_ns == 0) return;

    const start: i64 = @truncate(std.time.nanoTimestamp());
    const deadline: i64 = start + @as(i64, @intCast(linger_ns));

    while (true) {
        if (self.ring.isShutdown()) return;
        if (self.pendingBatchFull()) return;

        const now: i64 = @truncate(std.time.nanoTimestamp());
        if (now >= deadline) return;

        // Wait for more data with a bounded timeout. The futex word is
        // write_head's low 32 bits; a producer committing advances write_head
        // and wakes us. We re-check the deadline and batch-full condition on
        // each wake/timeout.
        const remaining: u64 = @intCast(deadline - now);
        const wait_ns = @min(remaining, 50 * std.time.ns_per_ms);
        const known_head = self.ring.writeHead();
        _ = self.ring.waitForDataTimed(known_head, wait_ns);
    }
}

/// Whether the pending slots contain enough data to fill a `max_batch_bytes`
/// batch. Used by the linger to exit early under load (no over-linger).
/// Estimates the batch size by summing each pending slot's value_len + key_len
/// + a fixed per-record overhead. Early-exits once the sum reaches
/// `max_batch_bytes`. O(pending slots) but breaks at the first full batch —
/// under load this is O(batch_size), not O(ring_size).
fn pendingBatchFull(self: *Producer) bool {
    const max_batch = self.options.max_batch_bytes;
    const base = self.ring.readHead();
    const head = self.ring.writeHead();
    var total: u32 = 0;
    var pos = base;
    while (pos < head) : (pos += 1) {
        if (!self.ring.slotIsPending(pos)) continue;
        const slot = self.ring.slotAt(pos);
        total += slot.value_len + @as(u32, slot.key_len) + 20; // 20 ≈ per-record varint overhead
        if (total >= max_batch) return true;
    }
    return false;
}
/// fail) this pass — i.e. forward progress was made.
fn drainOnce(self: *Producer) !bool {
    // Refresh metadata when stale (error-triggered), not yet loaded (first
    // drain), or aged past `metadata_max_age_ms` (periodic refresh to catch
    // silent leader changes / new brokers / new partitions without an error).
    if (self.metadata_stale or !self.metadata_loaded or self.metadataAged()) {
        self.refreshMetadata() catch |err| {
            log.warn("metadata refresh failed: {s}", .{@errorName(err)});
            // Charge the failure against pending slots so they eventually fail
            // (bounded by max_retries) rather than hanging forever when all
            // brokers are unreachable. Without this, a persistently-down
            // cluster would leave every slot pending indefinitely — their
            // `await()` would never resolve.
            self.chargeMetadataFailure();
            return false; // back off; try again next drain
        };
    }

    // Idempotent PID recovery, deferred here from `applyResponse` so it runs
    // once (not per partition) and BEFORE any batch is built this drain. On
    // OUT_OF_ORDER_SEQUENCE / UNKNOWN_PRODUCER_ID the PID's broker-side
    // sequence state is gone; re-init and reset every pending slot's sequence
    // to 0 under the fresh PID before re-sending. If re-init fails, back off
    // and retry next drain (the flag stays set) rather than send with a stale
    // PID + reset counters.
    if (self.pid_reset_pending) {
        self.recoverProducerId() catch |err| {
            log.warn("PID reset failed, retrying next drain: {s}", .{@errorName(err)});
            return false;
        };
    }

    // Startup PID acquisition, deferred here when the initial `initProducerId`
    // in `run()` failed. With `enable_idempotency` true the producer must NOT
    // send Produce until a PID is acquired — silently downgrading to sentinel
    // -1 batches would violate the caller's no-duplicates contract. Retry
    // acquisition before building any batch; on persistent failure charge
    // pending slots' retry budgets so they eventually fail (bounded) rather
    // than hang. `pid_pending` clears only on a successful acquisition, so a
    // later send never inherits the non-idempotent path by accident.
    if (self.pid_pending) {
        self.initProducerId() catch |err| {
            log.warn("PID acquisition failed, retrying next drain: {s}", .{@errorName(err)});
            self.chargeRecoveryFailure();
            return false;
        };
        self.pid_pending = false;
    }

    const base = self.ring.readHead();
    const head = self.ring.writeHead();
    if (base >= head) return false;

    // Reset sticky partition state for this drain cycle: the first keyless
    // record per topic picks a new sticky partition (rotating from last
    // drain), and all subsequent keyless records in this drain reuse it.
    self.beginStickyDrain();

    // --- collect pending slots, assign partitions, resolve leaders ---
    var n: usize = 0;
    var last_topic: []const u8 = &.{};
    var last_topic_np: u32 = 0;
    var pos = base;
    while (pos < head and n < self.collected.len) : (pos += 1) {
        const slot = self.ring.slotAt(pos);
        if (!self.ring.slotIsPending(pos)) continue;

        const topic = self.ring.slotTopic(pos);
        const generation = self.ring.slotGeneration(pos);

        // num_partitions for this topic (cache last topic to avoid repeated
        // lookups across the common run of same-topic slots).
        const np = if (std.mem.eql(u8, topic, last_topic)) last_topic_np else self.numPartitions(topic);
        if (np == 0) {
            // Unknown topic: force a refresh, and fail the slot if we have
            // already retried it past the limit (avoids an infinite refresh
            // loop against a genuinely nonexistent topic).
            self.metadata_stale = true;
            if (self.bumpAttempts(pos, generation) > self.options.max_retries) {
                self.ring.markFailed(pos, generation, 3); // UNKNOWN_TOPIC_OR_PARTITION
                self.recordFailure();
            }
            continue;
        }
        last_topic = topic;
        last_topic_np = np;

        // Assign a partition if the producer left it unassigned.
        var partition = slot.partition;
        if (partition == Ring.partition_unassigned) {
            const key: ?[]const u8 = blk: {
                const k = self.ring.slotKey(pos);
                break :blk if (k.len == 0) null else k;
            };
            partition = self.pickPartition(topic, key, np);
            slot.partition = partition;
        }

        const leader = self.leaderFor(topic, partition);
        if (leader == null or leader.? < 0) {
            self.metadata_stale = true;
            if (self.bumpAttempts(pos, generation) > self.options.max_retries) {
                self.ring.markFailed(pos, generation, 5); // LEADER_NOT_AVAILABLE
                self.recordFailure();
            }
            continue;
        }

        self.collected[n] = .{
            .pos = pos,
            .generation = generation,
            .leader = leader.?,
            .partition = @intCast(partition),
            .topic = topic,
        };
        n += 1;
    }

    if (n == 0) {
        // Nothing sendable; may still have marked unknown-topic slots failed.
        self.ring.wakeCompletions();
        _ = self.ring.reclaim();
        return false;
    }

    std.sort.pdq(Collected, self.collected[0..n], {}, lessThan);

    // --- send one request per leader, process its response ---
    var progressed = false;
    var i: usize = 0;
    while (i < n) {
        const leader = self.collected[i].leader;
        var j = i;
        while (j < n and self.collected[j].leader == leader) j += 1;
        if (self.sendLeader(leader, self.collected[i..j])) |ok| {
            if (ok) progressed = true;
        } else |err| {
            log.warn("send to leader {d} failed: {s}", .{ leader, @errorName(err) });
            // Connection error: drop the connection and retry these slots.
            self.dropLeaderConnection(leader);
            self.metadata_stale = true;
            _ = self.errors_connection_drops.fetchAdd(1, .monotonic);
            self.retrySlots(self.collected[i..j]);
        }
        i = j;
    }

    self.ring.wakeCompletions();
    _ = self.ring.reclaim();
    return progressed;
}

fn lessThan(_: void, a: Collected, b: Collected) bool {
    if (a.leader != b.leader) return a.leader < b.leader;
    const ord = std.mem.order(u8, a.topic, b.topic);
    if (ord != .eq) return ord == .lt;
    if (a.partition != b.partition) return a.partition < b.partition;
    // Tie-breaker: ring position. `std.sort.pdq` is NOT stable, so without
    // this equal (leader, topic, partition) slots could be reordered
    // arbitrarily between drains. Per-record offset deltas (and thus
    // per-record sequence numbers) are assigned by slot order, so a
    // non-deterministic order would make idempotent sequence assignment
    // non-deterministic. Ordering by `pos` preserves commit order and makes
    // the base_sequence sub-run split (see `sendLeader`) well-defined.
    return a.pos < b.pos;
}

// ---------------------------------------------------------------------------
// Sending a leader's batches
// ---------------------------------------------------------------------------

/// Build and send one Produce request carrying every collected slot for
/// `leader`, then apply the response. Returns true if any slot reached a
/// terminal state (acked or failed) this round-trip; false if every partition
/// response was a pure retry (so the drain loop backs off before retrying).
/// Connection errors propagate so the caller can reconnect and retry.
fn sendLeader(self: *Producer, leader: i32, slots: []const Collected) !bool {
    const now_ms = std.time.milliTimestamp();
    const max_batch = self.options.max_batch_bytes;

    // Build topic_data / partition_data + record batches into the reusable
    // scratch, and remember each partition run as a Group for ack matching.
    // Batching is bounded by `max_batch_bytes`: once `encode_off` reaches the
    // cap, remaining slots are left pending for the next drain.
    var td_len: usize = 0;
    var pd_len: usize = 0;
    var group_len: usize = 0;
    var encode_off: usize = 0;
    var stopped_at_capacity = false;
    var terminal_progress = false;

    var k: usize = 0;
    while (k < slots.len) {
        const topic = slots[k].topic;
        var t_end = k;
        while (t_end < slots.len and std.mem.eql(u8, slots[t_end].topic, topic)) t_end += 1;

        const pd_start = pd_len;
        var p = k;
        while (p < t_end) {
            const part = slots[p].partition;
            var p_end = p;
            while (p_end < t_end and slots[p_end].partition == part) p_end += 1;

            // At most ONE record batch per (topic, partition) per request:
            // Kafka requires exactly one batch per partition_data entry, and
            // the response carries one partition_response per partition, so we
            // never emit two partition_data entries for the same index.
            //
            // Within the partition run, split by base_sequence identity and
            // take only the FIRST sub-run (Fix 2). Slots are ordered by ring
            // position (see `lessThan`), so an already-sent batch's slots
            // (base_sequence != -1, committed earlier → lower pos) form a
            // contiguous leading sub-run and fresh slots (base_sequence == -1,
            // committed later) form the trailing sub-run. Emitting only the
            // first sub-run means:
            //   - a retry batch stays byte-identical to its original send (it
            //     never absorbs fresh records committed after it was sent), so
            //     broker dedup by (PID, epoch, sequence) still works, and
            //   - idempotent ordering holds (base_sequence N is acked before
            //     N+k is ever sent).
            // Remaining sub-runs stay pending and are sent on a later drain
            // (re-collected from the ring each drain).
            const seq_id = self.ring.slotBaseSequence(slots[p].pos);
            var s_end = p;
            while (s_end < p_end and self.ring.slotBaseSequence(slots[s_end].pos) == seq_id) s_end += 1;

            // If we've already hit the max_batch_bytes cap, stop adding batches
            // to this leader's request. Remaining slots stay pending and are
            // sent in the next drain.
            if (encode_off >= max_batch) {
                stopped_at_capacity = true;
                break;
            }

            // One record batch for this base_sequence sub-run [p, s_end).
            var rc: usize = 0;
            for (slots[p..s_end], 0..) |c, idx| {
                const key = self.ring.slotKey(c.pos);
                self.records_scratch[rc] = .{
                    .offset_delta = @intCast(idx),
                    .timestamp_delta = 0,
                    .key = if (key.len == 0) null else key,
                    .value = self.ring.slotValueCommitted(c.pos),
                    .headers = &.{},
                };
                rc += 1;
            }

            // --- idempotent sequence: PEEK, do not advance yet (Fix 1) ---
            // Fresh sub-run (seq_id == -1): peek the per-partition counter to
            // obtain base_seq for encoding, but DON'T advance the counter or
            // stamp the slots until the batch clears every size gate below.
            // Otherwise a locally-failed oversized batch would burn sequence
            // numbers and wedge the partition. Retry sub-run (seq_id != -1):
            // reuse the stored base_sequence; the counter is never advanced on
            // retry (the broker dedupes by sequence).
            const enable_idem = self.options.enable_idempotency and self.pid_acquired;
            var base_seq: i32 = -1;
            var commit_fresh_seq = false;
            if (enable_idem) {
                if (seq_id != -1) {
                    base_seq = seq_id; // retry: reuse the assigned sequence
                } else if (self.peekSequence(topic, part)) |seq| {
                    base_seq = seq; // fresh: peek now, advance only at commit
                    commit_fresh_seq = true;
                } else |_| {
                    base_seq = -1; // sequence map OOM → non-idempotent fallback
                }
            }

            const pid: i64 = if (base_seq != -1) self.producer_id else -1;
            const epoch: i16 = if (base_seq != -1) self.producer_epoch else -1;

            const batch_options: record_batch.EncodeOptions = .{
                .base_timestamp = now_ms,
                .compression = self.options.compression,
                .scratch = self.compress_scratch,
                .producer_id = pid,
                .producer_epoch = epoch,
                .base_sequence = base_seq,
            };

            var batch: []u8 = undefined;
            var consumed_end = s_end;
            if (enable_idem and seq_id != -1) {
                batch = record_batch.encodeBatch(
                    self.encode_buf[encode_off..],
                    self.records_scratch[0..rc],
                    batch_options,
                ) catch |err| switch (err) {
                    error.BufferTooSmall => {
                        stopped_at_capacity = true;
                        break;
                    },
                    else => return err,
                };
                if (batch.len > max_batch or encode_off + batch.len > max_batch) {
                    stopped_at_capacity = true;
                    break;
                }
            } else {
                const batch_budget = @min(
                    @as(usize, max_batch) - encode_off,
                    self.encode_buf.len - encode_off,
                );
                const bounded = record_batch.encodeBatchBounded(
                    self.encode_buf[encode_off..],
                    self.records_scratch[0..rc],
                    batch_budget,
                    batch_options,
                ) catch |err| switch (err) {
                    // The batch doesn't fit in the remaining encode buffer. If
                    // nothing has been built yet for this leader's request,
                    // fail the first slot as MESSAGE_TOO_LARGE. Otherwise,
                    // leave the rest pending. The sequence counter was only
                    // peeked (not committed), so a failed batch burns no
                    // sequence numbers.
                    error.BufferTooSmall => {
                        if (pd_len == pd_start and td_len == 0 and encode_off == 0) {
                            self.failSlots(slots[p .. p + 1], 10); // MESSAGE_TOO_LARGE
                            terminal_progress = true;
                            p += 1;
                            continue;
                        }
                        stopped_at_capacity = true;
                        break;
                    },
                    else => return err,
                };

                if (bounded.consumed == 0) {
                    if (encode_off == 0 and pd_len == pd_start and td_len == 0) {
                        self.failSlots(slots[p .. p + 1], 10); // MESSAGE_TOO_LARGE
                        terminal_progress = true;
                        p += 1;
                        continue;
                    }
                    stopped_at_capacity = true;
                    break;
                }

                batch = bounded.bytes;
                consumed_end = p + bounded.consumed;
                assert(consumed_end <= s_end);
                assert(encode_off + batch.len <= max_batch);
            }
            assert(findGroup(self.group_scratch[0..group_len], topic, part) == null);

            // --- COMMIT: every size gate passed ---
            // Only now advance the per-partition sequence and stamp the fresh
            // slots, so an in-place retry reuses this exact base_sequence and a
            // never-committed batch leaves the counter untouched (Fix 1).
            if (commit_fresh_seq) {
                self.commitSequence(topic, part, base_seq, @intCast(consumed_end - p));
                for (slots[p..consumed_end]) |c| self.ring.setSlotBaseSequence(c.pos, base_seq);
            }

            encode_off += batch.len;

            self.partition_data_scratch[pd_len] = .{ .index = part, .records = batch };
            self.group_scratch[group_len] = .{
                .topic = topic,
                .partition = part,
                .start = p,
                .end = consumed_end,
            };
            pd_len += 1;
            group_len += 1;
            p = consumed_end;
            if (p < s_end) {
                stopped_at_capacity = true;
                break;
            }
            // Skip the rest of this partition run — remaining base_sequence
            // sub-runs (if any) are deferred to a later drain.
            p = p_end;
        }

        if (pd_len > pd_start) {
            self.topic_data_scratch[td_len] = .{
                .name = topic,
                .partition_data = self.partition_data_scratch[pd_start..pd_len],
            };
            td_len += 1;
        }
        if (stopped_at_capacity) break;
        k = t_end;
    }

    if (td_len == 0) {
        // Nothing to send: either all slots were failed as MESSAGE_TOO_LARGE
        // (terminal progress), or we stopped at capacity with nothing built
        // (no progress — slots are still pending for the next drain).
        return terminal_progress;
    }

    const conn = try self.leaderConnection(leader);
    const body: ProduceBody = .{
        .acks = self.options.acks,
        .timeout_ms = self.options.timeout_ms,
        .topic_data = self.topic_data_scratch[0..td_len],
    };
    const corr = try conn.sendRequest(.produce, 9, body);
    _ = self.batches_sent.fetchAdd(1, .monotonic);

    if (self.options.acks == 0) {
        // No response is sent for acks=0; treat every collected slot as acked.
        for (self.group_scratch[0..group_len]) |g| self.ackSlots(slots[g.start..g.end]);
        return true;
    }

    const resp_body = try conn.readResponse();
    var payload = try stripHeaderV1(resp_body, corr);

    // Decode the response with a FixedBufferAllocator over init-time scratch,
    // not self.allocator — the produce hot path must not allocate from the
    // general allocator. The FBA is reset per drain (the response is consumed
    // and deinit'd within this function). If the response doesn't fit (a
    // pathological response exceeding 64 KiB), fail the slots with a transport
    // error rather than falling back to the general allocator.
    var fba = std.heap.FixedBufferAllocator.init(self.resp_fba_buf);
    var resp = produce.decodeResponse(fba.allocator(), &payload) catch |err| switch (err) {
        error.OutOfMemory => {
            log.warn("produce response exceeded FBA scratch ({d} bytes)", .{self.resp_fba_buf.len});
            // Fail exactly the slots that were sent in this request, iterating
            // the group scratch rather than assuming sent groups are a prefix
            // of `slots` (earlier groups may have been skipped/failed as
            // too-large, so the sent groups are not necessarily contiguous
            // from slot 0).
            for (self.group_scratch[0..group_len]) |g| {
                self.failSlots(slots[g.start..g.end], 13); // NETWORK_EXCEPTION
            }
            return true; // failing slots is terminal progress
        },
        else => return err,
    };
    defer resp.deinit(fba.allocator());

    return self.applyResponse(resp, slots, self.group_scratch[0..group_len]);
}

/// Apply a Produce response's per-partition results to the collected slots.
/// Returns true if any slot reached a terminal state (acked or failed,
/// including a retry budget exhaustion that becomes markFailed); false if
/// every partition response was a pure retry (no terminal progress).
fn applyResponse(
    self: *Producer,
    resp: produce.Response,
    slots: []const Collected,
    groups: []const Group,
) bool {
    // Idempotent-specific error handling (only when a real PID is in use, i.e.
    // producer_id != -1). These three codes are checked BEFORE the generic
    // retriable/non-retriable classification; in non-idempotent mode they fall
    // through and fail (a broker must not send them without a PID).
    //
    //   - DUPLICATE_SEQUENCE_NUMBER (37): the broker already appended this exact
    //     (PID, epoch, sequence) — the first attempt's write is durable and only
    //     the ack was lost. Treat as an ACK, not a failure. (Java
    //     `ProducerStateManager.checkSequence` returns DUPLICATE for a batch
    //     whose sequence lies within the retained cache; `Sender` completes the
    //     batch successfully. KIP-98.)
    //   - OUT_OF_ORDER_SEQUENCE (45) / UNKNOWN_PRODUCER_ID (73): the broker's
    //     sequence state for this PID is unrecoverable from the client side (a
    //     gap it can't reconcile, or the producer state was dropped — segment
    //     deletion / retention / DeleteRecords). Re-arm the slots for retry and
    //     flag a PID reset: the next drain re-inits the PID (fresh epoch),
    //     resets ALL per-partition sequences to 0, and clears every pending
    //     slot's base_sequence so it re-sends from 0 under the new PID. This is
    //     the simplest globally-correct recovery for a non-transactional
    //     idempotent producer that does not track last-acked sequences per
    //     partition: a fresh PID makes the broker forget all prior sequence
    //     state, so restarting every partition at 0 can never gap or duplicate.
    //     (Java `TransactionManager` bumps the epoch and resets sequence numbers
    //     for the idempotent producer on both codes — KIP-360; re-init via
    //     InitProducerId is the network-visible equivalent here.)
    const idem = self.producer_id != -1;
    var progressed = false;
    for (resp.responses) |tr| {
        for (tr.partition_responses) |pr| {
            const group = findGroup(groups, tr.name, pr.index) orelse continue;
            const slot_run = slots[group.start..group.end];
            if (pr.error_code == 0) {
                self.ackSlots(slot_run);
                progressed = true;
            } else if (idem and pr.error_code == 37) {
                // DUPLICATE_SEQUENCE_NUMBER: data already durable — ack.
                self.ackSlots(slot_run);
                progressed = true;
            } else if (idem and (pr.error_code == 45 or pr.error_code == 73)) {
                // OUT_OF_ORDER_SEQUENCE / UNKNOWN_PRODUCER_ID: retry under a
                // fresh PID. Re-arm (or fail on retry-budget exhaustion) and
                // defer the actual re-init + full sequence reset to the next
                // drain (see `recoverProducerId`).
                if (self.retrySlotsWithCode(slot_run, pr.error_code)) progressed = true;
                self.pid_reset_pending = true;
            } else if (isRetriable(pr.error_code)) {
                if (isLeadershipError(pr.error_code)) self.metadata_stale = true;
                if (self.retrySlotsWithCode(slot_run, pr.error_code)) progressed = true;
            } else {
                self.failSlots(slot_run, pr.error_code);
                progressed = true;
            }
        }
    }
    return progressed;
}

fn findGroup(groups: []const Group, topic: []const u8, partition: i32) ?Group {
    for (groups) |g| {
        if (g.partition == partition and std.mem.eql(u8, g.topic, topic)) return g;
    }
    return null;
}

// ---------------------------------------------------------------------------
// Stats helpers: record terminal failures in the counters.
// ---------------------------------------------------------------------------

/// Record a single permanent slot failure in the stats counters.
fn recordFailure(self: *Producer) void {
    _ = self.messages_failed.fetchAdd(1, .monotonic);
    _ = self.errors_fatal.fetchAdd(1, .monotonic);
}

/// Record `count` permanent slot failures in the stats counters (bulk).
fn recordFailures(self: *Producer, count: usize) void {
    _ = self.messages_failed.fetchAdd(count, .monotonic);
    _ = self.errors_fatal.fetchAdd(count, .monotonic);
}

fn ackSlots(self: *Producer, slot_run: []const Collected) void {
    for (slot_run) |c| {
        self.ring.markAcked(c.pos, c.generation);
        self.resetAttempts(c.pos);
    }
    _ = self.messages_acked.fetchAdd(slot_run.len, .monotonic);
}

fn failSlots(self: *Producer, slot_run: []const Collected, code: i16) void {
    for (slot_run) |c| {
        self.ring.markFailed(c.pos, c.generation, @intCast(@as(u16, @bitCast(code))));
        self.resetAttempts(c.pos);
    }
    self.recordFailures(slot_run.len);
}

/// Retry a run after a retriable partition error: bump the attempt counter and
/// re-arm in place, or fail the slot once the retry budget is exhausted.
/// Returns true if any slot was failed (terminal progress); false if all were
/// re-armed for retry (no terminal progress).
fn retrySlotsWithCode(self: *Producer, slot_run: []const Collected, code: i16) bool {
    var any_failed = false;
    for (slot_run) |c| {
        if (self.bumpAttempts(c.pos, c.generation) > self.options.max_retries) {
            self.ring.markFailed(c.pos, c.generation, @intCast(@as(u16, @bitCast(code))));
            self.resetAttempts(c.pos);
            self.recordFailure();
            any_failed = true;
        } else {
            _ = self.ring.retry(c.pos, c.generation) catch {}; // ziglint-ignore: Z026 -- late ack, drop
            _ = self.errors_retriable.fetchAdd(1, .monotonic);
        }
    }
    return any_failed;
}

/// Retry a run after a connection failure (no broker error code). Same budget.
fn retrySlots(self: *Producer, slot_run: []const Collected) void {
    for (slot_run) |c| {
        if (self.bumpAttempts(c.pos, c.generation) > self.options.max_retries) {
            self.ring.markFailed(c.pos, c.generation, 13); // NETWORK_EXCEPTION
            self.resetAttempts(c.pos);
            self.recordFailure();
        } else {
            _ = self.ring.retry(c.pos, c.generation) catch {}; // ziglint-ignore: Z026 -- late ack, drop
            _ = self.errors_retriable.fetchAdd(1, .monotonic);
        }
    }
}

// ---------------------------------------------------------------------------
// Per-slot retry accounting (indexed by physical slot, generation-guarded)
// ---------------------------------------------------------------------------

fn bumpAttempts(self: *Producer, pos: u64, generation: u32) u8 {
    const idx: usize = @intCast(pos & self.ring.mask);
    if (self.attempt_gen[idx] != generation) {
        self.attempt_gen[idx] = generation;
        self.attempts[idx] = 0;
    }
    self.attempts[idx] +|= 1;
    return self.attempts[idx];
}

fn resetAttempts(self: *Producer, pos: u64) void {
    const idx: usize = @intCast(pos & self.ring.mask);
    self.attempts[idx] = 0;
}

// ---------------------------------------------------------------------------
// Partitioning
// ---------------------------------------------------------------------------

fn pickPartition(self: *Producer, topic: []const u8, key: ?[]const u8, num_partitions: u32) u32 {
    const rr = self.roundRobinFor(topic);
    const st = self.stickyFor(topic);
    return partitioner.pick(self.options.strategy, rr, st, key, num_partitions);
}

/// The persistent round-robin counter for `topic`, created on first use with
/// an owned key so it survives metadata refreshes (which free the cache).
fn roundRobinFor(self: *Producer, topic: []const u8) *partitioner.RoundRobin {
    if (self.round_robin.getPtr(topic)) |rr| return rr;
    const key = self.allocator.dupe(u8, topic) catch {
        // Allocation failure for the counter key is non-fatal: fall back to the
        // shared counter (loses per-topic round-robin continuity, not safety).
        return &self.rr_fallback;
    };
    self.round_robin.put(self.allocator, key, .{}) catch {
        self.allocator.free(key);
        return &self.rr_fallback;
    };
    return self.round_robin.getPtr(key).?;
}

/// The persistent sticky state for `topic`, created on first use with an
/// owned key. Shares the same key lifetime as `roundRobinFor` — both use
/// the same allocator and the keys are independent (each map owns its dupe).
fn stickyFor(self: *Producer, topic: []const u8) *partitioner.Sticky {
    if (self.sticky.getPtr(topic)) |st| return st;
    const key = self.allocator.dupe(u8, topic) catch {
        return &self.sticky_fallback;
    };
    self.sticky.put(self.allocator, key, .{}) catch {
        self.allocator.free(key);
        return &self.sticky_fallback;
    };
    return self.sticky.getPtr(key).?;
}

/// Reset sticky partition state for all topics at the start of a drain cycle.
/// The first keyless record per topic picks a new sticky partition; subsequent
/// keyless records in the same drain reuse it. Called once per `drainOnce`.
fn beginStickyDrain(self: *Producer) void {
    var it = self.sticky.iterator();
    while (it.next()) |entry| entry.value_ptr.beginDrain();
    self.sticky_fallback.beginDrain();
}

// ---------------------------------------------------------------------------
// Metadata cache
// ---------------------------------------------------------------------------

fn numPartitions(self: *Producer, topic: []const u8) u32 {
    for (self.topics) |t| {
        if (std.mem.eql(u8, t.name, topic)) return @intCast(t.partitions.len);
    }
    return 0;
}

fn leaderFor(self: *Producer, topic: []const u8, partition: u32) ?i32 {
    for (self.topics) |t| {
        if (!std.mem.eql(u8, t.name, topic)) continue;
        if (partition >= t.partitions.len) return null;
        return t.partitions[partition].leader_id;
    }
    return null;
}

fn brokerAddr(self: *Producer, node_id: i32) ?BrokerInfo {
    for (self.brokers) |b| {
        if (b.node_id == node_id) return b;
    }
    return null;
}

/// Whether the metadata cache is older than `metadata_max_age_ms` and should
/// be proactively refreshed. Cold-path check: one timestamp comparison per
/// drain. Returns false when `metadata_max_age_ms == 0` (proactive refresh
/// disabled) or when metadata has never been loaded (handled by the
/// `!metadata_loaded` check in `drainOnce`).
fn metadataAged(self: *Producer) bool {
    if (self.options.metadata_max_age_ms == 0) return false;
    if (!self.metadata_loaded) return false; // handled separately
    const now = std.time.milliTimestamp();
    if (now <= self.last_metadata_refresh_ms) return false; // guard backward NTP step
    const age: u64 = @intCast(now - self.last_metadata_refresh_ms);
    return age >= self.options.metadata_max_age_ms;
}

/// Send a Metadata v12 request and rebuild the cache. Cold path (refresh
/// cadence; allocations are allowed here).
///
/// HA failover (#2): when `force_bootstrap` is set (all known brokers were
/// unreachable on a prior refresh), use the bootstrap list exclusively.
/// Otherwise, first try known brokers from the last metadata (faster — no
/// TLS re-handshake if cached), and fall back to the bootstrap list if they
/// all fail. On total failure, set `force_bootstrap` so the next attempt
/// starts fresh from the bootstrap list.
fn refreshMetadata(self: *Producer) !void {
    const conn: *Connection = if (self.force_bootstrap)
        try self.metadataConnection()
    else
        self.metadataConnectionFromKnown() catch |err| {
            log.warn("all known brokers unreachable, falling back to bootstrap: {s}", .{@errorName(err)});
            self.force_bootstrap = true;
            return self.refreshMetadata();
        };

    const body: MetadataBody = .{};
    const corr = conn.sendRequest(.metadata, 12, body) catch |err| {
        self.dropConnectionFor(conn);
        return err;
    };
    const resp_body = conn.readResponse() catch |err| {
        self.dropConnectionFor(conn);
        return err;
    };
    var payload = try stripHeaderV1(resp_body, corr);
    var resp = try metadata.decodeResponse(self.allocator, &payload);
    defer resp.deinit(self.allocator);

    try self.rebuildMetadata(resp);
    self.metadata_loaded = true;
    self.metadata_stale = false;
    self.last_metadata_refresh_ms = std.time.milliTimestamp();
    self.force_bootstrap = false; // success: clear the re-bootstrap flag
}

/// Try to get a metadata connection from known brokers (from the last metadata
/// response). Falls back to `metadataConnection` (bootstrap) when no known
/// brokers exist or all are unreachable.
fn metadataConnectionFromKnown(self: *Producer) !*Connection {
    if (self.brokers.len == 0) return self.metadataConnection();
    // Try each known broker, respecting backoff.
    var last_err: anyerror = error.ConnectionRefused;
    for (self.brokers) |b| {
        const wait = self.backoffRemaining(b.host, b.port);
        if (wait > 0) continue;
        const conn = self.connectionFor(b.host, b.port) catch |err| {
            last_err = err;
            self.recordBackoff(b.host, b.port);
            log.warn("known broker {d} {s}:{d} failed: {s}", .{ b.node_id, b.host, b.port, @errorName(err) });
            continue;
        };
        self.clearBackoff(b.host, b.port);
        return conn;
    }
    return last_err;
}

/// Drop the connection for a given `*Connection` by finding its key in the
/// connections map. Used when a metadata request fails on a specific
/// connection and we need to drop just that one.
fn dropConnectionFor(self: *Producer, conn: *Connection) void {
    // Find and remove the connection from the map by value. Linear scan is
    // fine — the connections map is small (one entry per broker).
    var it = self.connections.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* == conn) {
            if (self.connections.fetchRemove(entry.key_ptr.*)) |kv| {
                kv.value.close();
                self.allocator.free(kv.key);
            }
            return;
        }
    }
    // Not found in the map — the connection was already dropped (e.g. a
    // bootstrap connection that failed mid-dial before being cached). Close
    // it directly to avoid a leak.
    conn.close();
}

fn rebuildMetadata(self: *Producer, resp: metadata.Response) !void {
    // Build the new cache fully before freeing the old one, so a mid-build
    // failure leaves the existing (working) cache intact.
    const brokers = try self.allocator.alloc(BrokerInfo, resp.brokers.len);
    var brokers_len: usize = 0;
    errdefer {
        for (brokers[0..brokers_len]) |b| self.allocator.free(b.host);
        self.allocator.free(brokers);
    }
    for (resp.brokers) |rb| {
        const host = try self.allocator.dupe(u8, rb.host);
        brokers[brokers_len] = .{ .node_id = rb.node_id, .host = host, .port = @intCast(rb.port) };
        brokers_len += 1;
    }

    const topics = try self.allocator.alloc(TopicInfo, resp.topics.len);
    var topics_len: usize = 0;
    errdefer {
        for (topics[0..topics_len]) |t| {
            self.allocator.free(t.name);
            self.allocator.free(t.partitions);
        }
        self.allocator.free(topics);
    }
    for (resp.topics) |rt| {
        const name = try self.allocator.dupe(u8, rt.name orelse "");
        errdefer self.allocator.free(name);
        // Size by max partition index + 1 so index-based lookup is O(1).
        var max_index: i64 = -1;
        for (rt.partitions) |rp| {
            if (rp.partition_index > max_index) max_index = rp.partition_index;
        }
        const count: usize = if (max_index < 0) 0 else @intCast(max_index + 1);
        const partitions = try self.allocator.alloc(PartitionInfo, count);
        for (partitions) |*pi| pi.* = .{ .leader_id = -1, .leader_epoch = -1 };
        for (rt.partitions) |rp| {
            const idx: usize = @intCast(rp.partition_index);
            partitions[idx] = .{ .leader_id = rp.leader_id, .leader_epoch = rp.leader_epoch };
        }
        topics[topics_len] = .{ .name = name, .partitions = partitions };
        topics_len += 1;
    }

    self.freeMetadata();
    self.brokers = brokers;
    self.topics = topics;
}

fn freeMetadata(self: *Producer) void {
    for (self.brokers) |b| self.allocator.free(b.host);
    self.allocator.free(self.brokers);
    for (self.topics) |t| {
        self.allocator.free(t.name);
        self.allocator.free(t.partitions);
    }
    self.allocator.free(self.topics);
    self.brokers = &.{};
    self.topics = &.{};
}

// ---------------------------------------------------------------------------
// Connections (keyed by "host:port", lazily dialed, reused)
// ---------------------------------------------------------------------------

fn metadataConnection(self: *Producer) !*Connection {
    // HA bootstrap failover (#2): cycle through ALL configured bootstrap
    // brokers with a rotating start index until one answers, instead of
    // bootstrap[0] only. On connection failure to one, try the next; if all
    // fail, return the last error. The rotating offset spreads load across
    // successive refreshes and avoids hammering a single dead endpoint.
    const bootstrap = self.options.bootstrap;
    assert(bootstrap.len > 0);
    const start = self.bootstrap_offset % bootstrap.len;
    self.bootstrap_offset +%= 1;

    var last_err: anyerror = error.ConnectionRefused;
    for (0..bootstrap.len) |i| {
        const idx = (start + i) % bootstrap.len;
        const b = bootstrap[idx];
        const conn = self.connectionFor(b.host, b.port) catch |err| {
            last_err = err;
            self.recordBackoff(b.host, b.port);
            log.warn("bootstrap {s}:{d} failed: {s}", .{ b.host, b.port, @errorName(err) });
            continue;
        };
        // Success: clear backoff for this endpoint.
        self.clearBackoff(b.host, b.port);
        return conn;
    }
    return last_err;
}

fn leaderConnection(self: *Producer, leader: i32) !*Connection {
    const addr = self.brokerAddr(leader) orelse return error.UnknownBroker;
    return self.connectionFor(addr.host, addr.port);
}

fn connectionFor(self: *Producer, host: []const u8, port: u16) !*Connection {
    var key_buf: [280]u8 = undefined;
    const key = try std.fmt.bufPrint(&key_buf, "{s}:{d}", .{ host, port });
    if (self.connections.get(key)) |conn| return conn;

    const conn = try Connection.dial(self.allocator, .{
        .host = host,
        .port = port,
        .sni = self.options.sni orelse host,
        .ca_bundle = self.options.ca_bundle,
        .insecure_skip_verify = self.options.insecure_skip_verify,
        .scram = self.options.scram,
        .client_id = self.options.client_id,
        .io_timeout_ms = self.options.io_timeout_ms,
        // A framed Produce request is header + body including the encoded
        // record batch (bounded by `max_batch_bytes`); 8 KiB covers the
        // Produce request header, transactional_id/acks/timeout, the
        // topic/partition compact-array + compact-string framing, the
        // record-length prefix, and tag buffers. Without this the framing
        // scratch defaults to 64 KiB and large batches fail with
        // `RequestBufferTooSmall`.
        .max_request_size = @as(usize, self.options.max_batch_bytes) + 8192,
    });
    errdefer conn.close();

    const owned_key = try self.allocator.dupe(u8, key);
    errdefer self.allocator.free(owned_key);
    try self.connections.put(self.allocator, owned_key, conn);
    // Successful dial: clear any prior backoff for this endpoint.
    self.clearBackoff(host, port);
    return conn;
}

fn dropLeaderConnection(self: *Producer, leader: i32) void {
    const addr = self.brokerAddr(leader) orelse return;
    var key_buf: [280]u8 = undefined;
    const key = std.fmt.bufPrint(&key_buf, "{s}:{d}", .{ addr.host, addr.port }) catch return;
    if (self.connections.fetchRemove(key)) |kv| {
        kv.value.close();
        self.allocator.free(kv.key);
    }
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

/// Strip a flexible response header v1 (correlation_id INT32 + tag buffer) and
/// return a reader at the body. Verifies the correlation id (a mismatch means
/// the stream is desynced). Produce v9 and Metadata v12 both use header v1.
fn stripHeaderV1(body: []const u8, expected_corr: i32) !Reader {
    var r: Reader = .init(body);
    const corr = primitives.readI32(&r) catch return error.MalformedResponse;
    if (corr != expected_corr) return error.CorrelationMismatch;
    primitives.readTagBuffer(&r) catch return error.MalformedResponse;
    return Reader.init(r.remaining());
}

/// Produce v9 request body context (duck-typed by `frameRequest`).
pub const ProduceBody = struct {
    acks: i16,
    timeout_ms: i32,
    topic_data: []const produce.TopicData,
    pub fn write(self: ProduceBody, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try produce.encodeRequest(w, .{
            .transactional_id = null,
            .acks = self.acks,
            .timeout_ms = self.timeout_ms,
            .topic_data = self.topic_data,
        });
    }
};

/// Metadata v12 request body: request all topics (v1 simplification — a real
/// client requests only topics it produces to, to avoid full-cluster metadata
/// on large clusters; noted as a follow-up).
pub const MetadataBody = struct {
    pub fn write(_: MetadataBody, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try metadata.encodeRequest(w, .{
            .topics = null,
            .allow_auto_topic_creation = false,
            .include_topic_authorized_operations = false,
        });
    }
};

/// InitProducerId v4 request body context (duck-typed by `frameRequest`).
/// For non-transactional idempotent init: transactional_id=null,
/// producer_id=-1, producer_epoch=-1.
pub const InitProducerIdBody = struct {
    transaction_timeout_ms: i32,
    pub fn write(self: InitProducerIdBody, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try wire.init_producer_id.encodeRequest(w, .{
            .transactional_id = null,
            .transaction_timeout_ms = self.transaction_timeout_ms,
            .producer_id = -1,
            .producer_epoch = -1,
        });
    }
};

// ---------------------------------------------------------------------------
// Idempotent producer: PID acquisition + per-partition sequence tracking
// ---------------------------------------------------------------------------

/// Acquire a producer ID/epoch via InitProducerId v4 to any bootstrap broker.
/// On success, sets `producer_id`, `producer_epoch`, and `pid_acquired`.
/// Cold path (once at startup, and again on PID recovery). The coordinator for
/// non-transactional idempotent init is ANY broker — FindCoordinator is NOT
/// needed. On UNKNOWN_PRODUCER_ID (73) / OUT_OF_ORDER_SEQUENCE (45) from a
/// Produce response, `recoverProducerId` re-calls this for a fresh PID/epoch.
fn initProducerId(self: *Producer) !void {
    const conn = try self.metadataConnection();
    const body: InitProducerIdBody = .{
        .transaction_timeout_ms = self.options.timeout_ms,
    };
    const corr = conn.sendRequest(.init_producer_id, 4, body) catch |err| {
        self.dropConnectionFor(conn);
        return err;
    };
    const resp_body = conn.readResponse() catch |err| {
        self.dropConnectionFor(conn);
        return err;
    };
    var payload = try stripHeaderV1(resp_body, corr);
    const resp = try wire.init_producer_id.decodeResponse(&payload);
    if (resp.error_code != 0) {
        log.warn("InitProducerId error: {d}", .{resp.error_code});
        return error.InitProducerIdFailed;
    }
    self.producer_id = resp.producer_id;
    self.producer_epoch = resp.producer_epoch;
    self.pid_acquired = true;
    log.debug("acquired PID={d} epoch={d}", .{ self.producer_id, self.producer_epoch });
}

/// Recover from OUT_OF_ORDER_SEQUENCE (45) / UNKNOWN_PRODUCER_ID (73): acquire
/// a fresh PID/epoch, reset ALL per-partition sequence counters to 0, and clear
/// the `base_sequence` stamp on every currently-pending slot so each re-sends
/// fresh from sequence 0 under the new PID. Clearing every pending slot (not
/// just the offending partition's) is required: a fresh PID resets the broker's
/// sequence state for ALL partitions, so any pending retry still carrying an
/// old-PID base_sequence would gap the new PID. On success, clears
/// `pid_reset_pending`; on failure it stays set (the caller backs off and
/// retries next drain) and no state is mutated — the re-init is attempted
/// first, so a failed round-trip leaves the counters and slots untouched.
/// BUT a persistently-failing InitProducerId must not strand in-flight slots
/// forever (their `await()` would never resolve): each failed attempt is
/// charged against every pending slot's retry budget via
/// `chargeRecoveryFailure`, so recovery is bounded — the slots are eventually
/// failed rather than spun on indefinitely.
/// Cold path (error recovery).
fn recoverProducerId(self: *Producer) !void {
    self.initProducerId() catch |err| {
        self.chargeRecoveryFailure();
        return err;
    };
    self.sequences.clearRetainingCapacity();
    const lo = self.ring.readHead();
    const hi = self.ring.writeHead();
    var pos = lo;
    while (pos < hi) : (pos += 1) {
        if (self.ring.slotIsPending(pos)) self.ring.setSlotBaseSequence(pos, -1);
    }
    self.pid_reset_pending = false;
    log.debug("PID reset: fresh PID={d} epoch={d}, sequences reset", .{ self.producer_id, self.producer_epoch });
}

/// Charge a failed PID-recovery round-trip against every pending slot's retry
/// budget. Without this a persistently-failing InitProducerId would back off
/// forever while `pid_reset_pending` stays set, and the in-flight slots would
/// never reach a terminal state — their `await()` would hang. Bumping each
/// pending slot's attempt counter (generation-guarded, like a normal retry)
/// and failing it once the budget is exhausted bounds recovery to at most
/// `max_retries` failed attempts. When no pending slots remain after the
/// charge, the reset flag is cleared so a later send does not inherit a stale
/// recovery state. Wakes completions + reclaims so any failed slot's `await()`
/// resolves immediately (the drain returns before its own wake). Cold path.
/// Charge a failed metadata refresh against every pending slot's retry
/// budget. Without this a persistently-down cluster (all bootstrap brokers
/// unreachable) would back off forever while slots stay pending, and their
/// `await()` would hang. Bumping each pending slot's attempt counter and
/// failing it once the budget is exhausted bounds the wait to at most
/// `max_retries` failed metadata attempts. Cold path (error recovery).
fn chargeMetadataFailure(self: *Producer) void {
    const lo = self.ring.readHead();
    const hi = self.ring.writeHead();
    var pos = lo;
    while (pos < hi) : (pos += 1) {
        if (!self.ring.slotIsPending(pos)) continue;
        const generation = self.ring.slotGeneration(pos);
        if (self.bumpAttempts(pos, generation) > self.options.max_retries) {
            self.ring.markFailed(pos, generation, 13); // NETWORK_EXCEPTION
            self.resetAttempts(pos);
            self.recordFailure();
        }
    }
    self.ring.wakeCompletions();
    _ = self.ring.reclaim();
}

fn chargeRecoveryFailure(self: *Producer) void {
    const lo = self.ring.readHead();
    const hi = self.ring.writeHead();
    var any_pending = false;
    var pos = lo;
    while (pos < hi) : (pos += 1) {
        if (!self.ring.slotIsPending(pos)) continue;
        const generation = self.ring.slotGeneration(pos);
        if (self.bumpAttempts(pos, generation) > self.options.max_retries) {
            self.ring.markFailed(pos, generation, 73); // UNKNOWN_PRODUCER_ID
            self.resetAttempts(pos);
            self.recordFailure();
        } else {
            any_pending = true;
        }
    }
    if (!any_pending) self.pid_reset_pending = false;
    self.ring.wakeCompletions();
    _ = self.ring.reclaim();
}

/// Compute the sequence map key for a (topic, partition) pair. Uses wyhash for
/// a good 64-bit mix — collision probability across a realistic topic set is
/// negligible, and this avoids owning topic string copies in the map.
fn seqKey(topic: []const u8, partition: i32) SeqKey {
    var h: u64 = std.hash.Wyhash.hash(0, topic);
    h = std.hash.Wyhash.hash(h, std.mem.asBytes(&partition));
    return h;
}

/// PEEK the next sequence number for a (topic, partition) WITHOUT advancing.
/// Creates the entry (value 0) on first use. Called on a fresh (never-sent)
/// batch to obtain the `base_sequence` for encoding; the counter is advanced
/// separately by `commitSequence` at the batch commit point, AFTER every size
/// gate has passed. Splitting peek from commit is what prevents a locally
/// failed batch (BufferTooSmall / oversized → MESSAGE_TOO_LARGE) from burning
/// sequence numbers: a burned sequence leaves the broker's expected sequence
/// ahead of ours and wedges the partition with OUT_OF_ORDER_SEQUENCE for the
/// life of the PID.
fn peekSequence(self: *Producer, topic: []const u8, partition: i32) !i32 {
    const key = seqKey(topic, partition);
    const gop = try self.sequences.getOrPut(self.allocator, key);
    if (!gop.found_existing) gop.value_ptr.* = 0;
    return gop.value_ptr.*;
}

/// COMMIT: advance the per-(topic, partition) counter by `count` at the batch
/// commit point. `base` must be the value just returned by `peekSequence` for
/// the same (topic, partition) this drain. The entry always exists here
/// (peekSequence created it and nothing removes entries), so the `getOrPut`
/// cannot allocate — the `catch` is defensive and unreachable in practice.
fn commitSequence(self: *Producer, topic: []const u8, partition: i32, base: i32, count: i32) void {
    const key = seqKey(topic, partition);
    const gop = self.sequences.getOrPut(self.allocator, key) catch return;
    gop.value_ptr.* = base + count;
}

const testing = std.testing;
const mock = @import("testing/mock_broker.zig");

test "single-partition backlog above max_batch_bytes splits across drains" {
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 1 });
    defer broker.stop();

    var ring = try Ring.init(testing.allocator, .{
        .max_message_size = 1024,
        .max_key_len = 64,
        .max_topic_len = 64,
        .num_slots = 16,
    });
    defer ring.deinit(testing.allocator);

    var msgs: [4]Ring.Message = undefined;
    for (&msgs) |*m| {
        m.* = try ring.acquire(.awaitable);
        try m.setTopic("events");
        m.setPartition(0);
        @memset(m.value()[0..200], 'x');
        try m.commit(200);
    }

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var producer = try Producer.init(testing.allocator, &ring, .{
        .bootstrap = &bootstrap,
        .sni = mock.server_name,
        .ca_bundle = null,
        .insecure_skip_verify = true,
        .scram = .{
            .mechanism = .scram_sha512,
            .username = mock.username,
            .password = mock.password,
        },
        .client_id = "kafka-zig-test",
        .acks = 1,
        .timeout_ms = 1000,
        .max_batch_bytes = 300,
        .max_message_size = 1024,
        .strategy = .default,
    });
    defer producer.deinit();

    var failed: [msgs.len]?u32 = @splat(null);
    var done: [msgs.len]bool = @splat(false);
    var completed: usize = 0;
    var drains: usize = 0;
    while (completed < msgs.len and drains < 32) : (drains += 1) {
        _ = try producer.drainOnce();
        for (&msgs, 0..) |*m, i| {
            if (done[i]) continue;
            m.tryAwait() catch |err| switch (err) {
                error.WouldBlock => continue,
                error.SendFailed => {
                    failed[i] = m.failureCode();
                    done[i] = true;
                    completed += 1;
                    continue;
                },
                else => return err,
            };
            done[i] = true;
            completed += 1;
        }
    }

    try testing.expectEqual(@as(usize, msgs.len), completed);
    for (failed) |code| try testing.expectEqual(@as(?u32, null), code);
    try testing.expectEqual(@as(u32, msgs.len), broker.produced.load(.acquire));
    try testing.expect(broker.max_produce_records_bytes.load(.acquire) <= 300);
}

test "stamped retry sub-run above max_batch_bytes is not split" {
    var broker = try mock.Broker.start(testing.allocator, .{ .mode = .ok, .num_partitions = 1 });
    defer broker.stop();

    var ring = try Ring.init(testing.allocator, .{
        .max_message_size = 1024,
        .max_key_len = 64,
        .max_topic_len = 64,
        .num_slots = 16,
    });
    defer ring.deinit(testing.allocator);

    var msgs: [3]Ring.Message = undefined;
    for (&msgs) |*m| {
        m.* = try ring.acquire(.awaitable);
        try m.setTopic("events");
        m.setPartition(0);
        @memset(m.value()[0..200], 'x');
        try m.commit(200);
        ring.setSlotBaseSequence(m.slot_index, 100);
    }

    const bootstrap = [_]Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var producer = try Producer.init(testing.allocator, &ring, .{
        .bootstrap = &bootstrap,
        .sni = mock.server_name,
        .ca_bundle = null,
        .insecure_skip_verify = true,
        .scram = .{
            .mechanism = .scram_sha512,
            .username = mock.username,
            .password = mock.password,
        },
        .client_id = "kafka-zig-test",
        .acks = 1,
        .timeout_ms = 1000,
        .max_batch_bytes = 300,
        .max_message_size = 1024,
        .strategy = .default,
    });
    defer producer.deinit();
    producer.producer_id = mock.mock_producer_id;
    producer.producer_epoch = mock.mock_producer_epoch;
    producer.pid_acquired = true;

    try testing.expect(!try producer.drainOnce());
    try testing.expectEqual(@as(u32, 0), broker.produce_requests.load(.acquire));
    try testing.expectEqual(@as(u32, 0), broker.produced.load(.acquire));
    for (&msgs) |*m| try testing.expectError(error.WouldBlock, m.tryAwait());
}
