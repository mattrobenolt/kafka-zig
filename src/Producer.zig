//! The producer network thread: the single consumer of the `Ring` (PLAN §2.5).
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
//! makes in-place retry safe (PLAN §2.4 point 3): a slot is only ever "in the
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
//!   4. Sort collected slots by (leader, topic, partition); each contiguous
//!      (topic, partition) run becomes exactly one v2 record batch (the broker
//!      requires one batch per partition_data entry). Group batches per leader
//!      into one Produce request.
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
//! counter key (allocated once per topic, with a safe fallback if it fails —
//! PLAN §8). The one copy on the produce path is slot payload -> record-batch
//! buffer, inside `record_batch.encodeBatch`.

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
    /// Record-batch compression applied to every batch. `.zstd` requires the
    /// library to be built with `-Dzstd=true`; requesting it without that is
    /// rejected at `Client.init` with `error.CompressionUnavailable`.
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

// --- connections keyed by "host:port" (owned keys) ------------------------
connections: std.StringHashMapUnmanaged(*Connection) = .empty,

// --- round-robin counters, persistent across metadata refreshes -----------
round_robin: std.StringHashMapUnmanaged(partitioner.RoundRobin) = .empty,

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

/// A (topic, partition) run in the sorted `collected` array: `[start, end)`
/// map to one record batch / one partition_response.
const Group = struct {
    topic: []const u8,
    partition: i32,
    start: usize,
    end: usize,
};

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
    self.* = undefined;
}

// ---------------------------------------------------------------------------
// Main loop (runs on the network thread)
// ---------------------------------------------------------------------------

/// The network-thread entry point. Blocks in `waitForData` between drains and
/// returns when the ring is shut down.
pub fn run(self: *Producer) void {
    self.read_head = self.ring.readHead();
    while (!self.ring.isShutdown()) {
        if (!self.ring.waitForData(self.read_head)) break; // shutdown
        const progressed = self.drainOnce() catch |err| brk: {
            log.warn("drain failed: {s}", .{@errorName(err)});
            break :brk false;
        };
        // Advance the wait cursor to the reclaim low-water mark. When slots
        // remain pending (in-flight retries / a stuck partition), read_head
        // stays below write_head, so `waitForData` returns immediately; the
        // backoff below stops that from becoming a busy-spin.
        self.read_head = self.ring.readHead();
        if (!progressed) std.Thread.sleep(self.options.retry_backoff_ns);
    }
}

/// One drain pass. Returns true if any slot reached a terminal state (ack or
/// fail) this pass — i.e. forward progress was made.
fn drainOnce(self: *Producer) !bool {
    if (self.metadata_stale or !self.metadata_loaded) {
        self.refreshMetadata() catch |err| {
            log.warn("metadata refresh failed: {s}", .{@errorName(err)});
            return false; // back off; try again next drain
        };
    }

    const base = self.ring.readHead();
    const head = self.ring.writeHead();
    if (base >= head) return false;

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
    return a.partition < b.partition;
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

            // If we've already hit the max_batch_bytes cap, stop adding batches
            // to this leader's request. Remaining slots stay pending and are
            // sent in the next drain.
            if (encode_off >= max_batch) {
                stopped_at_capacity = true;
                break;
            }

            // One record batch for this (topic, partition) run.
            var rc: usize = 0;
            for (slots[p..p_end], 0..) |c, idx| {
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
            const batch = record_batch.encodeBatch(
                self.encode_buf[encode_off..],
                self.records_scratch[0..rc],
                .{
                    .base_timestamp = now_ms,
                    .compression = self.options.compression,
                    .scratch = self.compress_scratch,
                },
            ) catch |err| switch (err) {
                // The batch doesn't fit in the remaining encode buffer. If
                // nothing has been built yet for this (topic, partition) run,
                // the single batch is too large — fail those slots as
                // MESSAGE_TOO_LARGE. Otherwise, leave the rest pending.
                error.BufferTooSmall => {
                    if (pd_len == pd_start and td_len == 0 and encode_off == 0) {
                        self.failSlots(slots[p..p_end], 10); // MESSAGE_TOO_LARGE
                        p = p_end;
                        continue;
                    }
                    stopped_at_capacity = true;
                    break;
                },
                else => return err,
            };

            // Refuse to exceed max_batch_bytes. Two cases:
            //
            // 1. The batch alone exceeds the cap (batch.len > max_batch). If
            //    nothing has been built for this leader's request yet, fail the
            //    slots as MESSAGE_TOO_LARGE (10) and continue — a single
            //    oversized message should not block the whole drain. If
            //    something is already built, stop and leave it pending for the
            //    next drain (which starts with a fresh encode buffer, so it'll
            //    either fit or fail then).
            // 2. Normal overflow: this batch would push the total past the cap.
            //    Stop adding and leave the remaining slots pending for the next
            //    drain. No `pd_len > pd_start` guard — a later topic's first
            //    partition must also respect the cap.
            if (batch.len > max_batch) {
                if (encode_off == 0 and pd_len == pd_start and td_len == 0) {
                    self.failSlots(slots[p..p_end], 10); // MESSAGE_TOO_LARGE
                    p = p_end;
                    continue;
                }
                stopped_at_capacity = true;
                break;
            }
            if (encode_off + batch.len > max_batch) {
                stopped_at_capacity = true;
                break;
            }

            encode_off += batch.len;

            self.partition_data_scratch[pd_len] = .{ .index = part, .records = batch };
            self.group_scratch[group_len] = .{
                .topic = topic,
                .partition = part,
                .start = p,
                .end = p_end,
            };
            pd_len += 1;
            group_len += 1;
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
        // If any failSlots ran, group_len is 0 but those slots are terminal;
        // track that with a simple heuristic: if we didn't stop at capacity,
        // the only way td_len==0 is if slots were failed.
        return !stopped_at_capacity;
    }

    const conn = try self.leaderConnection(leader);
    const body: ProduceBody = .{
        .acks = self.options.acks,
        .timeout_ms = self.options.timeout_ms,
        .topic_data = self.topic_data_scratch[0..td_len],
    };
    const corr = try conn.sendRequest(.produce, 9, body);

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
    var progressed = false;
    for (resp.responses) |tr| {
        for (tr.partition_responses) |pr| {
            const group = findGroup(groups, tr.name, pr.index) orelse continue;
            const slot_run = slots[group.start..group.end];
            if (pr.error_code == 0) {
                self.ackSlots(slot_run);
                progressed = true;
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

fn ackSlots(self: *Producer, slot_run: []const Collected) void {
    for (slot_run) |c| {
        self.ring.markAcked(c.pos, c.generation);
        self.resetAttempts(c.pos);
    }
}

fn failSlots(self: *Producer, slot_run: []const Collected, code: i16) void {
    for (slot_run) |c| {
        self.ring.markFailed(c.pos, c.generation, @intCast(@as(u16, @bitCast(code))));
        self.resetAttempts(c.pos);
    }
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
            any_failed = true;
        } else {
            _ = self.ring.retry(c.pos, c.generation) catch {}; // ziglint-ignore: Z026 -- late ack, drop
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
        } else {
            _ = self.ring.retry(c.pos, c.generation) catch {}; // ziglint-ignore: Z026 -- late ack, drop
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
    return partitioner.pick(self.options.strategy, rr, key, num_partitions);
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

/// Send a Metadata v12 request over the bootstrap connection and rebuild the
/// cache. Cold path (refresh cadence), allocations allowed (PLAN §8).
fn refreshMetadata(self: *Producer) !void {
    const conn = try self.metadataConnection();
    const body: MetadataBody = .{};
    const corr = conn.sendRequest(.metadata, 12, body) catch |err| {
        self.dropMetadataConnection();
        return err;
    };
    const resp_body = conn.readResponse() catch |err| {
        self.dropMetadataConnection();
        return err;
    };
    var payload = try stripHeaderV1(resp_body, corr);
    var resp = try metadata.decodeResponse(self.allocator, &payload);
    defer resp.deinit(self.allocator);

    try self.rebuildMetadata(resp);
    self.metadata_loaded = true;
    self.metadata_stale = false;
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
    // v1: always bootstrap[0] for metadata. A real client would round-robin
    // bootstrap and fail over; the single-broker mock and MSK bootstrap set
    // make this adequate for now.
    const b = self.options.bootstrap[0];
    return self.connectionFor(b.host, b.port);
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
    });
    errdefer conn.close();

    const owned_key = try self.allocator.dupe(u8, key);
    errdefer self.allocator.free(owned_key);
    try self.connections.put(self.allocator, owned_key, conn);
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

/// Drop the bootstrap metadata connection (v1: always bootstrap[0]). Called
/// when a metadata request fails so the next retry dials a fresh connection
/// instead of reusing a dead/broken one.
fn dropMetadataConnection(self: *Producer) void {
    const b = self.options.bootstrap[0];
    var key_buf: [280]u8 = undefined;
    const key = std.fmt.bufPrint(&key_buf, "{s}:{d}", .{ b.host, b.port }) catch return;
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
