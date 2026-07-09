//! MPSC payload ring — the producer submission buffer.
//!
//! Multiple producer threads acquire a slot, write their message *directly into
//! the slot's inline buffers* (zero-copy from the producer's side), then commit.
//! A single consumer (the network thread, phase 6) drains committed slots,
//! builds Produce requests, and — only after the broker acks — marks slots done
//! and reclaims them. The one unavoidable copy on the whole produce path is
//! slot payload -> record-batch buffer, and it happens in the network thread,
//! not here.
//!
//! This is a payload-owning port of exosphere's fire-and-forget
//! `AtomicRingBuffer`. Four things this design adds over the original, each
//! spelled out where it lives in the code:
//!
//!   1. `acquire()` blocks when the ring is full. The original's unbounded
//!      `fetchAdd(write_head)` would wrap and overwrite a still-pending slot
//!      once the network thread holds `read_head` waiting on broker acks. Here
//!      producers claim slots via a CAS loop that re-checks the full condition
//!      (`write_head - read_head >= num_slots`) and futex-waits on `read_head`;
//!      the network thread wakes them as it reclaims. See `acquire` /
//!      `waitNotFull` / `reclaim`.
//!
//!   2. Partial-ack reclamation. Acks arrive per-partition, out of order. The
//!      network thread marks *individual* slots acked/failed; `read_head` is
//!      only the low-water mark for the full-check. Physical reclaim is a
//!      forward scan over the contiguous terminal prefix (O(prefix) per drain).
//!      Completion (below) is stored in the handle and observed *immediately*
//!      when a slot is marked, so a slow partition never head-of-line-blocks a
//!      later slot's `await()` — it only delays physical slot reuse. See
//!      `markAcked` / `markFailed` / `reclaim`.
//!
//!   3. Stable handle across retry. `await()` is keyed to a handle, not a slot.
//!      Handles are a small pool decoupled from slots. `retry()` re-arms the
//!      message *in place* (same slot, still pending) and re-notifies the
//!      network thread — no new slot, so a full ring can never deadlock the
//!      retry path (the earlier move-to-a-new-slot design, sketched in PLAN
//!      §2.4, deadlocks when every slot is pending and needs re-enqueue). A
//!      per-slot `generation` counter still guards attribution so a *recycled*
//!      slot's stale ack is never misapplied, and `completeSlot` no-ops on any
//!      non-pending slot. See `Handle`, `retry`, `markAcked`, `completeSlot`.
//!
//!   4. Completion signaling. A futex per slot is too expensive at 8K+ slots.
//!      The completion result lives in the handle (survives slot reclaim), and a
//!      single global `completions` counter + futex wakes all waiters after a
//!      batch of acks. `await()` waits on it. See `Handle`, `await`,
//!      `wakeCompletions`.
//!
//! Judgment call (flagged for review): the task sketch offered a slot-based
//! `await()` that loads the handle's `(slot_index, generation)`, reads *that
//! slot's* status, and generation-checks. That has a genuine race — the slot can
//! be recycled between the slot_index load and the status load, and generation
//! alone can't recover the completion result once the slot is gone. Storing the
//! result in the *handle* (which is not recycled until `await()` frees it) makes
//! `await()` race-free with zero dependence on slot lifetime, and the generation
//! counter still does its real job: guarding the network thread's
//! slot -> handle attribution against stale acks for recycled slots. This is
//! strictly more correct, so that's what's implemented.

const std = @import("std");
const builtin = @import("builtin");

const assert = std.debug.assert;
const atomic = std.atomic;
const math = std.math;
const heap = std.heap;
const Allocator = std.mem.Allocator;
const Futex = std.Thread.Futex;
const cache_line = atomic.cache_line;
const page_size = heap.page_size_min;

const Ring = @This();

// Slot lifecycle states (the `written` atomic of the original, widened).
const status_free: u32 = 0; // reclaimed / never used; claimable.
const status_pending: u32 = 1; // committed by a producer; awaiting broker ack.
const status_acked: u32 = 2; // broker acked; reclaimable.
const status_failed: u32 = 3; // broker returned a non-retriable error; reclaimable.
const status_stale: u32 = 4; // superseded by a retry to a new slot; reclaimable, no completion.

// Handle completion results (what `await()` observes). Reuses the acked/failed
// numeric values so the same constants read the same everywhere.
const result_in_flight: u32 = 0;
const result_acked: u32 = status_acked;
const result_failed: u32 = status_failed;

// Sentinel free-list link terminator (no valid handle has this index).
const handle_nil: u32 = math.maxInt(u32);

// Bounded re-check interval for the two cursor waits (`waitNotFull`,
// `waitForData`). Those futex words (read_head/write_head low bits) can't be
// perturbed on shutdown without corrupting the cursor, so a waiter that races
// `requestShutdown` (passes its shutdown re-check, then parks before the wake
// lands) would sleep on an unchanged word forever. A bounded `timedWait` makes
// the surrounding loop re-check `shutdown` at worst this often, self-healing
// any missed wake. The counter waits (`completions`, `handle_freed`) don't need
// this — `requestShutdown` bumps those words, which self-heals via the value
// guard.
const shutdown_poll_ns: u64 = 20 * std.time.ns_per_ms;

// Sentinel partition meaning "let the partitioner decide at batch time".
pub const partition_unassigned: u32 = math.maxInt(u32);

pub const Error = error{
    /// The payload/topic/key exceeds the ring's configured maximum.
    MessageTooLarge,
    /// `tryAcquire`/`tryAwait` would have blocked.
    WouldBlock,
    /// The ring was shut down while the caller was blocked (or before it began).
    Shutdown,
    /// The broker rejected the message with a non-retriable error.
    SendFailed,
};

pub const Config = struct {
    /// Inline value buffer per slot. This dominates the reserved memory:
    /// reserved ~= num_slots * (max_topic_len + max_key_len + max_message_size).
    max_message_size: u32 = 16 * 1024,
    max_key_len: u16 = 256,
    max_topic_len: u8 = 128,
    /// Rounded up to a power of two. This is the *sole* backpressure bound —
    /// every slot reserves its full buffers whether filled or not, so slot
    /// count is the byte bound.
    num_slots: u32 = 8192,
};

/// A slot's fixed metadata. Inline topic/key/value bytes live in a parallel
/// `payload` arena (not inline here) so the network thread's reclaim scan
/// touches a dense array of statuses instead of striding over 16 KiB payloads.
pub const Slot = struct {
    status: atomic.Value(u32),
    /// Bumped by `reclaim` each time the physical slot is reused. Guards stale
    /// acks: `markAcked`/`markFailed` no-op unless the caller's captured
    /// generation still matches. Published to producers via the read_head
    /// full-check acquire-load; published to the network thread via `status`.
    generation: atomic.Value(u32),
    /// Owning handle, set at acquire, published via the commit release-store.
    handle_index: u32,
    /// Broker error code when `status == status_failed`.
    err: u32,
    /// Target partition, or `partition_unassigned`.
    partition: u32,
    value_len: u32,
    timestamp_ms: i64,
    key_len: u16,
    topic_len: u8,
    /// The base sequence assigned to this slot's batch by the idempotent
    /// producer's per-partition counter. -1 = unassigned (first send not yet
    /// built, or idempotency is off). Set when a batch is first built for the
    /// slot's (topic, partition); reused on in-place retry so the broker dedup
    /// window is preserved (the whole point of idempotent sequence tracking).
    /// Reset to -1 at acquire (`bindMessage`). Only the network thread reads
    /// and writes this; producers never touch it.
    base_sequence: i32 = -1,
};

/// A stable completion token, decoupled from any physical slot. Pooled; a
/// handle outlives its slot across retries and (briefly) across reclaim.
const Handle = struct {
    /// The completion result `await()` waits on. `.release` by the network
    /// thread, `.acquire` by the waiter.
    result: atomic.Value(u32),
    /// Broker error code when `result == result_failed`.
    err: atomic.Value(u32),
    /// Current physical slot. Bookkeeping / debugging only; not on the
    /// `await()` critical path.
    slot_index: u32,
    /// Generation of the current (handle <-> slot) binding.
    generation: u32,
    /// Treiber free-list link; only meaningful while the handle is free. Read
    /// in `poolAlloc` and written in `poolFree` concurrently, so it is atomic
    /// (monotonic) to avoid a data race on the field itself. The head CAS
    /// (acq_rel) carries the actual synchronization; a stale `next` read just
    /// loses the CAS and retries.
    next: atomic.Value(u32),
};

/// The producer-facing token: acquire returns one, you write into it, commit
/// it, then await it. Small and passed by value — the ring holds the state.
pub const Message = struct {
    ring: *Ring,
    handle_index: u32,
    /// The slot claimed at acquire. Retry re-arms in place, so this stays valid
    /// for the whole lifetime of the message.
    slot_index: u32,
    /// Broker error code cached out of the handle by `await`/`tryAwait` *before*
    /// the handle is freed. `failureCode()` reads this, never the handle — the
    /// handle can be recycled by another producer the instant it is freed, which
    /// would race a read of `handle.err`. See `finishAwait`.
    err_code: u32 = 0,

    pub fn setTopic(self: Message, name: []const u8) Error!void {
        try self.ring.setTopic(self.slot_index, name);
    }

    pub fn setKey(self: Message, key: []const u8) Error!void {
        try self.ring.setKey(self.slot_index, key);
    }

    pub fn setPartition(self: Message, partition: ?u32) void {
        self.ring.slotAt(self.slot_index).partition = partition orelse partition_unassigned;
    }

    pub fn setTimestamp(self: Message, timestamp_ms: i64) void {
        self.ring.slotAt(self.slot_index).timestamp_ms = timestamp_ms;
    }

    /// The slot's inline value buffer, length `max_message_size`. Write your
    /// payload here, then pass the used length to `commit`.
    pub fn value(self: Message) []u8 {
        return self.ring.slotValue(self.slot_index);
    }

    /// Convenience: copy `payload` into the slot's value buffer and commit in
    /// one call. Returns `error.MessageTooLarge` if the payload exceeds
    /// `max_message_size`. This is the common-case replacement for the
    /// `value()` + `@memcpy` + `commit(len)` two-step.
    pub fn writeMessage(self: Message, payload: []const u8) Error!void {
        const buf = self.ring.slotValue(self.slot_index);
        if (payload.len > buf.len) return error.MessageTooLarge;
        @memcpy(buf[0..payload.len], payload);
        try self.ring.commit(self.slot_index, @intCast(payload.len));
    }

    /// Publish the message to the network thread. After this the message is a
    /// pure await token; the slot (and its buffers) are the network thread's.
    pub fn commit(self: Message, value_len: u32) Error!void {
        try self.ring.commit(self.slot_index, value_len);
    }

    /// Block until the broker acks (or fails) this message. Returns
    /// `error.SendFailed` on a non-retriable broker error, `error.Shutdown` if
    /// the ring is torn down first. Takes `*Message` because it caches the
    /// failure code into `err_code` before freeing the handle.
    ///
    /// A `Message` is single-use: exactly one `await` (or `tryAwait` that
    /// completes). A double-await is a caller contract violation that would
    /// double-free the handle pool; the `handle_nil` sentinel catches it in
    /// Debug/ReleaseSafe. `failureCode` remains valid after await.
    pub fn await(self: *Message) Error!void {
        assert(self.handle_index != handle_nil); // double-await guard.
        self.ring.awaitHandle(self.handle_index, &self.err_code) catch |err| {
            // On Shutdown, awaitHandle already freed the handle; mark it nil
            // so a second call hits the assert instead of double-freeing.
            self.handle_index = handle_nil;
            return err;
        };
        self.handle_index = handle_nil;
    }

    /// Non-blocking `await`: `error.WouldBlock` if still in flight.
    pub fn tryAwait(self: *Message) Error!void {
        assert(self.handle_index != handle_nil); // double-await guard.
        self.ring.tryAwaitHandle(self.handle_index, &self.err_code) catch |err| {
            if (err != error.WouldBlock) self.handle_index = handle_nil;
            return err;
        };
        self.handle_index = handle_nil;
    }

    /// The broker error code for a failed send (valid after `await`/`tryAwait`
    /// returned `error.SendFailed`). Reads the value cached into this token, not
    /// the (possibly-recycled) handle. Does NOT assert the guard — it is valid
    /// after await has completed and set the sentinel.
    pub fn failureCode(self: Message) u32 {
        return self.err_code;
    }
};

// --- Read-only after init -------------------------------------------------
slots: []align(page_size) Slot,
handles: []Handle,
/// Flat payload arena: slot i owns bytes [i*bytes_per_slot ..][0..bytes_per_slot],
/// laid out topic | key | value.
payload: []align(page_size) u8,
mask: u64,
num_slots: u32,
max_topic_len: u8,
max_key_len: u16,
max_message_size: u32,
bytes_per_slot: usize,

// --- Producer claim cursor (hot; producers CAS, network reads) ------------
write_head: atomic.Value(u64) align(cache_line),
/// Set when the network thread is blocked in `waitForData`. Avoids a wake
/// syscall on every commit. Shares the write_head cache line intentionally.
data_waiter: atomic.Value(bool),

// --- Reclaim low-water mark (network advances; producers wait when full) --
read_head: atomic.Value(u64) align(cache_line),
/// Count of producers blocked in `waitNotFull`. Lets `reclaim` skip the wake
/// syscall when nobody is waiting.
full_waiters: atomic.Value(u32),

// --- Completion signaling (single global futex for all awaiters) ----------
completions: atomic.Value(u32) align(cache_line),
completion_waiters: atomic.Value(u32),

// --- Handle free-list (Treiber stack, ABA-tagged) -------------------------
/// (tag << 32) | head_index; head_index == handle_nil-low means empty.
pool_head: atomic.Value(u64) align(cache_line),
/// Bumped on every handle free; futex target for producers blocked on an empty
/// pool. (Exhaustion needs > num_slots producer threads mid-flight — effectively
/// never — but blocking here keeps the invariant honest rather than corrupting.)
handle_freed: atomic.Value(u32),
pool_waiters: atomic.Value(u32),

shutdown: atomic.Value(bool) align(cache_line),
/// Set by `requestDrain` (phase 1 of graceful shutdown). Blocks new
/// `acquire`/`tryAcquire` (returns `error.Shutdown`) but lets the network
/// thread keep draining pending slots. The network thread's `run` loop
/// detects this, finishes in-flight Produce round-trips up to a configurable
/// drain timeout, then exits. `requestShutdown` (phase 2) is called after the
/// drain completes or times out, waking any remaining `await` waiters with
/// `error.Shutdown`.
draining: atomic.Value(bool) align(cache_line),

comptime {
    // Little-endian assumption: we futex on the low 32 bits of the 64-bit
    // cursors, which are the bytes that change on each advance.
    assert(builtin.cpu.arch.endian() == .little);
}

pub fn init(gpa: Allocator, config: Config) !Ring {
    assert(config.num_slots > 0);
    assert(config.max_message_size > 0);

    const num_slots = math.ceilPowerOfTwo(u32, config.num_slots) catch unreachable;
    const bytes_per_slot: usize =
        @as(usize, config.max_topic_len) + config.max_key_len + config.max_message_size;

    const slots = try gpa.alignedAlloc(Slot, .fromByteUnits(page_size), num_slots);
    errdefer gpa.free(slots);
    const handles = try gpa.alloc(Handle, num_slots);
    errdefer gpa.free(handles);
    const payload = try gpa.alignedAlloc(u8, .fromByteUnits(page_size), num_slots * bytes_per_slot);
    errdefer gpa.free(payload);

    for (slots) |*slot| {
        slot.status = .init(status_free);
        slot.generation = .init(0);
        slot.handle_index = handle_nil;
    }
    // Build the free-list: 0 -> 1 -> ... -> (n-1) -> nil, head at 0.
    for (handles, 0..) |*handle, i| {
        handle.result = .init(result_in_flight);
        handle.err = .init(0);
        handle.slot_index = handle_nil;
        handle.generation = 0;
        handle.next = .init(if (i + 1 < num_slots) @intCast(i + 1) else handle_nil);
    }

    assert(math.isPowerOfTwo(num_slots));
    return .{
        .slots = slots,
        .handles = handles,
        .payload = payload,
        .mask = num_slots - 1,
        .num_slots = num_slots,
        .max_topic_len = config.max_topic_len,
        .max_key_len = config.max_key_len,
        .max_message_size = config.max_message_size,
        .bytes_per_slot = bytes_per_slot,
        .write_head = .init(0),
        .data_waiter = .init(false),
        .read_head = .init(0),
        .full_waiters = .init(0),
        .completions = .init(0),
        .completion_waiters = .init(0),
        .pool_head = .init(0), // tag 0, head index 0
        .handle_freed = .init(0),
        .pool_waiters = .init(0),
        .shutdown = .init(false),
        .draining = .init(false),
    };
}

pub fn deinit(self: *Ring, gpa: Allocator) void {
    gpa.free(self.payload);
    gpa.free(self.handles);
    gpa.free(self.slots);
    self.* = undefined;
}

// ---------------------------------------------------------------------------
// Producer side: acquire -> set*/value -> commit -> await
// ---------------------------------------------------------------------------

/// Acquire a slot, blocking (futex) while the ring is full. This is the sole
/// backpressure path. Returns `error.Shutdown` if the ring is torn down.
///
/// A handle is claimed FIRST (blocking on the handle pool if exhausted), then
/// the slot. Claiming the slot advances `write_head`, which publishes the slot
/// to the consumer as "data available"; doing that before we own a handle would
/// leave a phantom `status_free` slot below `write_head` with no bound handle
/// (breaks the invariant and spins the consumer). Handle-first keeps the
/// invariant: every claimed slot has a bound handle.
pub fn acquire(self: *Ring) Error!Message {
    const handle_index = try self.poolAlloc(.block);
    errdefer self.poolFree(handle_index);
    const pos = try self.claimSlot(.block);
    return self.bindMessage(pos, handle_index);
}

/// Non-blocking `acquire`: `error.WouldBlock` when the ring is full or handles
/// are exhausted.
pub fn tryAcquire(self: *Ring) Error!Message {
    const handle_index = try self.poolAlloc(.no_block);
    errdefer self.poolFree(handle_index);
    const pos = try self.claimSlot(.no_block);
    return self.bindMessage(pos, handle_index);
}

const ClaimMode = enum { block, no_block };

/// Claim a monotonic slot position via CAS, re-checking the full condition on
/// each attempt so we never claim past `read_head + num_slots` (the original's
/// bug). Returns the claimed logical position.
fn claimSlot(self: *Ring, mode: ClaimMode) Error!u64 {
    while (true) {
        if (self.shutdown.load(.acquire)) return error.Shutdown;
        if (self.draining.load(.acquire)) return error.Shutdown;

        // Load read_head BEFORE write_head. read_head only increases, so a
        // read_head read first is <= its value when write_head is read, which
        // keeps `w >= r` (no underflow) and makes the full-check conservative:
        // a stale-low r can only over-report fullness (harmless extra retry),
        // never let us claim past a still-pending slot.
        const r = self.read_head.load(.acquire);
        const w = self.write_head.load(.monotonic);
        assert(w >= r);

        if (w - r >= self.num_slots) {
            switch (mode) {
                .no_block => return error.WouldBlock,
                .block => {
                    try self.waitNotFull(r);
                    continue;
                },
            }
        }

        if (self.write_head.cmpxchgWeak(w, w + 1, .acq_rel, .monotonic) == null) {
            return w;
        }
        // Lost the race with another producer; retry.
    }
}

/// Block until `read_head` moves past the value we saw full at. Uses a futex on
/// read_head's low 32 bits; `reclaim` wakes us. The snapshot-then-wait ordering
/// closes the lost-wakeup window: if `reclaim` advanced read_head after our
/// full-check, the futex value no longer matches and `wait` returns at once.
fn waitNotFull(self: *Ring, read_head_seen: u64) Error!void {
    _ = self.full_waiters.fetchAdd(1, .acq_rel);
    defer _ = self.full_waiters.fetchSub(1, .acq_rel);

    if (self.shutdown.load(.acquire)) return error.Shutdown;
    // Re-verify still full against the value we intend to wait on.
    const r = self.read_head.load(.acquire);
    if (r != read_head_seen) return; // moved; go retry the claim.
    // Bounded wait so a missed shutdown wake (read_head word can't be bumped)
    // is recovered by the caller's loop re-checking `shutdown`.
    // ziglint-ignore: Z026 — Timeout is the intended recovery: the caller loops and re-checks shutdown.
    Futex.timedWait(readHeadLow(self), @truncate(read_head_seen), shutdown_poll_ns) catch {};
}

/// Bind an already-owned handle to the claimed slot and return the producer
/// token. The handle is acquired by the caller (`acquire`/`tryAcquire`) before
/// the slot is claimed — see `acquire` for why.
fn bindMessage(self: *Ring, pos: u64, handle_index: u32) Message {
    const slot_index: u32 = @intCast(pos & self.mask);
    const slot = &self.slots[slot_index];
    // Generation left by the reclaim that last freed this physical slot,
    // published to us via the read_head acquire-load in `claimSlot`.
    const generation = slot.generation.load(.acquire);

    const handle = &self.handles[handle_index];
    handle.result.store(result_in_flight, .monotonic);
    handle.err.store(0, .monotonic);
    handle.slot_index = slot_index;
    handle.generation = generation;

    // Reset per-slot metadata. handle_index is published to the network thread
    // by the commit release-store, so a plain write is fine here.
    slot.handle_index = handle_index;
    slot.partition = partition_unassigned;
    slot.timestamp_ms = 0;
    slot.topic_len = 0;
    slot.key_len = 0;
    slot.value_len = 0;
    slot.err = 0;
    slot.base_sequence = -1;

    return .{ .ring = self, .handle_index = handle_index, .slot_index = slot_index };
}

fn setTopic(self: *Ring, slot_index: u32, name: []const u8) Error!void {
    if (name.len > self.max_topic_len) return error.MessageTooLarge;
    const slot = &self.slots[slot_index];
    @memcpy(self.topicBuf(slot_index)[0..name.len], name);
    slot.topic_len = @intCast(name.len);
}

fn setKey(self: *Ring, slot_index: u32, key: []const u8) Error!void {
    if (key.len > self.max_key_len) return error.MessageTooLarge;
    const slot = &self.slots[slot_index];
    @memcpy(self.keyBuf(slot_index)[0..key.len], key);
    slot.key_len = @intCast(key.len);
}

fn commit(self: *Ring, slot_index: u32, value_len: u32) Error!void {
    if (value_len > self.max_message_size) return error.MessageTooLarge;
    const slot = &self.slots[slot_index];
    assert(slot.status.load(.monotonic) == status_free);
    slot.value_len = value_len;
    // Release: publishes handle_index + all metadata + payload to the network
    // thread's acquire-load of status.
    slot.status.store(status_pending, .release);
    self.notifyConsumer();
}

fn awaitHandle(self: *Ring, handle_index: u32, err_out: *u32) Error!void {
    const handle = &self.handles[handle_index];
    while (true) {
        const result = handle.result.load(.acquire);
        if (result != result_in_flight) return self.finishAwait(handle_index, result, err_out);
        if (self.shutdown.load(.acquire)) {
            // One last look in case the completion landed before shutdown.
            const final = handle.result.load(.acquire);
            if (final != result_in_flight) return self.finishAwait(handle_index, final, err_out);
            self.poolFree(handle_index);
            return error.Shutdown;
        }

        _ = self.completion_waiters.fetchAdd(1, .acq_rel);
        const seq = self.completions.load(.acquire);
        // Re-check after publishing our interest and snapshotting the counter.
        if (handle.result.load(.acquire) == result_in_flight and !self.shutdown.load(.acquire)) {
            Futex.wait(&self.completions, seq);
        }
        _ = self.completion_waiters.fetchSub(1, .acq_rel);
    }
}

fn tryAwaitHandle(self: *Ring, handle_index: u32, err_out: *u32) Error!void {
    const result = self.handles[handle_index].result.load(.acquire);
    if (result == result_in_flight) return error.WouldBlock;
    return self.finishAwait(handle_index, result, err_out);
}

/// Translate a terminal result into the public return. Caches the broker error
/// code into the caller's token *before* freeing the handle: once freed, the
/// handle can be re-bound by another producer (which resets `err` to 0), so
/// reading `handle.err` after the free is a data race / use-after-free. The
/// err read here is safe because we already observed `result != in_flight` via
/// an acquire-load, which synchronizes-with the network thread's release-store
/// of `result` that was sequenced after its `err` write in `completeSlot`.
fn finishAwait(self: *Ring, handle_index: u32, result: u32, err_out: *u32) Error!void {
    assert(result != result_in_flight);
    err_out.* = self.handles[handle_index].err.load(.acquire);
    self.poolFree(handle_index);
    if (result == result_failed) return error.SendFailed;
    assert(result == result_acked);
}

// ---------------------------------------------------------------------------
// Consumer side (network thread, phase 6): drain -> send -> ack -> reclaim
// ---------------------------------------------------------------------------

/// Wait for a committed slot at or beyond `read_pos`. Returns false on shutdown.
/// Mirrors the original's event loop: bounded spin, then futex on write_head.
pub fn waitForData(self: *Ring, read_pos: u64) bool {
    // Intentionally unbounded: this is the single-consumer event loop. It
    // terminates on shutdown, drain completion (draining + no pending data),
    // or on data becoming available.
    while (true) {
        if (self.shutdown.load(.acquire)) return false;
        const head = self.write_head.load(.acquire);
        if (read_pos < head) return true;
        // Draining with no pending data: the drain is complete (all in-flight
        // slots have been resolved). Return false so the network thread exits.
        if (self.draining.load(.acquire)) return false;

        for (0..64) |_| {
            atomic.spinLoopHint();
            if (read_pos < self.write_head.load(.monotonic)) return true;
        }

        self.data_waiter.store(true, .release);
        defer self.data_waiter.store(false, .monotonic);
        if (self.shutdown.load(.acquire)) return false;
        if (read_pos < self.write_head.load(.acquire)) return true;
        // Bounded wait: the write_head word can't be perturbed on shutdown, so
        // recover any missed wake by looping back to the shutdown re-check.
        // ziglint-ignore: Z026 — Timeout is the intended recovery: the outer loop re-checks shutdown.
        Futex.timedWait(writeHeadLow(self), @truncate(head), shutdown_poll_ns) catch {};
    }
}

/// Wait for a committed slot at or beyond `read_pos`, with a maximum wait
/// of `timeout_ns`. Returns true if data became available, false on timeout or
/// shutdown. Used by the producer's linger phase: after the initial
/// `waitForData` returns (data is available), the producer calls this with the
/// current `write_head` as `read_pos` and the remaining linger duration, to
/// wait for MORE records to accumulate without blocking indefinitely.
pub fn waitForDataTimed(self: *Ring, read_pos: u64, timeout_ns: u64) bool {
    const deadline_ns = blk: {
        const now: u64 = @intCast(std.time.nanoTimestamp());
        break :blk now + timeout_ns;
    };
    while (true) {
        if (self.shutdown.load(.acquire)) return false;
        const head = self.write_head.load(.acquire);
        if (read_pos < head) return true;
        if (self.draining.load(.acquire)) return false;

        const now: u64 = @intCast(std.time.nanoTimestamp());
        if (now >= deadline_ns) return false;

        const remaining = deadline_ns - now;
        const wait = @min(remaining, shutdown_poll_ns);

        for (0..64) |_| {
            atomic.spinLoopHint();
            if (read_pos < self.write_head.load(.monotonic)) return true;
        }

        self.data_waiter.store(true, .release);
        defer self.data_waiter.store(false, .monotonic);
        if (self.shutdown.load(.acquire)) return false;
        if (read_pos < self.write_head.load(.acquire)) return true;
        // ziglint-ignore: Z026 — timeout is the intended recovery: the outer loop re-checks deadline and shutdown.
        Futex.timedWait(writeHeadLow(self), @truncate(head), wait) catch {};
    }
}

/// Wake the network thread after a commit (only if it is actually parked).
pub fn notifyConsumer(self: *Ring) void {
    if (self.data_waiter.load(.acquire)) Futex.wake(writeHeadLow(self), 1);
}

pub fn writeHead(self: *const Ring) u64 {
    return self.write_head.load(.acquire);
}

pub fn readHead(self: *const Ring) u64 {
    return self.read_head.load(.acquire);
}

pub fn numSlots(self: *const Ring) u32 {
    return self.num_slots;
}

/// The slot at a logical position (or physical index — the mask makes them
/// equivalent). For the network thread reading committed metadata.
pub fn slotAt(self: *Ring, index: u64) *Slot {
    return &self.slots[index & self.mask];
}

/// The committed topic bytes for a slot (network thread, read-only view).
pub fn slotTopic(self: *Ring, index: u64) []const u8 {
    const slot = self.slotAt(index);
    return self.topicBuf(@intCast(index & self.mask))[0..slot.topic_len];
}

pub fn slotKey(self: *Ring, index: u64) []const u8 {
    const slot = self.slotAt(index);
    return self.keyBuf(@intCast(index & self.mask))[0..slot.key_len];
}

pub fn slotValue(self: *Ring, index: u64) []u8 {
    return self.valueBuf(@intCast(index & self.mask));
}

/// The committed value bytes for a slot (network thread, read-only view).
pub fn slotValueCommitted(self: *Ring, index: u64) []const u8 {
    const slot = self.slotAt(index);
    return self.valueBuf(@intCast(index & self.mask))[0..slot.value_len];
}

/// The generation to capture when the network thread sends a slot, to pass back
/// to `markAcked`/`markFailed`/`retry` so stale acks are rejected.
pub fn slotGeneration(self: *Ring, index: u64) u32 {
    return self.slotAt(index).generation.load(.acquire);
}

/// The base sequence assigned to this slot's batch (-1 = unassigned / non-
/// idempotent). The network thread reads this to decide whether a batch is a
/// first send (assign a new sequence) or a retry (reuse the stored sequence).
pub fn slotBaseSequence(self: *Ring, index: u64) i32 {
    return self.slotAt(index).base_sequence;
}

/// Set the base sequence on a slot. Called by the network thread when a batch
/// is first built for the slot's (topic, partition). On retry the slot keeps
/// its previously-assigned value — the caller does NOT re-set it.
pub fn setSlotBaseSequence(self: *Ring, index: u64, seq: i32) void {
    self.slotAt(index).base_sequence = seq;
}

/// Whether the slot at `index` is currently pending (committed by a producer,
/// awaiting broker ack). Encapsulates the internal status constant so callers
/// (the producer drain loop) don't reach into the slot's atomic directly.
pub fn slotIsPending(self: *Ring, index: u64) bool {
    return self.slotAt(index).status.load(.acquire) == status_pending;
}

/// Mark a slot's message acked. No-op (stale) if the slot was recycled since it
/// was sent — detected by `generation`. Stores the result into the *handle* so
/// `await()` observes it immediately, independent of physical reclaim.
pub fn markAcked(self: *Ring, index: u64, generation: u32) void {
    self.completeSlot(index, generation, status_acked, result_acked, 0);
}

pub fn markFailed(self: *Ring, index: u64, generation: u32, err: u32) void {
    self.completeSlot(index, generation, status_failed, result_failed, err);
}

fn completeSlot(self: *Ring, index: u64, generation: u32, status: u32, result: u32, err: u32) void {
    const slot = self.slotAt(index);
    // Status check FIRST, before the generation check. A slot that is not
    // currently pending (already acked/failed/stale/free) must never be
    // completed: a duplicate or late ack for a superseded/reclaimed occupant
    // could otherwise misapply a completion to whatever handle the slot now
    // records. This replaces a debug-only assert that silently corrupted in
    // ReleaseFast.
    if (slot.status.load(.acquire) != status_pending) return;
    if (slot.generation.load(.acquire) != generation) return; // stale: recycled.

    const handle = &self.handles[slot.handle_index];
    slot.err = err;
    if (err != 0) handle.err.store(err, .monotonic);
    // Publish completion to `await()`. Release so the waiter's acquire-load sees
    // the err write above.
    handle.result.store(result, .release);
    // Mark the slot terminal so `reclaim` can free it.
    slot.status.store(status, .release);
}

/// Wake all `await()` waiters after a batch of `markAcked`/`markFailed`. Bumping
/// the counter is what makes parked waiters re-check (any change wakes all).
pub fn wakeCompletions(self: *Ring) void {
    _ = self.completions.fetchAdd(1, .acq_rel);
    if (self.completion_waiters.load(.acquire) > 0) {
        Futex.wake(&self.completions, math.maxInt(u32));
    }
}

/// Reclaim the contiguous terminal prefix from `read_head`, resetting slots to
/// free (bumping their generation) and advancing `read_head`. Wakes producers
/// blocked in `waitNotFull`. O(reclaimed) per call. `read_head` is only the
/// low-water mark: a stuck partition delays *physical* reuse but never a later
/// slot's completion (that is signalled the moment `markAcked` runs).
pub fn reclaim(self: *Ring) u64 {
    var r = self.read_head.load(.monotonic);
    const w = self.write_head.load(.acquire);
    var reclaimed: u64 = 0;

    while (r < w) : (r += 1) {
        const slot = &self.slots[r & self.mask];
        const status = slot.status.load(.acquire);
        if (status == status_free or status == status_pending) break;
        assert(status == status_acked or status == status_failed or status == status_stale);
        // Bump generation before freeing: any late ack for this slot's prior
        // occupant now mismatches and is rejected by `completeSlot`.
        _ = slot.generation.fetchAdd(1, .monotonic);
        slot.handle_index = handle_nil;
        slot.status.store(status_free, .release);
        reclaimed += 1;
    }

    if (reclaimed > 0) {
        self.read_head.store(r, .release);
        if (self.full_waiters.load(.acquire) > 0) {
            Futex.wake(readHeadLow(self), math.maxInt(u32));
        }
    }
    return reclaimed;
}

/// Re-arm a still-pending message *in place* for the network thread's retry
/// path, and re-notify the consumer to re-send it. Returns the (unchanged)
/// logical position, or `error.WouldBlock` if the slot is stale/no longer
/// pending (a late ack won the race — caller drops the retry).
///
/// In-place is deliberate. The earlier design (PLAN §2.4) claimed a *new* slot
/// and marked the old one stale, but that deadlocks when the ring is full and
/// every slot needs re-enqueue: no old slot can become reclaimable until a
/// retry succeeds, and no retry can succeed until a slot is free. Staying in
/// place needs no new slot, keeps the handle<->slot binding and generation
/// intact (so the eventual ack still matches), and reclaim leaves the slot
/// alone because it is still pending. This is a cold path.
pub fn retry(self: *Ring, index: u64, generation: u32) Error!u64 {
    const slot = self.slotAt(index);
    if (slot.status.load(.acquire) != status_pending) return error.WouldBlock;
    if (slot.generation.load(.acquire) != generation) return error.WouldBlock; // stale.
    // Slot stays pending in place; clear any stale error and re-notify the
    // network thread to re-send. The handle result is still `in_flight`, so
    // there is nothing to reset there.
    slot.err = 0;
    self.notifyConsumer();
    return index;
}

/// Phase 1 of graceful shutdown: stop accepting new messages (acquire returns
/// `error.Shutdown`) but let the network thread keep draining pending slots.
/// The network thread's `run` loop detects `isDraining()` and finishes
/// in-flight Produce round-trips up to the drain timeout, then exits.
/// `Client.deinit` calls this first, joins the thread, then calls
/// `requestShutdown` (phase 2) to wake any remaining awaiters.
pub fn requestDrain(self: *Ring) void {
    self.draining.store(true, .release);
    // Bump the two dedicated counter words BEFORE waking them, exactly as
    // `requestShutdown` does. Without the bump, a producer that snapshotted the
    // old `handle_freed` value and races into `Futex.wait` (passed the
    // `draining` re-check, then parked before the wake landed) would sleep on
    // an unchanged word until phase-2 `requestShutdown` — a lost-wakeup hang
    // for the whole drain window. The bump makes `wait` return immediately
    // (value-guard self-heal). `completions` is bumped for symmetry (harmless,
    // keeps the self-heal invariant uniform across both counter waits).
    _ = self.completions.fetchAdd(1, .release);
    _ = self.handle_freed.fetchAdd(1, .release);
    // Wake every kind of waiter so nobody is stuck waiting for data/slots
    // that will never come (no new acquires during drain).
    if (self.data_waiter.load(.acquire)) Futex.wake(writeHeadLow(self), 1);
    Futex.wake(readHeadLow(self), math.maxInt(u32));
    Futex.wake(&self.completions, math.maxInt(u32));
    Futex.wake(&self.handle_freed, math.maxInt(u32));
}

pub fn isDraining(self: *const Ring) bool {
    return self.draining.load(.acquire);
}

pub fn requestShutdown(self: *Ring) void {
    self.shutdown.store(true, .release);
    // Bump the two dedicated counter words BEFORE waking them. A waiter that
    // snapshotted the old value and races into `Futex.wait` finds the word
    // changed and returns immediately (value-guard self-heal) — no lost wakeup.
    // The two cursor words (read_head/write_head) can't be bumped without
    // corrupting the cursor, so their waiters use a bounded `timedWait`.
    _ = self.completions.fetchAdd(1, .release);
    _ = self.handle_freed.fetchAdd(1, .release);
    // Wake every kind of waiter so nobody is stuck.
    if (self.data_waiter.load(.acquire)) Futex.wake(writeHeadLow(self), 1);
    Futex.wake(readHeadLow(self), math.maxInt(u32));
    Futex.wake(&self.completions, math.maxInt(u32));
    Futex.wake(&self.handle_freed, math.maxInt(u32));
}

pub fn isShutdown(self: *const Ring) bool {
    return self.shutdown.load(.acquire);
}

// ---------------------------------------------------------------------------
// Handle pool (Treiber stack, ABA-tagged via a 32-bit generation in the head)
// ---------------------------------------------------------------------------

fn poolAlloc(self: *Ring, mode: ClaimMode) Error!u32 {
    while (true) {
        const head = self.pool_head.load(.acquire);
        const index: u32 = @truncate(head);
        if (index == handle_nil) {
            if (self.shutdown.load(.acquire)) return error.Shutdown;
            if (self.draining.load(.acquire)) return error.Shutdown;
            switch (mode) {
                .no_block => return error.WouldBlock,
                .block => {
                    try self.waitPool();
                    continue;
                },
            }
        }
        const tag: u32 = @truncate(head >> 32);
        // Monotonic: the head CAS below carries the real synchronization; this
        // load only needs to be race-free, not ordered. A stale read loses the
        // CAS and retries.
        const next = self.handles[index].next.load(.monotonic);
        const new_head: u64 = (@as(u64, tag +% 1) << 32) | next;
        if (self.pool_head.cmpxchgWeak(head, new_head, .acq_rel, .monotonic) == null) {
            return index;
        }
    }
}

fn poolFree(self: *Ring, index: u32) void {
    while (true) {
        const head = self.pool_head.load(.acquire);
        const head_index: u32 = @truncate(head);
        const tag: u32 = @truncate(head >> 32);
        self.handles[index].next.store(head_index, .monotonic);
        const new_head: u64 = (@as(u64, tag +% 1) << 32) | index;
        if (self.pool_head.cmpxchgWeak(head, new_head, .acq_rel, .monotonic) == null) break;
    }
    _ = self.handle_freed.fetchAdd(1, .acq_rel);
    if (self.pool_waiters.load(.acquire) > 0) {
        Futex.wake(&self.handle_freed, math.maxInt(u32));
    }
}

fn waitPool(self: *Ring) Error!void {
    _ = self.pool_waiters.fetchAdd(1, .acq_rel);
    defer _ = self.pool_waiters.fetchSub(1, .acq_rel);
    const seq = self.handle_freed.load(.acquire);
    if (@as(u32, @truncate(self.pool_head.load(.acquire))) != handle_nil) return;
    if (self.shutdown.load(.acquire)) return error.Shutdown;
    // A producer that entered `waitPool` after `requestDrain` fired must see
    // `draining` and bail instead of parking: acquire is closed during drain,
    // so no handle will ever be freed back for it (this mirrors the shutdown
    // guard above). `requestDrain` also bumps `handle_freed`, so even a park
    // that races the store is recovered by the value guard — both guards close
    // the window from opposite sides.
    if (self.draining.load(.acquire)) return error.Shutdown;
    Futex.wait(&self.handle_freed, seq);
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Base offset of slot `i`'s payload region.
fn payloadBase(self: *Ring, i: u32) usize {
    return @as(usize, i) * self.bytes_per_slot;
}

fn topicBuf(self: *Ring, i: u32) []u8 {
    const base = self.payloadBase(i);
    return self.payload[base..][0..self.max_topic_len];
}

fn keyBuf(self: *Ring, i: u32) []u8 {
    const base = self.payloadBase(i) + self.max_topic_len;
    return self.payload[base..][0..self.max_key_len];
}

fn valueBuf(self: *Ring, i: u32) []u8 {
    const base = self.payloadBase(i) + self.max_topic_len + self.max_key_len;
    return self.payload[base..][0..self.max_message_size];
}

/// Low 32 bits of the 64-bit cursors, for futex ops. Little-endian only
/// (asserted at the top of the file).
fn writeHeadLow(self: *Ring) *const atomic.Value(u32) {
    return @ptrCast(&self.write_head);
}

fn readHeadLow(self: *Ring) *const atomic.Value(u32) {
    return @ptrCast(&self.read_head);
}

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;
const Thread = std.Thread;

fn tinyConfig(num_slots: u32) Config {
    return .{
        .max_message_size = 64,
        .max_key_len = 16,
        .max_topic_len = 16,
        .num_slots = num_slots,
    };
}

/// Consume+ack one pending slot at `pos` if present. Returns true if it acked.
fn ackOne(ring: *Ring, pos: u64) bool {
    const slot = ring.slotAt(pos);
    if (slot.status.load(.acquire) != status_pending) return false;
    const gen = ring.slotGeneration(pos);
    ring.markAcked(pos, gen);
    ring.wakeCompletions();
    return true;
}

test "single producer/consumer: acquire, commit, ack, await" {
    var ring = try Ring.init(testing.allocator, tinyConfig(8));
    defer ring.deinit(testing.allocator);

    var m = try ring.acquire();
    try m.setTopic("events");
    m.setPartition(null);
    try m.setKey("k1");
    const dst = m.value();
    try testing.expectEqual(@as(usize, 64), dst.len);
    const payload = "hello world";
    @memcpy(dst[0..payload.len], payload);
    try m.commit(payload.len);

    // Consumer side: read the committed slot and ack it.
    try testing.expect(ring.waitForData(0));
    try testing.expectEqualStrings("events", ring.slotTopic(0));
    try testing.expectEqualStrings("k1", ring.slotKey(0));
    try testing.expectEqualStrings(payload, ring.slotValueCommitted(0));
    try testing.expectEqual(partition_unassigned, ring.slotAt(0).partition);
    try testing.expect(ackOne(&ring, 0));

    try m.await();
    try testing.expectEqual(@as(u64, 1), ring.reclaim());
    try testing.expectEqual(@as(u64, 1), ring.readHead());
}

test "MessageTooLarge on oversized topic/key/value" {
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var m = try ring.acquire();
    try testing.expectError(error.MessageTooLarge, m.setTopic("this-topic-name-is-way-too-long"));
    try testing.expectError(error.MessageTooLarge, m.setKey("this-key-is-too-long-too"));
    try testing.expectError(error.MessageTooLarge, m.commit(65));
    // A within-bounds commit still works afterwards.
    try m.setTopic("t");
    try m.commit(10);
}

test "writeMessage convenience: copies payload + commits in one call" {
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var m = try ring.acquire();
    try m.setTopic("events");
    try m.writeMessage("hello world");
    // The message is now committed (pending) — await would block without a
    // consumer, so just verify it's in-flight via tryAwait.
    try testing.expectError(error.WouldBlock, m.tryAwait());

    // Oversized payload → MessageTooLarge, message NOT committed.
    var m2 = try ring.acquire();
    try m2.setTopic("events");
    const big: [65]u8 = @splat(0xAA);
    try testing.expectError(error.MessageTooLarge, m2.writeMessage(&big));
    try testing.expectError(error.WouldBlock, m2.tryAwait()); // still in-flight, not committed
}

test "tryAcquire returns WouldBlock when full" {
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var msgs: [4]Message = undefined;
    for (&msgs) |*m| {
        m.* = try ring.tryAcquire();
        try m.setTopic("t");
        try m.commit(1);
    }
    // Ring is full: 4 in-flight, none reclaimed.
    try testing.expectError(error.WouldBlock, ring.tryAcquire());

    // Ack + reclaim one, then a slot frees up.
    try testing.expect(ackOne(&ring, 0));
    try msgs[0].await();
    try testing.expectEqual(@as(u64, 1), ring.reclaim());
    var m5 = try ring.tryAcquire();
    try m5.setTopic("t");
    try m5.commit(1);

    // Drain the rest so the ring is quiescent.
    for (1..4) |i| {
        try testing.expect(ackOne(&ring, i));
        try msgs[i].await();
    }
    try testing.expect(ackOne(&ring, 4));
    try m5.await();
}

test "tryAwait reports in-flight then completion" {
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var m = try ring.acquire();
    try m.setTopic("t");
    try m.commit(1);
    try testing.expectError(error.WouldBlock, m.tryAwait());
    try testing.expect(ackOne(&ring, 0));
    try m.tryAwait();
}

test "failed send surfaces error.SendFailed with error code" {
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var m = try ring.acquire();
    try m.setTopic("t");
    try m.commit(1);
    ring.markFailed(0, ring.slotGeneration(0), 37);
    ring.wakeCompletions();
    try testing.expectError(error.SendFailed, m.await());
    try testing.expectEqual(@as(u32, 37), m.failureCode());
}

test "partial ack: out-of-order completion, no head-of-line block" {
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var msgs: [4]Message = undefined;
    for (&msgs) |*m| {
        m.* = try ring.acquire();
        try m.setTopic("t");
        try m.commit(1);
    }

    // Ack out of order: 2, then 0, then 1, then 3.
    for ([_]u64{ 2, 0, 1, 3 }) |pos| {
        ring.markAcked(pos, ring.slotGeneration(pos));
    }
    ring.wakeCompletions();

    // Every await completes regardless of ack order — completion is not gated
    // on physical reclaim.
    for (&msgs) |*m| try m.await();

    // Physical reclaim advances the whole contiguous prefix now.
    try testing.expectEqual(@as(u64, 4), ring.reclaim());
    try testing.expectEqual(@as(u64, 4), ring.readHead());
}

test "partial ack: a stuck head slot delays reclaim but not later completions" {
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var msgs: [4]Message = undefined;
    for (&msgs) |*m| {
        m.* = try ring.acquire();
        try m.setTopic("t");
        try m.commit(1);
    }

    // Ack everything except slot 0 (the head — simulate a stuck partition).
    for ([_]u64{ 1, 2, 3 }) |pos| ring.markAcked(pos, ring.slotGeneration(pos));
    ring.wakeCompletions();

    // Slots 1..3 complete even though slot 0 is stuck.
    try msgs[1].await();
    try msgs[2].await();
    try msgs[3].await();
    try testing.expectError(error.WouldBlock, msgs[0].tryAwait());

    // Physical reclaim is head-of-line-blocked by slot 0: nothing reclaimed.
    try testing.expectEqual(@as(u64, 0), ring.reclaim());
    try testing.expectEqual(@as(u64, 0), ring.readHead());

    // Once slot 0 acks, the whole prefix reclaims.
    ring.markAcked(0, ring.slotGeneration(0));
    ring.wakeCompletions();
    try msgs[0].await();
    try testing.expectEqual(@as(u64, 4), ring.reclaim());
}

test "stable handle across retry: message re-armed in place, ack completes it" {
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var m = try ring.acquire();
    try m.setTopic("orders");
    try m.setKey("k");
    const dst = m.value();
    @memcpy(dst[0..3], "abc");
    try m.commit(3);
    try testing.expectEqual(@as(u32, 0), m.slot_index);

    // Network thread decides to retry: in place, same slot, same generation.
    const gen0 = ring.slotGeneration(0);
    const new_pos = try ring.retry(0, gen0);
    try testing.expectEqual(@as(u64, 0), new_pos); // same slot.
    try testing.expectEqual(status_pending, ring.slotAt(0).status.load(.acquire));
    try testing.expectEqual(gen0, ring.slotGeneration(0)); // no generation churn.
    try testing.expectEqualStrings("orders", ring.slotTopic(0));
    try testing.expectEqualStrings("abc", ring.slotValueCommitted(0));

    // Still in flight after retry.
    try testing.expectError(error.WouldBlock, m.tryAwait());

    // Ack the (unchanged) slot; the handle observes it.
    ring.markAcked(0, ring.slotGeneration(0));
    ring.wakeCompletions();
    try m.await();
}

test "completeSlot no-ops on a non-pending slot (stale/double ack)" {
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var m = try ring.acquire();
    try m.setTopic("t");
    try m.commit(1);
    const gen0 = ring.slotGeneration(0);

    // First ack completes the message and marks the slot acked.
    ring.markAcked(0, gen0);
    ring.wakeCompletions();
    try m.await();
    try testing.expectEqual(status_acked, ring.slotAt(0).status.load(.acquire));

    // A second (duplicate/late) ack with the SAME generation must no-op — the
    // slot is no longer pending. The status check runs before the generation
    // check precisely to catch this; the old assert would have corrupted in
    // ReleaseFast by re-completing whatever handle the slot records.
    ring.markAcked(0, gen0);
    ring.markFailed(0, gen0, 99);
    try testing.expectEqual(status_acked, ring.slotAt(0).status.load(.acquire));
    try testing.expectEqual(@as(u32, 0), ring.slotAt(0).err); // untouched.
}

test "generation counter rejects a recycled slot's stale ack" {
    // Ring of one physical slot: the second acquire is guaranteed to reuse the
    // same physical slot, so we can exercise generation-based rejection.
    var ring = try Ring.init(testing.allocator, tinyConfig(1));
    defer ring.deinit(testing.allocator);
    try testing.expectEqual(@as(u32, 1), ring.numSlots());

    // Handle A on slot 0, generation gA.
    var a = try ring.acquire();
    try a.setTopic("t");
    try a.commit(1);
    const gen_a = ring.slotGeneration(0);
    ring.markAcked(0, gen_a);
    ring.wakeCompletions();
    try a.await();
    try testing.expectEqual(@as(u64, 1), ring.reclaim()); // frees slot 0, bumps gen.

    // Handle B reuses physical slot 0 at generation gB > gA.
    var b = try ring.acquire();
    try b.setTopic("t");
    try b.commit(1);
    const gen_b = ring.slotGeneration(1); // logical pos 1, physical slot 0.
    try testing.expect(gen_b != gen_a);

    // A stale ack carrying A's old generation must NOT complete B.
    ring.markAcked(1, gen_a);
    ring.wakeCompletions();
    try testing.expectError(error.WouldBlock, b.tryAwait());

    // The correct generation completes B.
    ring.markAcked(1, gen_b);
    ring.wakeCompletions();
    try b.await();
}

test "backpressure: acquire blocks until the consumer reclaims" {
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var msgs: [4]Message = undefined;
    for (&msgs) |*m| {
        m.* = try ring.acquire();
        try m.setTopic("t");
        try m.commit(1);
    }

    // A producer thread blocks in acquire (ring is full).
    const Blocked = struct {
        ring: *Ring,
        done: *atomic.Value(bool),
        fn run(r: *Ring, done: *atomic.Value(bool)) void {
            var m = r.acquire() catch return;
            m.setTopic("t") catch return;
            m.commit(1) catch return;
            done.store(true, .release);
        }
    };
    var done: atomic.Value(bool) = .init(false);
    const t = try Thread.spawn(.{}, Blocked.run, .{ &ring, &done });

    // Give it time to park in waitNotFull; it must still be blocked.
    Thread.sleep(20 * std.time.ns_per_ms);
    try testing.expect(!done.load(.acquire));

    // Ack + reclaim one slot: the blocked producer must wake and complete.
    ring.markAcked(0, ring.slotGeneration(0));
    ring.wakeCompletions();
    try msgs[0].await();
    try testing.expectEqual(@as(u64, 1), ring.reclaim());

    t.join();
    try testing.expect(done.load(.acquire));

    // Drain the remainder (3 originals + 1 from the unblocked thread).
    for (1..4) |i| {
        try testing.expect(ackOne(&ring, i));
        try msgs[i].await();
    }
    try testing.expect(ackOne(&ring, 4));
    _ = ring.reclaim();
}

test "multi-producer, single consumer: no loss, all acked" {
    var ring = try Ring.init(testing.allocator, tinyConfig(256));
    defer ring.deinit(testing.allocator);

    const num_producers = 4;
    const per_producer = 200;
    const total = num_producers * per_producer;

    const Producer = struct {
        ring: *Ring,
        acked: *atomic.Value(u32),
        fn run(r: *Ring, acked: *atomic.Value(u32)) void {
            var i: u32 = 0;
            while (i < per_producer) : (i += 1) {
                var m = r.acquire() catch return;
                m.setTopic("t") catch return;
                const dst = m.value();
                dst[0] = @truncate(i);
                m.commit(1) catch return;
                m.await() catch return;
                _ = acked.fetchAdd(1, .acq_rel);
            }
        }
    };

    const Consumer = struct {
        ring: *Ring,
        seen: *atomic.Value(u32),
        fn run(r: *Ring, seen: *atomic.Value(u32)) void {
            var read_pos: u64 = 0;
            while (true) {
                const head = r.writeHead();
                while (read_pos < head) {
                    const slot = r.slotAt(read_pos);
                    if (slot.status.load(.acquire) != status_pending) break;
                    r.markAcked(read_pos, r.slotGeneration(read_pos));
                    _ = seen.fetchAdd(1, .acq_rel);
                    read_pos += 1;
                }
                r.wakeCompletions();
                _ = r.reclaim();
                if (read_pos >= head) {
                    if (!r.waitForData(read_pos)) break;
                }
            }
        }
    };

    var acked: atomic.Value(u32) = .init(0);
    var seen: atomic.Value(u32) = .init(0);

    const consumer = try Thread.spawn(.{}, Consumer.run, .{ &ring, &seen });
    var producers: [num_producers]Thread = undefined;
    for (&producers) |*t| t.* = try Thread.spawn(.{}, Producer.run, .{ &ring, &acked });
    for (&producers) |*t| t.join();

    ring.requestShutdown();
    consumer.join();

    try testing.expectEqual(@as(u32, total), acked.load(.acquire));
    try testing.expectEqual(@as(u32, total), seen.load(.acquire));
}

// Fix 3: these shutdown tests deliberately do NOT sleep to "ensure the waiter
// is parked first" — that only proves the easy case (waiter already asleep when
// the wake lands). The hard case is the lost-wakeup race: the waiter passes its
// shutdown re-check, then parks *after* `requestShutdown`'s wake already fired.
// We trigger shutdown immediately after spawning and require the waiter to
// unblock within a bounded deadline, over many iterations, so a regression
// (removing the counter bump / the timedWait) would hang and fail the deadline.

fn expectUnblocksBy(result: *atomic.Value(u32), want: u32, deadline_ns: u64) !void {
    var waited: u64 = 0;
    const step = 1 * std.time.ns_per_ms;
    while (result.load(.acquire) == 0) {
        if (waited >= deadline_ns) return error.TestWaiterStuck;
        Thread.sleep(step);
        waited += step;
    }
    try testing.expectEqual(want, result.load(.acquire));
}

test "shutdown unblocks an in-flight acquire (no park-first assumption)" {
    const Waiter = struct {
        fn run(r: *Ring, result: *atomic.Value(u32)) void {
            if (r.acquire()) |_| {
                result.store(1, .release); // unexpected success
            } else |err| switch (err) {
                error.Shutdown => result.store(2, .release),
                else => result.store(3, .release),
            }
        }
    };

    var iter: u32 = 0;
    while (iter < 200) : (iter += 1) {
        var ring = try Ring.init(testing.allocator, tinyConfig(2));
        defer ring.deinit(testing.allocator);
        // Fill the ring so acquire must block on the full futex.
        var msgs: [2]Message = undefined;
        for (&msgs) |*m| {
            m.* = try ring.acquire();
            try m.setTopic("t");
            try m.commit(1);
        }

        var result: atomic.Value(u32) = .init(0);
        const t = try Thread.spawn(.{}, Waiter.run, .{ &ring, &result });
        // No sleep: race the wake against the waiter parking.
        ring.requestShutdown();
        expectUnblocksBy(&result, 2, 2 * std.time.ns_per_s) catch |e| {
            t.join();
            return e;
        };
        t.join();
    }
}

test "shutdown unblocks an in-flight await (no park-first assumption)" {
    const Waiter = struct {
        fn run(msg_in: Message, result: *atomic.Value(u32)) void {
            var msg = msg_in;
            if (msg.await()) |_| {
                result.store(1, .release);
            } else |err| switch (err) {
                error.Shutdown => result.store(2, .release),
                else => result.store(3, .release),
            }
        }
    };

    var iter: u32 = 0;
    while (iter < 200) : (iter += 1) {
        var ring = try Ring.init(testing.allocator, tinyConfig(4));
        defer ring.deinit(testing.allocator);
        var m = try ring.acquire();
        try m.setTopic("t");
        try m.commit(1);

        var result: atomic.Value(u32) = .init(0);
        const t = try Thread.spawn(.{}, Waiter.run, .{ m, &result });
        // No sleep: shutdown must bump `completions` so a not-yet-parked waiter
        // re-checks and returns.
        ring.requestShutdown();
        expectUnblocksBy(&result, 2, 2 * std.time.ns_per_s) catch |e| {
            t.join();
            return e;
        };
        t.join();
    }
}

test "failureCode survives the handle being recycled by another producer (Fix 1)" {
    // One physical slot + one handle: the moment the failing message frees its
    // handle, a second producer re-binds it and resets `handle.err` to 0. If
    // failureCode read the handle (not the cached token value), it would race
    // to 0. Loop enough to expose the race were it present.
    var iter: u32 = 0;
    while (iter < 2000) : (iter += 1) {
        var ring = try Ring.init(testing.allocator, tinyConfig(1));
        defer ring.deinit(testing.allocator);

        var a = try ring.acquire();
        try a.setTopic("t");
        try a.commit(1);
        ring.markFailed(0, ring.slotGeneration(0), 42);
        ring.wakeCompletions();

        // A second producer that will grab the freed handle + reclaimed slot
        // the instant they are available, resetting err to 0.
        const Grabber = struct {
            fn run(r: *Ring, started: *atomic.Value(bool)) void {
                started.store(true, .release);
                var b = r.acquire() catch return;
                b.setTopic("t") catch return;
                b.commit(1) catch return;
                r.markAcked(b.slot_index, r.slotGeneration(b.slot_index));
                r.wakeCompletions();
                b.await() catch {}; // ziglint-ignore: Z026 — grabber only churns the handle; outcome irrelevant.
            }
        };
        var started: atomic.Value(bool) = .init(false);

        try testing.expectError(error.SendFailed, a.await());
        // Free the slot so the grabber can proceed, then let it race.
        try testing.expectEqual(@as(u64, 1), ring.reclaim());
        const t = try Thread.spawn(.{}, Grabber.run, .{ &ring, &started });
        // Read failureCode while the grabber is actively re-binding the handle.
        try testing.expectEqual(@as(u32, 42), a.failureCode());
        t.join();
        try testing.expectEqual(@as(u32, 42), a.failureCode());
    }
}

test "full ring: every slot fails retriably, retry all in place, then ack all (Fix 5)" {
    // The move-to-a-new-slot retry deadlocks here: no slot is reclaimable until
    // a retry succeeds, and no retry can claim a slot because all are pending.
    // In-place retry must not deadlock.
    var ring = try Ring.init(testing.allocator, tinyConfig(8));
    defer ring.deinit(testing.allocator);

    var msgs: [8]Message = undefined;
    for (&msgs) |*m| {
        m.* = try ring.acquire();
        try m.setTopic("t");
        try m.commit(1);
    }
    // Ring is full; retry every slot in place — must all succeed, no WouldBlock.
    for (0..8) |pos| {
        const new_pos = try ring.retry(pos, ring.slotGeneration(pos));
        try testing.expectEqual(@as(u64, pos), new_pos);
        try testing.expectEqual(status_pending, ring.slotAt(pos).status.load(.acquire));
    }
    // Still all in flight.
    for (&msgs) |*m| try testing.expectError(error.WouldBlock, m.tryAwait());
    // Now ack all; every await returns.
    for (0..8) |pos| ring.markAcked(pos, ring.slotGeneration(pos));
    ring.wakeCompletions();
    for (&msgs) |*m| try m.await();
    try testing.expectEqual(@as(u64, 8), ring.reclaim());
}

test "acquire blocks on handle exhaustion, not a phantom write_head slot (Fix 6)" {
    var ring = try Ring.init(testing.allocator, tinyConfig(2));
    defer ring.deinit(testing.allocator);

    // Fill the ring, ack + reclaim every slot, but DON'T await — handles stay
    // owned. Ring is now empty (read_head caught up) yet 0 handles are free.
    var msgs: [2]Message = undefined;
    for (&msgs) |*m| {
        m.* = try ring.acquire();
        try m.setTopic("t");
        try m.commit(1);
    }
    for (0..2) |pos| ring.markAcked(pos, ring.slotGeneration(pos));
    ring.wakeCompletions();
    try testing.expectEqual(@as(u64, 2), ring.reclaim());
    const wh_before = ring.writeHead();

    // acquire() must block on the handle pool (all handles owned) and must NOT
    // advance write_head into a phantom, handle-less slot.
    const Waiter = struct {
        fn run(r: *Ring, done: *atomic.Value(bool)) void {
            var m = r.acquire() catch return;
            m.setTopic("t") catch return;
            m.commit(1) catch return;
            done.store(true, .release);
        }
    };
    var done: atomic.Value(bool) = .init(false);
    const t = try Thread.spawn(.{}, Waiter.run, .{ &ring, &done });

    Thread.sleep(20 * std.time.ns_per_ms);
    try testing.expect(!done.load(.acquire)); // blocked on handle exhaustion.
    try testing.expectEqual(wh_before, ring.writeHead()); // no phantom slot.

    // Await one owned handle — frees it — and the blocked acquire proceeds.
    try msgs[0].await();
    t.join();
    try testing.expect(done.load(.acquire));
    try testing.expectEqual(wh_before + 1, ring.writeHead());

    // Drain the tail so the ring is quiescent.
    try msgs[1].await();
    const tail = wh_before; // logical pos of the waiter's slot.
    try testing.expect(ackOne(&ring, tail));
    ring.wakeCompletions();
    _ = ring.reclaim();
}

test "stress: multi-producer + consumer with randomized ack/retry/reclaim/shutdown" {
    // The committed stress harness. Small ring + many producers forces heavy
    // backpressure (acquire blocking on full and on handle exhaustion), while a
    // single consumer randomly acks or retries-in-place and reclaims. This is
    // what exercises fixes 1/2/3/4/6 under contention across optimize modes.
    // Runs under whatever `-Doptimize` the test suite is built with.
    const num_producers = 6;
    const per_producer = 60;
    const total: u32 = num_producers * per_producer;

    const Producer = struct {
        fn run(r: *Ring, acked: *atomic.Value(u32), seed: u64) void {
            var prng = std.Random.DefaultPrng.init(seed);
            const rng = prng.random();
            var i: u32 = 0;
            while (i < per_producer) : (i += 1) {
                var m = r.acquire() catch return;
                m.setTopic("t") catch return;
                const dst = m.value();
                dst[0] = @truncate(i);
                m.commit(1) catch return;
                m.await() catch return; // any completion (ack) is success here.
                _ = acked.fetchAdd(1, .acq_rel);
                if (rng.boolean()) std.atomic.spinLoopHint();
            }
        }
    };

    const Consumer = struct {
        fn run(r: *Ring, seed: u64) void {
            var prng = std.Random.DefaultPrng.init(seed);
            const rng = prng.random();
            while (true) {
                const w = r.writeHead();
                var pos = r.readHead();
                while (pos < w) : (pos += 1) {
                    const slot = r.slotAt(pos);
                    if (slot.status.load(.acquire) != status_pending) continue;
                    const gen = r.slotGeneration(pos);
                    // ~1 in 4 pending slots gets a retry-in-place instead of an
                    // ack this pass; it stays pending and is acked on a later
                    // pass, so completion is guaranteed to terminate.
                    if (rng.uintLessThan(u8, 4) == 0) {
                        // ziglint-ignore: Z026 — WouldBlock/Shutdown here: nothing to do, ack it next pass.
                        _ = r.retry(pos, gen) catch {};
                    } else {
                        r.markAcked(pos, gen);
                    }
                }
                r.wakeCompletions();
                _ = r.reclaim();
                if (r.readHead() >= w) {
                    if (!r.waitForData(r.readHead())) return;
                }
            }
        }
    };

    var iter: u32 = 0;
    while (iter < 20) : (iter += 1) {
        var ring = try Ring.init(testing.allocator, tinyConfig(4));
        defer ring.deinit(testing.allocator);

        var acked: atomic.Value(u32) = .init(0);
        const consumer = try Thread.spawn(.{}, Consumer.run, .{ &ring, 0x5eed ^ iter });
        var producers: [num_producers]Thread = undefined;
        for (&producers, 0..) |*p, k| {
            p.* = try Thread.spawn(.{}, Producer.run, .{ &ring, &acked, iter *% 131 +% k });
        }
        for (&producers) |*p| p.join();

        ring.requestShutdown();
        consumer.join();
        try testing.expectEqual(total, acked.load(.acquire));
    }
}

test "double-await guard: handle_index is handle_nil after await" {
    // A Message is single-use: exactly one await. A second await would
    // double-free the handle pool. The `await`/`tryAwait`/`failureCode` methods
    // assert `handle_index != handle_nil` on entry, catching the misuse in
    // Debug/ReleaseSafe. This test verifies the observable contract: after
    // a successful await, handle_index is the sentinel. (The assert itself
    // panics rather than returning an error, so it can't be caught with
    // expectError — the sentinel state is the testable surface.)
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var m = try ring.acquire();
    try m.setTopic("t");
    try m.commit(1);

    try testing.expect(ackOne(&ring, 0));
    ring.wakeCompletions();
    try m.await();

    try testing.expectEqual(handle_nil, m.handle_index);
}

test "handle_index is handle_nil after failed await" {
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var m = try ring.acquire();
    try m.setTopic("t");
    try m.commit(1);

    ring.markFailed(0, ring.slotGeneration(0), 42);
    ring.wakeCompletions();
    try testing.expectError(error.SendFailed, m.await());

    try testing.expectEqual(handle_nil, m.handle_index);
}

test "handle_index is NOT handle_nil after tryAwait WouldBlock" {
    // tryAwait returning WouldBlock must NOT set the sentinel — the message
    // is still in flight and can be awaited later.
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    var m = try ring.acquire();
    try m.setTopic("t");
    try m.commit(1);

    try testing.expectError(error.WouldBlock, m.tryAwait());
    try testing.expect(m.handle_index != handle_nil);

    // Now complete and await normally.
    try testing.expect(ackOne(&ring, 0));
    ring.wakeCompletions();
    try m.await();
    try testing.expectEqual(handle_nil, m.handle_index);
}

test "init rounds slot count up to a power of two" {
    var ring = try Ring.init(testing.allocator, tinyConfig(5));
    defer ring.deinit(testing.allocator);
    try testing.expectEqual(@as(u32, 8), ring.numSlots());
    try testing.expectEqual(@as(u64, 7), ring.mask);
}

test "requestDrain blocks new acquires but lets pending slots drain" {
    // requestDrain (phase 1 of graceful shutdown) must block new acquire /
    // tryAcquire while letting the network thread keep draining pending slots.
    var ring = try Ring.init(testing.allocator, tinyConfig(4));
    defer ring.deinit(testing.allocator);

    // Commit a message (pending) — it stays drainable.
    var m = try ring.acquire();
    try m.setTopic("t");
    try m.commit(1);
    try testing.expect(ring.slotIsPending(0));

    // Enter drain mode.
    ring.requestDrain();
    try testing.expect(ring.isDraining());
    try testing.expect(!ring.isShutdown());

    // New acquires are blocked with error.Shutdown.
    try testing.expectError(error.Shutdown, ring.tryAcquire());

    // The pending slot is still drainable: the consumer can still ack it.
    try testing.expect(ackOne(&ring, 0));
    ring.wakeCompletions();
    try m.await();
    try testing.expectEqual(@as(u64, 1), ring.reclaim()); // advance read_head

    // waitForData returns false when draining and no data remains.
    try testing.expect(!ring.waitForData(ring.readHead()));

    // Full shutdown still works after drain.
    ring.requestShutdown();
    try testing.expect(ring.isShutdown());
}

test "requestDrain unblocks an in-flight acquire" {
    // Like the shutdown unblock test, but for drain: a producer blocked in
    // acquire (ring full) must unblock with error.Shutdown when requestDrain
    // is called, not hang.
    const Waiter = struct {
        fn run(r: *Ring, result: *atomic.Value(u32)) void {
            if (r.acquire()) |_| {
                result.store(1, .release);
            } else |err| switch (err) {
                error.Shutdown => result.store(2, .release),
                else => result.store(3, .release),
            }
        }
    };

    var ring = try Ring.init(testing.allocator, tinyConfig(2));
    defer ring.deinit(testing.allocator);

    // Fill the ring so acquire must block.
    var msgs: [2]Message = undefined;
    for (&msgs) |*m| {
        m.* = try ring.acquire();
        try m.setTopic("t");
        try m.commit(1);
    }

    var result: atomic.Value(u32) = .init(0);
    const t = try Thread.spawn(.{}, Waiter.run, .{ &ring, &result });
    ring.requestDrain();
    expectUnblocksBy(&result, 2, 2 * std.time.ns_per_s) catch |e| {
        t.join();
        return e;
    };
    t.join();
}

test {
    testing.refAllDecls(@This());
}
