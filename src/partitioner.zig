//! Partition selection for produced records.
//!
//! Three strategies, matching the Java client's partitioner semantics:
//!
//!   - key-hash: `murmur2(key) & 0x7fffffff) % num_partitions`. The hash is
//!     Kafka's own `org.apache.kafka.common.utils.Utils.murmur2` (seed
//!     `0x9747b28c`), NOT the more common MurmurHash2/3 variants — a different
//!     hash would route keys to different partitions than every other Kafka
//!     client. `toPositive` is `& 0x7fffffff` (Java `Utils.toPositive`).
//!   - round-robin: a monotonic counter modulo `num_partitions`. A null key
//!     falls back to round-robin in `.key_hash` mode.
//!   - sticky (the modern `.default`): keyed records use murmur2 key-hash
//!     (unchanged); keyless records coalesce to ONE partition per topic per
//!     drain cycle, then rotate to the next partition on the next drain.
//!     This is KIP-794's `DefaultPartitioner` behavior: sticky for null keys,
//!     key-hash for non-null keys. Batching keyless records to a single
//!     partition per linger window materially increases batch size and
//!     compression ratio vs round-robin, which scatters keyless records
//!     across partitions and keeps batches small.
//!
//! The partitioner runs on the single network thread at batch time, so the
//! round-robin counter and sticky state need no synchronization. The
//! round-robin counter is kept atomic anyway so the type is safe to share if a
//! caller ever pre-assigns partitions off-thread (cheap: a relaxed fetchAdd on
//! an uncontended line).

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

/// Partition-selection strategy for producer configuration.
pub const Strategy = enum {
    /// Modern Java-default behavior (KIP-794): hash the key when present,
    /// sticky-partition keyless records (one partition per topic per drain,
    /// rotating between drains). This is the default.
    default,
    /// Always round-robin, ignoring the key.
    round_robin,
    /// Hash the key when present; round-robin when null (pre-sticky Java
    /// default behavior).
    key_hash,
};

/// Round-robin cursor. One per topic in the producer's cache; single-threaded
/// access on the network thread, atomic only for defensive shareability.
pub const RoundRobin = struct {
    counter: std.atomic.Value(u32) = .init(0),

    /// Next partition in `[0, num_partitions)`. `num_partitions` must be > 0.
    pub fn next(self: *RoundRobin, num_partitions: u32) u32 {
        assert(num_partitions > 0);
        const n = self.counter.fetchAdd(1, .monotonic);
        return n % num_partitions;
    }
};

/// Sentinel for "no sticky partition chosen yet this drain".
pub const sticky_unset: u32 = std.math.maxInt(u32);

/// Sticky partition state. One per topic in the producer's cache.
/// Single-threaded (network thread only).
///
/// The sticky epoch is one drain cycle: `beginDrain()` clears the current
/// partition, and the first keyless record for this topic picks a partition
/// (via the rotating cursor) and caches it. All subsequent keyless records
/// for this topic in the same drain reuse the cached partition. The cursor
/// advances on each pick, so the next drain's keyless records go to a
/// different partition (deterministic rotation, not random — the Java client
/// uses random; we rotate for testability).
pub const Sticky = struct {
    /// Rotating cursor: advances by 1 each drain that uses sticky, so
    /// successive drains pick different partitions.
    cursor: u32 = 0,
    /// The partition chosen for the current drain, or `sticky_unset` if
    /// no keyless record has been encountered yet this drain.
    current: u32 = sticky_unset,

    /// Reset at the start of each drain cycle. The first `pick` call
    /// thereafter selects and caches the sticky partition for the drain.
    pub fn beginDrain(self: *Sticky) void {
        self.current = sticky_unset;
    }

    /// Return the sticky partition for this drain, selecting one if not yet
    /// chosen. `num_partitions` must be > 0.
    pub fn pick(self: *Sticky, num_partitions: u32) u32 {
        assert(num_partitions > 0);
        if (self.current != sticky_unset) return self.current;
        const p = self.cursor % num_partitions;
        self.current = p;
        self.cursor +%= 1;
        return p;
    }
};

/// Select a partition for a record.
///
/// Strategy decides: key present + hashing → murmur2 (`.default` and
/// `.key_hash`); null key → sticky (`.default`) or round-robin (`.key_hash`
/// and `.round_robin`). `num_partitions` must be > 0.
///
/// `sticky` is only read when `strategy == .default` and `key == null`;
/// `rr` is only read when the keyless path is round-robin. The caller must
/// call `sticky.beginDrain()` once at the start of each drain cycle before
/// any `pick` call (for all topics), so the sticky partition rotates between
/// drains.
pub fn pick(
    strategy: Strategy,
    rr: *RoundRobin,
    sticky: *Sticky,
    key: ?[]const u8,
    num_partitions: u32,
) u32 {
    assert(num_partitions > 0);
    switch (strategy) {
        .default => {
            if (key) |k| return partitionForKey(k, num_partitions);
            return sticky.pick(num_partitions);
        },
        .key_hash => {
            if (key) |k| return partitionForKey(k, num_partitions);
            return rr.next(num_partitions);
        },
        .round_robin => return rr.next(num_partitions),
    }
}

/// `(murmur2(key) & 0x7fffffff) % num_partitions` — the Java key-hash mapping.
pub fn partitionForKey(key: []const u8, num_partitions: u32) u32 {
    assert(num_partitions > 0);
    const positive: u32 = murmur2(key) & 0x7fffffff;
    return positive % num_partitions;
}

/// Kafka's `Utils.murmur2` (MurmurHash2, seed `0x9747b28c`). Faithful port of
/// the Java source: all arithmetic wraps in 32 bits, `>>>` is a logical shift
/// (we compute in `u32` so `>>` is already logical).
///
/// Reference: `org.apache.kafka.common.utils.Utils.murmur2`.
pub fn murmur2(data: []const u8) u32 {
    const m: u32 = 0x5bd1e995;
    const r: u5 = 24;
    const len: u32 = @intCast(data.len);

    var h: u32 = 0x9747b28c ^ len;

    const blocks = data.len / 4;
    var i: usize = 0;
    while (i < blocks) : (i += 1) {
        const b = i * 4;
        var k: u32 = @as(u32, data[b]) |
            (@as(u32, data[b + 1]) << 8) |
            (@as(u32, data[b + 2]) << 16) |
            (@as(u32, data[b + 3]) << 24);
        k *%= m;
        k ^= k >> r;
        k *%= m;
        h *%= m;
        h ^= k;
    }

    // Tail: the Java switch falls through, so a length%4 of 3 folds in bytes
    // 2, 1, 0; of 2 folds in 1, 0; of 1 folds in 0 — then multiplies once.
    const tail = data.len & ~@as(usize, 3);
    switch (data.len & 3) {
        3 => {
            h ^= @as(u32, data[tail + 2]) << 16;
            h ^= @as(u32, data[tail + 1]) << 8;
            h ^= @as(u32, data[tail]);
            h *%= m;
        },
        2 => {
            h ^= @as(u32, data[tail + 1]) << 8;
            h ^= @as(u32, data[tail]);
            h *%= m;
        },
        1 => {
            h ^= @as(u32, data[tail]);
            h *%= m;
        },
        else => {},
    }

    h ^= h >> 13;
    h *%= m;
    h ^= h >> 15;
    return h;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "murmur2 matches Kafka Utils.testMurmur2 vectors" {
    // Vectors lifted from Kafka's UtilsTest.testMurmur2 (asserted as signed
    // i32, which is how the Java test expresses them). A mismatch means our
    // hash would route keys to different partitions than every other client.
    const cases = [_]struct { in: []const u8, want: i32 }{
        .{ .in = "21", .want = -973932308 },
        .{ .in = "foobar", .want = -790332482 },
        .{ .in = "a-little-bit-long-string", .want = -985981536 },
        .{ .in = "a-little-bit-longer-string", .want = -1486304829 },
        .{ .in = "lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", .want = -58897971 },
    };
    for (cases) |c| {
        const got: i32 = @bitCast(murmur2(c.in));
        try testing.expectEqual(c.want, got);
    }
}

test "partitionForKey is deterministic and in range" {
    const num_partitions: u32 = 7;
    const p1 = partitionForKey("entity-42", num_partitions);
    const p2 = partitionForKey("entity-42", num_partitions);
    try testing.expectEqual(p1, p2);
    try testing.expect(p1 < num_partitions);

    // Cross-check the exact mapping against the Python-verified positive value:
    // murmur2("entity-42") & 0x7fffffff == 1421688142.
    try testing.expectEqual(@as(u32, 1421688142 % 7), p1);
}

test "round-robin cycles through all partitions" {
    var rr: RoundRobin = .{};
    const num_partitions: u32 = 4;
    var seen = [_]bool{false} ** 4;
    for (0..8) |_| {
        const p = rr.next(num_partitions);
        try testing.expect(p < num_partitions);
        seen[p] = true;
    }
    for (seen) |s| try testing.expect(s);
}

test "pick: explicit strategies" {
    var rr: RoundRobin = .{};
    var sticky: Sticky = .{};

    // key_hash with a key → deterministic hash, ignores rr and sticky.
    const kh = pick(.key_hash, &rr, &sticky, "entity-42", 7);
    try testing.expectEqual(partitionForKey("entity-42", 7), kh);

    // round_robin ignores the key entirely.
    var rr2: RoundRobin = .{};
    var sticky2: Sticky = .{};
    const a = pick(.round_robin, &rr2, &sticky2, "entity-42", 4);
    const b = pick(.round_robin, &rr2, &sticky2, "entity-42", 4);
    try testing.expect(a != b or 4 == 1); // consecutive rr picks differ for n>1

    // null key + key_hash → round-robin.
    var rr3: RoundRobin = .{};
    var sticky3: Sticky = .{};
    const n1 = pick(.key_hash, &rr3, &sticky3, null, 3);
    try testing.expect(n1 < 3);
}

test "sticky: keyless records coalesce to one partition per drain" {
    // Within a single drain (after beginDrain), all keyless picks return the
    // same partition. `.default` strategy exercises the sticky path.
    var rr: RoundRobin = .{};
    var sticky: Sticky = .{};
    const np: u32 = 4;

    sticky.beginDrain();
    const p0 = pick(.default, &rr, &sticky, null, np);
    const p1 = pick(.default, &rr, &sticky, null, np);
    const p2 = pick(.default, &rr, &sticky, null, np);
    try testing.expectEqual(p0, p1);
    try testing.expectEqual(p0, p2);
    try testing.expect(p0 < np);
}

test "sticky: rotates between drains" {
    // Successive drains pick different partitions (deterministic rotation).
    var rr: RoundRobin = .{};
    var sticky: Sticky = .{};
    const np: u32 = 4;

    sticky.beginDrain();
    const d0 = pick(.default, &rr, &sticky, null, np);
    sticky.beginDrain();
    const d1 = pick(.default, &rr, &sticky, null, np);
    sticky.beginDrain();
    const d2 = pick(.default, &rr, &sticky, null, np);

    try testing.expect(d0 != d1);
    try testing.expect(d1 != d2);
    try testing.expect(d0 != d2);
}

test "sticky: keyed records use key-hash, not sticky" {
    // `.default` with a non-null key → murmur2 key-hash, not sticky.
    var rr: RoundRobin = .{};
    var sticky: Sticky = .{};
    const np: u32 = 7;

    sticky.beginDrain();
    const pk = pick(.default, &rr, &sticky, "entity-42", np);
    try testing.expectEqual(partitionForKey("entity-42", np), pk);
    // The sticky partition was not consumed by the keyed pick.
    try testing.expectEqual(sticky.current, sticky_unset);
}

test "sticky: rotation wraps modulo num_partitions" {
    // After np drains, the cursor wraps and we revisit partition 0.
    var rr: RoundRobin = .{};
    var sticky: Sticky = .{};
    const np: u32 = 3;

    sticky.beginDrain();
    const d0 = pick(.default, &rr, &sticky, null, np); // cursor 0 → 0
    sticky.beginDrain();
    const d1 = pick(.default, &rr, &sticky, null, np); // cursor 1 → 1
    sticky.beginDrain();
    const d2 = pick(.default, &rr, &sticky, null, np); // cursor 2 → 2
    sticky.beginDrain();
    const d3 = pick(.default, &rr, &sticky, null, np); // cursor 3 → 0

    try testing.expectEqual(@as(u32, 0), d0);
    try testing.expectEqual(@as(u32, 1), d1);
    try testing.expectEqual(@as(u32, 2), d2);
    try testing.expectEqual(@as(u32, 0), d3);
}
