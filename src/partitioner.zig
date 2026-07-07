//! Partition selection for produced records.
//!
//! Two strategies, matching the Java client's `DefaultPartitioner` semantics
//! closely enough to co-produce to the same partitions:
//!
//!   - key-hash: `murmur2(key) & 0x7fffffff) % num_partitions`. The hash is
//!     Kafka's own `org.apache.kafka.common.utils.Utils.murmur2` (seed
//!     `0x9747b28c`), NOT the more common MurmurHash2/3 variants — a different
//!     hash would route keys to different partitions than every other Kafka
//!     client. `toPositive` is `& 0x7fffffff` (Java `Utils.toPositive`).
//!   - round-robin: a monotonic counter modulo `num_partitions`. A null key
//!     always falls back to round-robin, in both `.default` and `.key_hash`
//!     modes (this mirrors the Java default: keyed records hash, keyless
//!     records spread round-robin — we do not implement the newer sticky
//!     partitioner, which only changes keyless batching efficiency, not
//!     correctness).
//!
//! The partitioner runs on the single network thread at batch time, so the
//! round-robin counter needs no synchronization. It is kept atomic anyway so
//! the type is safe to share if a caller ever pre-assigns partitions off-thread
//! (cheap: a relaxed fetchAdd on an uncontended line).

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

/// Partition-selection strategy (public config; PLAN §3).
pub const Strategy = enum {
    /// Java-default behavior: hash the key when present, round-robin when null.
    default,
    /// Always round-robin, ignoring the key.
    round_robin,
    /// Hash the key when present; round-robin when null (same as `.default`).
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

/// Select a partition for a record.
///
/// `explicit` honors a caller-set partition directly (bounds-asserted).
/// Otherwise the strategy decides: key present + hashing → murmur2, else
/// round-robin. `num_partitions` must be > 0.
pub fn pick(
    strategy: Strategy,
    rr: *RoundRobin,
    key: ?[]const u8,
    num_partitions: u32,
) u32 {
    assert(num_partitions > 0);
    const hash_keys = switch (strategy) {
        .default, .key_hash => true,
        .round_robin => false,
    };
    if (hash_keys) {
        if (key) |k| return partitionForKey(k, num_partitions);
    }
    return rr.next(num_partitions);
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

    // key_hash with a key → deterministic hash, ignores rr.
    const kh = pick(.key_hash, &rr, "entity-42", 7);
    try testing.expectEqual(partitionForKey("entity-42", 7), kh);

    // round_robin ignores the key entirely.
    var rr2: RoundRobin = .{};
    const a = pick(.round_robin, &rr2, "entity-42", 4);
    const b = pick(.round_robin, &rr2, "entity-42", 4);
    try testing.expect(a != b or 4 == 1); // consecutive rr picks differ for n>1

    // null key → round-robin even in default/key_hash mode.
    var rr3: RoundRobin = .{};
    const n1 = pick(.default, &rr3, null, 3);
    try testing.expect(n1 < 3);
}
