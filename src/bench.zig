//! Benchmark harness (issue #9): measures produce throughput (msgs/s, bytes/s)
//! and per-message latency (min/max/avg from commit to ack) through the real
//! `Client` against the in-process mock broker (ztls + SCRAM over loopback).
//!
//! This is a separate build step (`zig build bench`), not part of the unit
//! test suite. Run under ReleaseFast for meaningful numbers:
//!   `zig build bench -Doptimize=ReleaseFast`
//! or via justfile:
//!   `just bench`
//!
//! The mock broker is deterministic and fast (no real Kafka, no disk, no
//! replication) — it measures the client's single-network-thread /
//! single-connection throughput ceiling, not broker-side costs. The TLS
//! handshake + SCRAM auth still happen on every connection (real crypto), so
//! the benchmark includes real serialization + TLS framing overhead, just not
//! network latency or broker I/O.
//!
//! Usage:
//!   bench [--num N] [--msg-size BYTES] [--linger MS] [--ring-slots N]
//!         [--no-idempotency] [--runs N]
//!
//! Defaults: 5000 messages, 100B payload, linger=0, idempotency on, 3 runs.

const std = @import("std");
const kafka = @import("kafka");
const mock = kafka.mock_broker;

const Allocator = std.mem.Allocator;

// The producer's send buffer (Connection.req_scratch) is 64KB. The produce
// request is batch + framing, so max_batch_bytes must be < 64KB. We cap at
// 56KB to leave room for the produce request framing (transactional_id, acks,
// timeout, topic name, partition data, tag buffers).
const max_batch_bytes: u32 = 56 * 1024;

// How many messages of a given size fit in one batch (record overhead ~30B/record,
// batch header ~61B). The producer doesn't split a sub-run to fit max_batch_bytes,
// so we produce in waves of this size to avoid exceeding the send buffer.
fn waveSize(msg_size: u32) u32 {
    const per_record: u32 = msg_size + 30;
    const usable: u32 = 50 * 1024; // leave room for batch header + framing
    return usable / per_record;
}

const BenchConfig = struct {
    num_msgs: u32 = 5_000,
    msg_size: u32 = 100,
    linger_ms: u32 = 0,
    ring_slots: u32 = 8192,
    idempotency: bool = true,
    runs: u32 = 3,
};

const LatencyStats = struct {
    min_ns: u64 = std.math.maxInt(u64),
    max_ns: u64 = 0,
    sum_ns: u64 = 0,
    count: u64 = 0,

    fn record(self: *LatencyStats, ns: u64) void {
        if (ns < self.min_ns) self.min_ns = ns;
        if (ns > self.max_ns) self.max_ns = ns;
        self.sum_ns += ns;
        self.count += 1;
    }

    fn avgNs(self: LatencyStats) u64 {
        if (self.count == 0) return 0;
        return self.sum_ns / self.count;
    }
};

const BenchResult = struct {
    msgs_per_sec: f64,
    bytes_per_sec: f64,
    total_bytes: u64,
    elapsed_ns: u64,
    latency: LatencyStats,
    batches_sent: u64,
};

pub fn main() !void {
    var args_iter = try std.process.argsWithAllocator(std.heap.page_allocator);
    defer args_iter.deinit();

    var cfg: BenchConfig = .{};
    _ = args_iter.next(); // skip program name
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--num")) {
            const v = args_iter.next() orelse fatal("missing value for --num", .{});
            cfg.num_msgs = std.fmt.parseInt(u32, v, 10) catch
                fatal("invalid --num: {s}", .{v});
        } else if (std.mem.eql(u8, arg, "--msg-size")) {
            const v = args_iter.next() orelse fatal("missing value for --msg-size", .{});
            cfg.msg_size = std.fmt.parseInt(u32, v, 10) catch
                fatal("invalid --msg-size: {s}", .{v});
        } else if (std.mem.eql(u8, arg, "--linger")) {
            const v = args_iter.next() orelse fatal("missing value for --linger", .{});
            cfg.linger_ms = std.fmt.parseInt(u32, v, 10) catch
                fatal("invalid --linger: {s}", .{v});
        } else if (std.mem.eql(u8, arg, "--ring-slots")) {
            const v = args_iter.next() orelse fatal("missing value for --ring-slots", .{});
            cfg.ring_slots = std.fmt.parseInt(u32, v, 10) catch
                fatal("invalid --ring-slots: {s}", .{v});
        } else if (std.mem.eql(u8, arg, "--runs")) {
            const v = args_iter.next() orelse fatal("missing value for --runs", .{});
            cfg.runs = std.fmt.parseInt(u32, v, 10) catch
                fatal("invalid --runs: {s}", .{v});
        } else if (std.mem.eql(u8, arg, "--no-idempotency")) {
            cfg.idempotency = false;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else {
            fatal("unknown argument: {s} (see --help)", .{arg});
        }
    }

    const allocator = std.heap.page_allocator;

    // Payload buffer: fill once, reuse across all messages.
    const payload = try allocator.alloc(u8, cfg.msg_size);
    defer allocator.free(payload);
    @memset(payload, 'x');

    // Wave size: messages per produce batch. We produce in waves of this size,
    // awaiting each wave before starting the next. This creates multiple
    // round-trips (sustained throughput) and avoids exceeding max_batch_bytes.
    const wave = waveSize(cfg.msg_size);
    const num_waves = (cfg.num_msgs + wave - 1) / wave;

    stdout("kafka-zig benchmark — mock broker (ztls + SCRAM-SHA-512 loopback)\n", .{});
    stdout("config: {d} msgs x {d}B, linger={d}ms, ring_slots={d}, idempotency={s}, runs={d}\n", .{
        cfg.num_msgs,                         cfg.msg_size, cfg.linger_ms, cfg.ring_slots,
        if (cfg.idempotency) "on" else "off", cfg.runs,
    });
    stdout("waves: {d} waves of ~{d} msgs/batch\n\n", .{ num_waves, wave });

    // Latency: measure commit→ack for every message.
    const commit_ts = try allocator.alloc(u64, cfg.num_msgs);
    defer allocator.free(commit_ts);
    const ack_ts = try allocator.alloc(u64, cfg.num_msgs);
    defer allocator.free(ack_ts);

    var best: ?BenchResult = null;
    var best_mps: f64 = 0;

    for (0..cfg.runs) |run_idx| {
        const result = try runBench(allocator, cfg, payload, wave, commit_ts, ack_ts);
        stdout("run {d}: {d:>10.0} msgs/s  {d:>12.0} bytes/s  ({d:.2} ms)  ", .{
            run_idx + 1,
            result.msgs_per_sec,
            result.bytes_per_sec,
            @as(f64, @floatFromInt(result.elapsed_ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms)),
        });
        stdout("latency min={d}us max={d}us avg={d}us  batches={d}\n", .{
            result.latency.min_ns / std.time.ns_per_us,
            result.latency.max_ns / std.time.ns_per_us,
            result.latency.avgNs() / std.time.ns_per_us,
            result.batches_sent,
        });

        if (result.msgs_per_sec > best_mps) {
            best_mps = result.msgs_per_sec;
            best = result;
        }
    }

    stdout("\nbest: {d:>10.0} msgs/s  {d:>12.0} bytes/s  ({d:.2} Mbit/s)\n", .{
        best.?.msgs_per_sec,
        best.?.bytes_per_sec,
        best.?.bytes_per_sec * 8.0 / 1_000_000.0,
    });
    stdout("latency (best run): min={d}us  max={d}us  avg={d}us\n", .{
        best.?.latency.min_ns / std.time.ns_per_us,
        best.?.latency.max_ns / std.time.ns_per_us,
        best.?.latency.avgNs() / std.time.ns_per_us,
    });
}

fn runBench(
    allocator: Allocator,
    cfg: BenchConfig,
    payload: []const u8,
    wave: u32,
    commit_ts: []u64,
    ack_ts: []u64,
) !BenchResult {
    // Start the mock broker fresh for each run (clean state, fresh TLS).
    var broker = try mock.Broker.start(allocator, .{ .mode = .ok, .num_partitions = 4 });
    defer broker.stop();

    const bootstrap = [_]kafka.Client.Broker{.{ .host = "127.0.0.1", .port = broker.port }};
    var client = try kafka.Client.init(allocator, .{
        .bootstrap_brokers = &bootstrap,
        .tls = .{ .sni = mock.server_name, .insecure_skip_verify = true },
        .sasl = .{ .scram_sha512 = .{
            .username = mock.username,
            .password = mock.password,
        } },
        .acks = .all,
        .linger_ms = cfg.linger_ms,
        .max_batch_bytes = max_batch_bytes,
        .max_message_size = @max(cfg.msg_size, 1024),
        .max_key_len = 64,
        .max_topic_len = 64,
        .ring_slots = cfg.ring_slots,
        .partitioner = .default,
        .client_id = "kafka-zig-bench",
        .enable_idempotency = cfg.idempotency,
        .request_timeout_ms = 30_000,
        .io_timeout_ms = 10_000,
    });
    defer client.deinit();

    // Wave-based produce: commit a wave of `wave` messages, await all, repeat.
    // This creates one batch per wave (one Produce round-trip), measuring
    // sustained throughput over multiple round-trips. The wave size is chosen
    // so one batch fits in max_batch_bytes and the mock broker's 128KB buffer.
    const handles = try allocator.alloc(kafka.Client.Message, wave);
    defer allocator.free(handles);

    var latency: LatencyStats = .{};
    var msg_idx: u32 = 0;
    const start_ns = monotonicNs();

    while (msg_idx < cfg.num_msgs) {
        const this_wave = @min(wave, cfg.num_msgs - msg_idx);

        // Acquire + fill + commit all messages in this wave.
        for (0..this_wave) |i| {
            handles[i] = try client.acquire();
            try handles[i].setTopic("events");
            handles[i].setPartition(null);
            const dst = handles[i].value();
            @memcpy(dst[0..payload.len], payload);
            commit_ts[msg_idx + i] = monotonicNs();
            try handles[i].commit(@intCast(payload.len));
        }

        // Await all messages in this wave.
        for (0..this_wave) |i| {
            handles[i].await() catch |err| {
                const code = handles[i].failureCode();
                fatal("msg {d} failed: {s} (code={d})", .{ msg_idx + i, @errorName(err), code });
            };
            ack_ts[msg_idx + i] = monotonicNs();
            latency.record(ack_ts[msg_idx + i] - commit_ts[msg_idx + i]);
        }

        msg_idx += this_wave;
    }

    const end_ns = monotonicNs();

    // Wait for the ring to fully drain (reclaim complete) before reading stats.
    const drain_start: u64 = @intCast(@as(i64, @truncate(std.time.nanoTimestamp())));
    const deadline_ns: u64 = drain_start + std.time.ns_per_s;
    while (true) {
        if (client.stats().queue_depth == 0) break;
        const now: u64 = @intCast(@as(i64, @truncate(std.time.nanoTimestamp())));
        if (now >= deadline_ns) break;
        std.Thread.sleep(std.time.ns_per_ms);
    }

    const stats = client.stats();
    const total_bytes: u64 = @as(u64, cfg.num_msgs) * @as(u64, cfg.msg_size);
    const elapsed_ns: u64 = end_ns - start_ns;
    const elapsed_secs: f64 = @as(f64, @floatFromInt(elapsed_ns)) /
        @as(f64, @floatFromInt(std.time.ns_per_s));

    return .{
        .msgs_per_sec = @as(f64, @floatFromInt(cfg.num_msgs)) / elapsed_secs,
        .bytes_per_sec = @as(f64, @floatFromInt(total_bytes)) / elapsed_secs,
        .total_bytes = total_bytes,
        .elapsed_ns = elapsed_ns,
        .latency = latency,
        .batches_sent = stats.batches_sent,
    };
}

fn monotonicNs() u64 {
    return @intCast(@as(i64, @truncate(std.time.nanoTimestamp())));
}

fn printUsage() void {
    stdout(
        \\Usage: bench [options]
        \\
        \\Options:
        \\  --num N          Number of messages to produce (default: 5000)
        \\  --msg-size BYTES Payload size per message (default: 100)
        \\  --linger MS      Linger time in ms (default: 0)
        \\  --ring-slots N   Ring slot count (default: 8192)
        \\  --no-idempotency Disable idempotent producer (default: on)
        \\  --runs N         Number of runs, best is reported (default: 3)
        \\  --help           Show this help
        \\
    , .{});
}

fn stdout(comptime fmt: []const u8, args: anytype) void {
    var buf: [4096]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    defer writer.interface.flush() catch {}; // ziglint-ignore: Z026 -- best-effort flush on exit
    writer.interface.print(fmt, args) catch {}; // ziglint-ignore: Z026 -- best-effort write
}

fn fatal(comptime fmt: []const u8, args: anytype) noreturn {
    // Write directly to stderr without buffering so the message is not lost
    // when std.process.exit runs.
    var buf: [256]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "bench: ERROR: " ++ fmt ++ "\n", args) catch
        "bench: ERROR: message too long\n";
    _ = std.posix.write(2, msg) catch {}; // ziglint-ignore: Z026 -- best-effort write before exit
    std.process.exit(1);
}
