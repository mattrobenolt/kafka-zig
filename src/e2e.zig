//! E2e smoke binary (PLAN §5.3, phase 7): produces N messages through the
//! real kafka-zig Client against a live Kafka broker (SASL_SSL +
//! SCRAM-SHA-512 over TLS), then the justfile recipe consumes them back with
//! kafka-console-consumer.sh and asserts the count matches.
//!
//! This is a separate build step (`zig build e2e`), not part of the unit test
//! suite — it requires a running broker started by `just kafka-up`.
//!
//! Usage (single broker — local e2e):
//!   e2e --broker localhost:9093 --ca rootCA.pem --user kafka-zig \
//!       --pass <password> --topic e2e-events --num 20
//!
//! Usage (multi-broker HA — real MSK, exercises failover from #2):
//!   e2e --bootstrap b1:9096,b2:9096,b3:9096 --ca /path/to/ca.pem \
//!       --user <scram-user> --pass <password> --topic msk-e2e --num 50

const std = @import("std");
const kafka = @import("kafka");

pub fn main() !void {
    var args_iter = try std.process.argsWithAllocator(std.heap.page_allocator);
    defer args_iter.deinit();

    var broker: []const u8 = "";
    var bootstrap: []const u8 = "";
    var ca_path: []const u8 = "";
    var username: []const u8 = "kafka-zig";
    var password: []const u8 = "";
    var topic: []const u8 = "e2e-events";
    var num_msgs: u32 = 20;
    var compression: []const u8 = "none";

    // Cheap arg parser: --key value pairs.
    _ = args_iter.next(); // skip program name
    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--broker")) {
            broker = args_iter.next() orelse fatal("missing value for --broker", .{});
        } else if (std.mem.eql(u8, arg, "--bootstrap")) {
            bootstrap = args_iter.next() orelse fatal("missing value for --bootstrap", .{});
        } else if (std.mem.eql(u8, arg, "--ca")) {
            ca_path = args_iter.next() orelse fatal("missing value for --ca", .{});
        } else if (std.mem.eql(u8, arg, "--user")) {
            username = args_iter.next() orelse fatal("missing value for --user", .{});
        } else if (std.mem.eql(u8, arg, "--pass")) {
            password = args_iter.next() orelse fatal("missing value for --pass", .{});
        } else if (std.mem.eql(u8, arg, "--topic")) {
            topic = args_iter.next() orelse fatal("missing value for --topic", .{});
        } else if (std.mem.eql(u8, arg, "--num")) {
            const num_str = args_iter.next() orelse fatal("missing value for --num", .{});
            num_msgs = std.fmt.parseInt(u32, num_str, 10) catch
                fatal("invalid --num value: {s}", .{num_str});
        } else if (std.mem.eql(u8, arg, "--compression")) {
            compression = args_iter.next() orelse fatal("missing value for --compression", .{});
        } else {
            fatal("unknown argument: {s}", .{arg});
        }
    }

    if (ca_path.len == 0) fatal("--ca is required (path to CA PEM file)", .{});
    if (password.len == 0) fatal("--pass is required", .{});
    if (broker.len == 0 and bootstrap.len == 0)
        fatal("either --broker or --bootstrap is required", .{});
    if (broker.len > 0 and bootstrap.len > 0)
        fatal("--broker and --bootstrap are mutually exclusive", .{});

    const allocator = std.heap.page_allocator;

    // --- Load the CA into a Certificate.Bundle ---
    var ca_bundle: std.crypto.Certificate.Bundle = .{};
    defer ca_bundle.deinit(allocator);
    // Resolve the CA path to an absolute path (addCertsFromFilePathAbsolute
    // requires an absolute path).
    const abs_ca = try std.fs.cwd().realpathAlloc(allocator, ca_path);
    defer allocator.free(abs_ca);
    ca_bundle.addCertsFromFilePathAbsolute(allocator, abs_ca) catch |err|
        fatal("failed to load CA from {s}: {s}", .{ abs_ca, @errorName(err) });

    // --- Parse bootstrap brokers ---
    // --broker gives a single host:port; --bootstrap gives comma-separated
    // host:port pairs (for HA — all configured as bootstrap_brokers,
    // exercising the failover from #2). When multiple brokers are provided,
    // SNI is left null so each connection uses its own broker hostname.
    var brokers: std.ArrayList(kafka.Client.Broker) = .empty;
    defer brokers.deinit(allocator);

    if (bootstrap.len > 0) {
        var it = std.mem.splitScalar(u8, bootstrap, ',');
        while (it.next()) |entry| {
            const trimmed = std.mem.trim(u8, entry, " \t");
            if (trimmed.len == 0) continue;
            try brokers.append(allocator, try parseBroker(trimmed));
        }
        if (brokers.items.len == 0)
            fatal("--bootstrap must contain at least one host:port pair", .{});
    } else {
        try brokers.append(allocator, try parseBroker(broker));
    }

    // --- Init the Client ---
    // For a single broker, set SNI to that host (matches the local e2e).
    // For multiple brokers, leave SNI null so each connection uses its own
    // broker hostname — a single fixed SNI would be wrong for >1 broker.
    const sni: ?[]const u8 = if (brokers.items.len == 1) brokers.items[0].host else null;
    var client = try kafka.Client.init(allocator, .{
        .bootstrap_brokers = brokers.items,
        .tls = .{
            .sni = sni,
            .ca_bundle = &ca_bundle,
        },
        .sasl = .{ .scram_sha512 = .{
            .username = username,
            .password = password,
        } },
        .acks = .all,
        .compression = if (std.mem.eql(u8, compression, "zstd"))
            .zstd
        else if (std.mem.eql(u8, compression, "snappy"))
            .snappy
        else if (std.mem.eql(u8, compression, "none"))
            .none
        else
            fatal("--compression must be none|snappy|zstd, got: {s}", .{compression}),
        .linger_ms = 100,
        .max_message_size = 4 * 1024,
        .max_key_len = 64,
        .max_topic_len = 64,
        .ring_slots = 256,
        .partitioner = .default,
        .client_id = "kafka-zig-e2e",
    });
    defer client.deinit();

    // --- Produce N messages ---
    const handles = try allocator.alloc(kafka.Client.Message, num_msgs);
    defer allocator.free(handles);

    for (handles, 0..) |*m, i| {
        m.* = try client.acquire(.awaitable);
        try m.setTopic(topic);
        m.setPartition(null); // round-robin across partitions
        const dst = m.value();
        const payload = try std.fmt.bufPrint(dst, "msg-{d}", .{i});
        try m.commit(@intCast(payload.len));
    }

    // --- Await all acks ---
    for (handles, 0..) |*m, i| {
        m.await() catch |err| {
            fatal("message {d} failed: {s} (code={d})", .{ i, @errorName(err), m.failureCode() });
        };
    }

    stdout("e2e: produced {d} messages to {s}\n", .{ num_msgs, topic });
}

fn fatal(comptime fmt: []const u8, args: anytype) noreturn {
    stderr("e2e: ERROR: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

fn stdout(comptime fmt: []const u8, args: anytype) void {
    var buf: [256]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buf);
    defer writer.interface.flush() catch {}; // ziglint-ignore: Z026 -- best-effort flush on exit
    writer.interface.print(fmt, args) catch {}; // ziglint-ignore: Z026 -- best-effort write
}

fn stderr(comptime fmt: []const u8, args: anytype) void {
    var buf: [256]u8 = undefined;
    var writer = std.fs.File.stderr().writer(&buf);
    defer writer.interface.flush() catch {}; // ziglint-ignore: Z026 -- best-effort flush on exit
    writer.interface.print(fmt, args) catch {}; // ziglint-ignore: Z026 -- best-effort write
}

/// Parse a `host:port` string into a Broker. Aborts on malformed input.
fn parseBroker(entry: []const u8) !kafka.Client.Broker {
    const colon = std.mem.lastIndexOfScalar(u8, entry, ':') orelse
        fatal("broker must be host:port, got: {s}", .{entry});
    const host = entry[0..colon];
    const port = std.fmt.parseInt(u16, entry[colon + 1 ..], 10) catch
        fatal("invalid broker port: {s}", .{entry[colon + 1 ..]});
    return .{ .host = host, .port = port };
}
