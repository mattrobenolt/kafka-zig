# kafka-zig

A native Zig Kafka **producer** library. Pre-alpha — the API will move.

Targets modern Kafka 3.x / AWS MSK. TLS 1.3 via
[ztls](https://github.com/mattrobenolt/ztls). SASL SCRAM-SHA-512 over TLS
(port 9096 on MSK; SHA-256 also supported by the `scram` module). Plaintext
batches and optional zstd compression (`-Dzstd=true`). Sans-I/O wire
protocol, no heap allocation on the encode/decode hot path.

## Status

What works: producing against modern Kafka / MSK with round-robin and
key-hash partitioning, TLS 1.3, SCRAM-SHA-512, plaintext and zstd record
batches, bounded in-flight backpressure, in-place retry on retriable errors.

Not in scope: consumer, consumer groups, Fetch, transactions, idempotent
producer (PID/epoch), gzip/snappy/lz4, Schema Registry, async I/O. See
[`PLAN.md`](PLAN.md) for the full design and roadmap.

## Usage

Slot-first API: acquire a message, write directly into the slot's inline
buffers, commit, then await the broker ack. The ring owns the payload — no
borrowed-buffer lifetime to manage.

```zig
const kafka = @import("kafka");

var client = try kafka.Client.init(allocator, .{
    .bootstrap_brokers = &.{
        .{ .host = "broker-1", .port = 9096 },
    },
    .tls = .{ .ca_bundle = &ca, .sni = "broker-1" },
    .sasl = .{ .scram_sha512 = .{ .username = "...", .password = "..." } },
    .acks = .all,
    .max_message_size = 16 * 1024,
    .ring_slots = 8192, // reserved ≈ 8192 × 16KB ≈ 128MB
});
defer client.deinit();

var m = try client.acquire();   // blocks (futex) when the ring is full
try m.setTopic("events");
m.setPartition(null);           // null → partitioner decides at batch time
try m.setKey("entity-42");
const dst = m.value();           // []u8, len == max_message_size
@memcpy(dst[0..payload.len], payload);
try m.commit(payload.len);       // submits to the network thread
try m.await();                   // blocks until the broker acks
```

The only copy on the produce path is slot payload → record-batch buffer, in
the network thread (unavoidable under TLS + Kafka's v2 record format).

## Dev setup

Requires Zig 0.15.2 and the Nix flake devshell (`nix develop`) for the full
toolchain (zstd static lib, Kafka, mkcert, ziglint). ztls is pulled in
automatically as a git dependency — no sibling checkout needed.

## Build & test

```sh
just test        # run tests
just test-zstd   # run tests with zstd compression (needs the devshell's static libzstd)
just e2e         # start a local Kafka broker and run the end-to-end smoke (plaintext)
just e2e-zstd    # same, with zstd-compressed batches
just fmt-check   # check formatting
just lint        # run ziglint (needs the devshell)
```

## Roadmap

See [`PLAN.md`](PLAN.md) for the design, phased acceptance ladder, and open
questions.

## License

Apache License 2.0; see [LICENSE](LICENSE).
