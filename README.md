# kafka-zig

kafka-zig is a native Zig Kafka producer. It speaks the Kafka protocol directly
instead of wrapping librdkafka, and it does not bring a JVM with it. The useful
trade is a small, Zig-native surface with fixed-size submission storage; the
other side of that trade is a producer-only library whose API is still settling.

It targets Kafka 3.x and AWS MSK deployments using TLS 1.3 and SASL/SCRAM. One
network thread owns broker connections, metadata, batching, retries, and acks;
producer threads submit through a bounded MPSC ring.

## What is implemented

- TLS 1.3 through [ztls](https://github.com/mattrobenolt/ztls), with certificate
  and hostname verification.
- SASL/SCRAM-SHA-512 and SCRAM-SHA-256. AWS MSK uses SHA-512.
- Idempotent production by default: PID/epoch acquisition, per-partition
  sequence numbers, and sequence reuse when a batch is retried.
- Plain, Snappy, and zstd record batches. Snappy is an in-tree Zig codec with a
  hash-table match finder and SIMD decode paths. zstd statically links libzstd
  when built with `-Dzstd=true`.
- Multiple bootstrap endpoints, metadata refresh, leader reconnection, and
  in-place retry for retriable broker errors.
- Sticky, Kafka-compatible key-hash, and round-robin partitioning.
- Awaitable and fire-and-forget submission, bounded backpressure, batch-size
  limits, graceful drain on shutdown, and pull-based producer statistics.
- Sans-I/O wire codecs with caller-owned buffers. The tested steady-state
  produce path does not allocate from the client's general allocator after
  connection and metadata setup.

## Quick start

`ca_bundle` below is a `std.crypto.Certificate.Bundle` loaded with the CA that
signed the broker certificates. It must outlive the client.

```zig
const kafka = @import("kafka");

var client = try kafka.Client.init(allocator, .{
    .bootstrap_brokers = &.{
        .{ .host = "broker-1.example.com", .port = 9096 },
        .{ .host = "broker-2.example.com", .port = 9096 },
    },
    .tls = .{ .ca_bundle = &ca_bundle },
    .sasl = .{ .scram_sha512 = .{
        .username = "producer",
        .password = "secret",
    } },
    .acks = .all,
    .compression = .snappy,
});
defer client.deinit();

var message = try client.acquire(.awaitable);
try message.setTopic("events");
try message.setKey("entity-42");
message.setPartition(null); // let the partitioner choose
try message.writeMessage(payload);
try message.await();
```

`acquire(.awaitable)` blocks when the ring is full and returns a handle that
must be awaited exactly once. For a sink that only needs aggregate outcomes,
use `acquire(.fire_and_forget)` and inspect `client.stats()` instead. Do not
await a fire-and-forget message.

The [usage guide](docs/USAGE.md) covers configuration, non-blocking acquire,
fire-and-forget mode, error handling, compression, metrics, idempotency, and
the shutdown contract. The public API starts in [`src/Client.zig`](src/Client.zig).
Run `just docs` for Zig's HTML API reference at `zig-out/docs/index.html`.

## Build and test

The project uses Zig 0.15.2. `nix develop` provides the pinned compiler and the
full toolchain, including static libzstd, ziglint, Kafka, and the e2e scripts'
certificate tools.

```sh
nix develop
just build          # default build, without libzstd
just test           # unit and in-process broker tests
just test-zstd      # same tests with zstd enabled
just lint           # ziglint and GitHub Actions audit
just fmt-check
just docs
```

`just e2e`, `just e2e-snappy`, and `just e2e-zstd` start a local Kafka broker
with SASL/SCRAM-SHA-512 over TLS, produce records through kafka-zig, consume
them with Kafka's console consumer, and compare the count. `just msk-e2e` is a
manual test for a reachable AWS MSK cluster; see the script header for the
required environment variables.

## Status

This is an early release. The target producer path is implemented and covered
by unit tests, an in-process TLS/SCRAM broker, and local Kafka e2e scripts. The
project has also used two-model review during development. It has not carried
production traffic yet, and the public API is stabilizing rather than frozen.
Test it against your cluster and workload before making it important.

## Deliberate omissions

There is no consumer, consumer-group client, transaction API, Schema Registry
integration, or async I/O backend. gzip and lz4 are not implemented; their wire
constants exist, but selecting either is rejected during client initialization.
If those codecs are added, they should use established libraries rather than a
new compressor written for this repository.

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow and review
expectations. kafka-zig is licensed under the [Apache License 2.0](LICENSE).
