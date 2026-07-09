# kafka-zig

A native Zig Kafka **producer** library. Pre-alpha — the API will move.

Targets modern Kafka 3.x / AWS MSK. TLS 1.3 via
[ztls](https://github.com/mattrobenolt/ztls). SASL SCRAM-SHA-512 over TLS
(port 9096 on MSK; SHA-256 also supported by the `scram` module). Plaintext
batches and optional zstd compression (`-Dzstd=true`). Sans-I/O wire
protocol, no heap allocation on the encode/decode hot path.

## Status

What works: producing against modern Kafka / MSK with sticky (default) /
key-hash / round-robin partitioning, TLS 1.3, SCRAM-SHA-512, plaintext,
snappy, and zstd record batches, bounded in-flight backpressure, in-place
retry on retriable errors, idempotent producer (PID/epoch/sequence), and
pull-based metrics.

Not in scope: consumer, consumer groups, Fetch, transactions,
gzip/lz4, Schema Registry, async I/O. See
[`PLAN.md`](PLAN.md) for the full design and roadmap.

## Documentation

- **[USAGE guide](docs/USAGE.md)** — the full public API, config reference,
  error semantics, backpressure/lifetime contract, idempotent producer,
  metrics, and graceful shutdown. Read this to start producing.
- **API reference (autodoc)** — run `just docs` to generate HTML API docs
  into `zig-out/docs/`; open `zig-out/docs/index.html`.
- **[PLAN.md](PLAN.md)** — design document, phased acceptance ladder, and
  open questions.

## Usage

Slot-first API: acquire a message, write directly into the slot's inline
buffers, commit, then await the broker ack. The ring owns the payload — no
borrowed-buffer lifetime to manage.

```zig
const kafka = @import("kafka");

var client: kafka.Client = try .init(allocator, .{
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
try m.writeMessage(payload);     // copies into the slot + commits to the network thread
try m.await();                   // blocks until the broker acks
```

The only copy on the produce path is slot payload → record-batch buffer, in
the network thread (unavoidable under TLS + Kafka's v2 record format).

Shutdown contract: await (or drop) every outstanding `Message` **before**
calling `client.deinit()`. `deinit` runs a graceful drain (finishing in-flight
acks up to `drain_timeout_ms`) and then frees the ring, so it must not run
concurrently with `await`/`acquire` on that client — the same discipline
librdkafka (`flush` before `destroy`) and the Java client (`close()` blocks
in-flight sends) require.

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
just e2e-snappy  # same, with snappy-compressed batches
just msk-e2e     # run against a real AWS MSK cluster (requires VPC access + creds)
just docs        # generate Zig API docs into zig-out/docs/
just fmt-check   # check formatting
just lint        # run ziglint (needs the devshell)
```

## Testing against real MSK

The local e2e (`just e2e`) runs against a self-hosted KRaft broker with
mkcert certs. To test against a real AWS MSK cluster, use `just msk-e2e`.
This is a **manual** test — it requires VPC access to the MSK brokers and
real SCRAM credentials, so it is not part of the default CI gate.

### Prerequisites

- **VPC access:** MSK brokers are VPC-internal. Run from a machine or pod
  inside the VPC (e.g. an EC2 instance or k8s pod in the same VPC as the
  cluster).
- **SCRAM credentials:** Create SCRAM-SHA-512 credentials via AWS Secrets
  Manager and associate them with the MSK cluster (see the [AWS MSK SCRAM
docs](https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html)).
  MSK supports SCRAM-SHA-512 only.
- **CA bundle:** MSK broker TLS certs are signed by AWS's CA. The system trust
  store on most Linux images already includes the AWS CA. If not, export the
  CA PEM (e.g. from the AWS ACM cert or the broker's cert chain) and pass it
  via `MSK_CA`. If the system trust store has the AWS CA, you can still pass
  it explicitly for determinism.
- **Bootstrap endpoints:** Get all 3 bootstrap endpoints from the MSK
  console or CLI (`aws kafka get-bootstrap-brokers`). Pass them
  comma-separated via `MSK_BOOTSTRAP` — the e2e binary configures all of them
  as `bootstrap_brokers`, exercising the HA failover path.
- **Topic:** Create a test topic on the MSK cluster (auto-create is usually
  disabled). The default topic name is `msk-e2e`.

### Running

```sh
MSK_BOOTSTRAP="b1-xxx:9096,b2-xxx:9096,b3-xxx:9096" \
  MSK_CA=/path/to/aws-ca.pem \
  MSK_USER=alice MSK_PASS='...' \
  just msk-e2e
```

Optional variables:

- `MSK_TOPIC` — topic name (default: `msk-e2e`)
- `MSK_NUM` — number of messages (default: `50`)
- `MSK_COMPRESSION` — `none`|`snappy`|`zstd` (default: `none`; `zstd`
  requires a build with `-Dzstd=true`)

The recipe produces N messages through kafka-zig (TLS 1.3 + SCRAM-SHA-512),
then consumes them back with `kafka-console-consumer.sh` and asserts the count
matches.

### MSK-specific notes for Matt to verify

- **Hostname verification:** The recipe blanks
  `ssl.endpoint.identification.algorithm` for the consumer, matching the local
  e2e. If the MSK broker certs have the bootstrap DNS names in their SANs,
  remove this override to enable hostname verification (more secure). Check
  with `openssl s_client -connect <broker>:9096 | openssl x509 -text | grep -A1 'Subject Alternative Name'`.
- **CA path:** If the MSK certs are signed by the AWS root CA that's already
  in the system trust store, `MSK_CA` can point to the system bundle (e.g.
  `/etc/ssl/certs/ca-bundle.crt`). Verify the chain with `openssl verify -CAfile
  $MSK_CA <broker-cert.pem>`.
- **Port:** MSK's SASL/SCRAM-over-TLS port is **9096** (not 9093, which is
  mTLS client-cert). PLAN §9 confirmed this.

## Roadmap

See [`PLAN.md`](PLAN.md) for the design, phased acceptance ladder, and open
questions.

## License

Apache License 2.0; see [LICENSE](LICENSE).
