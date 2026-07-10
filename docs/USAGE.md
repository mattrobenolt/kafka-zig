# kafka-zig — Usage Guide

A native Zig Kafka **producer** library. This guide covers the full public
API, the caller contracts, and the operational behavior you need to know
before running it against a real cluster.

For the generated API reference (every type, field, and function with
doc comments), run `just docs` and open `zig-out/docs/index.html`.

---

## Quick start

```zig
const kafka = @import("kafka");

var client = try kafka.Client.init(allocator, .{
    .bootstrap_brokers = &.{
        .{ .host = "broker-1.example.com", .port = 9096 },
        .{ .host = "broker-2.example.com", .port = 9096 },
    },
    .tls = .{ .ca_bundle = &ca }, // each connection uses its broker host as SNI
    .sasl = .{ .scram_sha512 = .{ .username = "user", .password = "pass" } },
    .acks = .all,
    .compression = .zstd,    // requires -Dzstd=true
});
defer client.deinit();

var m = try client.acquire(.awaitable); // blocks when the ring is full
try m.setTopic("events");
try m.setKey("entity-42");
try m.writeMessage(payload);             // copies into the slot + commits
try m.await();                           // blocks until the broker acks
```

The CA bundle (`std.crypto.Certificate.Bundle`) is borrowed for the client's
lifetime. Everything else (bootstrap hosts, SNI, credentials, client_id) is
copied into client-owned storage at `init` — you can free the config slices
the moment `init` returns.

---

## The produce flow: acquire → setTopic → setKey → writeMessage → await

The API is **slot-first**. You acquire a message handle, write directly into
the slot's inline buffers, commit it to the network thread, then await the
broker ack. The ring owns the payload — no borrowed-buffer lifetime to
manage.

### 1. Acquire a slot

Choose the completion mode when acquiring:

```zig
var m = try client.acquire(.awaitable); // per-message completion handle
```

`acquire` blocks on a futex when all ring slots are occupied. In `.awaitable`
mode it can also wait for a completion handle. The ring is fixed-size, so a
full ring stalls producers until the network thread reclaims completed slots.

For a non-blocking variant:

```zig
var m = client.tryAcquire(.awaitable) catch |err| switch (err) {
    error.WouldBlock => return, // ring or completion-handle pool is full
    else => return err,
};
```

Use `.fire_and_forget` when no caller will await the individual result. That
mode allocates no completion handle; outcomes are available only through
`client.stats()`. See [Fire-and-forget](#fire-and-forget).

### 2. Set topic, key, partition

```zig
try m.setTopic("events");        // max_topic_len (default 128)
try m.setKey("entity-42");       // max_key_len (default 256); optional
m.setPartition(null);            // null → partitioner decides at batch time
```

`setPartition(null)` means "let the partitioner choose." The partitioner runs
at batch time (on the network thread), not at commit time, so the partition
selection sees the current metadata and the current sticky/round-robin state.

You can also pin a partition explicitly:

```zig
m.setPartition(3);               // force partition 3
```

### 3. Write the message and commit

Two equivalent ways:

```zig
// Option A: writeMessage (common case — copy + commit in one call)
try m.writeMessage(payload);

// Option B: value() + commit (when you want to write incrementally)
const dst = m.value();           // []u8, length = max_message_size
@memcpy(dst[0..payload.len], payload);
try m.commit(@intCast(payload.len));
```

After `commit` (or `writeMessage`), the slot and its buffers belong to the
network thread. The `Message` handle is now purely an await token — do not
touch `value()`, `setTopic()`, etc. after committing.

### 4. Await the broker ack

```zig
try m.await();                   // blocks until the broker acks
```

`await` blocks until the broker acknowledges the message (or permanently
fails it). On success it returns `void`. On a non-retriable broker error it
returns `error.SendFailed`; use `m.failureCode()` to inspect the Kafka error
code. If the client is torn down first, it returns `error.Shutdown`.

### The full Message API

| Method | Description |
|---|---|
| `setTopic(name)` | Set the topic (bounded by `max_topic_len`). |
| `setKey(key)` | Set the message key (bounded by `max_key_len`). Optional. |
| `setPartition(partition)` | `null` = partitioner decides; `u32` = pinned. |
| `setTimestamp(ms)` | Set the timestamp (milliseconds since epoch). Optional. |
| `value()` | The slot's inline value buffer (`[]u8`). Write here, then `commit`. |
| `writeMessage(payload)` | Copy `payload` into the slot + commit in one call. |
| `commit(value_len)` | Publish the message to the network thread. |
| `await()` | Block until the broker acks or fails. **Single-use.** |
| `tryAwait()` | Non-blocking `await`; `error.WouldBlock` if still in flight. |
| `failureCode()` | The Kafka error code after `await` returned `SendFailed`. |

`await()` and `tryAwait()` are valid only for messages acquired with
`.awaitable`.

---

## Configuration reference

All config lives in `Client.Config`. Required fields have no defaults; the
rest use the values shown.

### Required fields

| Field | Type | Description |
|---|---|---|
| `bootstrap_brokers` | `[]const Broker` | One or more seed brokers. Each is `{ .host, .port }`. The client connects to these to discover the full cluster via Metadata, then connects directly to partition leaders. |
| `tls` | `Tls` | TLS configuration (SNI, CA bundle, insecure flag). See below. |
| `sasl` | `Sasl` | SASL mechanism + credentials. `.scram_sha256` or `.scram_sha512`. MSK is SHA-512. |

### Optional fields (with defaults)

| Field | Type | Default | Description |
|---|---|---|---|
| `acks` | `Acks` | `.all` | Ack semantics: `.none` (fire-and-forget), `.leader` (leader-only), `.all` (all ISR — default). |
| `linger_ms` | `u32` | `5` | After the first pending record, wait up to this long for more records before draining, to form larger batches. 0 = eager. |
| `retry_backoff_ms` | `u32` | `5` | Backoff between drains that made no forward progress (retries / transient failures). Prevents CPU spinning against an unhealthy cluster. |
| `metadata_max_age_ms` | `u32` | `300_000` | Proactive metadata refresh interval (5 min, matching Kafka's `metadata.max.age.ms`). 0 = only refresh on error. |
| `max_batch_bytes` | `u32` | `16384` | Maximum bytes per record batch sent to a broker. |
| `max_message_size` | `u32` | `16384` | Inline value buffer per slot. Payloads exceeding this are rejected with `error.MessageTooLarge`. |
| `max_key_len` | `u16` | `256` | Maximum key length per slot. |
| `max_topic_len` | `u8` | `128` | Maximum topic name length per slot. |
| `ring_slots` | `u32` | `8192` | Ring slot count (rounded up to a power of two). Reserved memory ≈ `ring_slots × (max_topic_len + max_key_len + max_message_size)`. This is the sole backpressure bound. |
| `partitioner` | `Strategy` | `.default` | Partition selection: `.default` (sticky keyless + key hash), `.round_robin`, `.key_hash`. |
| `client_id` | `?[]const u8` | `null` | Client ID sent in request headers. |
| `request_timeout_ms` | `i32` | `30_000` | Produce request timeout sent to the broker. |
| `io_timeout_ms` | `u32` | `30_000` | Socket I/O timeout (SO_RCVTIMEO + SO_SNDTIMEO). A stalled broker hits this and triggers reconnect. |
| `max_retries` | `u8` | `8` | Maximum retry attempts per message on retriable errors. After exhaustion, the message fails with `error.SendFailed`. |
| `compression` | `Compression` | `.none` | Record-batch compression: `.none`, `.snappy`, `.zstd`. `.zstd` requires building with `-Dzstd=true`. |
| `enable_idempotency` | `bool` | `true` | Enable the idempotent producer (PID/epoch/sequence). See [Idempotent producer](#idempotent-producer). |
| `drain_timeout_ms` | `u32` | `10_000` | Graceful shutdown drain timeout. `deinit` waits this long for in-flight acks before forcing shutdown. |

### TLS (`Tls`)

| Field | Type | Default | Description |
|---|---|---|---|
| `sni` | `?[]const u8` | `null` | SNI hostname for every connection. When `null`, each connection uses its own broker host as SNI. |
| `ca_bundle` | `?*const Certificate.Bundle` | `null` | CA bundle for chain verification. **Caller-owned; must outlive the client.** |
| `insecure_skip_verify` | `bool` | `false` | Skip chain-anchor verification (still verifies CertificateVerify + hostname). Dev/test only. |

### SASL (`Sasl`)

```zig
.sasl = .{ .scram_sha512 = .{ .username = "...", .password = "..." } }
```

SCRAM-SHA-512 is the MSK default. SHA-256 is also supported by the `scram`
module. Credentials are copied into client-owned storage at `init` and
zeroized with `secureZero` at `deinit`.

### Acks (`Acks`)

| Value | Meaning |
|---|---|
| `.none` (0) | Fire-and-forget. No response from the broker. `await` returns as soon as the request is on the wire. |
| `.leader` (1) | The leader acks after writing to its local log. |
| `.all` (-1) | The leader acks after all in-sync replicas have the record. **Default.** |

### Partitioner (`Strategy`)

| Value | Behavior |
|---|---|
| `.default` | KIP-794 modern Java default: hash the key when present, sticky-partition keyless records (one partition per topic per drain, rotating between drains). |
| `.round_robin` | Always round-robin, ignoring the key. |
| `.key_hash` | Hash the key when present; round-robin when null (pre-sticky Java behavior). |

---

## Compression

Compression is per record batch. Kafka stores the compressed batch verbatim
on disk and replicates it compressed; consumers decompress. The codec
interface is a single attribute in the record batch.

| Mode | Value | Build flag | Notes |
|---|---|---|---|
| `.none` | 0 | — | Plaintext, always available. Default. |
| `.snappy` | 2 | — | Pure-Zig, always available. No build flag needed. |
| `.zstd` | 4 | `-Dzstd=true` | Statically links libzstd. Rejected at `init` with `error.CompressionUnavailable` if built without the flag. |
| `.gzip` | 1 | — | **Not yet implemented.** Wire-format constant only; returns `error.CompressionNotImplemented` at runtime. |
| `.lz4` | 3 | — | **Not yet implemented.** Wire-format constant only; returns `error.CompressionNotImplemented` at runtime. |

```zig
.compression = .snappy,   // no build flag
.compression = .zstd,     // build with: zig build -Dzstd=true
```

Compression runs on the records region of the batch before finalizing
CRC32C and batch length — the CRC covers attributes through end, so the
ordering is non-negotiable.

gzip and lz4 are not yet implemented. The enum values exist as wire-format
constants (they match the Kafka attributes field compression bits), but
using them returns `error.CompressionNotImplemented` at runtime. They need
real implementations (link a C lib or do a proven port) rather than
"valid but doesn't compress" placeholders. Plaintext + snappy + zstd are
the supported codecs.

---

## Backpressure and lifetime contract

The ring is a fixed-size circular buffer of slots. Each slot reserves payload
storage (`max_topic_len + max_key_len + max_message_size`) at initialization.
The slot count is the payload backpressure bound:

- **When the ring is full**, `acquire` blocks (futex) until the network
  thread reclaims an acked/failed slot. This is natural backpressure — no
  unbounded buffering, no OOM under load.
- **Reserved payload memory** at init is approximately `ring_slots ×
  (max_topic_len + max_key_len + max_message_size)`, plus slot and completion
  metadata. With defaults, the payload arena alone is about 130 MiB. Tune
  `ring_slots` and the size fields to match your workload and memory budget.
- **The only copy** on the produce path is slot payload → record-batch
  buffer, in the network thread (unavoidable under TLS + Kafka's v2 record
  format). The producer writes directly into the ring slot — no intermediate
  copies.

After `commit` (or `writeMessage`), the slot belongs to the network thread.
Do not touch the slot's buffers after committing. The `Message` handle is
now just an await token.

---

## Error semantics

### Ring errors (`Ring.Error`)

| Error | When |
|---|---|
| `MessageTooLarge` | Payload, topic, or key exceeds the configured maximum. |
| `WouldBlock` | `tryAcquire` / `tryAwait` would have blocked. |
| `Shutdown` | The ring was shut down while blocked (or before it began). |
| `SendFailed` | The send failed permanently: a non-retriable broker error, an oversized batch, or retry-budget exhaustion. |

### SendFailed and failure codes

When `await` returns `error.SendFailed`, call `m.failureCode()` to get the
Kafka error code (an unsigned integer matching the broker's error code
enum). This is valid after `await` has completed — it reads a cached value,
not the (possibly recycled) handle.

```zig
try m.await() catch |err| switch (err) {
    error.SendFailed => {
        const code = m.failureCode();
        // code is the Kafka error code (e.g. 10 = MESSAGE_TOO_LARGE)
    },
    error.Shutdown => { ... },
    else => return err,
};
```

### Retriable vs fatal

The network thread retries retriable errors in-place (up to `max_retries`).
Retriable codes (per the
[Kafka protocol error codes](https://kafka.apache.org/protocol.html#protocol_error_codes)):

| Code | Name | Also invalidates metadata? |
|---|---|---|
| 3 | UNKNOWN_TOPIC_OR_PARTITION | Yes |
| 5 | LEADER_NOT_AVAILABLE | Yes |
| 6 | NOT_LEADER_OR_FOLLOWER | Yes |
| 9 | REPLICA_NOT_AVAILABLE | Yes |
| 7 | REQUEST_TIMED_OUT | No |
| 13 | NETWORK_EXCEPTION | No |
| 14 | COORDINATOR_LOAD_IN_PROGRESS | No |
| 15 | COORDINATOR_NOT_AVAILABLE | No |
| 19 | NOT_ENOUGH_REPLICAS | No |
| 20 | NOT_ENOUGH_REPLICAS_AFTER_APPEND | No |
| 56 | KAFKA_STORAGE_ERROR | No |
| 75 | OFFSET_NOT_AVAILABLE | No |

Leadership errors (3, 5, 6, 9) also invalidate the metadata cache, forcing
a refresh before the next send. Connection drops during a Produce send are
retried with exponential backoff (100ms base, 10s cap, 50% jitter).

All other error codes are terminal and fail the message with `SendFailed`
without retry.

---

## HA / bootstrap behavior

The client takes one or more bootstrap brokers, connects to them to fetch
Metadata (key 3, v12), and discovers the full cluster topology: all brokers,
all topics, partition → leader mappings, leader epochs, and topic IDs. It
then connects directly to each partition's leader for Produce requests.

- **Metadata refresh**: periodically (every `metadata_max_age_ms`) and on
  demand (when a leadership error invalidates the cache). This catches
  silent leader changes, new brokers, and new partitions.
- **Reconnect backoff**: per host:port, exponential (100ms base, 10s cap,
  50% jitter). A successful connection clears the backoff state.
- **Failover**: if a partition leader becomes unavailable, the next Produce
  gets a leadership error, metadata is refreshed, and the message is retried
  against the new leader.
- **Bootstrap redundancy**: provide multiple bootstrap brokers for HA. The
  client tries them in order; any live broker can serve the initial
  Metadata request.

---

## The `await` single-use contract

A message acquired with `.awaitable` must complete exactly one `await` (or a
`tryAwait` that does not return `error.WouldBlock`). A double-await is a caller
contract violation that would double-free the handle pool. In
Debug/ReleaseSafe, the `handle_nil` sentinel catches it with an assertion. In
ReleaseFast, it is undefined behavior.

After `await` returns (success or failure), the handle is freed back to the
pool. `failureCode()` remains valid because it reads the value cached into the
`Message` token before the handle was freed. Dropping an awaitable message
without awaiting it leaks its completion handle until client teardown.

## Fire-and-forget

Acquire with `.fire_and_forget` when the caller does not need an individual
result:

```zig
var m = try client.acquire(.fire_and_forget);
try m.setTopic("events");
try m.writeMessage(payload);
// Do not call await or tryAwait on m.
```

This mode consumes no completion handle. The network thread still sends and
reclaims the slot, including when `acks` is `.leader` or `.all`; success and
failure are visible only in `client.stats()`. `tryAcquire(.fire_and_forget)` is
the non-blocking load-shedding form and returns `error.WouldBlock` only when the
ring itself is full.

`acks = .none` is a separate wire-level choice: Kafka sends no Produce
response. It can be used with either acquire mode. An awaitable message then
completes after the request has been written.

---

## Graceful shutdown

`client.deinit()` runs a two-phase graceful drain:

1. **Phase 1 (drain)**: stop accepting new acquires (`requestDrain`). The
   network thread finishes in-flight Produce round-trips up to
   `drain_timeout_ms`, then exits.
2. **Phase 2 (shutdown)**: wake any remaining awaiters with `error.Shutdown`
   (slots still pending after the drain timeout), close connections, free
   everything.

**Contract**: await (or drop) every outstanding `Message` **before** calling
`deinit()`. `deinit` is not safe to call concurrently with `await`/`acquire`
on the same client — phase 2 frees the ring, and a concurrent `await` would
use it after free. This matches real Kafka clients: librdkafka's `flush`
before `destroy`, the Java client's `close()` blocking in-flight sends.

In practice:

```zig
// Await all your messages first...
try m1.await();
try m2.await();

// Then shut down.
client.deinit();
```

The drain timeout bounds the total drain, checked between blocking I/O
operations — not mid-syscall. A single in-flight Produce response read may
overshoot by up to `io_timeout_ms` (the socket read timeout).

---

## Metrics

`client.stats()` returns a `Stats` snapshot — a value struct, no locks, no
allocation. All values are relaxed-consistency (torn/stale reads are
acceptable; it's metrics, not accounting).

```zig
const s = client.stats();
// s.queue_depth       — pending slots (committed, not yet reclaimed)
// s.in_flight         — currently equals queue_depth
// s.messages_produced — total messages committed by producer threads
// s.bytes_produced    — total bytes committed by producer threads
// s.batches_sent      — total Produce requests sent to brokers
// s.messages_acked    — total messages acked by the broker
// s.messages_failed   — total messages permanently failed
// s.errors.retriable  — retriable error encounters (per slot per attempt)
// s.errors.fatal      — permanent failures (including retry exhaustion)
// s.errors.connection_drops — connection drops during Produce sends
```

The producer-thread counters (`messages_produced`, `bytes_produced`) are
atomic (multi-threaded). The network-thread counters are read with
`.monotonic` loads (single-writer, relaxed cross-thread reads).

---

## Idempotent producer

When `enable_idempotency = true` (the default), the producer:

1. Acquires a **PID (producer ID) and epoch** from the broker via
   InitProducerId v4 at startup. If acquisition fails, Produce is held back
   while acquisition is retried; pending messages eventually fail when their
   retry budgets are exhausted rather than being silently sent without a PID.
2. Tracks **per-(topic, partition) sequence numbers**. Each batch's
   `base_sequence` is assigned from a per-partition counter. On retry, the
   slot's stored `base_sequence` is reused — the broker dedupes by sequence,
   so a retried batch carries the same sequence as the original.
3. Sends real `producer_id` / `producer_epoch` / `base_sequence` in record
   batches.

### Error handling with idempotency

Three error codes are handled specially when a real PID is in use:

| Code | Name | Handling |
|---|---|---|
| 37 | DUPLICATE_SEQUENCE_NUMBER | The broker already appended this exact (PID, epoch, sequence) — the first attempt's write is durable and only the ack was lost. **Treated as an ACK, not a failure.** (KIP-98.) |
| 45 | OUT_OF_ORDER_SEQUENCE | The broker's sequence state is unrecoverable (a gap it can't reconcile). Re-init the PID (fresh epoch), reset all per-partition sequences to 0, and re-send. (KIP-360.) |
| 73 | UNKNOWN_PRODUCER_ID | The broker dropped the producer state (segment deletion / retention / DeleteRecords). Same recovery as OUT_OF_ORDER_SEQUENCE: fresh PID, reset sequences, re-send. (KIP-360.) |

The PID reset is the simplest globally-correct recovery for a
non-transactional idempotent producer that doesn't track last-acked
sequences per partition: a fresh PID makes the broker forget all prior
sequence state, so restarting every partition at 0 can never gap or
duplicate.

When `enable_idempotency = false`, the producer uses sentinel -1 for
producer_id/epoch and -1 for base_sequence (non-idempotent mode). No
InitProducerId round-trip or sequence tracking is performed.

Sequence state is stored in an allocator-backed per-partition map. The current
implementation falls back to a non-idempotent batch if growing that map returns
`OutOfMemory`. Treat allocator exhaustion as loss of the idempotency guarantee;
this is a known limitation, not a useful operating mode.

---

## The `scram` module

The SCRAM-SHA-256/512 client is exposed as a standalone build module
(`scram`), importable independently of the Kafka client. It implements
RFC 5802 + RFC 7677 and imports only `std`.

```zig
const scram = @import("scram");

// SCRAM-SHA-512 (MSK default)
const Client = scram.ScramSha512;
const client_first = try Client.generateClientFirstMessage("username");
// ... send client_first.bytes() to the broker ...
const parsed = try Client.parseServerFirst(server_first_bytes, &client_first);
const client_final = try Client.processServerFirstWithPassword(
    "password", &client_first, &parsed,
);
// ... send client_final.bytes() to the broker ...
try Client.verifyServerFinal(server_final_bytes, &client_final.server_signature);
```

The module also exports `scram.Scram` (a generic constructor), `scram.ScramSha256`,
`scram.ScramSha512`, `scram.Pbkdf2`, and `scram.Error`.

---

## The `wire` module

The sans-I/O protocol codec is exposed as `kafka.wire`. It contains the
per-API request/response encode/decode functions, the response framing
buffer (`ResponseBuffer`), the record batch encoder/decoder, and the
compression codec interface. This is the low-level protocol layer — most
users won't need it directly, but it's public for testing and for building
custom protocol paths.

No heap allocation on the encode/decode hot path. Decode paths for nested
response data take an `Allocator` (cold-path, allowed).

---

## Building

```sh
zig build                    # build the library + CLI
zig build -Dzstd=true        # build with zstd compression support
just docs                    # generate API docs into zig-out/docs/
just test                    # run tests
just test-zstd               # run tests with zstd enabled
```

The zstd build flag statically links libzstd and enables `.zstd`
compression. Without it, requesting `.zstd` at `init` fails with
`error.CompressionUnavailable`.
