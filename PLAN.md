# kafka-zig — native Kafka producer in Zig

A native Zig Kafka **producer** library targeting modern managed Kafka
(AWS MSK, Kafka 3.x+). Single-threaded network core, multi-threaded
submission, TLS 1.3 via ztls (see §9 — verify MSK TLS version), SCRAM-SHA-512
auth (SHA-256 also supported by the scram module; MSK is SHA-512 only). Sans-I/O wire protocol.

Consumer, consumer groups, transactions, and admin ops are explicitly out of
scope for v1.

---

## 1. Scope

**In (v1):**
- Produce to a topic, round-robin and key-hash partitioning.
- TLS 1.3 transport (ztls over a blocking `std.net.Stream`).
- SASL SCRAM-SHA-256 (and SHA-512) authentication.
- Bootstrap → metadata discovery → per-partition-leader connections.
- Batching, linger, `acks` configurable (default `all`), per-message ack
  callbacks/futures.
- Bounded in-flight backpressure (messages + bytes).
- Non-idempotent producer (no producer epoch / PID). Retry on retriable errors
  with leader-change metadata refresh.

**In (v1, compression):** plaintext (no compression) and zstd.
- zstd is **optional**, gated behind a build flag `-Dzstd=true` (default off).
  With it on, statically link libzstd and enable the zstd compression attribute
  on record batches. With it off, only plaintext batches are produced and the
  build links no extra libraries.
- Compression is per record batch (Kafka stores the compressed batch verbatim
  on disk and replicates it compressed; consumers decompress). Compress the
  records region of the batch in place **before** finalizing CRC32C and batch
  length — the CRC covers attributes→end, so ordering is non-negotiable.
- Rationale for linking libzstd rather than a pure-Zig codec: Zig 0.15.2
  `std.compress.zstd` ships **decompression only** (no `Compress`). No mature
  pure-Zig zstd compressor exists as of writing. The C reference is the sane
  choice for the compressor. We do **not** need libzstd for decompression in
  the producer path.
- gzip/snappy/lz4: out of v1. The codec interface in `record_batch.zig` is a
  single function slot, so adding them later is one codec, not a restructure.

**Out (v1):**
- Consumer, Fetch, consumer groups, FindCoordinator, OffsetFetch/Commit.
- Idempotent producer / EOS transactions (ProducerId/epoch).
- gzip/snappy/lz4 compression (zstd + plaintext only; see above).
- Schema registry, Avro/Protobuf wiring.
- Async I/O (epoll/io_uring). Single blocking TCP socket per broker, one
  network thread.

**Target API versions** (modern Kafka; pin these so the codec is bounded):
- `ApiVersions` key 18, v3 — negotiate at connect (optional but cheap).
- `Metadata` key 3, v12 — topic→partition→leader, leader epoch, topic id.
- `SaslHandshake` key 17, v1.
- `SaslAuthenticate` key 36, v1. (Note: it is key **36**, not 366 — a
corrected from review.)
- `Produce` key 0, v9 — v2 record batches, compression-attribute-capable
  (zstd attr value 4). No idempotence fields used. Ship plaintext first; zstd
  behind the build flag.

If a broker negotiates lower, fall back only if trivial; otherwise error. AWS
MSK supports all of the above.

---

## 2. Building blocks

### 2.1 `wire` — sans-I/O protocol framing (the core)

Pure request/response codec. No sockets, no allocators in the hot path, no
state beyond what's needed to parse. Modeled on ztls's `RecordBuffer`/`Outbox`
discipline: caller owns every buffer, engine is a set of pure functions and
small state machines.

Responsibilities:
- Frame parse: `i32` length-prefixed response bodies. `ResponseBuffer` turns a
  byte stream into whole response bodies, exactly like ztls's `RecordBuffer`
  (`writable()` / `advance(n)` / `next()` → complete body slice).
- Per-API encode/decode: one file per API key under `src/wire/api/`, each
  exporting `Request` and `Response` structs plus `encode(writer, req)` and
  `decode(reader) !Response`. Versioned: each struct carries the version it
  targets; encode/decode branch on version where the schema differs.
- Primitive readers/writers: `i8/u8/i16/u16/i32/u32/i64` big-endian,
  `varint/varlong` (zigzag), `string` (i16 len + utf8), `compact_string`,
  `bytes`, `compact_bytes`, `array<T>` (i32 count) and `compact_array`, nullable
  variants, `uuid` (topic id), `records` (the record-batch sub-format). These
  live in `src/wire/primitives.zig` and are the only place endianness/varint
  live.
- A `Reader` is a cursor over a `[]const u8` body (bounds-checked, no alloc).
  A `Writer` writes into a caller-provided `Io.Writer` (fixed buffer or
  growable). No heap.

**Zero-copy discipline:** `Response` structs hold **slices into the response
buffer**, not copies. The buffer is borrowed until the caller is done with the
response. Document the lifetime on every decode function, same way ztls does
for `application_data`. The metadata response's topic/partition arrays are
slices-into-buffer; the client copies only what it keeps past the response
lifetime. **Caveat (from review):** raw wire bytes can't be reinterpreted as a
`[]TopicInfo` of nested structs (alignment, variable-length fields). For nested
arrays of structs, either (a) cold-path allocate parsed structs into the
metadata cache, or (b) expose zero-alloc iterator decoders. Don't pretend raw
bytes are a typed slice. Metadata refresh is cold-path, so (a) is fine there.

**Flexible versions (KIP-482) — specify explicitly, this is a one-shot trap:**
The pinned APIs straddle the flexible boundary. ApiVersions v3, Metadata v12,
and Produce v9 are **flexible** (compact strings/arrays, unsigned-varint
lengths, request header v2, and a trailing **tagged-field buffer** on every
flexible struct). SaslHandshake v1 and SaslAuthenticate v1 are **NOT flexible**
(int16-length strings, request header v1, no tag buffer). The primitives module
must include `unsigned_varint`, compact-string/array length = N+1 (0 = null),
and an empty tag buffer (`0x00`) encoder/decoder. Add a
`header_version(api_key, version) -> {request, response}` table to
`api_keys.zig` — and call out the classic gotcha: the **ApiVersions response
uses header version 0 even at v3** (the broker parses it before knowing the
client's flexibility). Get that wrong and the connection desyncs right after
connect. Also: the Java client sends **ApiVersions v0** pre-auth (brokers
historically treat a schema exception in the first request as a GSSAPI token);
v3 is fine post-auth, but use v0 for the pre-auth probe as cheap insurance.

Tests: round-trip every API against captured byte vectors. Keep a
`testdata/` directory of real frames (hex) and assert exact bytes out / exact
structs in. This is the deterministic spine — if the codec round-trips real
Kafka bytes, everything above it is glue.

Reference for schemas: the Kafka protocol is documented at
https://kafka.apache.org/protocol.html (protocol + protocol messages). The
agent must read the spec for each API key/version listed above and implement
to the byte. Do not guess from memory; LLM training data for this protocol is
frequently wrong on versions and field order.

### 2.2 `scram` — standalone SCRAM client (separate module)

A self-contained SCRAM-SHA-256 **and** SCRAM-SHA-512 client (RFC 5802 +
RFC 7677 for SHA-256; SCRAM-SHA-512 is the same construction over SHA-512 —
Kafka supports it per the Kafka security docs, not an IANA-registered SCRAM
family mechanism; do **not** cite RFC 7804, which is HTTP SCRAM).
**No Kafka imports, no `wire` imports.** Structured so it can be lifted to its
own repo as a standalone Zig SCRAM module. Exposed as its own build module
(`scram`) in addition to being used internally.

Generalize the existing exosphere `src/http/scram.zig` + `pbkdf2.zig`:
- Mechanism enum: `{ scram_sha_256, scram_sha_512 }`, parameterized over
  `HmacSha256`/`Sha256` vs `HmacSha512`/`Sha512`. Same logic, two instances.
  Use a comptime-generic over the hash pair rather than duplicating.
- Keep the incremental PBKDF2 state machine (`Pbkdf2.step(n)`) — it's good and
  non-blocking-friendly even though our producer thread is blocking today.
- API: `generateClientFirstMessage()`, `parseServerFirst()`,
  `completeServerFirst()`, `verifyServerFinal()`. Same shape as today, minus
  the PostgreSQL `pg_writer` coupling. Output is bare SCRAM messages as
  `[]u8`/fixed arrays; the Kafka layer wraps them in `SaslAuthenticate` bodies.
- **Required behavior change from the precedent:** the exosphere
  `generateClientFirstMessage()` hardcodes `n,,n=,r=<nonce>` with an **empty**
  username because PostgreSQL takes the user from the startup packet and ignores
  the SCRAM `n=`. Kafka does NOT — the SCRAM `n=<username>` in client-first is
  the authenticating identity. So `generateClientFirstMessage(username)` must
  build `n,,n=<username>,r=<nonce>`, with the username SASLprep-normalized and
  `,`/`=` escaped to `=2C`/`=3D`. This is a functional change, not a mechanical
  port.
- Keep the GS2 header as `n,,` (no channel binding, no authzid): both commas,
  not `n,`. `c=biws` in client-final is base64 of `n,,`.

This module owns: nonce gen, PBKDF2 key derivation, proof/signature math,
message parse/build. It does **not** own: the SASL framing, the TCP, the TLS.
The Kafka connection layer calls into `scram` and shuttles its messages through
`wire`'s `SaslHandshake`/`SaslAuthenticate`.

Tests: unit tests for each step with RFC 5802 example vectors; fuzz the parsers
(keep the existing fuzz shape); verify incremental path == blocking path.

### 2.3 `ztls` — TLS 1.3 (path dependency)

Add `github.com/mattrobenolt/ztls` as a dependency. Prefer a path dep during
development (`../ztls`) and a git+hash dep for the shippable module. Drive the
ztls engine over a blocking `std.net.Stream` from the network thread. Per
`ztls/docs/USAGE.md` and `examples/tcp_loopback.zig`, the blocking loop is:
`RecordBuffer` for inbound framing (`writable`/`advance`/`next`), and for
outbound just `stream.writeAll(record)` then `hs.completeWrite()` — **not**
`Outbox`, which exists for partial-write/non-blocking transports. Keep driving
the **same `ClientHandshake` object** after `isConnected()` via
`sendApplicationData`/`handleRecord`; there is no separate `RecordLayer` to
transition into (it's internal). No epoll/io_uring.

### 2.4 `ring` — MPSC payload ring (port of AtomicRingBuffer)

Port `exosphere-zig/src/AtomicRingBuffer.zig` keeping its core shape: power-of-
two slot array, `acquireSlot()` via `fetchAdd` on `write_head`, per-slot
`written`/`status` atomic, futex `waitForData`/`notifyConsumer`, shutdown path.

The change is what a slot **is**: the slot owns the payload buffer, not a
descriptor of borrowed pointers. Producers acquire a slot and write their
message **directly into the slot** — zero-copy from the producer's perspective,
no borrowed-buffer lifetime contract. The one unavoidable copy (slot →
record-batch buffer) happens later in the network thread.

```zig
pub const Slot = struct {
    status: atomic.Value(u32),  // 0=free, 1=pending, 2=acked, 3=failed
    err: u32,                   // error code when status==failed
    topic_len: u8,
    key_len: u16,
    value_len: u32,
    partition: u32,             // assigned by partitioner at acquire or by caller
    timestamp_ms: i64,
    topic: [max_topic_len]u8,   // inline; topic names are short
    key: [max_key_len]u8,       // inline; keys are small (configurable)
    value: [max_message_size]u8,// inline payload buffer, the producer writes here
};
```

Sizing is explicit and up-front (caller configures at `Client.init`):
- `max_message_size` — value buffer per slot.
- `max_key_len` — small, default e.g. 256.
- `max_topic_len` — small, default e.g. 128.
- `ring_slots` — number of slots, power-of-two.

Reserved memory = `ring_slots × sizeof(Slot)` ≈ `ring_slots × (max_message_size
+ overhead)`. Document this loudly so callers size deliberately. Example:
8192 slots × 16KB ≈ 128MB. The slot count is the **sole** backpressure bound —
no separate in-flight-byte counter, because every slot already reserves
`max_message_size` whether filled or not, so slot count *is* the byte bound.

Lifecycle, the one real difference from the fire-and-forget original: the
network thread advances `read_head` (and thus reclaims the slot) only **after
the Produce response acks that slot**, not when it reads the bytes off the
ring. A slot is reused only once the broker has acked it. Document this on the
slot struct.

**Three things the original does NOT do that this port must add (called out by
review):**

1. **`acquire()` must block when the ring is full.** The original's
   `acquireSlot()` is an unconditional `fetchAdd(write_head)` with no full check
   — it relies on the consumer keeping up. With ack-before-reclaim, the network
   thread may hold `read_head` for a long time waiting on broker acks, so an
   unbounded `fetchAdd` will wrap and **overwrite a still-pending slot**.
   `acquire()` must block (futex) while `write_head - read_head >= num_slots`,.
   and the network thread must wake producers as it reclaims slots. This is the
   core backpressure mechanism — specify it, don't inherit it.
2. **Partial-ack reclamation.** `read_head` is a single sequential cursor, but a
   Produce request batches slots from **multiple partitions** and the response
   can ack some partitions while returning a retriable error for others. Acks
   arrive out of order per partition. Reclaiming strictly by `read_head` would
   head-of-line-block every later slot behind one slow/leaderless partition.
   Design: track a per-slot `status` done flag and reclaim via a separate scan
   (or a free-list) rather than a single monotonic `read_head`. `read_head` is
   only the low-water mark for the acquire-full check.
3. **Handle identity across retry.** Retry is **in-place**: the message
   stays in its slot (still `pending`, same generation, same handle binding),
   clears `err`, and re-notifies the consumer. The handle never moves, so
   `await()` trivially follows it. (An earlier draft proposed moving the
   message to a new tail slot with a stable indirection-table handle; that
   deadlocks when the ring is full and every slot needs re-enqueue — there's
   no free slot to claim until a retry succeeds, and no retry succeeds until a
   slot frees. In-place retry breaks the cycle with zero new resource and
   preserves the handle binding.) A consequence: the producer loop must not
   retry speculatively while an older Produce request for the same slot may
   still complete — retry only after a terminal retriable response or a
   reconnect.

**Completion signaling for `await()`:** a futex per slot is too expensive at
8K+ slots. Use a single (or few) global completion futex plus an acked-bitmap
(or a small ring of completed handle-ids) that the network thread pushes to and
producers poll/wait on. Specify the concrete mechanism, not 'futex or queue.'

**Generation counter:** to make slot reuse safe with the stable-handle model,
each slot carries a generation counter; the handle records `(slot_index,
generation)` and `await()` only observes a completion whose generation matches.
This prevents a recycled slot's later ack from being misattributed to an older
handle.

The `written` atomic from the original becomes `status` with the four states
above; `commit` stores `status=pending` with `.release`, the network thread
loads `.acquire`, and on ack stores `status=acked` (or `failed`) with
`.release` so the producer's `await()` observes it.

### 2.5 `producer` — the network thread + public API

One thread, spawned by `Client.init`, owns: all broker connections, the
metadata cache, the ring drain loop, batching, and ack signaling. No locks on
the hot path because only this thread touches connections and metadata.

Loop:
1. `ring.waitForData(read_head)` (or linger timeout, whichever first).
2. Pull a run of descriptors. Group by `topic-partition` → resolve leader from
   metadata cache. Build per-leader `Produce` requests: open batch buffer,
   encode record batch (v2 format), append records until batch size or linger
   hits.
3. Write each Produce request through that leader's connection (ztls+TCP).
4. Read ProduceResponse. For each partition: on success, mark those slots
   `acked` and advance `read_head`; reduce `in_flight_bytes`. On retriable
   error (NotLeader/LeaderNotAvailable/…), invalidate metadata for that
   partition, retry the slots **in place** (same slot, same generation, same
   handle; status stays `pending`, `err` cleared, consumer re-notifies — see
   §2.4 point 3), and trigger a metadata refresh. On non-retriable error,
   mark `failed`.
5. Wake the producer threads waiting on the affected handles (single global
   completion futex + acked-bitmap; see §2.4).
6. Periodically / on invalidation: send `Metadata`, refresh the cache.

Metadata cache: `[]TopicInfo` where `TopicInfo` has `[]PartitionInfo{ leader,
leader_epoch, isr }`. Borrowed slices from the metadata response buffer; copied
into the cache since the buffer is reused. This is one of the few allocations
and it's on the cold path (refresh cadence, not per-message).

Connections: `Connection` struct = `std.net.Stream` + the ztls `ClientHandshake`
engine (driven end-to-end, including post-handshake `sendApplicationData`/
`handleRecord`) + auth state machine. Auth flow:
connect TCP → ztls handshake (SNI = broker host) → `SaslHandshake(SCRAM-SHA-512
or -256)` → drive `scram` messages through `SaslAuthenticate` (client-first,
server-first, client-final, server-final verify) → ready. Keep one connection
per broker, reconnect on close.

Note on the TLS config field: ztls consumes a `std.crypto.Certificate.Bundle`,
not a raw PEM string; the config layer must build the bundle from the provided
CA PEM. ztls is pre-alpha, server-auth 1-RTT only, X25519/P-256 key exchange,
no HelloRetryRequest, no resumption. **Verify the broker actually completes a
TLS 1.3 handshake** (see §9 open question on MSK TLS version) before assuming
ztls works against the production target.

---

## 3. Public API surface

The API is **slot-first**: you acquire a slot, write directly into it, then
commit. The ring owns the payload, so there is no borrowed-buffer lifetime to
manage — once `commit` returns, the slot (including its buffers) is the
network thread's problem, not yours.

```zig
const kafka = @import("kafka");

var client: kafka.Client = try .init(allocator, .{
    .bootstrap_brokers = &.{ .{ .host = "broker-1", .port = 9096 }, ... }, // 9096 = SASL/SCRAM-over-TLS on MSK
    .tls = .{ .server_ca = pem, .sni = "broker-hostname" }, // ztls-backed
    .sasl = .{ .scram_sha512 = .{ .username = "...", .password = "..." } }, // MSK default
    .acks = .all,
    .linger_ms = 50,
    .max_batch_bytes = 16 * 1024,
    .max_message_size = 16 * 1024,
    .max_key_len = 256,
    .max_topic_len = 128,
    .ring_slots = 8192,           // reserved ≈ 8192 × 16KB ≈ 128MB
    .partitioner = .default,      // round-robin; .key_hash for keyed
});
defer client.deinit(); // joins network thread

// From any thread. acquire() blocks (futex) when the ring is full —
// that is the backpressure path.
var m = try client.acquire();
m.setTopic("events");
m.setPartition(null);          // null → partitioner decides at batch time
m.setKey("entity-42");         // copies into the slot's inline key buffer
const dst = m.value();         // []u8, length == max_message_size
@memcpy(dst[0..payload.len], payload);
m.commit(payload.len);         // submits; m is now a handle
try m.await();                  // blocks until broker ack; error on failure
// Slot has been reclaimed by the network thread; nothing for the caller to free.
```

Two acquire variants:
- `acquire()` — blocks (futex) when no slot is free. Default. This is the
  sole backpressure path: ring full = all slots in-flight.
- `tryAcquire()` — returns `error.WouldBlock` instead of blocking, for callers
  that want to shed load.

`m.await()` is the completion boundary. Because the ring owns the payload,
there is no buffer-lifetime contract on the caller — the only thing `await`
guards is "is my message durable yet." `setTopic`/`setKey` copy into the slot's
inline buffers (small, fixed); the value is written directly by the caller, no
 copy. The one copy on the whole produce path is slot → record-batch buffer in
the network thread, unavoidable under TLS + Kafka's v2 record format.

No callback variant in v1 — `await` is simpler to reason about and composes
with the single-threaded network core. Callbacks can be added later as sugar
on top of `await` in a spin/yield loop.

Errors larger than `max_message_size` / `max_key_len` / `max_topic_len` must be
caught at `set*`/`commit` time with `error.MessageTooLarge` etc. These are
config-time tradeoffs, not runtime surprises — document them on the config.

---

## 4. File layout

```
src/
  root.zig              — public re-exports (Client, Config, Handle, errors)
  Client.zig           — public Client API + config arena + thread spawn
  Connection.zig       — TCP + ztls + SCRAM auth → ready connection
  Producer.zig         — network thread: batching, Produce encode/ack, retry/metadata refresh
  partitioner.zig      — round-robin / key-hash (murmur2, matches Kafka Utils.murmur2)
  Ring.zig             — MPSC payload ring (ported from AtomicRingBuffer)
  scram/
    scram.zig           — generic SCRAM client (sha256/sha512)
    pbkdf2.zig          — incremental PBKDF2 (ported)
    root.zig            — module root for the standalone `scram` module
  wire/
    primitives.zig      — int/varint/string/bytes/array readers+writers
    frame.zig           — ResponseBuffer (length-prefixed framing), request writer
    api/
      metadata.zig      — key 3 v12
      produce.zig       — key 0 v9 (+ record_batch.zig)
      api_versions.zig  — key 18 v3
      sasl.zig          — keys 17 v1 + 36 v1
      api_keys.zig      — enum of api keys + version table
    record_batch.zig    — v2 record batch encode (the Produce payload format);
                        compression codec slot (plaintext always; zstd behind
                        -Dzstd=true). Compress records region in place before
                        CRC32C/length finalize. Non-idempotent sentinel fields:
                        producerId=-1, producerEpoch=-1, baseSequence=-1
                        (Java RecordBatch defaults). COMPACT_RECORDS length is
                        encoded as unsigned-varint(recordsByteSize + 1) (the +1
                        is the compact-array non-null tag). Exactly one record
                        batch per partition_data entry (ProduceRequest
                        validates this).
    compress.zig        — thin codec interface; zstd impl behind build flag
    testdata/           — captured frames as .hex/.zig fixtures
build.zig               — modules: kafka (lib), kafka_cli (exe), scram (lib)
build.zig.zon           — ztls dep, scram path/git
justfile                — kafka-up, kafka-down, e2e, test, fmt, lint
```

`scram/` must not import `wire` or anything Kafka-specific. `build.zig` exposes
it as a separate module so consumers can `@import("scram")` without pulling
kafka, and so it can be split to its own repo by moving the directory and its
build rule.

---

## 5. Testing strategy

Three layers, ordered by how much they exercise vs. how heavy they are.

### 5.1 Unit: wire codec fixtures (deterministic, no network)

For every API in scope, capture (or hand-construct) real request and response
bytes and commit them to `src/wire/testdata/`. Tests:
- `encode(request) == expected_bytes` (exact).
- `decode(expected_bytes) == expected_struct` (exact, slices compared).
- Round-trip: `decode(encode(x)) == x`.

This is the most important layer. If the codec is byte-exact against real
Kafka frames, the rest is plumbing. Prioritize getting a real
`MetadataResponse` and `ProduceResponse` from a running broker (use the e2e
harness below to capture once, commit the bytes). The agent should generate
fixtures by talking to a real broker once, then freeze them.

### 5.2 Integration: in-process mock broker (deterministic)

A small blocking TCP server in `tests/mock_broker.zig` that speaks the protocol
**using our own `wire` codec** (dogfooded). It answers:
- `ApiVersions` → advertise the versions we target.
- `SaslHandshake`/`SaslAuthenticate` → a no-op auth (or a real SCRAM server
  side using `scram`, so we test both ends).
- `Metadata` → a fixed cluster (1 leader, N partitions).
- `Produce` → ack the batches, optionally inject retriable errors to exercise
  the retry/metadata-refresh path.

This exercises the client's batching, linger, ack, retry, and backpressure
logic without a JVM. Run under `zig build test` as ordinary tests spawning a
thread. Keep it fast (<100ms).

### 5.3 E2E: real Kafka via justfile + Nix (smoke)

Bring `apache-kafka` (or `redpanda`) in through the flake. justfile recipes:

```just
# Start a single-node Kafka + Zookeeper (or KRaft) on localhost:9093 with
# SASL_SSL/SCRAM. Generate a self-signed CA + broker keystore, create a
# SCRAM user. Recipe blocks until the broker is up.
kafka-up:
    ...

kafka-down:
    ...

# Run the e2e smoke: produce N messages through kafka-zig, then consume them
# back with kafka-console-consumer (or a tiny zig consumer) and assert count.
e2e: kafka-up
    zig build e2e
    kafka-down
```

Use KRaft mode (no Zookeeper) if the nixpkgs Kafka version supports it cleanly;
it's one process less. SASL_SSL config is the painful part — the recipe must
produce a broker that requires SCRAM-SHA-256 over TLS, matching AWS MSK, so the
e2e actually exercises the auth path we care about. If wiring SASL_SSL into a
local broker is too much ceremony, ship a `PLAINTEXT + SASL_PLAINTEXT` broker
for the e2e (still exercises SCRAM, skips TLS) and rely on a separate
TLS-loopback test against ztls's own server for the TLS path. Decide this
during implementation; don't let SASL_SSL setup block the whole plan.

The e2e is a smoke test, not the primary signal. The mock broker is.

---

## 6. Build & deps

- `build.zig.zon`: add `ztls` (path `../ztls` during dev; git+hash for
  release). `scram` is in-tree, exposed as its own module. No other deps in the
  default build.
- `build.zig`: modules `kafka` (lib, root `src/root.zig`), `scram` (lib, root
  `src/scram/root.zig`), `kafka_cli` (exe, a thin demo producer). Test step
  runs wire unit tests + mock-broker integration tests. Add an `e2e` step that
  builds the e2e binary (run against a live broker).
  - zstd, optional: `const zstd = b.option(bool, "zstd", "enable zstd compression") orelse false;`
    When true, `mod.linkSystemLibrary("zstd", .{ .preferred_link_mode = .static, .use_pkg_config = .force })`
    (note: `linkSystemLibrary2` is deprecated/removed on `*Module` in 0.15.2 —
    use `mod.linkSystemLibrary(name, opts)`)
    and `mod.link_libc = true` (libzstd needs libc). Pass a build option
    (`build_options.zstd_enabled`) into the module so `compress.zig` can
    `@import("build_options").zstd_enabled` and either expose the zstd codec or
    return `error.CompressionNotImplemented` at runtime. Default build links no
    extra library.
- `flake.nix`: add `apache-kafka` (and `openjdk` if transitively needed) and
  `zstd` (for the `-Dzstd=true` path) to the devshell. Keep `zig_0_15`/`zls_0_15`.
  ztls needs `pkg-config` + `openssl` (or `aws-lc`) — pull those in too (ztls's
  own flake shows the set). **nixpkgs' default `zstd` does NOT ship `libzstd.a`**
  on normal non-static targets (`enableStatic` defaults to
  `stdenv.hostPlatform.isStatic` = false). The flake must use
  `(zstd.override { enableStatic = true; })` (or `pkgsStatic.zstd`) for the
  `-Dzstd=true` path. Also note Zig does not pass `--static` to pkg-config for
  `preferred_link_mode = .static` (ziglang/zig#23382); libzstd's static archive
  is self-contained (single-threaded), so this is usually fine, but if the link
  fails fall back to `addObjectFile(.{ .cwd_relative = ".../libzstd.a" })`.
- `justfile` is the CI entry point: `just test`, `just e2e`, `just fmt`,
  `just lint`. Follow the justfile skill for modern syntax.

---

## 7. Phasing / acceptance for the one-shot

Ordered so each phase is independently verifiable before the next starts.

1. **wire primitives + one API (Metadata).** Primitives, frame, `Metadata`
   v12 encode/decode, fixture round-trip test. Done = `zig build test` passes
   with Metadata fixtures.
2. **wire: the rest of the APIs + record batch + compression.** `ApiVersions`,
   `SaslHandshake/Authenticate`, `Produce` v9 + `record_batch`. Plaintext batch
   encode with fixtures. Then the compression codec slot in `record_batch.zig`
   with the zstd path behind `-Dzstd=true` (link libzstd, compress records
   region in place, re-finalize CRC/length). Done = all wire tests green in both
   the default and `-Dzstd=true` builds.
3. **scram module.** Port + generalize to sha256/sha512, RFC vectors, fuzz.
   Done = `scram` tests green, no kafka imports.
4. **ring.** Port AtomicRingBuffer to the payload-ring shape (slot owns
   inline topic/key/value buffers), update the ack-before-reclaim lifecycle,
   wire `acquire`/`set*`/`commit`/`await` on the slot. Done = ring tests
   (multi-producer + completion, backpressure-when-full) green.
5. **connection.** TCP + ztls handshake + SCRAM auth, end-to-end against the
   mock broker's auth. Done = a test connects and authenticates.
6. **producer + client.** Network thread, batching, Produce, ack, retry,
   metadata refresh, public API. Done = mock-broker integration tests pass
   (produce N, all acked; inject error → recovers).
7. **e2e.** justfile `kafka-up`/`e2e` against real Kafka, smoke produces and
   consumes. Done = `just e2e` green on the dev machine.

**Definition of done for the one-shot:** phases 1–6 green under
`zig build test`, phase 7 green under `just e2e`, `scram` importable standalone,
no allocations on the produce hot path except the documented metadata-cache
copy, and the public API matches §3.

---

## 8. Non-negotiables (carry over from ztls style)

- `wire` and `scram` do no I/O and no heap allocation on the hot path. Caller
  owns buffers. Document any unavoidable allocation.
- No unnecessary copies. The produce path has exactly one copy: slot payload
  → record-batch buffer, in the network thread, unavoidable under TLS + the v2
  record format. The producer writes directly into the ring slot — zero-copy
  from the producer's side. Document this copy and why it can't be avoided.
- Follow the `zig` and `tiger-style` skills for all Zig code. Read them before
  writing.
- Every byte of a Kafka frame is implemented to the spec, not from memory. Read
  https://kafka.apache.org/protocol.html for each API before encoding it.
- Profile/benchmark before claiming fast. For v1, correctness and the
  no-alloc/no-copy discipline matter more than microbench numbers, but the
  structure must not foreclose performance later.

---

## 9. TLS / auth target — confirmed

Resolved against the real broker (port 9096, SASL/SCRAM-over-TLS):

- **MSK negotiates TLS 1.3.** `openssl s_client -connect <broker>:9096
  -tls1_3` returns `Protocol: TLSv1.3`, `Verify return code: 0 (ok)`. ztls
  (TLS 1.3 only) connects. The earlier AWS docs saying "uses TLS 1.2" are the
  documented minimum, not the negotiated reality on 9096.
- **Port 9096** is the SASL/SCRAM-over-TLS port (9093 is mTLS client-cert).
  Examples and e2e should use 9096.
- **MSK supports SCRAM-SHA-512 only** (per AWS docs). The `scram` module keeps
  both SHA-256 and SHA-512 (generic is cheap, reusable); **MSK-facing config,
  examples, and e2e default to SCRAM-SHA-512**.

No open questions remain. The plan is ready to hand off.
