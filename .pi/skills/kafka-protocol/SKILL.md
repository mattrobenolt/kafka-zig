---
name: kafka-protocol
description: >
  Pinned Kafka wire-protocol reference for kafka-zig. Use when writing or
  reviewing Kafka protocol codec code (src/wire/**), record batch encoding,
  SCRAM-over-SASL auth, or anything touching API keys/versions. Critical:
  LLM training data for the Kafka protocol is frequently wrong on API keys,
  version numbers, field order, and flexible-version encoding. Always verify
  against https://kafka.apache.org/protocol.html before encoding. This skill
  holds the pinned values and the known gotchas so they don't get re-derived
  (or re-guessed) every time.
---

# kafka-protocol — pinned reference for kafka-zig

This skill is a checklist of pinned values and gotchas, NOT a substitute for
the spec. When implementing an API, read the protocol page entry for that
API key + version and implement byte-for-byte.

## Pinned API keys and versions (target: modern Kafka 3.x / AWS MSK)

| API | key | version | flexible? |
|---|---|---|---|
| Produce | 0 | v9 | yes |
| Metadata | 3 | v12 | yes |
| SaslHandshake | 17 | v1 | **no** |
| ApiVersions | 18 | v3 | yes |
| SaslAuthenticate | **36** | v1 | **no** |

**SaslAuthenticate is key 36, NOT 366.** This was a typo that crept into early
plan drafts. If you see 366 anywhere, fix it.

**ApiVersions pre-auth:** send **v0** before SASL (brokers historically treat a
schema exception in the first request as a GSSAPI token). v3 is fine post-auth.

## Flexible versions (KIP-482) — the one-shot trap

Flexible APIs (Produce v9, Metadata v12, ApiVersions v3) use:
- **compact_string** / **compact_bytes** / **compact_array**: length encoded as
  unsigned-varint of `N+1` (so `0` means null, `1` means empty/non-null zero-len).
- **unsigned_varint** (LEB128, NOT zigzag — that's `varint`/`varlong` for int32/64).
- request **header v2** (flexible header) with a trailing tagged-field buffer.
- a trailing **tagged-field buffer** (empty = `0x00` uvarint) on every flexible
  struct/element.

Non-flexible APIs (SaslHandshake v1, SaslAuthenticate v1) use:
- int16-length **string** / int32-count **array**.
- request **header v1** (no tagged fields).

**Header version is per-API-per-version, not global.** Maintain a
`header_version(api_key, version) -> {request, response}` table.

**Classic gotcha:** the **ApiVersions response uses header version 0 even at
v3** — the broker must parse it before it knows the client's flexibility.

**REQUEST-header client_id is NOT compact-encoded (critical gotcha).** The
Kafka `RequestHeader.json` spec marks the `ClientId` field with
`"flexibleVersions": "none"`, which means the client_id is ALWAYS serialized
as a nullable_string (i16 length + bytes) — even in request header **v2**.
The v2 header only adds a trailing tag buffer; it does NOT convert client_id
to a compact_string. Writing it as a compact_string (uvarint length) makes the
broker read the uvarint's first byte as the high byte of an i16 length →
garbage length → `InvalidRequestException` / connection drop. (This bit us
against a real broker in phase 7; the mock missed it because the mock had the
same bug.) The same exemption applies to the response header v1's
correlation_id? No — correlation_id is always i32. Only the client_id field
is exempt from the compact-string rule. Get
this wrong and the connection desyncs right after connect.

## v2 record batch (Produce payload)

Layout (big-endian unless noted):
- baseOffset: i64
- batchLength: i32 (length from partitionLeaderEpoch to end)
- partitionLeaderEpoch: i32 (NOT covered by CRC)
- magic: i8 = 2
- crc: u32 = **CRC32C (Castagnoli)** over attributes→end
- attributes: u16 (bits 0–2 = compression: 0 none, 1 gzip, 2 snappy, 3 lz4,
  **4 zstd**; bit 3 = timestampType; bit 4 = transactional; bit 5 = control;
  bit 6 = **hasDeleteHorizonMs** (KIP-516) — NOT an idempotent flag; bits 7-15
  unused). **There is NO idempotent attributes bit.** The broker infers
  idempotency from `producerId != -1` (NO_PRODUCER_ID). The historical
  `IDEMPOTENT_FLAG_MASK = 0x40` was removed when bit 6 was repurposed. Do NOT
  set any attributes bit for idempotent mode — just send a real producerId.
  (Verified against https://kafka.apache.org/43/implementation/message-format/
  + the Java `RecordBatch` — `computeAttributes` sets no idempotent bit.)
- lastOffsetDelta: i32
- baseTimestamp: i64
- maxTimestamp: i64
- producerId: i64
- producerEpoch: i16
- baseSequence: i32
- records count: i32
- records: array of records (or compressed blob if compression attr set)

**Non-idempotent producer sentinels:** `producerId = -1`, `producerEpoch = -1`,
`baseSequence = -1` (Java `RecordBatch` defaults). State these exactly.

**Each record** (in the records region, before compression) — per the Kafka
message-format spec and Java `DefaultRecord.writeTo`, ALL of these are **zigzag
varint** (i32 via `(n<<1)^(n>>31)`), NOT unsigned_varint. This was a documented
error in an earlier draft of this skill; do not repeat it:
- `length`: varint (byte length of the record body AFTER this length field)
- `attributes`: i8 (0, reserved)
- `timestampDelta`: varlong (zigzag i64)
- `offsetDelta`: varint (zigzag i32)
- `keyLength`: varint (-1 = null), then key
- `valueLength`: varint (-1 = null), then value
- `headersCount`: varint (zigzag i32 — the spec says "the count of headers is
  also encoded as a varint"; Java uses `writeVarint(headers.length)`)
- per header: `headerKeyLength` varint + headerKey, `headerValueLength` varint
  (-1 = null) + headerValue.

Unsigned_varint (LEB128, no zigzag) is used ONLY for compact-type lengths
(compact_string/compact_bytes/compact_array = N+1) and tagged-field tags/sizes —
never for the record-level fields above.

**Compression ordering is non-negotiable:**
1. Serialize the records region.
2. Compress it in place (when compression attr set).
3. Compute CRC32C over attributes→end (so over the compressed bytes).
4. Finalize batchLength.
Compress BEFORE CRC. The broker stores the compressed batch verbatim on disk
and replicates it compressed; consumers decompress.

**COMPACT_RECORDS length quirk (Produce v9, flexible):** the records field is
`COMPACT_RECORDS`; its length is encoded as `unsigned_varint(recordsByteSize + 1)`
(the +1 is the compact-array non-null tag). Easy to miss.

**Exactly one record batch per `partition_data` entry.** ProduceRequest validates
this (throws if more than one batch or magic != 2).

## SASL SCRAM over Kafka

Flow: (optional ApiVersions v0) → `SaslHandshake(mechanism)` v1 → if handshake
v1, wrap each SCRAM token in `SaslAuthenticate` (key 36) v1 request/response.

Mechanisms Kafka supports: `SCRAM-SHA-256`, `SCRAM-SHA-512`.
**AWS MSK supports SCRAM-SHA-512 only.** MSK examples/defaults use SHA-512.

SCRAM client-first MUST include the **username**: `n,,n=<username>,r=<nonce>`.
The exosphere `scram.zig` precedent hardcodes `n,,n=,r=` with an EMPTY username
because PostgreSQL takes the user from the startup packet — that is Postgres-
specific and **wrong for Kafka**. Username must be SASLprep-normalized and
`,`/`=` escaped to `=2C`/`=3D`.

GS2 header: `n,,` (no channel binding, no authzid) — both commas, not `n,`.
`c=biws` in client-final is base64 of `n,,`.

Spec refs: RFC 5802 (SCRAM), RFC 7677 (SCRAM-SHA-256). SCRAM-SHA-512 is the same
construction over SHA-512; Kafka supports it per the Kafka security docs. **Do
NOT cite RFC 7804 — that is HTTP SCRAM, not SASL SCRAM.**

## Sans-I/O discipline (kafka-zig invariant)

`src/wire/**` and `src/scram/**` do no I/O and no heap allocation on the
encode/decode path. Caller owns every buffer. Response structs may borrow slices
into the response buffer ONLY for flat arrays; nested arrays of structs need
parsed copies (cold-path alloc is fine for metadata cache) or zero-alloc
iterator decoders — never raw-bytes-reinterpreted-as-typed-slice.

## One allowed copy (produce path)

Slot payload → record-batch buffer, in the network thread. That is the only
copy. The producer writes directly into the ring slot (zero-copy into the
queue). Anything else is a regression.

## Verify, don't trust memory

Before encoding any API, open https://kafka.apache.org/protocol.html and read
the entry for that API key + the version you're pinning. Cross-check field
order and types. The pinned versions above are correct for the target; the
field layouts must come from the spec, not recall.

## Error codes — verify against the table, do NOT recall

Kafka error codes are a fixed table at
https://kafka.apache.org/protocol.html#protocol_error_codes (the `Retriable`
column matters for the producer). LLM memory for specific code numbers is
frequently wrong (e.g. PRODUCER_FENCED is **90**, not 37 — a real fixture bug
we shipped). When a fixture, test, or handler references an error code by
number, look it up in the table and cite it. The retriable set the producer
cares about (verify against the table): NOT_LEADER_OR_FOLLOWER=6,
LEADER_NOT_AVAILABLE=5, UNKNOWN_TOPIC_OR_PARTITION=3, REQUEST_TIMED_OUT=7,
NOT_ENOUGH_REPLICAS=19, UNKNOWN_PRODUCER_ID=73, OUT_OF_ORDER_SEQUENCE=45
(non-retriable for idempotent — reset sequence), DUPLICATE_SEQUENCE_NUMBER=37
(non-retriable), PRODUCER_FENCED=90 (non-retriable — fatal, the producer is
done), COORDINATOR_NOT_AVAILABLE=14, NOT_COORDINATOR=16, REBOOTSTRAP_REQUIRED=129.
