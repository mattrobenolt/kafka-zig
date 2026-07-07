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
v3** — the broker must parse it before it knows the client's flexibility. Get
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
  bit 6 = idempotent)
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

**Each record** (in the records region, before compression): length uvarint,
then attributes i8, timestampDelta varlong, offsetDelta varint, keyLength varint
(-1 = null), key, valueLength varint (-1 = null), value, headers
(uvarint count, then per-header nameLength varint+name, valueLength varint+value).

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
