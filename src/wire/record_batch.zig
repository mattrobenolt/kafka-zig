//! v2 record batch — the Produce payload format (KIP-74).
//!
//! Spec: https://kafka.apache.org/43/implementation/message-format — "Record
//! Batch" and "Record". The v2 record batch is the on-wire format for records
//! sent in Produce requests (magic = 2).
//!
//! Layout (big-endian):
//!   baseOffset: i64
//!   batchLength: i32 (bytes from partitionLeaderEpoch to end of batch)
//!   partitionLeaderEpoch: i32 (NOT covered by CRC)
//!   magic: i8 = 2
//!   crc: u32 = CRC32C (Castagnoli) over attributes→end
//!   attributes: u16 (bits 0–2 compression, 3 timestampType, 4 transactional,
//!               5 control, 6 hasDeleteHorizonMs, 7–15 unused)
//!   lastOffsetDelta: i32
//!   baseTimestamp: i64
//!   maxTimestamp: i64
//!   producerId: i64 (-1 = non-idempotent sentinel)
//!   producerEpoch: i16 (-1 = non-idempotent sentinel)
//!   baseSequence: i32 (-1 = non-idempotent sentinel)
//!   recordsCount: i32
//!   records: [Record] (or compressed blob if compression attr set)
//!
//! Each Record:
//!   length: varint (zigzag i32 — size of remaining fields after this length)
//!   attributes: i8 (0, unused)
//!   timestampDelta: varlong (zigzag i64)
//!   offsetDelta: varint (zigzag i32)
//!   keyLength: varint (zigzag i32, -1 = null)
//!   key: byte[] (present when keyLength >= 0)
//!   valueLength: varint (zigzag i32, -1 = null)
//!   value: byte[] (present when valueLength >= 0)
//!   headersCount: varint (zigzag i32, 0 = no headers)
//!   per header:
//!     headerKeyLength: varint (zigzag i32, always non-null)
//!     headerKey: String (UTF-8 bytes)
//!     headerValueLength: varint (zigzag i32, -1 = null)
//!     headerValue: byte[] (present when headerValueLength >= 0)
//!
//! CRC: CRC-32C (Castagnoli, polynomial 0x1edc6f41). The CRC covers bytes from
//! the attributes field through the end of the batch — i.e. everything AFTER
//! the crc field. baseOffset, batchLength, partitionLeaderEpoch, magic, and
//! the crc field itself are NOT covered. The partitionLeaderEpoch is excluded
//! so the broker can assign it without recomputing the CRC.
//!
//! Compression ordering (non-negotiable, per PLAN §2.1):
//!   1. Serialize the records region.
//!   2. Compress it (when compression attr set) via `compress.zig`.
//!   3. Compute CRC32C over attributes→end (so over the compressed bytes).
//!   4. Finalize batchLength.
//! Compress BEFORE CRC. v1 supports plaintext + snappy + zstd (zstd behind
//! `-Dzstd=true`); the codec slot lives in `compress.zig`.
//!
//! Non-idempotent producer sentinels: producerId = -1, producerEpoch = -1,
//! baseSequence = -1 (Java RecordBatch defaults). These are the values a
//! non-idempotent producer sends; the broker accepts them without tracking
//! sequence numbers.
//!
//! Allocator policy: encode is ZERO heap allocation — it writes directly into
//! a caller-provided `out: []u8` buffer and patches CRC + batchLength in place.
//! No allocator parameter, no `std.heap.*` anywhere on the encode path. If the
//! buffer is too small it returns `error.BufferTooSmall` (the caller sizes it
//! to `max_batch_bytes`). Decode takes an `std.mem.Allocator` for owned copies
//! of key/value/header data (the decode path is for the mock broker / tests,
//! not the produce hot path). Call `Batch.deinit` to free decoded data.

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const primitives = @import("primitives.zig");
const Reader = primitives.Reader;
const compress = @import("compress.zig");

/// CRC-32C (Castagnoli). In Zig 0.15 std this is `std.hash.crc.Crc32Iscsi`,
/// which uses polynomial 0x1edc6f41 with init=0xffffffff, reflected I/O, and
/// xor_output=0xffffffff — the standard CRC32C parameters.
/// Source: std/hash/crc.zig line 800.
pub const Crc32C = std.hash.crc.Crc32Iscsi;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A record header. The key is always non-null (per spec); the value may be
/// null. For the encode path, both key and value are borrowed from the caller.
/// For the decode path, both are owned copies (allocated by `decodeBatch`).
pub const Header = struct {
    key: []const u8,
    value: ?[]const u8,
};

/// A single record within a batch. For the encode path (producer), key and
/// value are borrowed from the ring slot — the caller ensures they outlive
/// the `encodeBatch` call. For the decode path (mock broker / tests), key,
/// value, and headers are owned copies allocated by `decodeBatch`; call
/// `Batch.deinit` to free them.
pub const Record = struct {
    offset_delta: i32,
    timestamp_delta: i64,
    key: ?[]const u8,
    value: ?[]const u8,
    headers: []const Header,
};

/// Compression attribute values (bits 0–2 of the attributes field).
/// `gzip` (1) and `lz4` (3) are wire-format constants — they are NOT yet
/// implemented and return `error.CompressionNotImplemented` from
/// `encodeBatch`/`decodeBatch`. `none`, `snappy`, and `zstd` (requires
/// `-Dzstd=true`) are the supported codecs.
pub const Compression = enum(u3) {
    none = 0,
    gzip = 1,
    snappy = 2,
    lz4 = 3,
    zstd = 4,
};

/// Attributes field bit positions.
pub const attr_timestamp_type: u16 = 1 << 3;
pub const attr_transactional: u16 = 1 << 4;
pub const attr_control: u16 = 1 << 5;
pub const attr_delete_horizon: u16 = 1 << 6;
/// A decoded v2 record batch. Owned data (records, keys, values, headers) is
/// allocated by `decodeBatch`. Call `deinit` to free.
pub const Batch = struct {
    base_offset: i64,
    partition_leader_epoch: i32,
    attributes: u16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: []Record,

    pub fn deinit(self: *Batch, allocator: std.mem.Allocator) void {
        for (self.records) |*r| {
            if (r.key) |k| allocator.free(k);
            if (r.value) |v| allocator.free(v);
            for (r.headers) |h| {
                allocator.free(h.key);
                if (h.value) |v| allocator.free(v);
            }
            allocator.free(r.headers);
        }
        allocator.free(self.records);
        self.* = undefined;
    }

    /// The compression codec from the attributes field (bits 0–2).
    pub fn compression(self: Batch) Compression {
        return @enumFromInt(@as(u3, @truncate(self.attributes)));
    }

    /// Whether the batch is transactional (bit 4).
    pub fn isTransactional(self: Batch) bool {
        return (self.attributes & attr_transactional) != 0;
    }

    /// Whether this is a control batch (bit 5).
    pub fn isControl(self: Batch) bool {
        return (self.attributes & attr_control) != 0;
    }

    /// Whether this batch is from an idempotent producer. The broker infers
    /// idempotency from `producer_id != -1` (NO_PRODUCER_ID) — there is NO
    /// separate idempotent bit in the attributes field. Bit 6 is
    /// `hasDeleteHorizonMs` (KIP-516), NOT an idempotent flag. The historical
    /// `IDEMPOTENT_FLAG_MASK = 0x40` was removed from the Java client when
    /// bit 6 was repurposed; the broker now checks `producerId != -1`.
    /// See: https://kafka.apache.org/43/implementation/message-format/
    pub fn isIdempotent(self: Batch) bool {
        return self.producer_id != -1;
    }
};

// ---------------------------------------------------------------------------
// Encode (zero-alloc, writes to caller's Writer)
// ---------------------------------------------------------------------------

/// Options for encoding a record batch.
pub const EncodeOptions = struct {
    base_offset: i64 = 0,
    base_timestamp: i64 = 0,
    /// Java `RecordBatch.NO_PARTITION_LEADER_EPOCH` (-1): a producer-created
    /// batch leaves this unset so the broker assigns the real epoch. It is
    /// excluded from the CRC, so the broker can overwrite it in place.
    partition_leader_epoch: i32 = -1,
    /// Compression codec. `.none` writes the records region verbatim. `.snappy`
    /// (always available, pure-Zig) and `.zstd` (requires `-Dzstd=true`)
    /// compress the records region in place before CRC/length finalize, per
    /// PLAN §2.1. `.gzip` and `.lz4` are wire-format constants but return
    /// `error.CompressionNotImplemented` (not yet implemented).
    compression: Compression = .none,
    /// zstd compression level, used only when `compression == .zstd`.
    compression_level: i32 = compress.default_level,
    /// Scratch buffer for the compressed records region, REQUIRED when
    /// `compression != .none` and unused otherwise. Must be at least
    /// `compress.compressBound(codec, uncompressed_records_len)` bytes. The
    /// records region is compressed into `scratch`, then copied back into
    /// `out`; `out` itself must therefore be sized for the compressed batch
    /// (header + compressed records ≤ header + compressBound). Keeping the
    /// compressed bytes in a caller-owned scratch buffer preserves the
    /// zero-heap-allocation encode contract (no `std.heap.*` fallback).
    scratch: ?[]u8 = null,
    /// Non-idempotent producer sentinels. When all three are -1, the batch is
    /// non-idempotent (the broker does not track sequence numbers). For
    /// idempotent mode, the caller passes the real producer_id/producer_epoch
    /// from InitProducerId and a per-partition base_sequence. The broker
    /// infers idempotency from `producer_id != -1` — there is NO idempotent
    /// bit in the attributes field (bit 6 is `hasDeleteHorizonMs`, KIP-516).
    producer_id: i64 = -1,
    producer_epoch: i16 = -1,
    base_sequence: i32 = -1,
};

pub const EncodeError = error{
    BufferTooSmall,
    CompressionScratchRequired,
    CompressionNotImplemented,
    CompressionFailed,
};

pub const EncodeBatchResult = struct {
    bytes: []u8,
    consumed: usize,
};

/// Encode a complete v2 record batch into `out` and return the written slice.
///
/// ZERO heap allocation: the batch is serialized directly into the
/// caller-provided `out` buffer, then batchLength and CRC32C are patched in
/// place. If `out` is too small, returns `error.BufferTooSmall` (the caller
/// sizes it to `max_batch_bytes`). The records' key/value are borrowed from
/// the caller and must outlive this call.
///
/// Layout of `out` (big-endian), with the two patched fields marked:
///   [0..8)   baseOffset
///   [8..12)  batchLength           ← patched: bytes from [12) to end
///   [12..16) partitionLeaderEpoch  (NOT covered by CRC)
///   [16]     magic = 2
///   [17..21) crc                   ← patched: CRC32C over [21) to end
///   [21..)   CRC-covered region: attributes → recordsCount → records
///
/// When `options.compression != .none`, the records region is compressed in
/// place BEFORE the CRC/batchLength patches (PLAN §2.1 ordering). The codec
/// writes into `options.scratch` (required for a compressed batch), then the
/// compressed bytes are copied back into `out` — no heap allocation. `.zstd`
/// requires `-Dzstd=true`; without it, compress returns
/// `error.CompressionNotImplemented`. `.gzip` and `.lz4` are not yet
/// implemented and also return `error.CompressionNotImplemented`.
/// recordsCount stays the ORIGINAL uncompressed count — Kafka keeps the record
/// count even when the region is compressed.
///
/// Spec: https://kafka.apache.org/43/implementation/message-format — "Record
/// Batch".
pub fn encodeBatch(
    out: []u8,
    records: []const Record,
    options: EncodeOptions,
) EncodeError![]u8 {
    const crc_region_start = 21; // attributes field offset
    const batch_length_off = 8;
    const partition_leader_epoch_off = 12;
    const crc_off = 17;
    // Records region offset within `out`: crc_region_start + the fixed header
    // between attributes and recordsCount (attributes u16 + lastOffsetDelta i32
    // + baseTimestamp i64 + maxTimestamp i64 + producerId i64 + producerEpoch
    // i16 + baseSequence i32 + recordsCount i32 = 40 bytes).
    const records_region_off = crc_region_start + 40;

    // Serialize header (with placeholder batchLength/crc) + CRC-covered region
    // directly into `out`. A fixed writer returns error.WriteFailed when it
    // runs out of room; we map that to BufferTooSmall. No heap fallback. The
    // attributes field carries the compression bits (writeBatch writes
    // @intFromEnum(options.compression)); for a compressed codec we replace the
    // records region below so the attribute matches the on-wire bytes.
    var w: std.Io.Writer = .fixed(out);
    writeBatch(&w, records, options) catch |err| switch (err) {
        error.WriteFailed => return error.BufferTooSmall,
    };
    var written = w.buffered();
    assert(written.len >= records_region_off);

    // --- Compression: compress records region BEFORE CRC (PLAN §2.1) ---
    if (options.compression != .none) {
        const codec: compress.Codec = @enumFromInt(@intFromEnum(options.compression));
        const uncompressed = written[records_region_off..];
        // Scratch is a documented caller contract for a compressed batch.
        const scratch = options.scratch orelse return error.CompressionScratchRequired;
        const clen = compress.compress(
            codec,
            uncompressed,
            scratch,
            options.compression_level,
        ) catch |err| switch (err) {
            error.NotImplemented => return error.CompressionNotImplemented,
            error.BufferTooSmall => return error.BufferTooSmall,
            error.CompressionFailed => return error.CompressionFailed,
        };
        // Copy the compressed records back over the uncompressed region. No
        // overlap: scratch is a separate caller-owned buffer.
        assert(records_region_off + clen <= out.len);
        @memcpy(out[records_region_off..][0..clen], scratch[0..clen]);
        written = out[0 .. records_region_off + clen];
    }
    // --- End compression ---

    // Patch batchLength = bytes from partitionLeaderEpoch (offset 12) to end.
    const batch_length: i32 = @intCast(written.len - partition_leader_epoch_off);
    std.mem.writeInt(u32, out[batch_length_off..][0..4], @bitCast(batch_length), .big);

    // Patch CRC32C over the covered region (attributes → end).
    const crc = Crc32C.hash(written[crc_region_start..]);
    std.mem.writeInt(u32, out[crc_off..][0..4], crc, .big);

    return written;
}

/// Encode a record prefix that fits `budget` bytes on the wire. For
/// uncompressed batches this is the largest prefix. For compressed batches it
/// may return a smaller fitting prefix to avoid repeated compression attempts.
/// Returns zero consumed records when even the first record cannot fit.
pub fn encodeBatchBounded(
    out: []u8,
    records: []const Record,
    budget: usize,
    options: EncodeOptions,
) EncodeError!EncodeBatchResult {
    const limit = @min(budget, out.len);
    if (limit < batch_records_region_off) return .{ .bytes = out[0..0], .consumed = 0 };

    if (options.compression == .none) {
        const consumed = countUncompressedPrefix(records, limit);
        if (consumed == 0) return .{ .bytes = out[0..0], .consumed = 0 };
        const bytes = try encodeBatch(out, records[0..consumed], options);
        assert(bytes.len <= budget);
        return .{ .bytes = bytes, .consumed = consumed };
    }

    const codec: compress.Codec = @enumFromInt(@intFromEnum(options.compression));
    const scratch = options.scratch orelse return error.CompressionScratchRequired;
    var consumed = countCompressedPrefix(records, out.len, scratch.len, codec);
    if (consumed == 0) return .{ .bytes = out[0..0], .consumed = 0 };

    while (consumed > 0) {
        const bytes = encodeBatch(out, records[0..consumed], options) catch |err| switch (err) {
            error.BufferTooSmall => {
                consumed = smallerCompressedPrefix(consumed);
                continue;
            },
            else => return err,
        };
        if (bytes.len <= budget) return .{ .bytes = bytes, .consumed = consumed };
        consumed = smallerCompressedPrefix(consumed);
    }

    return .{ .bytes = out[0..0], .consumed = 0 };
}

/// Write the full batch (header with placeholder batchLength/crc, then the
/// CRC-covered region: attributes → recordsCount → records) into `writer`.
/// The caller patches batchLength and crc in place afterwards.
fn writeBatch(
    writer: *std.Io.Writer,
    records: []const Record,
    options: EncodeOptions,
) std.Io.Writer.Error!void {
    try primitives.writeI64(writer, options.base_offset);
    try primitives.writeI32(writer, 0); // batchLength placeholder (patched)
    try primitives.writeI32(writer, options.partition_leader_epoch);
    try primitives.writeI8(writer, 2); // magic
    try primitives.writeU32(writer, 0); // crc placeholder (patched)

    // --- CRC-covered region begins here (attributes) ---
    // attributes: u16 (compression bits in the low three bits).
    try primitives.writeU16(writer, @intFromEnum(options.compression));
    // lastOffsetDelta: i32 — offset delta of the last record in the batch.
    const last_delta: i32 = if (records.len == 0) 0 else records[records.len - 1].offset_delta;
    try primitives.writeI32(writer, last_delta);
    // baseTimestamp: i64
    try primitives.writeI64(writer, options.base_timestamp);
    // maxTimestamp: i64 = base + max timestamp delta among records.
    var max_ts_delta: i64 = 0;
    for (records) |r| {
        if (r.timestamp_delta > max_ts_delta) max_ts_delta = r.timestamp_delta;
    }
    try primitives.writeI64(writer, options.base_timestamp + max_ts_delta);
    try primitives.writeI64(writer, options.producer_id);
    try primitives.writeI16(writer, options.producer_epoch);
    try primitives.writeI32(writer, options.base_sequence);
    // recordsCount: i32
    try primitives.writeI32(writer, @intCast(records.len));
    for (records) |rec| {
        try writeRecord(writer, rec);
    }
}

const batch_records_region_off = 21 + 40;

fn countUncompressedPrefix(records: []const Record, budget: usize) usize {
    var len: usize = batch_records_region_off;
    var consumed: usize = 0;
    for (records) |rec| {
        const rec_len = recordLen(rec);
        if (len + rec_len > budget) break;
        len += rec_len;
        consumed += 1;
    }
    return consumed;
}

fn countCompressedPrefix(
    records: []const Record,
    out_len: usize,
    scratch_len: usize,
    codec: compress.Codec,
) usize {
    var records_len: usize = 0;
    var consumed: usize = 0;
    for (records) |rec| {
        const rec_len = recordLen(rec);
        const next_records_len = records_len + rec_len;
        const bound = compress.compressBound(codec, next_records_len);
        if (bound > scratch_len) break;
        if (batch_records_region_off + bound > out_len) break;
        records_len = next_records_len;
        consumed += 1;
    }
    return consumed;
}

fn smallerCompressedPrefix(consumed: usize) usize {
    if (consumed == 1) return 0;
    return @max(@as(usize, 1), consumed / 2);
}

/// Write a single record: a zigzag-varint length prefix (byte size of the
/// record body) followed by the body. The body length is computed
/// arithmetically via `recordBodyLen` so no per-record temp buffer is needed.
fn writeRecord(writer: *std.Io.Writer, rec: Record) std.Io.Writer.Error!void {
    const body_len = recordBodyLen(rec);
    try primitives.writeVarint(writer, @intCast(body_len));
    try writeRecordBody(writer, rec);
}

fn recordLen(rec: Record) usize {
    const body_len = recordBodyLen(rec);
    return primitives.varintSize(@intCast(body_len)) + body_len;
}

/// Exact byte length of the record body that `writeRecordBody` will emit
/// (everything after the length prefix). Used to write the varint length
/// prefix without serializing to a temp buffer first.
fn recordBodyLen(rec: Record) usize {
    var n: usize = 1; // attributes: i8
    n += primitives.varlongSize(rec.timestamp_delta);
    n += primitives.varintSize(rec.offset_delta);
    if (rec.key) |k| {
        n += primitives.varintSize(@intCast(k.len)) + k.len;
    } else {
        n += primitives.varintSize(-1);
    }
    if (rec.value) |v| {
        n += primitives.varintSize(@intCast(v.len)) + v.len;
    } else {
        n += primitives.varintSize(-1);
    }
    n += primitives.varintSize(@intCast(rec.headers.len));
    for (rec.headers) |h| {
        n += primitives.varintSize(@intCast(h.key.len)) + h.key.len;
        if (h.value) |v| {
            n += primitives.varintSize(@intCast(v.len)) + v.len;
        } else {
            n += primitives.varintSize(-1);
        }
    }
    return n;
}

/// Write the record body (everything after the length prefix): attributes,
/// timestampDelta, offsetDelta, keyLength+key, valueLength+value,
/// headersCount+headers.
fn writeRecordBody(writer: *std.Io.Writer, rec: Record) std.Io.Writer.Error!void {
    // attributes: i8 = 0 (unused)
    try primitives.writeI8(writer, 0);
    // timestampDelta: varlong
    try primitives.writeVarlong(writer, rec.timestamp_delta);
    // offsetDelta: varint
    try primitives.writeVarint(writer, rec.offset_delta);
    // keyLength: varint (-1 = null)
    if (rec.key) |k| {
        try primitives.writeVarint(writer, @intCast(k.len));
        try writer.writeAll(k);
    } else {
        try primitives.writeVarint(writer, -1);
    }
    // valueLength: varint (-1 = null)
    if (rec.value) |v| {
        try primitives.writeVarint(writer, @intCast(v.len));
        try writer.writeAll(v);
    } else {
        try primitives.writeVarint(writer, -1);
    }
    // headersCount: varint (zigzag i32, 0 = no headers)
    try primitives.writeVarint(writer, @intCast(rec.headers.len));
    for (rec.headers) |h| {
        // headerKeyLength: varint (always non-null)
        try primitives.writeVarint(writer, @intCast(h.key.len));
        try writer.writeAll(h.key);
        // headerValueLength: varint (-1 = null)
        if (h.value) |v| {
            try primitives.writeVarint(writer, @intCast(v.len));
            try writer.writeAll(v);
        } else {
            try primitives.writeVarint(writer, -1);
        }
    }
}

// ---------------------------------------------------------------------------
// Decode (allocates owned data — for mock broker / tests)
// ---------------------------------------------------------------------------

/// Errors from `decodeBatch`.
pub const DecodeError = error{
    EndOfStream,
    Malformed,
    CrcMismatch,
    BadMagic,
    CompressionNotImplemented,
    OutOfMemory,
};

/// Decode a v2 record batch from `reader`. The batch must start at the
/// reader's current position (baseOffset). Returns an owned `Batch` with
/// allocated copies of all key/value/header data. Call `deinit` to free.
///
/// Verifies the CRC32C and returns `error.CrcMismatch` on mismatch. Verifies
/// magic == 2 and returns `error.BadMagic` otherwise.
///
/// Spec: https://kafka.apache.org/43/implementation/message-format — "Record
/// Batch".
pub fn decodeBatch(allocator: std.mem.Allocator, reader: *Reader) (DecodeError || std.mem.Allocator.Error)!Batch {
    const base_offset = try primitives.readI64(reader);
    const batch_length = try primitives.readI32(reader);
    if (batch_length < 0) return error.Malformed;

    // batchLength = bytes from partitionLeaderEpoch to end. The batch payload
    // (everything after batchLength) is batch_length bytes.
    const batch_payload_len: usize = @intCast(batch_length);
    if (reader.remaining().len < batch_payload_len) return error.EndOfStream;

    // The CRC covers attributes→end. We need to read the bytes from
    // partitionLeaderEpoch through the end to parse the batch, but the CRC
    // excludes partitionLeaderEpoch, magic, and the crc field itself. So the
    // CRC-covered region is: batch_payload[9..] (skip 4 bytes
    // partitionLeaderEpoch + 1 byte magic + 4 bytes crc = 9 bytes).
    if (batch_payload_len < 9) return error.Malformed;

    const batch_payload = reader.buf[reader.pos..][0..batch_payload_len];
    reader.pos += batch_payload_len; // consume the entire batch

    const partition_leader_epoch = std.mem.readInt(i32, batch_payload[0..4], .big);
    const magic: i8 = @bitCast(batch_payload[4]);
    if (magic != 2) return error.BadMagic;

    const stored_crc = std.mem.readInt(u32, batch_payload[5..9], .big);
    const crc_region = batch_payload[9..];

    const computed_crc = Crc32C.hash(crc_region);
    if (computed_crc != stored_crc) return error.CrcMismatch;

    // Parse the CRC-covered region (attributes → end) from a sub-reader.
    var sub: Reader = .init(crc_region);
    const attributes = try primitives.readU16(&sub);
    const last_offset_delta = try primitives.readI32(&sub);
    const base_timestamp = try primitives.readI64(&sub);
    const max_timestamp = try primitives.readI64(&sub);
    const producer_id = try primitives.readI64(&sub);
    const producer_epoch = try primitives.readI16(&sub);
    const base_sequence = try primitives.readI32(&sub);
    const records_count = try primitives.readI32(&sub);
    if (records_count < 0) return error.Malformed;

    // When the compression bits are set, the records region (everything after
    // recordsCount) is a compressed blob. Decompress it into an allocated
    // buffer (decode is the cold mock-broker/test path, so an allocator is
    // permitted per the module allocator policy) and parse records from there.
    // recordsCount is the ORIGINAL uncompressed count, unchanged by compression.
    const codec: compress.Codec = @enumFromInt(@as(u3, @truncate(attributes)));
    var records_reader: *Reader = &sub;
    var decompressed_reader: Reader = undefined;
    var decompressed: ?[]u8 = null;
    defer if (decompressed) |d| allocator.free(d);
    if (codec != .none) {
        const compressed = sub.buf[sub.pos..];
        const dlen = compress.decompressedLen(codec, compressed) catch |err| switch (err) {
            error.NotImplemented => return error.CompressionNotImplemented,
            else => return error.Malformed,
        };
        const buf = try allocator.alloc(u8, dlen);
        decompressed = buf;
        _ = compress.decompress(codec, compressed, buf) catch |err| switch (err) {
            error.NotImplemented => return error.CompressionNotImplemented,
            else => return error.Malformed,
        };
        decompressed_reader = .init(buf);
        records_reader = &decompressed_reader;
    }

    // Cap: every record is ≥1 byte on the wire, so a count > remaining bytes
    // is malformed. This prevents a huge alloc from a small/corrupt frame.
    // NOTE: record_batch decode is not reachable from broker input in this
    // producer-only lib (the producer encodes, never decodes from a broker),
    // but the cap is cheap defense-in-depth for the mock-broker/test path.
    if (@as(usize, @intCast(records_count)) > records_reader.remaining().len) return error.Malformed;

    const records = try allocator.alloc(Record, @intCast(records_count));
    var records_len: usize = 0;
    errdefer {
        for (records[0..records_len]) |*r| {
            if (r.key) |k| allocator.free(k);
            if (r.value) |v| allocator.free(v);
            for (r.headers) |h| {
                allocator.free(h.key);
                if (h.value) |v| allocator.free(v);
            }
            allocator.free(r.headers);
        }
        allocator.free(records);
    }
    while (records_len < @as(usize, @intCast(records_count))) : (records_len += 1) {
        records[records_len] = try decodeRecord(allocator, records_reader);
    }

    return .{
        .base_offset = base_offset,
        .partition_leader_epoch = partition_leader_epoch,
        .attributes = attributes,
        .last_offset_delta = last_offset_delta,
        .base_timestamp = base_timestamp,
        .max_timestamp = max_timestamp,
        .producer_id = producer_id,
        .producer_epoch = producer_epoch,
        .base_sequence = base_sequence,
        .records = records,
    };
}

/// Decode a single record from `reader`. Owned key/value/header data is
/// allocated from `allocator`.
fn decodeRecord(allocator: std.mem.Allocator, reader: *Reader) !Record {
    const body_len = try primitives.readVarint(reader);
    if (body_len < 0) return error.Malformed;
    const body_end = reader.pos + @as(usize, @intCast(body_len));
    if (body_end > reader.buf.len) return error.EndOfStream;

    // attributes: i8 (unused, 0)
    _ = try primitives.readI8(reader);
    const timestamp_delta = try primitives.readVarlong(reader);
    const offset_delta = try primitives.readVarint(reader);

    // keyLength: varint (-1 = null)
    const key_len = try primitives.readVarint(reader);
    var key: ?[]const u8 = null;
    if (key_len >= 0) {
        const k = try reader.readSlice(@intCast(key_len));
        key = try allocator.dupe(u8, k);
    }
    errdefer if (key) |k| allocator.free(k);

    // valueLength: varint (-1 = null)
    const value_len = try primitives.readVarint(reader);
    var value: ?[]const u8 = null;
    if (value_len >= 0) {
        const v = try reader.readSlice(@intCast(value_len));
        value = try allocator.dupe(u8, v);
    }
    errdefer if (value) |v| allocator.free(v);

    // headersCount: varint (zigzag i32, 0 = no headers)
    const headers_count = try primitives.readVarint(reader);
    if (headers_count < 0) return error.Malformed;
    // Cap: every header is ≥1 byte on the wire (key length varint + key bytes).
    if (@as(usize, @intCast(headers_count)) > reader.remaining().len) return error.Malformed;
    const headers = try allocator.alloc(Header, @intCast(headers_count));
    var headers_len: usize = 0;
    errdefer {
        for (headers[0..headers_len]) |h| {
            allocator.free(h.key);
            if (h.value) |v| allocator.free(v);
        }
        allocator.free(headers);
    }
    while (headers_len < @as(usize, @intCast(headers_count))) : (headers_len += 1) {
        const hkey_len = try primitives.readVarint(reader);
        if (hkey_len < 0) return error.Malformed;
        const hkey_raw = try reader.readSlice(@intCast(hkey_len));
        const hkey = try allocator.dupe(u8, hkey_raw);
        errdefer allocator.free(hkey);

        const hval_len = try primitives.readVarint(reader);
        var hval: ?[]const u8 = null;
        if (hval_len >= 0) {
            const hv = try reader.readSlice(@intCast(hval_len));
            hval = try allocator.dupe(u8, hv);
        }
        errdefer if (hval) |v| allocator.free(v);

        headers[headers_len] = .{ .key = hkey, .value = hval };
    }

    // The record body should be fully consumed. If not, the length field was
    // wrong or the record encoding is malformed.
    if (reader.pos != body_end) return error.Malformed;

    return .{
        .offset_delta = offset_delta,
        .timestamp_delta = timestamp_delta,
        .key = key,
        .value = value,
        .headers = headers,
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "encodeBatch: single record key=k1 value=v1, known CRC32C" {
    // Hand-constructed fixture verified against the spec and an independent
    // CRC32C implementation (Python, polynomial 0x82f63b78 reflected).
    //
    // Batch layout (72 bytes total):
    //   baseOffset: i64 = 0 → 00 00 00 00 00 00 00 00
    //   batchLength: i32 = 60 → 00 00 00 3c
    //     (4 partitionLeaderEpoch + 1 magic + 4 crc + 51 crc_region = 60)
    //   partitionLeaderEpoch: i32 = -1 → ff ff ff ff (default; NOT in CRC, so
    //     the CRC is unchanged from a 0 epoch)
    //   magic: i8 = 2 → 02
    //   crc: u32 = 0xe11f09e7 → e1 1f 09 e7
    //   --- CRC-covered region (51 bytes) ---
    //   attributes: u16 = 0 → 00 00
    //   lastOffsetDelta: i32 = 0 → 00 00 00 00
    //   baseTimestamp: i64 = 0 → 00 00 00 00 00 00 00 00
    //   maxTimestamp: i64 = 0 → 00 00 00 00 00 00 00 00
    //   producerId: i64 = -1 → ff ff ff ff ff ff ff ff
    //   producerEpoch: i16 = -1 → ff ff
    //   baseSequence: i32 = -1 → ff ff ff ff
    //   recordsCount: i32 = 1 → 00 00 00 01
    //   Record:
    //     length: varint(10) → 14 (zigzag(10) = 20 = 0x14)
    //     attributes: i8 = 0 → 00
    //     timestampDelta: varlong(0) → 00
    //     offsetDelta: varint(0) → 00
    //     keyLength: varint(2) → 04 (zigzag(2) = 4)
    //     key: "k1" → 6b 31
    //     valueLength: varint(2) → 04 (zigzag(2) = 4)
    //     value: "v1" → 76 31
    //     headersCount: varint(0) → 00
    const expected = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // baseOffset = 0
        0x00, 0x00, 0x00, 0x3c, // batchLength = 60
        0xff, 0xff, 0xff, 0xff, // partitionLeaderEpoch = -1 (default)
        0x02, // magic = 2
        0xe1, 0x1f, 0x09, 0xe7, // crc32c
        0x00, 0x00, // attributes = 0
        0x00, 0x00, 0x00, 0x00, // lastOffsetDelta = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // baseTimestamp = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // maxTimestamp = 0
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // producerId = -1
        0xff, 0xff, // producerEpoch = -1
        0xff, 0xff, 0xff, 0xff, // baseSequence = -1
        0x00, 0x00, 0x00, 0x01, // recordsCount = 1
        0x14, // record length = 10 (zigzag)
        0x00, // record attributes = 0
        0x00, // timestampDelta = 0
        0x00, // offsetDelta = 0
        0x04, 0x6b, 0x31, // keyLength=2, key="k1"
        0x04, 0x76, 0x31, // valueLength=2, value="v1"
        0x00, // headersCount = 0
    };

    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &.{},
    }};

    var buf: [256]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{});
    try testing.expectEqualSlices(u8, &expected, bytes);
}

test "encodeBatch + decodeBatch round-trip: single record" {
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &.{},
    }};

    var buf: [256]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{});

    var r: Reader = .init(bytes);
    var batch = try decodeBatch(testing.allocator, &r);
    defer batch.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 0), batch.base_offset);
    // Default partition_leader_epoch is -1 (NO_PARTITION_LEADER_EPOCH).
    try testing.expectEqual(@as(i32, -1), batch.partition_leader_epoch);
    try testing.expectEqual(@as(u16, 0), batch.attributes);
    try testing.expectEqual(@as(i32, 0), batch.last_offset_delta);
    try testing.expectEqual(@as(i64, 0), batch.base_timestamp);
    try testing.expectEqual(@as(i64, 0), batch.max_timestamp);
    try testing.expectEqual(@as(i64, -1), batch.producer_id);
    try testing.expectEqual(@as(i16, -1), batch.producer_epoch);
    try testing.expectEqual(@as(i32, -1), batch.base_sequence);
    try testing.expectEqual(@as(usize, 1), batch.records.len);

    const rec = &batch.records[0];
    try testing.expectEqual(@as(i32, 0), rec.offset_delta);
    try testing.expectEqual(@as(i64, 0), rec.timestamp_delta);
    try testing.expectEqualStrings("k1", rec.key.?);
    try testing.expectEqualStrings("v1", rec.value.?);
    try testing.expectEqual(@as(usize, 0), rec.headers.len);

    // All bytes consumed.
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "encodeBatch: null key and null value" {
    const records = [_]Record{.{
        .offset_delta = 5,
        .timestamp_delta = 100,
        .key = null,
        .value = null,
        .headers = &.{},
    }};

    var buf: [256]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{
        .base_offset = 42,
        .base_timestamp = 1700000000000,
    });

    var r: Reader = .init(bytes);
    var batch = try decodeBatch(testing.allocator, &r);
    defer batch.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 42), batch.base_offset);
    try testing.expectEqual(@as(i64, 1700000000000), batch.base_timestamp);
    try testing.expectEqual(@as(i64, 1700000000100), batch.max_timestamp);
    try testing.expectEqual(@as(i32, 5), batch.last_offset_delta);
    try testing.expectEqual(@as(usize, 1), batch.records.len);

    const rec = &batch.records[0];
    try testing.expectEqual(@as(i32, 5), rec.offset_delta);
    try testing.expectEqual(@as(i64, 100), rec.timestamp_delta);
    try testing.expectEqual(@as(?[]const u8, null), rec.key);
    try testing.expectEqual(@as(?[]const u8, null), rec.value);
}

test "encodeBatch: empty value (non-null, zero length)" {
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k",
        .value = "",
        .headers = &.{},
    }};

    var buf: [256]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{});

    var r: Reader = .init(bytes);
    var batch = try decodeBatch(testing.allocator, &r);
    defer batch.deinit(testing.allocator);

    const rec = &batch.records[0];
    try testing.expectEqualStrings("k", rec.key.?);
    // Empty value is non-null but zero-length.
    try testing.expectEqual(@as(usize, 0), rec.value.?.len);
    try testing.expect(rec.value != null);
}

test "encodeBatch: multiple records with timestamp/offset deltas" {
    const records = [_]Record{
        .{ .offset_delta = 0, .timestamp_delta = 0, .key = "k1", .value = "v1", .headers = &.{} },
        .{ .offset_delta = 1, .timestamp_delta = 50, .key = "k2", .value = "v2", .headers = &.{} },
        .{ .offset_delta = 2, .timestamp_delta = 75, .key = null, .value = "v3", .headers = &.{} },
    };

    var buf: [256]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{
        .base_offset = 100,
        .base_timestamp = 1000,
    });

    var r: Reader = .init(bytes);
    var batch = try decodeBatch(testing.allocator, &r);
    defer batch.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 100), batch.base_offset);
    try testing.expectEqual(@as(i32, 2), batch.last_offset_delta);
    try testing.expectEqual(@as(i64, 1000), batch.base_timestamp);
    // maxTimestamp = base + max(delta) = 1000 + 75 = 1075.
    try testing.expectEqual(@as(i64, 1075), batch.max_timestamp);
    try testing.expectEqual(@as(usize, 3), batch.records.len);

    try testing.expectEqualStrings("v1", batch.records[0].value.?);
    try testing.expectEqual(@as(i32, 0), batch.records[0].offset_delta);

    try testing.expectEqualStrings("v2", batch.records[1].value.?);
    try testing.expectEqual(@as(i32, 1), batch.records[1].offset_delta);
    try testing.expectEqual(@as(i64, 50), batch.records[1].timestamp_delta);

    try testing.expectEqual(@as(?[]const u8, null), batch.records[2].key);
    try testing.expectEqualStrings("v3", batch.records[2].value.?);
    try testing.expectEqual(@as(i32, 2), batch.records[2].offset_delta);
}

test "encodeBatchBounded: returns largest prefix within budget" {
    const records = [_]Record{
        .{ .offset_delta = 0, .timestamp_delta = 0, .key = "k1", .value = "v1", .headers = &.{} },
        .{ .offset_delta = 1, .timestamp_delta = 0, .key = "k2", .value = "v2", .headers = &.{} },
        .{ .offset_delta = 2, .timestamp_delta = 0, .key = "k3", .value = "v3", .headers = &.{} },
    };

    var expected_buf: [256]u8 = undefined;
    const expected = try encodeBatch(&expected_buf, records[0..2], .{});

    var buf: [256]u8 = undefined;
    const bounded = try encodeBatchBounded(&buf, &records, expected.len, .{});
    try testing.expectEqual(@as(usize, 2), bounded.consumed);
    try testing.expectEqualSlices(u8, expected, bounded.bytes);

    var r: Reader = .init(bounded.bytes);
    var batch = try decodeBatch(testing.allocator, &r);
    defer batch.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 2), batch.records.len);
    try testing.expectEqual(@as(i32, 1), batch.last_offset_delta);
}

test "encodeBatchBounded: returns zero consumed when one record cannot fit" {
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "payload" ** 32,
        .headers = &.{},
    }};

    var buf: [256]u8 = undefined;
    const bounded = try encodeBatchBounded(&buf, &records, 60, .{});
    try testing.expectEqual(@as(usize, 0), bounded.consumed);
    try testing.expectEqual(@as(usize, 0), bounded.bytes.len);
}

test "encodeBatchBounded: compressed budget uses compressed size" {
    if (!compress.zstd_enabled) return;

    const value = "compressible-payload-" ** 32;
    const records = [_]Record{
        .{ .offset_delta = 0, .timestamp_delta = 0, .key = "k1", .value = value, .headers = &.{} },
        .{ .offset_delta = 1, .timestamp_delta = 0, .key = "k2", .value = value, .headers = &.{} },
        .{ .offset_delta = 2, .timestamp_delta = 0, .key = "k3", .value = value, .headers = &.{} },
        .{ .offset_delta = 3, .timestamp_delta = 0, .key = "k4", .value = value, .headers = &.{} },
    };

    var full_buf: [4096]u8 = undefined;
    var scratch: [4096]u8 = undefined;
    const full = try encodeBatch(&full_buf, &records, .{ .compression = .zstd, .scratch = &scratch });

    try testing.expect(batch_records_region_off + recordLen(records[0]) > full.len);

    var bounded_buf: [4096]u8 = undefined;
    const bounded = try encodeBatchBounded(
        &bounded_buf,
        &records,
        full.len,
        .{ .compression = .zstd, .scratch = &scratch },
    );
    try testing.expectEqual(@as(usize, records.len), bounded.consumed);
    try testing.expect(bounded.bytes.len <= full.len);
}

test "encodeBatch: compression requires scratch" {
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &.{},
    }};

    var buf: [256]u8 = undefined;
    try testing.expectError(
        error.CompressionScratchRequired,
        encodeBatch(&buf, &records, .{ .compression = .zstd }),
    );
    try testing.expectError(
        error.CompressionScratchRequired,
        encodeBatchBounded(&buf, &records, buf.len, .{ .compression = .zstd }),
    );
}

test "encodeBatch: records with headers" {
    const hdrs = [_]Header{
        .{ .key = "h1", .value = "d1" },
        .{ .key = "h2", .value = null },
        .{ .key = "trace-id", .value = "abc123" },
    };
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &hdrs,
    }};

    var buf: [256]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{});

    var r: Reader = .init(bytes);
    var batch = try decodeBatch(testing.allocator, &r);
    defer batch.deinit(testing.allocator);

    const rec = &batch.records[0];
    try testing.expectEqual(@as(usize, 3), rec.headers.len);
    try testing.expectEqualStrings("h1", rec.headers[0].key);
    try testing.expectEqualStrings("d1", rec.headers[0].value.?);
    try testing.expectEqualStrings("h2", rec.headers[1].key);
    try testing.expectEqual(@as(?[]const u8, null), rec.headers[1].value);
    try testing.expectEqualStrings("trace-id", rec.headers[2].key);
    try testing.expectEqualStrings("abc123", rec.headers[2].value.?);
}

test "CRC mismatch detected on flip" {
    // Encode a valid batch, flip one byte in the records region, and confirm
    // decodeBatch returns error.CrcMismatch. This is essential — a producer
    // that sends wrong CRCs gets rejected by the broker.
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &.{},
    }};

    var buf: [256]u8 = undefined;
    const batch_bytes = try encodeBatch(&buf, &records, .{});

    // Flip a byte in the records region (near the end — the 'v' in "v1").
    var corrupted: [256]u8 = undefined;
    @memcpy(corrupted[0..batch_bytes.len], batch_bytes);
    // The 'v' byte (0x76) is at offset 68 in the known fixture.
    corrupted[68] = 0x77; // flip 'v' to 'w'

    var r: Reader = .init(corrupted[0..batch_bytes.len]);
    try testing.expectError(
        error.CrcMismatch,
        decodeBatch(testing.allocator, &r),
    );
}

test "decodeBatch: bad magic returns BadMagic" {
    // Encode a valid batch, then corrupt the magic byte.
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &.{},
    }};

    var buf: [256]u8 = undefined;
    const batch_bytes = try encodeBatch(&buf, &records, .{});

    var corrupted: [256]u8 = undefined;
    @memcpy(corrupted[0..batch_bytes.len], batch_bytes);
    corrupted[16] = 1; // magic byte is at offset 16 (8 baseOffset + 4 batchLength + 4 partitionLeaderEpoch)

    var r: Reader = .init(corrupted[0..batch_bytes.len]);
    try testing.expectError(error.BadMagic, decodeBatch(testing.allocator, &r));
}

test "decodeBatch: truncated batch returns EndOfStream" {
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &.{},
    }};

    var buf: [256]u8 = undefined;
    const batch_bytes = try encodeBatch(&buf, &records, .{});

    // Truncate after the batchLength field — claims 60 bytes but only 10 follow.
    var truncated: [16]u8 = undefined;
    @memcpy(&truncated, batch_bytes[0..16]);

    var r: Reader = .init(&truncated);
    try testing.expectError(error.EndOfStream, decodeBatch(testing.allocator, &r));
}

test "decodeBatch: recordsCount exceeds encoded records leaks nothing" {
    // Fix 4: exercise the record-decode errdefer, not the pre-parse length
    // check. A raw truncation can't reach record decode — the CRC check runs
    // first over the full batch payload and rejects any corruption/short read
    // before a single Record is allocated. To actually enter the record loop
    // and fail mid-way (so the errdefer must free already-decoded records), we
    // build a valid 2-record batch, inflate recordsCount from 2 to 3, and
    // recompute the CRC so decode passes the CRC gate. Decoding then allocates
    // the 3-Record array, decodes records 0 and 1 (each with an owned key +
    // value dupe), and hits EndOfStream reading record 2's length varint off
    // the end of the CRC region. The errdefer must free records[0..2] and the
    // slice; std.testing.allocator fails the test on any leak.
    const records = [_]Record{
        .{ .offset_delta = 0, .timestamp_delta = 0, .key = "k1", .value = "v1", .headers = &.{} },
        .{ .offset_delta = 1, .timestamp_delta = 0, .key = "k2", .value = "v2", .headers = &.{} },
    };

    var buf: [256]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{});

    // recordsCount is the i32 at offset 57 (see the header-layout comment in
    // encodeBatch: attributes@21, then 2+4+8+8+8+2+4 = 36 → recordsCount@57).
    const records_count_off = 57;
    const crc_region_start = 21;
    const crc_off = 17;
    try testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, bytes[records_count_off..][0..4], .big));
    std.mem.writeInt(u32, bytes[records_count_off..][0..4], 3, .big); // claim 3, only 2 encoded
    // Recompute the CRC over attributes→end so decode passes the CRC gate.
    const fixed_crc = Crc32C.hash(bytes[crc_region_start..]);
    std.mem.writeInt(u32, bytes[crc_off..][0..4], fixed_crc, .big);

    var r: Reader = .init(bytes);
    try testing.expectError(
        error.EndOfStream,
        decodeBatch(testing.allocator, &r),
    );
    // std.testing.allocator checks for leaks on test exit.
}

test "Crc32C is Castagnoli (polynomial 0x1edc6f41)" {
    // Verify that std.hash.crc.Crc32Iscsi uses the Castagnoli polynomial.
    // Source: std/hash/crc.zig line 800:
    //   pub const Crc32Iscsi = Crc(u32, .{
    //     .polynomial = 0x1edc6f41,
    //     .initial = 0xffffffff,
    //     .reflect_input = true,
    //     .reflect_output = true,
    //     .xor_output = 0xffffffff,
    //   });
    // This is CRC-32C (Castagnoli), the polynomial required by the Kafka
    // record batch spec.
    //
    // Known CRC32C test vector: CRC32C("123456789") = 0xe3069283
    // (from the CRC32C catalog: check value for CRC-32C/ISO-HDLC).
    const check = Crc32C.hash("123456789");
    try testing.expectEqual(@as(u32, 0xe3069283), check);
}

test "encodeBatch: zero records (empty batch)" {
    // An empty batch with zero records. This can happen when a producer
    // needs to preserve a sequence number after compaction (see spec note).
    var buf: [256]u8 = undefined;
    const bytes = try encodeBatch(&buf, &.{}, .{});

    var r: Reader = .init(bytes);
    var batch = try decodeBatch(testing.allocator, &r);
    defer batch.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 0), batch.records.len);
    try testing.expectEqual(@as(i32, 0), batch.last_offset_delta);
    try testing.expectEqual(@as(i64, 0), batch.max_timestamp);
}

test "encodeBatch: partition_leader_epoch is set" {
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = null,
        .value = "x",
        .headers = &.{},
    }};

    var buf: [256]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{
        .partition_leader_epoch = 42,
    });

    var r: Reader = .init(bytes);
    var batch = try decodeBatch(testing.allocator, &r);
    defer batch.deinit(testing.allocator);

    try testing.expectEqual(@as(i32, 42), batch.partition_leader_epoch);
}

test "encodeBatch: idempotent producer fields (producerId/epoch/baseSequence)" {
    // An idempotent batch carries a real producer_id (not -1), producer_epoch,
    // and base_sequence. The broker infers idempotency from producer_id != -1
    // — there is NO idempotent bit in the attributes field (bit 6 is
    // hasDeleteHorizonMs, KIP-516). See:
    // https://kafka.apache.org/43/implementation/message-format/
    const records = [_]Record{
        .{ .offset_delta = 0, .timestamp_delta = 0, .key = "k1", .value = "v1", .headers = &.{} },
        .{ .offset_delta = 1, .timestamp_delta = 5, .key = "k2", .value = "v2", .headers = &.{} },
    };

    var buf: [256]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{
        .producer_id = 7000,
        .producer_epoch = 1,
        .base_sequence = 42,
    });

    var r: Reader = .init(bytes);
    var batch = try decodeBatch(testing.allocator, &r);
    defer batch.deinit(testing.allocator);

    // The idempotent fields must round-trip exactly.
    try testing.expectEqual(@as(i64, 7000), batch.producer_id);
    try testing.expectEqual(@as(i16, 1), batch.producer_epoch);
    try testing.expectEqual(@as(i32, 42), batch.base_sequence);
    // isIdempotent is inferred from producer_id != -1, NOT from attributes.
    try testing.expect(batch.isIdempotent());
    // Attributes must NOT have any special idempotent bit set — bit 6 is
    // hasDeleteHorizonMs, not idempotent.
    try testing.expectEqual(@as(u16, 0), batch.attributes);
}

test "encodeBatch: non-idempotent sentinels (producerId=-1, epoch=-1, seq=-1)" {
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &.{},
    }};

    var buf: [256]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{});

    var r: Reader = .init(bytes);
    var batch = try decodeBatch(testing.allocator, &r);
    defer batch.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, -1), batch.producer_id);
    try testing.expectEqual(@as(i16, -1), batch.producer_epoch);
    try testing.expectEqual(@as(i32, -1), batch.base_sequence);
    try testing.expect(!batch.isIdempotent());
}

test "encodeBatch: gzip and lz4 return CompressionNotImplemented" {
    // gzip (codec 1) and lz4 (codec 3) are wire-format constants but not yet
    // implemented. The compress slot returns NotImplemented, which encodeBatch
    // maps to CompressionNotImplemented.
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &.{},
    }};
    var buf: [256]u8 = undefined;
    var scratch: [256]u8 = undefined;
    try testing.expectError(
        error.CompressionNotImplemented,
        encodeBatch(&buf, &records, .{ .compression = .gzip, .scratch = &scratch }),
    );
    try testing.expectError(
        error.CompressionNotImplemented,
        encodeBatch(&buf, &records, .{ .compression = .lz4, .scratch = &scratch }),
    );
}

test "encodeBatch: zstd without -Dzstd=true returns CompressionNotImplemented" {
    // When the build did NOT enable zstd, requesting zstd compression is a
    // runtime error (the codec slot returns NotImplemented). This test only
    // runs in the default build; the enabled build exercises the round-trip.
    if (compress.zstd_enabled) return;
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &.{},
    }};
    var buf: [256]u8 = undefined;
    var scratch: [256]u8 = undefined;
    try testing.expectError(
        error.CompressionNotImplemented,
        encodeBatch(&buf, &records, .{ .compression = .zstd, .scratch = &scratch }),
    );
}

test "encodeBatch + decodeBatch: zstd round-trip (records region compressed)" {
    if (!compress.zstd_enabled) return;
    // Repetitive payloads so zstd actually shrinks the region; verifies the
    // compress-before-CRC ordering (decode recomputes CRC over the compressed
    // bytes) and that recordsCount stays the uncompressed count.
    const v1 = "payload-payload-payload-payload-payload" ** 4;
    const v2 = "another-record-value-another-record-value" ** 4;
    const records = [_]Record{
        .{ .offset_delta = 0, .timestamp_delta = 0, .key = "k1", .value = v1, .headers = &.{} },
        .{ .offset_delta = 1, .timestamp_delta = 5, .key = "k2", .value = v2, .headers = &.{} },
    };

    var buf: [2048]u8 = undefined;
    var scratch: [2048]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{ .compression = .zstd, .scratch = &scratch });

    // The attributes field (offset 21, u16 big-endian) must carry zstd bits (4).
    try testing.expectEqual(@as(u16, 4), std.mem.readInt(u16, bytes[21..23], .big));

    var r: Reader = .init(bytes);
    var batch = try decodeBatch(testing.allocator, &r);
    defer batch.deinit(testing.allocator);

    try testing.expectEqual(Compression.zstd, batch.compression());
    try testing.expectEqual(@as(usize, 2), batch.records.len);
    try testing.expectEqualStrings("k1", batch.records[0].key.?);
    try testing.expectEqualStrings(v1, batch.records[0].value.?);
    try testing.expectEqualStrings("k2", batch.records[1].key.?);
    try testing.expectEqualStrings(v2, batch.records[1].value.?);
    try testing.expectEqual(@as(i32, 1), batch.last_offset_delta);
    // All bytes consumed.
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "encodeBatch: zstd CRC verifies over compressed bytes" {
    if (!compress.zstd_enabled) return;
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "compressible-compressible-compressible" ** 4,
        .headers = &.{},
    }};
    var buf: [1024]u8 = undefined;
    var scratch: [1024]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{ .compression = .zstd, .scratch = &scratch });
    // Recompute CRC over attributes→end (offset 21) and compare to the stored
    // crc at offset 17 — must match, proving CRC was taken over compressed bytes.
    const stored = std.mem.readInt(u32, bytes[17..21], .big);
    const recomputed = Crc32C.hash(bytes[21..]);
    try testing.expectEqual(recomputed, stored);

    // Flip a byte in the compressed region → decode must reject on CRC.
    var corrupt: [1024]u8 = undefined;
    @memcpy(corrupt[0..bytes.len], bytes);
    corrupt[bytes.len - 1] ^= 0xff;
    var r: Reader = .init(corrupt[0..bytes.len]);
    try testing.expectError(error.CrcMismatch, decodeBatch(testing.allocator, &r));
}

test "encodeBatch + decodeBatch: snappy round-trip (records region compressed)" {
    // Snappy compression (codec attribute 2, Kafka's legacy default). The
    // encoder is literal-only (no match-finding), but the output is a valid
    // snappy block that the decoder (which handles all tag types) can
    // decompress. Verifies the compress-before-CRC ordering and that
    // recordsCount stays the uncompressed count.
    const v1 = "payload-payload-payload-payload-payload" ** 4;
    const v2 = "another-record-value-another-record-value" ** 4;
    const records = [_]Record{
        .{ .offset_delta = 0, .timestamp_delta = 0, .key = "k1", .value = v1, .headers = &.{} },
        .{ .offset_delta = 1, .timestamp_delta = 5, .key = "k2", .value = v2, .headers = &.{} },
    };

    var buf: [2048]u8 = undefined;
    var scratch: [2048]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{ .compression = .snappy, .scratch = &scratch });

    // The attributes field (offset 21, u16 big-endian) must carry snappy bits (2).
    try testing.expectEqual(@as(u16, 2), std.mem.readInt(u16, bytes[21..23], .big));

    var r: Reader = .init(bytes);
    var batch = try decodeBatch(testing.allocator, &r);
    defer batch.deinit(testing.allocator);

    try testing.expectEqual(Compression.snappy, batch.compression());
    try testing.expectEqual(@as(usize, 2), batch.records.len);
    try testing.expectEqualStrings("k1", batch.records[0].key.?);
    try testing.expectEqualStrings(v1, batch.records[0].value.?);
    try testing.expectEqualStrings("k2", batch.records[1].key.?);
    try testing.expectEqualStrings(v2, batch.records[1].value.?);
    try testing.expectEqual(@as(i32, 1), batch.last_offset_delta);
    // All bytes consumed.
    try testing.expectEqual(@as(usize, 0), r.remaining().len);
}

test "encodeBatch: snappy CRC verifies over compressed bytes" {
    // The CRC covers attributes→end, so it must be computed over the
    // snappy-compressed records region (not the uncompressed records). Flip a
    // byte in the compressed region → decode must reject on CRC.
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "compressible-compressible-compressible" ** 4,
        .headers = &.{},
    }};
    var buf: [1024]u8 = undefined;
    var scratch: [1024]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{ .compression = .snappy, .scratch = &scratch });

    // Recompute CRC over attributes→end (offset 21) and compare to stored.
    const stored = std.mem.readInt(u32, bytes[17..21], .big);
    const recomputed = Crc32C.hash(bytes[21..]);
    try testing.expectEqual(recomputed, stored);

    // Flip a byte in the compressed region → decode must reject on CRC.
    var corrupt: [1024]u8 = undefined;
    @memcpy(corrupt[0..bytes.len], bytes);
    corrupt[bytes.len - 1] ^= 0xff;
    var r: Reader = .init(corrupt[0..bytes.len]);
    try testing.expectError(error.CrcMismatch, decodeBatch(testing.allocator, &r));
}

test "encodeBatch: zero heap allocation — bounded by caller buffer" {
    // encodeBatch takes no allocator and touches no std.heap.* — the encode is
    // structurally zero-alloc. The runtime proof that there is no hidden heap
    // fallback: a buffer one byte too small returns error.BufferTooSmall
    // instead of silently growing. The pre-repair code fell back to the global
    // heap when its fixed temp overflowed and SUCCEEDED here — so this
    // assertion fails against that regression (verified during the repair).
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &.{},
    }};

    // The known fixture is exactly 72 bytes. Exact fit succeeds.
    var exact: [72]u8 = undefined;
    const bytes = try encodeBatch(&exact, &records, .{});
    try testing.expectEqual(@as(usize, 72), bytes.len);

    // One byte short must error, not allocate.
    var short: [71]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, encodeBatch(&short, &records, .{}));

    // (checkAllAllocationFailures is not applicable: encodeBatch takes no
    // allocator, so there is no passed allocator to fail. The signature plus
    // the BufferTooSmall bound above are the guarantee — there is no heap
    // fallback for it to reach.)
}

// ---------------------------------------------------------------------------
// Fuzz target
// ---------------------------------------------------------------------------

const record_batch_corpus: []const []const u8 = &.{
    &.{}, // empty
    // Valid 72-byte single-record batch from the hand-constructed fixture.
    &.{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // baseOffset = 0
        0x00, 0x00, 0x00, 0x3c, // batchLength = 60
        0xff, 0xff, 0xff, 0xff, // partitionLeaderEpoch = -1
        0x02, // magic = 2
        0xe1, 0x1f, 0x09, 0xe7, // crc32c
        0x00, 0x00, // attributes = 0
        0x00, 0x00, 0x00, 0x00, // lastOffsetDelta = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // baseTimestamp = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // maxTimestamp = 0
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // producerId = -1
        0xff, 0xff, // producerEpoch = -1
        0xff, 0xff, 0xff, 0xff, // baseSequence = -1
        0x00, 0x00, 0x00, 0x01, // recordsCount = 1
        0x14, // record length = 10
        0x00, // record attributes = 0
        0x00, // timestampDelta = 0
        0x00, // offsetDelta = 0
        0x04, 0x6b, 0x31, // keyLength=2, key="k1"
        0x04, 0x76, 0x31, // valueLength=2, value="v1"
        0x00, // headersCount = 0
    },
    // Truncated after batchLength.
    &.{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3c },
    // Bad magic: same as valid fixture but magic byte set to 0x01.
    &.{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x3c, 0xff, 0xff, 0xff, 0xff,
        0x01, // magic = 1 (bad)
        0xe1,
        0x1f,
        0x09,
        0xe7,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0x00,
        0x00,
        0x00,
        0x01,
        0x14,
        0x00,
        0x00,
        0x00,
        0x04,
        0x6b,
        0x31,
        0x04,
        0x76,
        0x31,
        0x00,
    },
    &([_]u8{0x00} ** 64), // all zeros
    &([_]u8{0xff} ** 64), // all ones
};

test "decodeBatch: huge records_count with few bytes → Malformed (no huge alloc)" {
    // A valid batch header with records_count inflated to a huge value. The
    // CRC is recomputed so the CRC gate passes, then the count cap rejects
    // before the alloc. This proves the cap fires on the record-allocation
    // path (not just the CRC gate).
    const records = [_]Record{.{
        .offset_delta = 0,
        .timestamp_delta = 0,
        .key = "k1",
        .value = "v1",
        .headers = &.{},
    }};

    var buf: [256]u8 = undefined;
    const bytes = try encodeBatch(&buf, &records, .{});

    // Inflate recordsCount from 1 to 1_000_000 and recompute the CRC.
    const records_count_off = 57;
    const crc_region_start = 21;
    const crc_off = 17;
    std.mem.writeInt(u32, bytes[records_count_off..][0..4], 1_000_000, .big);
    const fixed_crc = Crc32C.hash(bytes[crc_region_start..]);
    std.mem.writeInt(u32, bytes[crc_off..][0..4], fixed_crc, .big);

    var r: Reader = .init(bytes);
    try testing.expectError(error.Malformed, decodeBatch(testing.allocator, &r));
}

test "fuzz record_batch decodeBatch" {
    try std.testing.fuzz(std.testing.allocator, fuzzDecodeBatch, .{ .corpus = record_batch_corpus });
}

fn fuzzDecodeBatch(allocator: std.mem.Allocator, input: []const u8) !void {
    var r: Reader = .init(input);
    var batch = decodeBatch(allocator, &r) catch |err| switch (err) {
        error.EndOfStream,
        error.Malformed,
        error.CrcMismatch,
        error.BadMagic,
        error.CompressionNotImplemented,
        error.OutOfMemory,
        => return,
        else => return err,
    };
    defer batch.deinit(allocator);
}
