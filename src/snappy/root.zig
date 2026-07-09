//! Standalone Snappy block codec (raw block format, no framing). No Kafka
//! imports. Exposed as its own build module (`snappy`) so it can be lifted to a
//! separate repo unchanged. Imports only `std`.
//!
//! The Kafka wire layer uses the Xerial-framed format (a 16-byte header plus
//! blocks of `BE int32 compressed_size + raw snappy block`). That framing lives
//! in `src/wire/compress.zig`; this module is just the reusable raw-block
//! codec: a hash-table match-finder encoder and a SIMD-accelerated decoder.
//!
//! Format: https://github.com/google/snappy/blob/main/format_description.txt

const std = @import("std");

const decode = @import("decode.zig");
/// Decompress a raw snappy block from `input` into `out`. Returns bytes written.
/// `error.BufferTooSmall` when `out` is too small (size via
/// `decompressedBlockLen`); `error.DecompressionFailed` on corrupt input.
/// Zero heap allocation.
pub const decompressBlock = decode.decompressBlock;
/// Decompressed byte length of a raw snappy block (the leading varint).
pub const decompressedBlockLen = decode.decompressedBlockLen;
const encode = @import("encode.zig");
/// Worst-case raw-block compressed size for `input_len` bytes (varint length
/// prefix + literal blowup bound). Size `out` to this before `compressBlock`.
pub const maxCompressedLength = encode.maxCompressedLength;
/// Compress `src` into `out` as a raw snappy block. Returns bytes written.
/// `error.BufferTooSmall` when `out` is too small — size it via
/// `maxCompressedLength`. Zero heap allocation.
pub const compressBlock = encode.compressBlock;

test {
    _ = encode;
    _ = decode;
    std.testing.refAllDecls(@This());
}
