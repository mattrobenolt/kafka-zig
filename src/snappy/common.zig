//! Shared helpers for the snappy block codec: the LEB128 varint used for the
//! uncompressed-length prefix.

/// Number of bytes to encode `value` as an unsigned LEB128 varint.
pub fn uvarintSize(value: usize) usize {
    if (value == 0) return 1;
    var size: usize = 0;
    var v = value;
    while (v != 0) : (v >>= 7) size += 1;
    return size;
}

/// Write `value` as an unsigned LEB128 varint into `out`, returning bytes
/// written. `error.BufferTooSmall` when `out` is too small.
pub fn writeUvarint(out: []u8, value: usize) error{BufferTooSmall}!usize {
    var v = value;
    var pos: usize = 0;
    while (v >= 0x80) {
        if (pos >= out.len) return error.BufferTooSmall;
        out[pos] = @as(u8, @truncate(v)) | 0x80;
        pos += 1;
        v >>= 7;
    }
    if (pos >= out.len) return error.BufferTooSmall;
    out[pos] = @as(u8, @truncate(v));
    return pos + 1;
}

/// Read an unsigned LEB128 varint from `input` at `pos`, advancing `pos`.
/// `error.DecompressionFailed` on truncation or overflow (varint > 5 bytes).
pub fn readUvarint(input: []const u8, pos: *usize) error{DecompressionFailed}!usize {
    var result: usize = 0;
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        if (pos.* >= input.len) return error.DecompressionFailed;
        const byte = input[pos.*];
        pos.* += 1;
        result |= (@as(usize, byte & 0x7F)) << @intCast(i * 7);
        if (byte & 0x80 == 0) return result;
    }
    return error.DecompressionFailed; // varint too long (> 5 bytes)
}
