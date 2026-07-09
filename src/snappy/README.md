# snappy

A standalone Snappy raw-block codec in pure Zig — a hash-table match-finder
encoder and a SIMD-accelerated decoder. No Kafka knowledge, no C dependency,
no heap allocation on the codec paths. Exposed as the `snappy` build module so
it can be lifted to a separate repo unchanged.

This implements the [Snappy block format][format], not the framing/streaming
format. The Kafka wire layer's Xerial framing (a 16-byte header plus
length-prefixed raw blocks) lives in `src/wire/compress.zig` and delegates the
raw-block work here.

## API

```zig
const snappy = @import("snappy");

// Worst-case compressed size — size `out` to this before compress.
snappy.maxCompressedLength(input_len) usize

// Compress `src` into `out` as a raw snappy block. Zero heap allocation.
// `src.len` must be <= 65536 (snappy's u16-position limit); the Xerial
// framing layer splits larger inputs into 32 KiB blocks.
snappy.compressBlock(src: []const u8, out: []u8) error{BufferTooSmall}!usize

// Decompress a raw snappy block. Zero heap allocation.
snappy.decompressBlock(input: []const u8, out: []u8)
    error{ BufferTooSmall, DecompressionFailed }!usize

// Read the decompressed length (the leading varint) to size `out`.
snappy.decompressedBlockLen(input: []const u8)
    error{DecompressionFailed}!usize
```

Files: `root.zig` (public surface), `encode.zig` (match-finder), `decode.zig`
(SIMD decoder + golden vectors), `common.zig` (LEB128 varint), `bench.zig`
(benchmarks).

## Encoder

A port of the algorithm in [klauspost/compress][kp]'s `encodeBlockSnappyGo64K`
(which is itself descended from the Snappy-Go reference and is the same shape
as Google's C++ `CompressFragment`):

- A `1 << 14` `u16` position hash table (a 32 KiB stack frame — zero heap
  allocation). The 6-byte hash is `(u << 16) * prime6bytes >> (64 - 14)` with
  `prime6bytes = 227718039650203`.
- A skip heuristic: the longer the run of bytes with no match, the larger the
  stride, so incompressible regions are skipped quickly instead of hashed
  byte-by-byte.
- Match extension 8 bytes at a time: XOR two `u64` loads and `@ctz` to find the
  first mismatch byte — no byte-by-byte compare loop. On AArch64 this lowers to
  `eor` + `rbit` + `clz`.
- A bail threshold: if the encoded form would exceed `src - src>>5 - 5` bytes,
  the whole block is emitted as a single literal. Snappy guarantees it never
  expands past its bound.
- A repeat-offset check that catches RLE-style input (runs of the same record)
  without a fresh hash lookup.

The previous implementation in `compress.zig` was literal-only — it produced
valid blocks but with a ~1:1 ratio (slightly *worse* than verbatim due to tag
overhead). Real match-finding turns that into actual compression: on a 32 KiB
block of repetitive text the ratio is ~0.05 (20:1); HTML and single-byte runs
compress similarly. Incompressible (random) input correctly bails to a literal
at ratio 1.0.

Snappy copies are capped at 64 bytes, so long matches split into 60-byte
copy-2 chunks (60, not 64, so the final remainder is guaranteed `>= 4` and can
use a copy-1). A `<= 64 KiB` block never produces a 4-byte-offset copy (offsets
fit in `u16`), but the decoder handles copy-4 for compatibility with blocks
produced by other compressors.

## Decoder

The tag stream is decoded scalarly (it's inherently sequential — a copy can
reference bytes written moments ago). The win is in the copy operations:

- **`offset >= length`** (non-overlapping): a plain `@memcpy`, which the
  compiler lowers to vector loads/stores.
- **`offset <= 16`** (overlapping / RLE): the `offset`-byte pattern repeats.
  Load the pattern bytes, permute them into the repeating 16-byte window with
  one SIMD shuffle — `tbl` on AArch64, `pshufb` on x86-64 — and store 16-byte
  chunks, re-shuffling each chunk to advance the phase. The shuffle masks are
  comptime-generated (`mask[i] = i % offset` for generation,
  `(16 + i) % offset` for the per-chunk phase advance). For offsets that divide
  16 (1, 2, 4, 8, 16) the reshuffle is a no-op; for others it shifts the phase.
- **`offset > 16`**: the 16-byte source window doesn't overlap the destination
  within a 16-byte span, so a vectorized chunked `@memcpy` is safe.
- **Other architectures**: a portable byte-wise permute fallback (the compiler
  may still recognize it as a shuffle).

One subtlety that bit us: the output buffer is sized to the decompressed
length with **no slop**, so a 16-byte pattern load at `out[pos - offset]` can
overread past `out.len` near the tail. The fix is to copy just the `offset`
source bytes into a zero-padded `[16]u8` before the shuffle — the mask only
references indices `< offset`, so the padding is dead. This is verified by the
golden-vector overrun checks (see below).

## Disassembly

On an Apple M1 (AArch64) `ReleaseFast` build, the three optimizations are
visible in the machine code, not just the source:

- `tbl.16b` — the SIMD shuffle for overlapping copies (5 call sites).
- `ldr q` / `str q` — 16-byte vector loads/stores for the literal and
  non-overlapping copy paths (~276 in the benchmark binary).
- `eor` + `rbit` + `clz` — `@ctz` for the 8-byte match extension.

Inspect your own build with:

```sh
zig build bench -Doptimize=ReleaseFast
otool -tv $(find .zig-cache -name benchmark -type f | head -1) | grep tbl.16b
```

## Benchmarks

`zig build bench -Doptimize=ReleaseFast` (Go-style, via
[mattrobenolt/zig-benchmark][zigbench]). 32 KiB blocks on an Apple M1:

| shape   | compress MB/s | decompress MB/s | ratio |
|---------|---------------|-----------------|-------|
| text    | ~16 000       | ~9 100          | 0.05  |
| html    | ~15 200       | ~9 100          | 0.05  |
| rle     | ~18 800       | ~5 100          | 0.05  |
| random  | ~24 100       | ~61 100         | 1.00  |
| mixed   | ~23 700       | ~61 600         | 1.00  |

Incompressible input decompresses fastest (~61 GB/s — it's just literal
copies). RLE is the slowest decompress path because it's many short
overlapping copies, each doing a shuffle.

## Testing & golden vectors

The decoder is checked against the authoritative conformance vectors ported
from [golang/snappy][golsnappy]'s `TestDecode`, `TestDecodeCopy4`, and
`TestDecodeLengthOffset`:

- **`TestDecode` table** (30 cases): every tag type, every extended-literal
  length form (tags 60–63), and the corrupt-input rejections (zero offset,
  offset past start, inconsistent decoded length, truncated length/offset
  bytes). Each case includes an **overrun check**: the output buffer is
  pre-filled with cycling sentinel bytes and every byte past the decoded
  length must be untouched afterward — so a copy that writes past the end is
  caught immediately.
- **`TestDecodeCopy4`**: a 64 KiB literal plus a copy-4 at offset 65540 — the
  only case exercising the 4-byte-offset path with a real large offset.
- **`TestDecodeLengthOffset`**: an exhaustive `length × offset × suffixLen`
  sweep (6156 combinations) of a literal + copy2 + literal pattern, with the
  overrun check. Hammers the SIMD copy path across every small offset/length
  pair, including overlapping RLE.
- The `format_description.txt` hand examples and the spec's varint examples.

**On encoder golden vectors**: Snappy does not mandate a canonical compressed
form — as golang/snappy's own test code puts it, "there is more than one valid
encoding of any given input." So we do **not** assert byte-identical encoder
output against reference corpora. That would test "we match a specific
encoder's heuristics," not correctness. Our encoder produces valid blocks that
round-trip through our decoder and through any conformant snappy decoder; the
decode vectors above are the real interop bar.

Run everything with `zig build test` (359 tests, Debug and ReleaseFast).

## S2 vs Snappy

[klauspost/compress][kp] ships two related things in its `snappy/` and `s2/`
packages. **S2** is Klaus Post's own format — a superset of Snappy that
*decodes* standard Snappy but *encodes* a richer, Snappy-incompatible stream.
The extensions:

- **Repeat offsets**: a copy-1 tag with offset 0 means "reuse the previous
  offset" (an LZ4-style distance cache). Standard Snappy has no such state and
  treats offset 0 as corrupt. This helps a lot on data with clustered matches
  at the same distance.
- **Larger blocks**: up to 4 MiB vs Snappy's hard 64 KiB.
- **Concurrent streaming compression** and extra **better/best** encoder modes.

The relationship is asymmetric: **S2 decodes Snappy, but Snappy does not decode
S2.** His `snappy/` package is a thin wrapper over the `s2/` engine that caps
blocks at 64 KiB and uses the Snappy-compatible encoder path
(`encodeBlockSnappyGo64K`) so the output stays decodable by any standard Snappy
consumer — snappy-java, kafka-python, C++ snappy.

Kafka's wire format is standard Snappy in the Xerial frame, so this module
ports the **Snappy-compatible path only**: no repeat offsets, blocks capped at
64 KiB, and the decoder rejects offset-0 copies as `DecompressionFailed`
rather than interpreting them as repeats. The S2 ratio tricks would break Kafka
interop, which is a non-starter. The compression gain we got came from the part
Snappy always had but the old code lacked — real hash-table match-finding.

## Licensing & attribution

This module is original Zig, written from scratch. The **algorithm** (hash
table match-finding) and the **format** are uncopyrightable ideas and a public
specification respectively; the **expression** here is ours. We did, however,
study three reference implementations closely and port the test vectors, so
attribution is in order:

- **[klauspost/compress][kp]** (`s2/encode_all.go`, `encode_go.go`) — BSD-3-Clause.
  The encoder algorithm (`encodeBlockSnappyGo64K`), the `prime6bytes` hash, the
  skip heuristic, and the bail-to-literal threshold are ported from here.
- **[golang/snappy][golsnappy]** (`snappy_test.go`) — BSD-3-Clause. The golden
  decode vector tables (`TestDecode`, `TestDecodeCopy4`,
  `TestDecodeLengthOffset`) and the overrun-check sentinel technique are ported
  from here.
- **[google/snappy][gpcpp]** (`snappy.cc`, `snappy-internal.h`, `snappy.h`) —
  BSD-3-Clause. The SIMD overlapping-copy technique (`IncrementalCopy` /
  `Copy64BytesWithPatternExtension` with PSHUFB/TBL shuffle masks and the
  generation+reshuffle mask tables) and the block/hash-table constants
  (`kBlockLog = 16`, `kMaxHashTableBits`, hash magic `0x1e35a7bd`) were studied
  here. Our decoder's shuffle approach is the Zig equivalent of their
  `pattern_generation_masks` / `pattern_reshuffle_masks`.

All three are BSD-3-Clause, which is compatible with this repository's
[Apache-2.0](../LICENSE) license. The BSD-3-Clause "no endorsement" clause is
satisfied by this attribution; no code was copied verbatim.

[format]: https://github.com/google/snappy/blob/main/format_description.txt
[kp]: https://github.com/klauspost/compress
[golsnappy]: https://github.com/golang/snappy
[gpcpp]: https://github.com/google/snappy
[zigbench]: https://github.com/mattrobenolt/zig-benchmark
