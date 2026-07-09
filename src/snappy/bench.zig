//! Snappy block codec benchmarks. Run with `zig build bench -Doptimize=ReleaseFast`.
//!
//! Covers compress and decompress across several data shapes (repetitive text,
//! random, html-like, a single-byte run, and the 32 KiB Xerial block boundary).
//! Throughput is reported as MB/s over the *uncompressed* input size so compress
//! and decompress are directly comparable.
//!
//! Output is benchstat-friendly: `zig build bench -Doptimize=ReleaseFast -- --count=10 > bench.txt`.

const std = @import("std");

const bench = @import("benchmark");
const snappy = @import("snappy");

/// Input corpus shapes. Each returns a freshly-allocated buffer the benchmark
/// is responsible for freeing (once, in setup — not in the timed loop).
const Shape = enum {
    text, // repetitive English-ish text (highly compressible)
    random, // PRNG bytes (incompressible -> bail to literal)
    html, // tag-heavy markup (compressible, short matches)
    rle, // single-byte run (extreme RLE)
    mixed, // structured + random (realistic-ish record batch)
};

fn makeShape(allocator: std.mem.Allocator, shape: Shape, len: usize) ![]u8 {
    const buf = try allocator.alloc(u8, len);
    switch (shape) {
        .text => {
            const phrase = "the quick brown fox jumps over the lazy dog. ";
            var i: usize = 0;
            while (i < len) {
                const n = @min(phrase.len, len - i);
                @memcpy(buf[i..][0..n], phrase[0..n]);
                i += n;
            }
        },
        .random => {
            var rng: std.Random.DefaultPrng = .init(0xC0FFEE);
            for (buf) |*b| b.* = rng.random().int(u8);
        },
        .html => {
            const phrase = "<div class=\"row\"><span>hello</span><span>kafka</span></div>";
            var i: usize = 0;
            while (i < len) {
                const n = @min(phrase.len, len - i);
                @memcpy(buf[i..][0..n], phrase[0..n]);
                i += n;
            }
        },
        .rle => @memset(buf, 0x41),
        .mixed => {
            var rng: std.Random.DefaultPrng = .init(0x5A4BEEF);
            for (buf, 0..) |*b, i| b.* = @truncate(rng.random().int(u8) ^ @as(u8, @truncate(i)));
        },
    }
    return buf;
}

fn shapeName(shape: Shape) []const u8 {
    return switch (shape) {
        .text => "text",
        .random => "random",
        .html => "html",
        .rle => "rle",
        .mixed => "mixed",
    };
}

/// Compress benchmarks. One sub-benchmark per shape, at a fixed 32 KiB block
/// (the Xerial chunk size). Reports MB/s over the uncompressed input.
pub fn benchmarkCompress(b: *bench.B) !void {
    inline for (.{ Shape.text, Shape.random, Shape.html, Shape.rle, Shape.mixed }) |shape| {
        _ = try b.run(shapeName(shape), struct {
            fn run(bb: *bench.B) !void {
                const input = try makeShape(bb.allocator, shape, 32 * 1024);
                defer bb.allocator.free(input);
                const bound = snappy.maxCompressedLength(input.len);
                const comp = try bb.allocator.alloc(u8, bound);
                defer bb.allocator.free(comp);

                while (try bb.loop()) {
                    const n = try snappy.compressBlock(input, comp);
                    bb.keepAlive(n);
                }
                bb.setBytes(@intCast(input.len));
            }
        }.run);
    }
}

/// Decompress benchmarks. The compressed input is prepared once (outside the
/// timed loop), then decompressed repeatedly. Reports MB/s over the
/// *decompressed* size.
pub fn benchmarkDecompress(b: *bench.B) !void {
    inline for (.{ Shape.text, Shape.random, Shape.html, Shape.rle, Shape.mixed }) |shape| {
        _ = try b.run(shapeName(shape), struct {
            fn run(bb: *bench.B) !void {
                const input = try makeShape(bb.allocator, shape, 32 * 1024);
                defer bb.allocator.free(input);
                const bound = snappy.maxCompressedLength(input.len);
                const comp = try bb.allocator.alloc(u8, bound);
                defer bb.allocator.free(comp);
                const clen = try snappy.compressBlock(input, comp);

                const dlen = try snappy.decompressedBlockLen(comp[0..clen]);
                const back = try bb.allocator.alloc(u8, dlen);
                defer bb.allocator.free(back);

                while (try bb.loop()) {
                    const n = try snappy.decompressBlock(comp[0..clen], back);
                    bb.keepAlive(n);
                }
                bb.setBytes(@intCast(dlen));
            }
        }.run);
    }
}

/// Compression ratio (compressed / uncompressed) per shape, reported as a
/// custom metric so the benchmark table shows how much the match-finder helps.
/// Not a timed loop — it runs once to print the ratio alongside the throughput.
pub fn benchmarkRatio(b: *bench.B) !void {
    inline for (.{ Shape.text, Shape.random, Shape.html, Shape.rle, Shape.mixed }) |shape| {
        _ = try b.run(shapeName(shape), struct {
            fn run(bb: *bench.B) !void {
                const input = try makeShape(bb.allocator, shape, 32 * 1024);
                defer bb.allocator.free(input);
                const bound = snappy.maxCompressedLength(input.len);
                const comp = try bb.allocator.alloc(u8, bound);
                defer bb.allocator.free(comp);
                const clen = try snappy.compressBlock(input, comp);
                const ratio = @as(f64, @floatFromInt(clen)) / @as(f64, @floatFromInt(input.len));
                try bb.reportMetric(ratio, "ratio");
                // One iteration so the harness is happy; the metric is the point.
                while (try bb.loop()) {
                    bb.keepAlive(clen);
                }
            }
        }.run);
    }
}
