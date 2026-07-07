# kafka-zig — just recipes (CI entry point)
# Requires Zig 0.15.2 and `just` on PATH. Use `nix develop` for the full
# devshell (zstd static lib, etc.).

[doc("Show available recipes")]
[private]
default:
    @just --list

[doc("Run tests")]
[group("test")]
test:
    zig build test --summary all

[doc("Run tests with zstd compression enabled")]
[group("test")]
test-zstd:
    zig build -Dzstd=true test --summary all

[doc("Format source in place")]
[group("lint")]
fmt:
    zig fmt .

[doc("Check formatting without modifying")]
[group("lint")]
fmt-check:
    zig fmt --check $(git ls-files '*.zig')

[doc("Run ziglint")]
[group("lint")]
lint:
    # Z017: `return try` is not redundant when the return type is an optional
    # error-union (e.g. `!?[]const u8`) and the callee returns `![]const u8` —
    # the `try` unwraps so the payload coerces to the optional.
    # Z023: method receivers (`self`) must come first; the rule does not
    # exempt receivers, so `deinit(self, allocator)` is a false positive.
    ziglint --ignore Z017 --ignore Z023

[doc("Build the library and CLI")]
[group("build")]
build:
    zig build

[doc("Run all CI gates")]
[group("ci")]
ci: test fmt-check test-zstd lint

[doc("Start a local Kafka broker for e2e (phase 7)")]
[group("e2e")]
kafka-up:
    @echo "TODO: phase 7 — KRaft + SASL/SCRAM-SHA-512 over TLS on port 9096"

[doc("Stop the local Kafka broker")]
[group("e2e")]
kafka-down:
    @echo "TODO: phase 7"

[doc("Run end-to-end tests against a local broker")]
[group("e2e")]
e2e:
    @echo "TODO: phase 7"
