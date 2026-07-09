# kafka-zig — just recipes (CI entry point)
# Requires Zig 0.15.2 and `just` on PATH. Use `nix develop` for the full
# devshell (zstd static lib, apache-kafka, mkcert, shellcheck, etc.).
#
# The e2e recipes are thin dispatchers over scripts/*.sh. Those scripts are
# self-contained (defaults baked in, overridable via env vars) so they run
# standalone without `just`. See scripts/ for the settings and their defaults.

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

[doc("Run ziglint + zizmor (CI security audit)")]
[group("lint")]
lint:
    ziglint
    # zizmor: GitHub Actions security audit. Fail on medium+ findings
    # (informational/low like missing concurrency are reported but don't fail).
    zizmor --persona=auditor --min-severity=medium .

[doc("Lint the e2e shell scripts with shellcheck")]
[group("lint")]
shellcheck:
    shellcheck scripts/*.sh

[doc("Pin/bump GitHub Actions to SHAs (latest tags) via pinact")]
[group("lint")]
pin-actions:
    # No arg: pinact auto-discovers .github/workflows/*.yml
    pinact run

[doc("Build the library and CLI")]
[group("build")]
build:
    zig build

[doc("Generate Zig API docs (HTML) into zig-out/docs/")]
[group("build")]
docs:
    zig build docs
    @echo "docs: generated into zig-out/docs/ (open zig-out/docs/index.html)"

[doc("Run all CI gates")]
[group("ci")]
ci: test fmt-check test-zstd lint shellcheck docs

[doc("Run the produce benchmark against the mock broker (ReleaseFast)")]
[group("bench")]
bench:
    zig build bench -Doptimize=ReleaseFast
    zig-out/bin/bench

[doc("Run the benchmark with custom args (ReleaseFast)")]
[group("bench")]
bench-args *args:
    zig build bench -Doptimize=ReleaseFast
    zig-out/bin/bench {{ args }}

# ---------------------------------------------------------------------------
# Phase 7 — real Kafka e2e (KRaft + SASL_SSL/SCRAM-SHA-512 over TLS)
#
# The heavy lifting lives in scripts/*.sh (extracted so shellcheck can lint
# them and so they run standalone without `just`). Each script bakes in
# sensible defaults and reads overrides from env vars — see the script headers
# for the full variable list (E2E_DIR, SASL_SSL_PORT, SCRAM_USER, ...).
#
# Local single-node Kafka 4.x in KRaft mode with three listeners:
#   - PLAINTEXT :9092 — admin (kafka-topics, health checks)
#   - SASL_SSL  :9093 — the real client path (SCRAM-SHA-512 over TLS 1.3)
#   - CONTROLLER:9094 — KRaft quorum (internal, PLAINTEXT)
# The MSK target is port 9096; we use 9093 locally (PLAN §9).
# ---------------------------------------------------------------------------

[doc("Start a local Kafka broker (KRaft + SASL_SSL/SCRAM-SHA-512) for e2e")]
[group("e2e")]
kafka-up:
    scripts/kafka-up.sh

[doc("Stop the local Kafka broker and clean up state")]
[group("e2e")]
kafka-down:
    scripts/kafka-down.sh

[doc("Run end-to-end smoke: produce N via kafka-zig, consume N via kafka-console-consumer, tear down")]
[group("e2e")]
e2e:
    scripts/e2e.sh

[doc("Run the zstd-compression e2e against a real Kafka broker")]
[group("e2e")]
e2e-zstd:
    scripts/e2e-zstd.sh

[doc("Run the snappy-compression e2e against a real Kafka broker")]
[group("e2e")]
e2e-snappy:
    scripts/e2e-snappy.sh

[doc("Run the e2e against a real AWS MSK cluster (requires VPC access + SCRAM creds)")]
[group("e2e")]
msk-e2e:
    scripts/msk-e2e.sh
