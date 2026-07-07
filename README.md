# kafka-zig

A native Zig Kafka **producer** library. Pre-alpha; the API will move.

Targets modern Kafka 3.x / AWS MSK. TLS 1.3 via
[ztls](https://github.com/mattrobenolt/ztls). SASL SCRAM-SHA-512 over TLS
(port 9096 on MSK; SHA-256 also supported by the scram module). Plaintext
batches and optional zstd compression (`-Dzstd=true`).

Not in scope (yet): no consumer, no consumer groups, no transactions, no
idempotent producer.

## Dev setup

Requires Zig 0.15.2 and the Nix flake devshell (`nix develop`).

**Sibling-directory requirement:** ztls is a path dependency at `../ztls`
during development. Clone ztls alongside this repo, or `zig build` will fail
on the missing path. Publish-time this switches to a git+hash dep.

## Build & test

```sh
just build       # build the library and CLI
just test        # run tests
just test-zstd   # run tests with zstd compression (needs the devshell's static libzstd)
```

## Roadmap

Read [`PLAN.md`](PLAN.md) for the design and phased roadmap.

## License

TBD.
