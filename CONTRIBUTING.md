# Contributing to kafka-zig

kafka-zig is accepting focused fixes, tests, documentation, and producer-side
features. A Kafka consumer or transaction API is a different project-sized
conversation; open an issue before building either.

## Development environment

The supported environment is the repository's Nix flake:

```sh
nix develop
just test
```

The dev shell pins Zig 0.15.2 and supplies libzstd, ziglint, shellcheck, Kafka,
and the certificate tooling used by the e2e scripts. Do not add instructions
for globally installed copies of those tools when the flake can own them.

Useful checks are:

```sh
just test           # default unit and in-process integration tests
just test-zstd      # tests with libzstd enabled
just fmt-check      # Zig formatting
just lint           # ziglint and GitHub Actions audit
just shellcheck     # e2e shell scripts
just docs           # build the API reference
just ci             # all local CI gates
```

The real-Kafka e2e test is `just e2e`. It is slower and stateful, so it is not
part of every CI run. Changes to connection setup, authentication, record
batches, or Produce behavior should run it before review.

## Making a change

Fork the repository, create a branch from `main`, and keep the diff scoped to
one concern. Add tests for behavior changes. Run the relevant checks in
`nix develop`, push the branch to your fork, and open a pull request describing
what changed, why, and what you ran.

Protocol changes need more evidence than “the broker accepted it.” Link the
Kafka protocol or KIP, name the API key and version, and include byte-level
fixtures or tests where practical. Do not infer field order or flexible-version
encoding from memory.

## Review

The project uses two-model consensus review for substantive changes: two
independent model families review the diff, then the maintainer reconciles
their findings and verifies the result. This is an extra review input, not an
automatic merge policy. A maintainer still makes the decision, and CI still
has to pass.

Expect closer review around ring ownership, retry identity, idempotent
sequences, SCRAM transcript handling, TLS verification, bounded memory, and
wire-format changes. Those are the places where a locally plausible shortcut
usually becomes a distributed-systems bug.

## Conventions

Code targets Zig 0.15.2 and is formatted with `zig fmt`. Follow the repository's
Zig and Tiger Style guidance: explicit bounds, assertions on invariants,
bounded resource use, small functions, and names that include units where the
unit is not obvious. Comments should explain a constraint or tradeoff, not
restate the code. Avoid heap allocation and hidden copies on the produce hot
path; if an allocation is necessary, keep it on a documented cold path and add
a test when the distinction matters.

The Kafka protocol rules pinned in `.pi/skills/kafka-protocol/SKILL.md` and the
mock-broker conventions in `.pi/skills/testing/SKILL.md` are repository
documentation, despite the unusual directory name. Automated coding tools
should also load current Zig 0.15 and Tiger Style skills rather than generating
syntax from older Zig releases.

Dependencies need a concrete maintenance or correctness payoff. Compression
code should use a maintained library or a separately testable codec with
interop vectors; this repository is not where a casual gzip implementation
should be born.

Contributions are submitted under the repository's
[Apache License 2.0](LICENSE).
