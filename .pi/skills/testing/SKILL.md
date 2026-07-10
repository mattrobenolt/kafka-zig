---
name: testing
description: >
  Mock broker test harness conventions for kafka-zig. Use when writing or
  editing tests that use the mock broker (src/testing/mock_broker.zig), Ring
  tests, or Client integration tests. Covers the async connect/auth timing
  gotcha, pointer returns, and the load-shed test pattern.
---

# kafka-zig test harness conventions

## Mock broker is async

`mock.Broker.start` spawns a thread that listens, accepts, does a TLS
handshake + SCRAM auth, then serves Kafka requests. `Client.init` spawns
the network thread which connects, authenticates, fetches metadata, and
acquires a PID (if idempotent). Both are **async** — by the time
`Client.init` returns, the network thread may still be handshaking.

This means:
- **A tight synchronous `acquire` + `commit` loop will fill the ring before
  the network thread drains anything.** If you need the network thread to
  make progress during a test, sleep on `WouldBlock` (e.g.
  `std.Thread.sleep(1ms)` in the catch) rather than spinning.
- **Timing assertions** (e.g. "elapsed < X") must account for the TLS +
  SCRAM + PID + metadata round-trip on the first produce. Use a warmup
  produce+await before the timing window if you want to measure only the
  drain, not the handshake.
- **`io_timeout_ms`** in `testConfig` defaults to 30s (the production
  default). Don't set it too low (e.g. 200ms) or the TLS handshake will
  time out on CI's slower hardware.

## Pointer returns

`mock.Broker.start()` returns `*Broker` and `Client.init()` returns
`*Client`. Both are already pointers — pass them bare (`broker.field`,
`client.method()`), not `&broker` / `&client`.

## Load-shed test pattern

For `tryAcquire` load-shed tests (fire-and-forget or awaitable):
```zig
while (committed < target) {
    const m = client.tryAcquire(.fire_and_forget) catch |err| switch (err) {
        error.WouldBlock => { std.Thread.sleep(1 * std.time.ns_per_ms); continue; },
        else => return err,
    };
    // ... fill + commit ...
    committed += 1;
}
```
The sleep-on-WouldBlock lets the network thread drain slots between
acquire attempts. Without it, the ring fills instantly and the test
measures "WouldBlock immediately" rather than "sustained produce."

## Queue depth after await

The network thread wakes `await()` waiters **before** reclaiming slots.
So `stats().queue_depth` may still be > 0 immediately after `await()`
returns. Use `expectQueueDepthZero(client)` (polls up to 1s) instead of
an immediate assertion.

## Fuzz test flakiness

The `std.testing.fuzz` targets run their corpus seeds in normal `zig
build test` mode. On CI, a slow build (e.g. Magic Nix Cache throttling)
can cause resource pressure that trips the fuzz runner. If a fuzz test
fails on CI but passes locally across multiple seeds, it's likely a CI
resource issue, not a seed-sensitive assertion. Re-run to confirm.
