#!/usr/bin/env bash
# Stop the local Kafka broker and clean up state.
#
# Sends SIGTERM (then SIGKILL after 15s) to the broker started by
# kafka-up.sh, then removes the data dir. Logs/certs are retained for
# inspection. Settings come from env vars (defaults below).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

E2E_DIR="${E2E_DIR:-.kafka-e2e}"

if [ ! -f "$E2E_DIR/broker.pid" ]; then
    echo "kafka-down: no broker pid file found — nothing to stop"
    exit 0
fi

PID="$(cat "$E2E_DIR/broker.pid")"
if kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    echo "kafka-down: sent SIGTERM to broker (pid $PID)"
    for _ in $(seq 1 15); do
        if ! kill -0 "$PID" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    if kill -0 "$PID" 2>/dev/null; then
        kill -9 "$PID" || true
        echo "kafka-down: sent SIGKILL"
    fi
else
    echo "kafka-down: broker (pid $PID) not running"
fi

rm -rf "$E2E_DIR/data"
echo "kafka-down: cleaned up data dir (logs/certs retained for inspection)"
