#!/usr/bin/env bash
# End-to-end smoke: produce N via kafka-zig, consume N via
# kafka-console-consumer, assert the counts match, tear down.
#
# Brings the broker up (kafka-up.sh), then tears it down (kafka-down.sh) on
# both success and failure. Settings come from env vars (defaults below).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

E2E_DIR="${E2E_DIR:-.kafka-e2e}"
SASL_SSL_PORT="${SASL_SSL_PORT:-9093}"
SCRAM_USER="${SCRAM_USER:-kafka-zig}"
SCRAM_PASS="${SCRAM_PASS:-kafka-zig-e2e-password}"
E2E_TOPIC="${E2E_TOPIC:-e2e-events}"
E2E_MSGS="${E2E_MSGS:-20}"

scripts/kafka-up.sh

# --- Build the e2e binary ---
echo "e2e: building..."
zig build e2e

# --- Produce N messages through kafka-zig's real Client ---
echo "e2e: producing $E2E_MSGS messages via kafka-zig..."
zig-out/bin/e2e --broker "localhost:${SASL_SSL_PORT}" \
    --ca "$E2E_DIR/rootCA.pem" \
    --user "$SCRAM_USER" --pass "$SCRAM_PASS" \
    --topic "$E2E_TOPIC" --num "$E2E_MSGS"

# --- Consume with kafka-console-consumer (proves records are real) ---
printf '%s\n' \
    'security.protocol=SASL_SSL' \
    'sasl.mechanism=SCRAM-SHA-512' \
    "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${SCRAM_USER}\" password=\"${SCRAM_PASS}\";" \
    'ssl.truststore.type=PEM' \
    "ssl.truststore.location=$E2E_DIR/rootCA.pem" \
    'ssl.endpoint.identification.algorithm=' \
    > "$E2E_DIR/consumer.properties"

echo "e2e: consuming $E2E_MSGS messages via kafka-console-consumer..."
OUTPUT="$(kafka-console-consumer.sh \
    --bootstrap-server "localhost:${SASL_SSL_PORT}" \
    --topic "$E2E_TOPIC" \
    --from-beginning \
    --max-messages "$E2E_MSGS" \
    --command-config "$E2E_DIR/consumer.properties" \
    --timeout-ms 10000 2>&1)"
COUNT="$(echo "$OUTPUT" | grep -c '^msg-' || true)"
echo "$OUTPUT" | tail -5
echo "e2e: consumed $COUNT messages"

if [ "$COUNT" -ne "$E2E_MSGS" ]; then
    echo "e2e: FAIL — expected $E2E_MSGS, got $COUNT"
    scripts/kafka-down.sh
    exit 1
fi

echo "e2e: PASS — produced $E2E_MSGS, consumed $COUNT"
scripts/kafka-down.sh
