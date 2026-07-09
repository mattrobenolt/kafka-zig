#!/usr/bin/env bash
# Real-Kafka e2e with snappy-compressed batches. Snappy is always available
# (pure-Zig, no build flag), so this builds the default e2e binary and
# produces with --compression snappy. Exercises the full snappy compression
# path against a real broker (the unit/mock tests cover the codec; this proves
# it interops with kafka-console-consumer's snappy decode).
#
# Settings come from env vars (defaults below).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

E2E_DIR="${E2E_DIR:-.kafka-e2e}"
SASL_SSL_PORT="${SASL_SSL_PORT:-9093}"
SCRAM_USER="${SCRAM_USER:-kafka-zig}"
SCRAM_PASS="${SCRAM_PASS:-kafka-zig-e2e-password}"
E2E_TOPIC="${E2E_TOPIC:-e2e-events}"
E2E_MSGS="${E2E_MSGS:-20}"

scripts/kafka-up.sh

echo "e2e-snappy: building..."
zig build e2e

# Create the snappy topic before producing.
printf '%s\n' \
    'security.protocol=SASL_SSL' \
    'sasl.mechanism=SCRAM-SHA-512' \
    "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${SCRAM_USER}\" password=\"${SCRAM_PASS}\";" \
    'ssl.truststore.type=PEM' \
    "ssl.truststore.location=$E2E_DIR/rootCA.pem" \
    'ssl.endpoint.identification.algorithm=' \
    > "$E2E_DIR/consumer.properties"
kafka-topics.sh --bootstrap-server "localhost:${SASL_SSL_PORT}" \
    --create --if-not-exists --topic "${E2E_TOPIC}-snappy" \
    --partitions 4 --replication-factor 1 \
    --command-config "$E2E_DIR/consumer.properties"

echo "e2e-snappy: producing $E2E_MSGS snappy-compressed messages via kafka-zig..."
zig-out/bin/e2e --broker "localhost:${SASL_SSL_PORT}" \
    --ca "$E2E_DIR/rootCA.pem" \
    --user "$SCRAM_USER" --pass "$SCRAM_PASS" \
    --topic "${E2E_TOPIC}-snappy" --num "$E2E_MSGS" --compression snappy

echo "e2e-snappy: consuming $E2E_MSGS messages via kafka-console-consumer..."
OUTPUT="$(kafka-console-consumer.sh \
    --bootstrap-server "localhost:${SASL_SSL_PORT}" \
    --topic "${E2E_TOPIC}-snappy" \
    --from-beginning \
    --max-messages "$E2E_MSGS" \
    --command-config "$E2E_DIR/consumer.properties" \
    --timeout-ms 10000 2>&1)"
COUNT="$(echo "$OUTPUT" | grep -c '^msg-' || true)"
echo "$OUTPUT" | tail -5
echo "e2e-snappy: consumed $COUNT messages"

if [ "$COUNT" -ne "$E2E_MSGS" ]; then
    echo "e2e-snappy: FAIL — expected $E2E_MSGS, got $COUNT"
    scripts/kafka-down.sh
    exit 1
fi

echo "e2e-snappy: PASS — produced $E2E_MSGS snappy, consumed $COUNT"
scripts/kafka-down.sh
