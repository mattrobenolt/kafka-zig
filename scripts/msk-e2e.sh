#!/usr/bin/env bash
# Real AWS MSK integration test (issue #11) — MANUAL, requires VPC access.
#
# Produces to a real MSK cluster via kafka-zig, consumes back via
# kafka-console-consumer, asserts the counts match. Unlike the local e2e
# (which starts a KRaft broker), this needs:
#   - VPC access to the MSK cluster (the brokers are VPC-internal).
#   - SCRAM-SHA-512 credentials (created via AWS Secrets Manager + MSK CLI).
#   - The CA bundle that signed the MSK broker TLS certs.
#   - All bootstrap endpoints (comma-separated) to exercise HA failover.
#
# NOT part of the default CI gate (`just ci`). Run manually from the VPC:
#
#   MSK_BOOTSTRAP="b1:9096,b2:9096,b3:9096" \
#     MSK_CA=/path/to/ca.pem MSK_USER=alice MSK_PASS=secret \
#     scripts/msk-e2e.sh
#
# Variables (all required unless noted):
#   MSK_BOOTSTRAP   — comma-separated bootstrap endpoints (all 3 for HA)
#   MSK_CA          — path to the CA PEM that signed the MSK broker certs
#   MSK_USER        — SCRAM-SHA-512 username
#   MSK_PASS        — SCRAM-SHA-512 password
#   MSK_TOPIC       — topic name (default: msk-e2e)
#   MSK_NUM         — number of messages (default: 50)
#   MSK_COMPRESSION — none|zstd|snappy (default: none; zstd needs -Dzstd=true)
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

: "${MSK_BOOTSTRAP:?MSK_BOOTSTRAP is required (comma-separated bootstrap endpoints, e.g. b1:9096,b2:9096,b3:9096)}"
: "${MSK_CA:?MSK_CA is required (path to the CA PEM that signed the MSK broker certs)}"
: "${MSK_USER:?MSK_USER is required (SCRAM-SHA-512 username)}"
: "${MSK_PASS:?MSK_PASS is required (SCRAM-SHA-512 password)}"

MSK_TOPIC="${MSK_TOPIC:-msk-e2e}"
MSK_NUM="${MSK_NUM:-50}"
MSK_COMPRESSION="${MSK_COMPRESSION:-none}"

echo "msk-e2e: building..."
zig build e2e

# --- Produce N messages through kafka-zig with all bootstrap endpoints ---
echo "msk-e2e: producing $MSK_NUM messages to $MSK_TOPIC via kafka-zig..."
zig-out/bin/e2e --bootstrap "$MSK_BOOTSTRAP" \
    --ca "$MSK_CA" \
    --user "$MSK_USER" --pass "$MSK_PASS" \
    --topic "$MSK_TOPIC" --num "$MSK_NUM" \
    --compression "$MSK_COMPRESSION"

# --- Consume with kafka-console-consumer (proves records are real) ---
# MSK-specific consumer properties: SASL_SSL + SCRAM-SHA-512 over TLS.
# ssl.endpoint.identification.algorithm is blanked because MSK broker certs
# may not have the bootstrap DNS names in their SANs — verify and adjust.
CONSUMER_PROPS="$(mktemp)"
trap 'rm -f "$CONSUMER_PROPS"' EXIT
printf '%s\n' \
    'security.protocol=SASL_SSL' \
    'sasl.mechanism=SCRAM-SHA-512' \
    "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$MSK_USER\" password=\"$MSK_PASS\";" \
    'ssl.truststore.type=PEM' \
    "ssl.truststore.location=$MSK_CA" \
    'ssl.endpoint.identification.algorithm=' \
    > "$CONSUMER_PROPS"

echo "msk-e2e: consuming $MSK_NUM messages via kafka-console-consumer..."
# Use the first bootstrap endpoint for the consumer (kafka-console-consumer
# takes a single bootstrap-server; it discovers the rest via metadata).
FIRST_BROKER="${MSK_BOOTSTRAP%%,*}"
OUTPUT="$(kafka-console-consumer.sh \
    --bootstrap-server "$FIRST_BROKER" \
    --topic "$MSK_TOPIC" \
    --from-beginning \
    --max-messages "$MSK_NUM" \
    --command-config "$CONSUMER_PROPS" \
    --timeout-ms 30000 2>&1)"
COUNT="$(echo "$OUTPUT" | grep -c '^msg-' || true)"
echo "$OUTPUT" | tail -5
echo "msk-e2e: consumed $COUNT messages"

if [ "$COUNT" -ne "$MSK_NUM" ]; then
    echo "msk-e2e: FAIL — expected $MSK_NUM, got $COUNT"
    exit 1
fi

echo "msk-e2e: PASS — produced $MSK_NUM, consumed $COUNT"
