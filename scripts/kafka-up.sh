#!/usr/bin/env bash
# Start a local Kafka broker (KRaft + SASL_SSL/SCRAM-SHA-512) for e2e.
#
# Single-node Kafka 4.x in KRaft mode with three listeners:
#   - PLAINTEXT  :9092 — admin (kafka-topics, health checks)
#   - SASL_SSL   :9093 — the real client path (SCRAM-SHA-512 over TLS 1.3)
#   - CONTROLLER :9094 — KRaft quorum (internal, PLAINTEXT)
#
# The MSK target is port 9096 (SASL/SCRAM-over-TLS); we use 9093 locally — the
# config shape is identical; only the port differs.
#
# Certs: mkcert generates a locally-trusted cert for "localhost" + "127.0.0.1"
# + "::1". The SCRAM user is created at storage-format time via
# `kafka-storage.sh format --add-scram`, so the broker has the credential
# before it serves the SASL_SSL listener.
#
# All settings come from env vars (defaults below), so this runs standalone
# without `just`.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

E2E_DIR="${E2E_DIR:-.kafka-e2e}"
PLAINTEXT_PORT="${PLAINTEXT_PORT:-9092}"
SASL_SSL_PORT="${SASL_SSL_PORT:-9093}"
CONTROLLER_PORT="${CONTROLLER_PORT:-9094}"
SCRAM_USER="${SCRAM_USER:-kafka-zig}"
SCRAM_PASS="${SCRAM_PASS:-kafka-zig-e2e-password}"
E2E_TOPIC="${E2E_TOPIC:-e2e-events}"

if [ -f "$E2E_DIR/broker.pid" ] && kill -0 "$(cat "$E2E_DIR/broker.pid")" 2>/dev/null; then
    echo "kafka-up: broker already running (pid $(cat "$E2E_DIR/broker.pid"))"
    exit 0
fi

mkdir -p "$E2E_DIR/logs" "$E2E_DIR/data"

# --- Generate cluster ID ---
CLUSTER_ID="$(kafka-storage.sh random-uuid)"
echo "$CLUSTER_ID" > "$E2E_DIR/cluster-id"

# --- mkcert: localhost cert (SAN includes localhost, 127.0.0.1, ::1) ---
mkcert -install -CAROOT "$(mkcert -CAROOT)" >/dev/null 2>&1 || true
mkcert -key-file "$E2E_DIR/broker-key.pem" \
       -cert-file "$E2E_DIR/broker-cert.pem" \
       localhost 127.0.0.1 ::1
cp "$(mkcert -CAROOT)/rootCA.pem" "$E2E_DIR/rootCA.pem"
# Convert mkcert PEM to PKCS12 for the broker keystore (Kafka's PEM
# keystore.location parser is finicky; PKCS12 is the reliable path).
openssl pkcs12 -export -out "$E2E_DIR/broker-keystore.p12" \
    -inkey "$E2E_DIR/broker-key.pem" \
    -in "$E2E_DIR/broker-cert.pem" \
    -passout pass:kafka-zig

# --- server.properties (KRaft + PLAINTEXT + SASL_SSL + CONTROLLER) ---
printf '%s\n' \
    'process.roles=broker,controller' \
    'node.id=1' \
    "controller.quorum.bootstrap.servers=localhost:${CONTROLLER_PORT}" \
    '' \
    "listeners=PLAINTEXT://:${PLAINTEXT_PORT},SASL_SSL://:${SASL_SSL_PORT},CONTROLLER://:${CONTROLLER_PORT}" \
    "advertised.listeners=PLAINTEXT://localhost:${PLAINTEXT_PORT},SASL_SSL://localhost:${SASL_SSL_PORT}" \
    'inter.broker.listener.name=PLAINTEXT' \
    'controller.listener.names=CONTROLLER' \
    'listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_SSL:SASL_SSL' \
    '' \
    'sasl.enabled.mechanisms=SCRAM-SHA-512' \
    'sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512' \
    "listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${SCRAM_USER}\" password=\"${SCRAM_PASS}\";" \
    '' \
    "ssl.keystore.location=$E2E_DIR/broker-keystore.p12" \
    'ssl.keystore.password=kafka-zig' \
    'ssl.key.password=kafka-zig' \
    'ssl.truststore.type=PEM' \
    "ssl.truststore.location=$E2E_DIR/rootCA.pem" \
    'ssl.client.auth=none' \
    'ssl.endpoint.identification.algorithm=' \
    '' \
    "log.dirs=$E2E_DIR/data" \
    'num.partitions=4' \
    'offsets.topic.replication.factor=1' \
    'transaction.state.log.replication.factor=1' \
    'transaction.state.log.min.isr=1' \
    'auto.create.topics.enable=false' \
    'log.retention.hours=1' \
    > "$E2E_DIR/server.properties"

# --- Format storage with SCRAM credential embedded ---
# --standalone: single-node dynamic quorum (Kafka 4.x uses
# controller.quorum.bootstrap.servers, not controller.quorum.voters)
kafka-storage.sh format \
    --config "$E2E_DIR/server.properties" \
    --cluster-id "$CLUSTER_ID" \
    --standalone \
    --add-scram "SCRAM-SHA-512=[name=\"${SCRAM_USER}\",password=\"${SCRAM_PASS}\"]" \
    --ignore-formatted

# --- Start the broker in the background ---
kafka-server-start.sh "$E2E_DIR/server.properties" \
    > "$E2E_DIR/logs/broker.log" 2>&1 &
BROKER_PID=$!
echo "$BROKER_PID" > "$E2E_DIR/broker.pid"

# --- Wait for the broker to be healthy ---
echo "kafka-up: waiting for broker (pid $BROKER_PID)..."
for i in $(seq 1 60); do
    if ! kill -0 "$BROKER_PID" 2>/dev/null; then
        echo "kafka-up: broker process died — see $E2E_DIR/logs/broker.log"
        tail -20 "$E2E_DIR/logs/broker.log" || true
        exit 1
    fi
    if kafka-topics.sh --bootstrap-server "localhost:${PLAINTEXT_PORT}" --list >/dev/null 2>&1; then
        echo "kafka-up: broker healthy after ${i}s"
        break
    fi
    sleep 1
done

if ! kafka-topics.sh --bootstrap-server "localhost:${PLAINTEXT_PORT}" --list >/dev/null 2>&1; then
    echo "kafka-up: broker did not become healthy within 60s — see $E2E_DIR/logs/broker.log"
    tail -30 "$E2E_DIR/logs/broker.log" || true
    exit 1
fi

# --- Create the test topic ---
kafka-topics.sh --bootstrap-server "localhost:${PLAINTEXT_PORT}" \
    --create --topic "$E2E_TOPIC" --partitions 4 --replication-factor 1 \
    --if-not-exists
echo "kafka-up: topic '$E2E_TOPIC' created"

# --- Verify SCRAM user exists ---
kafka-configs.sh --bootstrap-server "localhost:${PLAINTEXT_PORT}" \
    --describe --entity-type users --entity-name "$SCRAM_USER"
echo "kafka-up: done (PLAINTEXT :${PLAINTEXT_PORT}, SASL_SSL :${SASL_SSL_PORT})"
