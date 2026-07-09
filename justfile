# kafka-zig — just recipes (CI entry point)
# Requires Zig 0.15.2 and `just` on PATH. Use `nix develop` for the full
# devshell (zstd static lib, apache-kafka, mkcert, etc.).

e2e_dir := ".kafka-e2e"
plaintext_port := "9092"
sasl_ssl_port := "9093"
controller_port := "9094"
scram_user := "kafka-zig"
scram_pass := "kafka-zig-e2e-password"
e2e_topic := "e2e-events"
e2e_msgs := "20"

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

[doc("Pin/bump GitHub Actions to SHAs (latest tags) via pinact")]
[group("lint")]
pin-actions:
    # No arg: pinact auto-discovers .github/workflows/*.yml
    pinact run

[doc("Build the library and CLI")]
[group("build")]
build:
    zig build

[doc("Run all CI gates")]
[group("ci")]
ci: test fmt-check test-zstd lint

# ---------------------------------------------------------------------------
# Phase 7 — real Kafka e2e (KRaft + SASL_SSL/SCRAM-SHA-512 over TLS)
#
# Local single-node Kafka 4.2.0 in KRaft mode with three listeners:
#   - PLAINTEXT :9092 — admin (kafka-topics, health checks)
#   - SASL_SSL  :9093 — the real client path (SCRAM-SHA-512 over TLS 1.3)
#   - CONTROLLER:9094 — KRaft quorum (internal, PLAINTEXT)
#
# The MSK target is port 9096 (SASL/SCRAM-over-TLS); we use 9093 locally —
# the config shape is identical, only the port differs (PLAN §9).
#
# Certs: mkcert generates a locally-trusted cert for "localhost" + "127.0.0.1"
# + "::1". The broker uses the PEM directly (Kafka >=2.7 supports PEM
# keystore/truststore inline — no PKCS12/JKS conversion needed). The kafka-zig
# client loads mkcert's root CA into a std.crypto.Certificate.Bundle.
#
# SCRAM user is created at storage-format time via `kafka-storage.sh format
# --add-scram` (Kafka 3.x+), so the broker has the credential before it serves
# the SASL_SSL listener — no separate kafka-configs step needed.
# ---------------------------------------------------------------------------

[doc("Start a local Kafka broker (KRaft + SASL_SSL/SCRAM-SHA-512) for e2e")]
[group("e2e")]
kafka-up:
    #!/usr/bin/env bash
    set -euo pipefail
    cd "$(git rev-parse --show-toplevel)"

    if [ -f {{ e2e_dir }}/broker.pid ] && kill -0 "$(cat {{ e2e_dir }}/broker.pid)" 2>/dev/null; then
        echo "kafka-up: broker already running (pid $(cat {{ e2e_dir }}/broker.pid))"
        exit 0
    fi

    mkdir -p {{ e2e_dir }}/logs {{ e2e_dir }}/data

    # --- Generate cluster ID ---
    CLUSTER_ID="$(kafka-storage.sh random-uuid)"
    echo "$CLUSTER_ID" > {{ e2e_dir }}/cluster-id

    # --- mkcert: localhost cert (SAN includes localhost, 127.0.0.1, ::1) ---
    mkcert -install -CAROOT "$(mkcert -CAROOT)" >/dev/null 2>&1 || true
    mkcert -key-file {{ e2e_dir }}/broker-key.pem \
           -cert-file {{ e2e_dir }}/broker-cert.pem \
           localhost 127.0.0.1 ::1
    cp "$(mkcert -CAROOT)/rootCA.pem" {{ e2e_dir }}/rootCA.pem
    # Convert mkcert PEM to PKCS12 for the broker keystore (Kafka's PEM
    # keystore.location parser is finicky; PKCS12 is the reliable path).
    openssl pkcs12 -export -out {{ e2e_dir }}/broker-keystore.p12 \
        -inkey {{ e2e_dir }}/broker-key.pem \
        -in {{ e2e_dir }}/broker-cert.pem \
        -passout pass:kafka-zig

    # --- server.properties (KRaft + PLAINTEXT 9092 + SASL_SSL 9093 + CONTROLLER 9094) ---
    # SSL keystore in PEM format (Kafka >=2.7: ssl.keystore.key / .certificate.chain).
    # JAAS inline via listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config.
    printf '%s\n' \
        'process.roles=broker,controller' \
        'node.id=1' \
        "controller.quorum.bootstrap.servers=localhost:{{ controller_port }}" \
        '' \
        "listeners=PLAINTEXT://:{{ plaintext_port }},SASL_SSL://:{{ sasl_ssl_port }},CONTROLLER://:{{ controller_port }}" \
        "advertised.listeners=PLAINTEXT://localhost:{{ plaintext_port }},SASL_SSL://localhost:{{ sasl_ssl_port }}" \
        'inter.broker.listener.name=PLAINTEXT' \
        'controller.listener.names=CONTROLLER' \
        'listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_SSL:SASL_SSL' \
        '' \
        'sasl.enabled.mechanisms=SCRAM-SHA-512' \
        'sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512' \
        'listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="{{ scram_user }}" password="{{ scram_pass }}";' \
        '' \
        'ssl.keystore.location={{ e2e_dir }}/broker-keystore.p12' \
        'ssl.keystore.password=kafka-zig' \
        'ssl.key.password=kafka-zig' \
        'ssl.truststore.type=PEM' \
        'ssl.truststore.location={{ e2e_dir }}/rootCA.pem' \
        'ssl.client.auth=none' \
        'ssl.endpoint.identification.algorithm=' \
        '' \
        'log.dirs={{ e2e_dir }}/data' \
        'num.partitions=4' \
        'offsets.topic.replication.factor=1' \
        'transaction.state.log.replication.factor=1' \
        'transaction.state.log.min.isr=1' \
        'auto.create.topics.enable=false' \
        'log.retention.hours=1' \
        > {{ e2e_dir }}/server.properties

    # --- Format storage with SCRAM credential embedded ---
    # --standalone: single-node dynamic quorum (Kafka 4.x uses
    # controller.quorum.bootstrap.servers, not controller.quorum.voters)
    kafka-storage.sh format \
        --config {{ e2e_dir }}/server.properties \
        --cluster-id "$CLUSTER_ID" \
        --standalone \
        --add-scram "SCRAM-SHA-512=[name=\"{{ scram_user }}\",password=\"{{ scram_pass }}\"]" \
        --ignore-formatted

    # --- Start the broker in the background ---
    kafka-server-start.sh {{ e2e_dir }}/server.properties \
        > {{ e2e_dir }}/logs/broker.log 2>&1 &
    BROKER_PID=$!
    echo "$BROKER_PID" > {{ e2e_dir }}/broker.pid

    # --- Wait for the broker to be healthy ---
    echo "kafka-up: waiting for broker (pid $BROKER_PID)..."
    for i in $(seq 1 60); do
        if ! kill -0 "$BROKER_PID" 2>/dev/null; then
            echo "kafka-up: broker process died — see {{ e2e_dir }}/logs/broker.log"
            tail -20 {{ e2e_dir }}/logs/broker.log || true
            exit 1
        fi
        if kafka-topics.sh --bootstrap-server localhost:{{ plaintext_port }} --list >/dev/null 2>&1; then
            echo "kafka-up: broker healthy after ${i}s"
            break
        fi
        sleep 1
    done

    if ! kafka-topics.sh --bootstrap-server localhost:{{ plaintext_port }} --list >/dev/null 2>&1; then
        echo "kafka-up: broker did not become healthy within 60s — see {{ e2e_dir }}/logs/broker.log"
        tail -30 {{ e2e_dir }}/logs/broker.log || true
        exit 1
    fi

    # --- Create the test topic ---
    kafka-topics.sh --bootstrap-server localhost:{{ plaintext_port }} \
        --create --topic {{ e2e_topic }} --partitions 4 --replication-factor 1 \
        --if-not-exists
    echo "kafka-up: topic '{{ e2e_topic }}' created"

    # --- Verify SCRAM user exists ---
    kafka-configs.sh --bootstrap-server localhost:{{ plaintext_port }} \
        --describe --entity-type users --entity-name {{ scram_user }}
    echo "kafka-up: done (PLAINTEXT :{{ plaintext_port }}, SASL_SSL :{{ sasl_ssl_port }})"

[doc("Stop the local Kafka broker and clean up state")]
[group("e2e")]
kafka-down:
    #!/usr/bin/env bash
    set -euo pipefail
    cd "$(git rev-parse --show-toplevel)"

    if [ ! -f {{ e2e_dir }}/broker.pid ]; then
        echo "kafka-down: no broker pid file found — nothing to stop"
        exit 0
    fi

    PID="$(cat {{ e2e_dir }}/broker.pid)"
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "kafka-down: sent SIGTERM to broker (pid $PID)"
        for i in $(seq 1 15); do
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

    rm -rf {{ e2e_dir }}/data
    echo "kafka-down: cleaned up data dir (logs/certs retained for inspection)"

[doc("Run end-to-end smoke: produce N via kafka-zig, consume N via kafka-console-consumer, tear down")]
[group("e2e")]
e2e: kafka-up
    #!/usr/bin/env bash
    set -euo pipefail
    cd "$(git rev-parse --show-toplevel)"

    # --- Build the e2e binary ---
    echo "e2e: building..."
    zig build e2e

    # --- Produce N messages through kafka-zig's real Client ---
    echo "e2e: producing {{ e2e_msgs }} messages via kafka-zig..."
    zig-out/bin/e2e --broker localhost:{{ sasl_ssl_port }} \
        --ca {{ e2e_dir }}/rootCA.pem \
        --user {{ scram_user }} --pass {{ scram_pass }} \
        --topic {{ e2e_topic }} --num {{ e2e_msgs }}

    # --- Consume with kafka-console-consumer (proves records are real) ---
    printf '%s\n' \
        'security.protocol=SASL_SSL' \
        'sasl.mechanism=SCRAM-SHA-512' \
        'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="{{ scram_user }}" password="{{ scram_pass }}";' \
        'ssl.truststore.type=PEM' \
        'ssl.truststore.location={{ e2e_dir }}/rootCA.pem' \
        'ssl.endpoint.identification.algorithm=' \
        > {{ e2e_dir }}/consumer.properties

    echo "e2e: consuming {{ e2e_msgs }} messages via kafka-console-consumer..."
    OUTPUT="$(kafka-console-consumer.sh \
        --bootstrap-server localhost:{{ sasl_ssl_port }} \
        --topic {{ e2e_topic }} \
        --from-beginning \
        --max-messages {{ e2e_msgs }} \
        --command-config {{ e2e_dir }}/consumer.properties \
        --timeout-ms 10000 2>&1)"
    COUNT="$(echo "$OUTPUT" | grep -c '^msg-' || true)"
    echo "$OUTPUT" | tail -5
    echo "e2e: consumed $COUNT messages"

    if [ "$COUNT" -ne "{{ e2e_msgs }}" ]; then
        echo "e2e: FAIL — expected {{ e2e_msgs }}, got $COUNT"
        just kafka-down
        exit 1
    fi

    echo "e2e: PASS — produced {{ e2e_msgs }}, consumed $COUNT"
    just kafka-down

# Real-Kafka e2e with zstd-compressed batches. Builds the e2e binary with
# -Dzstd=true, produces with --compression zstd, consumes back. Exercises the
# full compression path against a real broker (the unit/mock tests cover the
# codec; this proves it interops with kafka-console-consumer's zstd decode).
[doc("Run the zstd-compression e2e against a real Kafka broker")]
[group("e2e")]
e2e-zstd: kafka-up
    #!/usr/bin/env bash
    set -euo pipefail
    cd "$(git rev-parse --show-toplevel)"

    echo "e2e-zstd: building (with -Dzstd=true)..."
    zig build e2e -Dzstd=true

    # Create the zstd topic before producing (the e2e-events topic is created
    # in kafka-up; this one needs its own).
    printf '%s\n' \
        'security.protocol=SASL_SSL' \
        'sasl.mechanism=SCRAM-SHA-512' \
        'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="{{ scram_user }}" password="{{ scram_pass }}";' \
        'ssl.truststore.type=PEM' \
        'ssl.truststore.location={{ e2e_dir }}/rootCA.pem' \
        'ssl.endpoint.identification.algorithm=' \
        > {{ e2e_dir }}/consumer.properties
    kafka-topics.sh --bootstrap-server localhost:{{ sasl_ssl_port }} \
        --create --if-not-exists --topic {{ e2e_topic }}-zstd \
        --partitions 4 --replication-factor 1 \
        --command-config {{ e2e_dir }}/consumer.properties

    echo "e2e-zstd: producing {{ e2e_msgs }} zstd-compressed messages via kafka-zig..."
    zig-out/bin/e2e --broker localhost:{{ sasl_ssl_port }} \
        --ca {{ e2e_dir }}/rootCA.pem \
        --user {{ scram_user }} --pass {{ scram_pass }} \
        --topic {{ e2e_topic }}-zstd --num {{ e2e_msgs }} --compression zstd

    echo "e2e-zstd: consuming {{ e2e_msgs }} messages via kafka-console-consumer..."
    OUTPUT="$(kafka-console-consumer.sh \
        --bootstrap-server localhost:{{ sasl_ssl_port }} \
        --topic {{ e2e_topic }}-zstd \
        --from-beginning \
        --max-messages {{ e2e_msgs }} \
        --command-config {{ e2e_dir }}/consumer.properties \
        --timeout-ms 10000 2>&1)"
    COUNT="$(echo "$OUTPUT" | grep -c '^msg-' || true)"
    echo "$OUTPUT" | tail -5
    echo "e2e-zstd: consumed $COUNT messages"

    if [ "$COUNT" -ne "{{ e2e_msgs }}" ]; then
        echo "e2e-zstd: FAIL — expected {{ e2e_msgs }}, got $COUNT"
        just kafka-down
        exit 1
    fi

    echo "e2e-zstd: PASS — produced {{ e2e_msgs }} zstd, consumed $COUNT"
    just kafka-down
