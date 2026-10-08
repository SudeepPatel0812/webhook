#!/usr/bin/env bash
# Creates the Kafka topics this service needs. Run once after `docker compose up`,
# the same way `migrate ... up` is run explicitly rather than at server boot —
# see "Migrations via CLI, not embedded at startup" in DECISIONS.MD. Safe to
# re-run: --if-not-exists makes it a no-op once the topic exists.
set -euo pipefail

BROKER="${KAFKA_BROKER:-localhost:9092}"

docker compose -f infrastructure/infrastructure.yaml exec kafka \
	kafka-topics --bootstrap-server "$BROKER" \
	--create --if-not-exists \
	--topic events \
	--partitions 3 \
	--replication-factor 1

docker compose -f infrastructure/infrastructure.yaml exec kafka \
	kafka-topics --bootstrap-server "$BROKER" --describe --topic events
