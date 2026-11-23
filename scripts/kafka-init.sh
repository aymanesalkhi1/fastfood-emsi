#!/bin/bash
set -euo pipefail
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
for attempt in $(seq 1 60); do
  if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    break
  fi
  echo "attente de Kafka ($attempt)"
  sleep 2
done
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic orders
