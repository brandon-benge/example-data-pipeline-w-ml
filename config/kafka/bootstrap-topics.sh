#!/bin/sh
set -eu

for topic_file in /config/kafka/topics/*.env; do
  unset TOPIC_NAME PARTITIONS REPLICATION_FACTOR CLEANUP_POLICY RETENTION_MS SEGMENT_BYTES MIN_INSYNC_REPLICAS COMPRESSION_TYPE
  . "$topic_file"

  /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server kafka:9092 \
    --topic "$TOPIC_NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config cleanup.policy="$CLEANUP_POLICY" \
    --config retention.ms="$RETENTION_MS" \
    --config segment.bytes="$SEGMENT_BYTES" \
    --config min.insync.replicas="$MIN_INSYNC_REPLICAS" \
    --config compression.type="$COMPRESSION_TYPE"
done

for internal_topic in \
  debezium_source_connect_configs \
  debezium_source_connect_offsets \
  debezium_source_connect_statuses \
  debezium_sink_connect_configs \
  debezium_sink_connect_offsets \
  debezium_sink_connect_statuses; do
  /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server kafka:9092 \
    --topic "$internal_topic" \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=compact \
    --config min.insync.replicas=1
done

echo "Kafka topics initialized."
