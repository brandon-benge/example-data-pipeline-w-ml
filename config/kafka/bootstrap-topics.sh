#!/bin/sh
set -eu

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka.data-platform-infra:9092}"
MAX_PARALLEL_CREATES="${MAX_PARALLEL_CREATES:-4}"

echo "Waiting for Kafka admin endpoint on ${BOOTSTRAP_SERVER}..."
attempt=0
until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "Kafka admin endpoint did not become ready after ${attempt} attempts." >&2
    exit 1
  fi
  sleep 2
done

EXISTING_TOPICS="$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list || true)"

echo "Kafka admin endpoint is ready. Creating topics..."

topic_exists() {
  topic_name="$1"
  printf '%s\n' "$EXISTING_TOPICS" | grep -Fxq "$topic_name"
}

topic_retention_override() {
  topic_name="$1"
  case "$topic_name" in
    cdc.*|events.*|dlq.events.*)
      printf '%s\n' 10800000
      ;;
    *)
      printf '%s\n' ""
      ;;
  esac
}

enforce_topic_retention() {
  topic_name="$1"
  retention_override="$(topic_retention_override "$topic_name")"
  if [ -z "$retention_override" ]; then
    return 0
  fi
  echo "[kafka-bootstrap] setting retention.ms=${retention_override} on ${topic_name}"
  /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --entity-type topics \
    --entity-name "$topic_name" \
    --alter \
    --add-config retention.ms="${retention_override}"
}

create_topic_from_file() {
  topic_file="$1"
  unset TOPIC_NAME PARTITIONS REPLICATION_FACTOR CLEANUP_POLICY RETENTION_MS SEGMENT_BYTES MIN_INSYNC_REPLICAS COMPRESSION_TYPE
  . "$topic_file"
  retention_override="$(topic_retention_override "$TOPIC_NAME")"
  if [ -n "$retention_override" ]; then
    RETENTION_MS="$retention_override"
  fi
  if topic_exists "$TOPIC_NAME"; then
    echo "[kafka-bootstrap] reconciling existing topic ${TOPIC_NAME}"
    enforce_topic_retention "$TOPIC_NAME"
    return 0
  fi
  echo "[kafka-bootstrap] creating topic ${TOPIC_NAME}"

  /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --topic "$TOPIC_NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config cleanup.policy="$CLEANUP_POLICY" \
    --config retention.ms="$RETENTION_MS" \
    --config segment.bytes="$SEGMENT_BYTES" \
    --config min.insync.replicas="$MIN_INSYNC_REPLICAS" \
    --config compression.type="$COMPRESSION_TYPE"
  enforce_topic_retention "$TOPIC_NAME"
}

create_internal_topic() {
  internal_topic="$1"
  if topic_exists "$internal_topic"; then
    echo "[kafka-bootstrap] skipping existing internal topic ${internal_topic}"
    return 0
  fi
  echo "[kafka-bootstrap] creating internal topic ${internal_topic}"
  /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --topic "$internal_topic" \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=compact \
    --config min.insync.replicas=1
}

wait_for_batch() {
  batch_pids="$1"
  for pid in $batch_pids; do
    wait "$pid"
  done
}

run_topic_batch() {
  mode="$1"
  label="$2"
  total_items="$3"
  shift 3
  batch_pids=""
  batch_count=0
  batch_number=1
  processed=0

  echo "[kafka-bootstrap] starting ${label}: total=${total_items} parallel=${MAX_PARALLEL_CREATES}"
  for item in "$@"; do
    if [ "$mode" = "file" ]; then
      create_topic_from_file "$item" &
    else
      create_internal_topic "$item" &
    fi
    pid=$!
    batch_pids="${batch_pids} ${pid}"
    batch_count=$((batch_count + 1))

    if [ "$batch_count" -ge "$MAX_PARALLEL_CREATES" ]; then
      echo "[kafka-bootstrap] waiting for ${label} batch ${batch_number}"
      wait_for_batch "$batch_pids"
      processed=$((processed + batch_count))
      echo "[kafka-bootstrap] completed ${label} batch ${batch_number} (${processed}/${total_items})"
      batch_pids=""
      batch_count=0
      batch_number=$((batch_number + 1))
    fi
  done

  if [ "$batch_count" -gt 0 ]; then
    echo "[kafka-bootstrap] waiting for ${label} batch ${batch_number}"
    wait_for_batch "$batch_pids"
    processed=$((processed + batch_count))
    echo "[kafka-bootstrap] completed ${label} batch ${batch_number} (${processed}/${total_items})"
  fi
}

topic_files=""
topic_file_count=0
for topic_file in /config/kafka/topics/*.env; do
  topic_files="${topic_files} ${topic_file}"
  topic_file_count=$((topic_file_count + 1))
done
run_topic_batch file "topic definitions" "$topic_file_count" $topic_files

internal_topics=""
internal_topic_count=0
for internal_topic in \
  _schemas \
  debezium_source_connect_configs \
  debezium_source_connect_offsets \
  debezium_source_connect_statuses \
  debezium_sink_connect_configs \
  debezium_sink_connect_offsets \
  debezium_sink_connect_statuses; do
  internal_topics="${internal_topics} ${internal_topic}"
  internal_topic_count=$((internal_topic_count + 1))
done
run_topic_batch internal "internal topics" "$internal_topic_count" $internal_topics

echo "Kafka topics initialized."
