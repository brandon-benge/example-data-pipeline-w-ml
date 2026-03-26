#!/bin/sh
set -eu

CONNECT_BASE_URL="${CONNECT_BASE_URL:-http://kafka-connect:8083}"
CONNECT_ROLE="${CONNECT_ROLE:-all}"
CONNECT_SINK_ALLOWLIST="${CONNECT_SINK_ALLOWLIST:-}"
CONNECT_READY_ATTEMPTS="${CONNECT_READY_ATTEMPTS:-60}"
CONNECT_READY_INTERVAL_SECONDS="${CONNECT_READY_INTERVAL_SECONDS:-5}"

SINK_SPECS=$(cat <<'EOF'
sales_rep|postgres-cdc-sales-rep-iceberg-sink|cdc.sales_rep|bronze.bronze_sales_rep_cdc|connect-iceberg-control-sales-rep-v1
customer|postgres-cdc-customer-iceberg-sink|cdc.customer|bronze.bronze_customer_cdc|connect-iceberg-control-customer-v1
advertiser|postgres-cdc-advertiser-iceberg-sink|cdc.advertiser|bronze.bronze_advertiser_cdc|connect-iceberg-control-advertiser-v1
product|postgres-cdc-product-iceberg-sink|cdc.product|bronze.bronze_product_cdc|connect-iceberg-control-product-v1
campaign|postgres-cdc-campaign-iceberg-sink|cdc.campaign|bronze.bronze_campaign_cdc|connect-iceberg-control-campaign-v1
campaign_product|postgres-cdc-campaign-product-iceberg-sink|cdc.campaign_product|bronze.bronze_campaign_product_cdc|connect-iceberg-control-campaign-product-v1
customer_session|postgres-cdc-customer-session-iceberg-sink|cdc.customer_session|bronze.bronze_customer_session_cdc|connect-iceberg-control-customer-session-v2
order_header|postgres-cdc-order-header-iceberg-sink|cdc.order_header|bronze.bronze_order_header_cdc|connect-iceberg-control-order-header-v1
order_item|postgres-cdc-order-item-iceberg-sink|cdc.order_item|bronze.bronze_order_item_cdc|connect-iceberg-control-order-item-v1
sales_activity|postgres-cdc-sales-activity-iceberg-sink|cdc.sales_activity|bronze.bronze_sales_activity_cdc|connect-iceberg-control-sales-activity-v1
EOF
)

register_connector() {
  name="$1"
  create_payload="$2"
  config_payload="${3:-}"
  connector_url="$CONNECT_BASE_URL/connectors/$name"
  config_url="$connector_url/config"

  if curl -fsS "$connector_url" >/dev/null 2>&1; then
    if [ -n "$config_payload" ]; then
      curl -fsS -X PUT \
        -H "Content-Type: application/json" \
        --data @"$config_payload" \
        "$config_url" >/dev/null
      echo "Connector updated: $name"
    else
      echo "Connector already registered, leaving unchanged: $name"
    fi
  else
    curl -fsS -X POST \
      -H "Content-Type: application/json" \
      --data @"$create_payload" \
      "$CONNECT_BASE_URL/connectors" >/dev/null
  fi
}

wait_for_connect() {
  attempt=1
  while [ "$attempt" -le "$CONNECT_READY_ATTEMPTS" ]; do
    if curl -fsS "$CONNECT_BASE_URL/connectors" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$CONNECT_READY_INTERVAL_SECONDS"
    attempt=$((attempt + 1))
  done

  echo "Kafka Connect REST API did not become ready at $CONNECT_BASE_URL" >&2
  return 1
}

register_postgres_source_connector() {
  payload_file="$(mktemp)"
  config_file="$(mktemp)"
  dollar='$'

  cat >"$config_file" <<EOF
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "${POSTGRES_CONNECT_HOST:-postgres.data-platform-infra}",
  "database.port": "${POSTGRES_CONNECT_PORT:-5432}",
  "database.user": "${POSTGRES_USER:-app_user}",
  "database.password": "${POSTGRES_PASSWORD:-change-me}",
  "database.dbname": "${POSTGRES_DB:-app_db}",
  "topic.prefix": "cdc",
  "plugin.name": "pgoutput",
  "slot.name": "debezium_slot",
  "publication.name": "debezium_publication",
  "publication.autocreate.mode": "filtered",
  "table.include.list": "public.sales_rep,public.customer,public.advertiser,public.product,public.campaign,public.campaign_product,public.customer_session,public.order_header,public.order_item,public.sales_activity",
  "transforms": "route",
  "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.route.regex": "cdc\\\\.public\\\\.(.*)",
  "transforms.route.replacement": "cdc.${dollar}1",
  "tombstones.on.delete": "true",
  "provide.transaction.metadata": "true",
  "decimal.handling.mode": "precise",
  "time.precision.mode": "connect",
  "snapshot.fetch.size": "2000",
  "max.batch.size": "2048",
  "max.queue.size": "8192",
  "poll.interval.ms": "1000"
}
EOF

  cat >"$payload_file" <<EOF
{
  "name": "postgres-cdc-connector",
  "config": $(cat "$config_file")
}
EOF

  register_connector "postgres-cdc-connector" "$payload_file" "$config_file"
  rm -f "$payload_file" "$config_file"
}

delete_connector() {
  name="$1"
  connector_url="$CONNECT_BASE_URL/connectors/$name"

  if curl -fsS "$connector_url" >/dev/null 2>&1; then
    curl -fsS -X DELETE "$connector_url" >/dev/null
    echo "Connector removed: $name"
  fi
}

register_cdc_sink_connector() {
  connector_name="$1"
  source_topic="$2"
  target_table="$3"
  control_topic="$4"
  payload_file="$(mktemp)"
  config_file="$(mktemp)"
  extra_config_file="$(mktemp)"

  : >"$extra_config_file"
  if [ "$connector_name" = "postgres-cdc-customer-session-iceberg-sink" ] || [ "$connector_name" = "postgres-cdc-order-header-iceberg-sink" ]; then
    cat >"$extra_config_file" <<EOF
  ,"iceberg.control.commit.interval-ms": "15000"
  ,"iceberg.control.commit.timeout-ms": "300000"
EOF
  fi

  cat >"$config_file" <<EOF
{
  "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
  "tasks.max": "1",
  "topics": "$source_topic",
  "consumer.override.auto.offset.reset": "earliest",
  "consumer.override.max.poll.records": "500",
  "consumer.override.max.poll.interval.ms": "900000",
  "consumer.override.session.timeout.ms": "90000",
  "consumer.override.heartbeat.interval.ms": "15000",
  "consumer.override.max.partition.fetch.bytes": "1048576",
  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true",
  "errors.deadletterqueue.topic.name": "${source_topic}.dlq",
  "errors.deadletterqueue.topic.replication.factor": "1",
  "errors.deadletterqueue.context.headers.enable": "true",
  "iceberg.catalog.type": "rest",
  "iceberg.catalog.uri": "http://iceberg-rest.data-platform-infra:8181",
  "iceberg.catalog.warehouse": "s3://warehouse/",
  "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
  "iceberg.catalog.s3.endpoint": "http://minio.data-platform-infra:9000",
  "iceberg.catalog.s3.path-style-access": "true",
  "iceberg.catalog.s3.access-key-id": "minio",
  "iceberg.catalog.s3.secret-access-key": "minio123",
  "iceberg.tables": "$target_table",
  "iceberg.tables.auto-create-enabled": "false",
  "iceberg.control.topic": "$control_topic",
  "iceberg.control.commit.timeout-ms": "180000",
  "iceberg.control.commit.interval-ms": "5000",
  "transforms": "cdc,kafkaMeta",
  "transforms.cdc.type": "org.apache.iceberg.connect.transforms.DebeziumTransform",
  "transforms.cdc.cdc.target.pattern": "$target_table",
  "transforms.kafkaMeta.type": "org.apache.iceberg.connect.transforms.KafkaMetadataTransform",
  "transforms.kafkaMeta.nested": "false",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "true",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true"$(cat "$extra_config_file")
}
EOF

  cat >"$payload_file" <<EOF
{
  "name": "$connector_name",
  "config": $(cat "$config_file")
}
EOF

  register_connector "$connector_name" "$payload_file" "$config_file"
  rm -f "$payload_file" "$config_file" "$extra_config_file"
}

slug_allowed() {
  slug="$1"
  if [ -z "$CONNECT_SINK_ALLOWLIST" ]; then
    return 0
  fi

  old_ifs="$IFS"
  IFS=','
  for allowed in $CONNECT_SINK_ALLOWLIST; do
    if [ "$allowed" = "$slug" ]; then
      IFS="$old_ifs"
      return 0
    fi
  done
  IFS="$old_ifs"
  return 1
}

if [ "$CONNECT_ROLE" = "all" ] || [ "$CONNECT_ROLE" = "source" ]; then
  wait_for_connect
  register_postgres_source_connector
fi

if [ "$CONNECT_ROLE" = "all" ] || [ "$CONNECT_ROLE" = "sinks" ]; then
  wait_for_connect
  printf '%s\n' "$SINK_SPECS" | while IFS='|' read -r slug connector_name source_topic target_table control_topic; do
    if slug_allowed "$slug"; then
      register_cdc_sink_connector "$connector_name" "$source_topic" "$target_table" "$control_topic"
    else
      delete_connector "$connector_name"
    fi
  done
fi

echo "Kafka Connect connectors registered for role: $CONNECT_ROLE allowlist=${CONNECT_SINK_ALLOWLIST:-all}"
