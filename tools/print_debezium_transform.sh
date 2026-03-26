#!/bin/sh
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
  echo "Usage: $0 <topic> [max_messages] [--insert-trino]" >&2
  exit 1
fi

topic="$1"
max_messages="1"
insert_trino="false"
namespace="${K8S_NAMESPACE:-}"

canonical_namespace() {
  app_name="$1"
  case "$app_name" in
    postgres|kafka|schema-registry|minio|iceberg-rest)
      echo "data-platform-infra"
      ;;
    kafka-connect-source|kafka-connect-sinks|generator)
      echo "data-platform-ingest"
      ;;
    flink-jobmanager|flink-taskmanager|spark|spark-bootstrap|dbt|dbt-scheduler)
      echo "data-platform-process"
      ;;
    trino|superset)
      echo "data-platform-serve"
      ;;
    metadata)
      echo "data-platform-govern"
      ;;
    *)
      echo "data-platform"
      ;;
  esac
}

shift
for arg in "$@"; do
  if [ "$arg" = "--insert-trino" ]; then
    insert_trino="true"
  else
    max_messages="$arg"
  fi
done

resolve_pod() {
  app_name="$1"
  pod_namespace="$(resolve_namespace "$app_name")"
  kubectl -n "$pod_namespace" get pods -l "app.kubernetes.io/name=$app_name" -o jsonpath='{.items[0].metadata.name}'
}

resolve_namespace() {
  app_name="$1"
  candidate="${namespace:-$(canonical_namespace "$app_name")}"
  if kubectl -n "$candidate" get pods -l "app.kubernetes.io/name=$app_name" -o jsonpath='{.items[0].metadata.name}' >/dev/null 2>&1; then
    echo "$candidate"
  else
    echo "data-platform"
  fi
}

run_helper() {
  sink_pod="$(resolve_pod kafka-connect-sinks)"
  sink_namespace="$(resolve_namespace kafka-connect-sinks)"
  cat tools/PrintDebeziumTransform.java | kubectl -n "$sink_namespace" exec -i "$sink_pod" -- sh -lc '
    cat >/tmp/PrintDebeziumTransform.java
    CP=$(find /kafka -type f -name "*.jar" | paste -sd: -)
    java -cp "$CP" /tmp/PrintDebeziumTransform.java "$@"
  ' sh "$@"
}

if [ "$insert_trino" = "true" ]; then
  sql="$(run_helper "$topic" "1" "--sql-only")"
  printf '%s\n' "$sql"
  trino_pod="$(resolve_pod trino)"
  trino_namespace="$(resolve_namespace trino)"
  kubectl -n "$trino_namespace" exec -i "$trino_pod" -- trino --server http://localhost:8080 --execute "$sql"
else
  run_helper "$topic" "$max_messages"
fi
