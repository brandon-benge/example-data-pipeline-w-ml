#!/bin/sh
set -eu

TRINO_URL="${TRINO_URL:-http://trino:8080}"
TRINO_CATALOG="${TRINO_CATALOG:-iceberg}"
DDL_PATH="${DDL_PATH:-/config/iceberg/bootstrap-cdc-rest-catalog.sql}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-60}"
SLEEP_SECONDS="${SLEEP_SECONDS:-2}"

attempt=0
until trino --server "$TRINO_URL" --execute "SHOW SCHEMAS FROM ${TRINO_CATALOG}" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
    echo "Timed out waiting for Trino catalog ${TRINO_CATALOG} at ${TRINO_URL}" >&2
    exit 1
  fi
  sleep "$SLEEP_SECONDS"
done

echo "Bootstrapping Iceberg REST catalog namespaces and CDC Bronze tables via Trino catalog ${TRINO_CATALOG}..."

if ! trino --server "$TRINO_URL" --catalog "$TRINO_CATALOG" --file "$DDL_PATH"; then
  echo "Failed to apply CDC bootstrap DDL from ${DDL_PATH}" >&2
  exit 1
fi

echo "Iceberg REST catalog namespaces and CDC Bronze tables initialized."
