#!/bin/sh
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
  echo "Usage: $0 <topic> [max_messages] [--insert-trino]" >&2
  exit 1
fi

topic="$1"
max_messages="1"
insert_trino="false"

shift
for arg in "$@"; do
  if [ "$arg" = "--insert-trino" ]; then
    insert_trino="true"
  else
    max_messages="$arg"
  fi
done

run_helper() {
  cat tools/PrintDebeziumTransform.java | docker compose exec -T kafka-connect-sinks sh -lc '
    cat >/tmp/PrintDebeziumTransform.java
    CP=$(find /kafka -type f -name "*.jar" | paste -sd: -)
    java -cp "$CP" /tmp/PrintDebeziumTransform.java "$@"
  ' sh "$@"
}

if [ "$insert_trino" = "true" ]; then
  sql="$(run_helper "$topic" "1" "--sql-only")"
  printf '%s\n' "$sql"
  docker compose exec -T trino trino --server http://localhost:8080 --execute "$sql"
else
  run_helper "$topic" "$max_messages"
fi
