#!/bin/sh
set -eu

for subject in /config/schema-registry/subjects/*.json; do
  name="$(basename "$subject" .json)"
  curl -fsS -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data @"$subject" \
    "http://schema-registry:8081/subjects/${name}/versions" >/dev/null
done

curl -fsS -X PUT \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"BACKWARD"}' \
  http://schema-registry:8081/config/events.session_event-key >/dev/null

curl -fsS -X PUT \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility":"BACKWARD"}' \
  http://schema-registry:8081/config/events.session_event-value >/dev/null

echo "Schema Registry subjects initialized."
