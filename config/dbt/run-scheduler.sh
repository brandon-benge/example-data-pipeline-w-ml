#!/bin/sh
set -eu

/bin/sh /app/bootstrap/bootstrap.sh

interval_seconds="${DBT_BUILD_INTERVAL_SECONDS:-120}"
chunk_sleep_seconds="${DBT_BUILD_CHUNK_SLEEP_SECONDS:-5}"

run_chunk() {
  label="$1"
  shift

  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Starting dbt chunk: $label"
  if dbt build "$@"; then
    python3 /app/bootstrap/export_metadata.py "$label"
    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Completed dbt chunk: $label"
    return 0
  fi

  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Failed dbt chunk: $label"
  return 1
}

while true; do
  cycle_failed=0

  run_chunk "staging" --select staging || cycle_failed=1

  if [ "$cycle_failed" -eq 0 ]; then
    sleep "$chunk_sleep_seconds"
    run_chunk "features" --select features || cycle_failed=1
  fi

  if [ "$cycle_failed" -eq 0 ]; then
    sleep "$chunk_sleep_seconds"
    run_chunk "marts" --select marts || cycle_failed=1
  fi

  if [ "$cycle_failed" -eq 0 ]; then
    sleep "$chunk_sleep_seconds"
    run_chunk "semantic" --select semantic || cycle_failed=1
  fi

  if [ "$cycle_failed" -eq 0 ]; then
    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Completed dbt scheduler cycle successfully"
  else
    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] dbt scheduler cycle failed; retrying after interval"
  fi

  sleep "$interval_seconds"
done
