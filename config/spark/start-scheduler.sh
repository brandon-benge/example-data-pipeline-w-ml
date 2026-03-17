#!/bin/sh
set -eu

STALE_MINUTES="${TEMP_DIR_TTL_MINUTES:-20}"

cleanup_path() {
  path="$1"
  if [ -e "$path" ]; then
    rm -rf "$path"
  fi
}

cleanup_startup_paths() {
  cleanup_path /tmp/spark-local
  cleanup_stale_paths
}

cleanup_stale_paths() {
  find /tmp -maxdepth 1 \( \
    -name 'spark-*' -o \
    -name 'blockmgr-*' -o \
    -name 'artifacts-*' -o \
    -name 'liblz4-java*.so' -o \
    -name 'liblz4-java*.so.lck' \
  \) -mmin +"$STALE_MINUTES" -exec rm -rf {} +
}

cleanup_startup_paths

mkdir -p /tmp/spark-local
export SPARK_LOCAL_DIRS=/tmp/spark-local

exec /bin/sh /app/bootstrap/run-jobs.sh
