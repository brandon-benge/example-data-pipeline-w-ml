#!/bin/sh
set -eu

STALE_MINUTES="${TEMP_DIR_TTL_MINUTES:-20}"

cleanup_path() {
  path="$1"
  if [ -e "$path" ]; then
    if [ -d "$path" ]; then
      find "$path" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    else
      rm -f "$path"
    fi
  fi
}

cleanup_startup_paths() {
  cleanup_path /tmp/flink-tmp
  cleanup_stale_paths
}

cleanup_stale_paths() {
  find /tmp -maxdepth 1 \( \
    -name 'tm_*' -o \
    -name 'flink-dist-cache-*' -o \
    -name 'flink-io-*' -o \
    -name 'flink-netty-shuffle-*' -o \
    -name 'python-dist-*' -o \
    -name 'flink-rpc-akka*.jar' \
  \) -mmin +"$STALE_MINUTES" -exec rm -rf {} +
}

cleanup_startup_paths

mkdir -p /tmp/flink-tmp

role="${1:-}"

case "$role" in
  jobmanager)
    exec /opt/flink/bin/jobmanager.sh start-foreground
    ;;
  taskmanager)
    exec /opt/flink/bin/taskmanager.sh start-foreground
    ;;
  *)
    echo "Usage: $0 <jobmanager|taskmanager>" >&2
    exit 1
    ;;
esac
