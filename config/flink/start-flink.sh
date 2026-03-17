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

exec /docker-entrypoint.sh "$@"
