#!/bin/sh
set -eu

command -v git >/dev/null 2>&1 || {
  apt-get update
  apt-get install -y --no-install-recommends git
  rm -rf /var/lib/apt/lists/*
}

mkdir -p /tmp/dbt-home/.local/bin /tmp/dbt-profiles

python3 -c "import dbt.adapters.spark, pyspark; raise SystemExit(0 if pyspark.__version__ == '4.0.1' else 1)" >/dev/null 2>&1 || {
  python3 -m pip install --no-cache-dir 'dbt-spark[session]==1.8.0'
  python3 -m pip install --no-cache-dir pyspark==4.0.1
}
