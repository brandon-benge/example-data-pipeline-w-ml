#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

remove_if_exists() {
  local path="$1"
  if [[ -e "$path" ]]; then
    rm -rf "$path"
  fi
}

# Generated metadata history and latest-state snapshots.
remove_if_exists "$ROOT_DIR/metadata/lineage/bronze_to_silver_dimensions.jsonl"
remove_if_exists "$ROOT_DIR/metadata/lineage/bronze_to_silver_facts.jsonl"
remove_if_exists "$ROOT_DIR/metadata/lineage/silver_aggregates.jsonl"
remove_if_exists "$ROOT_DIR/metadata/lineage/build_ml_features.jsonl"
remove_if_exists "$ROOT_DIR/metadata/lineage/dbt_runs.jsonl"
remove_if_exists "$ROOT_DIR/metadata/lineage/latest_runs.json"
remove_if_exists "$ROOT_DIR/metadata/table_contracts/dq_results.jsonl"
remove_if_exists "$ROOT_DIR/metadata/table_contracts/dbt_test_results.jsonl"
remove_if_exists "$ROOT_DIR/metadata/table_contracts/latest_results.json"

# Generated dbt build state.
remove_if_exists "$ROOT_DIR/dbt/target"
mkdir -p "$ROOT_DIR/dbt/target"

# Generated Flink checkpoints and savepoints.
remove_if_exists "$ROOT_DIR/state/flink/checkpoints"
remove_if_exists "$ROOT_DIR/state/flink/savepoints"
mkdir -p "$ROOT_DIR/state/flink/checkpoints"
mkdir -p "$ROOT_DIR/state/flink/savepoints"

# Generated ML outputs.
remove_if_exists "$ROOT_DIR/ml/artifacts"
mkdir -p "$ROOT_DIR/ml/artifacts"
touch "$ROOT_DIR/ml/artifacts/.gitkeep"
