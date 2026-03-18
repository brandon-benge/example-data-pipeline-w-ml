#!/usr/bin/env bash
set -u -o pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGES=("ingestion" "stream-processing" "batch" "analytics" "ml")
DEFAULT_STOP_AT="ml"

usage() {
  cat <<'USAGE'
Usage:
  bash tools/run_stack_workflow.sh [--stop-at <stage>]

Options:
  --stop-at <stage>  Stop after validating this stage.
                     Allowed: ingestion, stream-processing, batch, analytics, ml
                     Default: ml
  -h, --help         Show this help message.
USAGE
}

is_valid_stage() {
  local stage="$1"
  local item
  for item in "${STAGES[@]}"; do
    if [[ "$item" == "$stage" ]]; then
      return 0
    fi
  done
  return 1
}

run_cmd() {
  local description="$1"
  shift
  echo
  echo "==> ${description}"
  "$@"
}

print_phase_state() {
  local stage="$1"
  local previous_stage="$2"
  local stop_at="$3"

  echo
  echo "---- Phase State ----"
  echo "Current phase : ${stage}"
  echo "Previous phase: ${previous_stage:-none}"
  echo "Stop target   : ${stop_at}"
  echo "---------------------"
}

validate_stage_with_retry() {
  local stage="$1"

  local max_attempts=12
  local interval_seconds=5

  if [[ "$stage" == "stream-processing" ]]; then
    max_attempts=120
  fi

  local attempt
  for (( attempt=1; attempt<=max_attempts; attempt++ )); do
    echo
    echo "[${stage}] Validation attempt ${attempt}/${max_attempts}"

    if python3 tools/validate_pipeline.py --stack "$stage"; then
      echo "[${stage}] Validation succeeded."
      return 0
    fi

    if (( attempt < max_attempts )); then
      echo "[${stage}] Validation not ready yet. Retrying in ${interval_seconds}s..."
      sleep "$interval_seconds"
    fi
  done

  if [[ "$stage" == "stream-processing" ]]; then
    echo "[${stage}] Validation failed after 10 minutes."
  else
    echo "[${stage}] Validation failed after 1 minute."
  fi
  return 1
}

main() {
  local stop_at="$DEFAULT_STOP_AT"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --stop-at)
        if [[ $# -lt 2 ]]; then
          echo "Error: --stop-at requires a stage value."
          usage
          return 2
        fi
        stop_at="$2"
        shift 2
        ;;
      -h|--help)
        usage
        return 0
        ;;
      *)
        echo "Error: Unknown argument: $1"
        usage
        return 2
        ;;
    esac
  done

  if ! is_valid_stage "$stop_at"; then
    echo "Error: Invalid --stop-at stage '$stop_at'."
    usage
    return 2
  fi

  cd "$ROOT_DIR" || return 1

  local stage
  local previous_stage=""
  for stage in "${STAGES[@]}"; do
    print_phase_state "$stage" "$previous_stage" "$stop_at"

    if [[ -n "$previous_stage" ]]; then
      run_cmd "Stopping stack: ${previous_stage}" python3 tools/manage_stack.py stop "$previous_stage" || return 1
    fi

    run_cmd "Starting stack: ${stage}" python3 tools/manage_stack.py up "$stage" || return 1

    if [[ "$stage" == "ingestion" ]]; then
      run_cmd "Running generator seed step" bash -c "source .venv/bin/activate && python3 generator/app.py --config params.yaml --mode both" || return 1
    fi

    validate_stage_with_retry "$stage" || return 1

    if [[ "$stage" == "$stop_at" ]]; then
      echo
      echo "Workflow complete through stage: ${stop_at}"
      return 0
    fi

    previous_stage="$stage"
  done

  echo
  echo "Workflow complete through all stages."
  return 0
}

main "$@"
