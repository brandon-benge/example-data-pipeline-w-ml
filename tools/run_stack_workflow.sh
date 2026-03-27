#!/usr/bin/env bash
set -u -o pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -x "${ROOT_DIR}/.venv/bin/python3" ]]; then
  PYTHON_BIN="${ROOT_DIR}/.venv/bin/python3"
else
  PYTHON_BIN="python3"
fi
STAGES=("ingestion" "stream-processing" "batch" "analytics")
DEFAULT_STOP_AT="analytics"
PORT_FORWARD_PIDS=()
DEFAULT_COMMAND="run"
READINESS_SKIPS=(--skip connect --skip kafka --skip flink --skip postgres --skip trino --skip metadata --skip dbt --skip generator)
GENERATOR_PREFLIGHT_SKIPS=(--skip services --skip http --skip connect --skip kafka --skip postgres --skip trino --skip metadata --skip dbt)
DEFAULT_GENERATOR_PREFLIGHT_ATTEMPTS=48
DEFAULT_GENERATOR_PREFLIGHT_INTERVAL_SECONDS=5

namespace_for_service() {
  local service="$1"
  case "$service" in
    postgres|kafka|schema-registry|minio|iceberg-rest)
      echo "data-platform-infra"
      ;;
    kafka-connect-source|kafka-connect-sinks|generator)
      echo "data-platform-ingest"
      ;;
    flink-jobmanager|flink-taskmanager|spark|spark-bootstrap|dbt|dbt-scheduler)
      echo "data-platform-process"
      ;;
    trino|superset)
      echo "data-platform-serve"
      ;;
    metadata)
      echo "data-platform-govern"
      ;;
    *)
      echo "data-platform"
      ;;
  esac
}

cleanup_port_forwards() {
  local pid
  for pid in "${PORT_FORWARD_PIDS[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
}

trap cleanup_port_forwards EXIT

usage() {
  cat <<'USAGE'
Usage:
  bash tools/run_stack_workflow.sh [run] [--stop-at <stage>]
  bash tools/run_stack_workflow.sh destroy
  bash tools/run_stack_workflow.sh stop <stack>
  bash tools/run_stack_workflow.sh status <stack>
  bash tools/run_stack_workflow.sh logs <stack>
  bash tools/run_stack_workflow.sh logs --follow <stack>

Options:
  --stop-at <stage>  Stop after validating this stage.
                     Allowed: ingestion, stream-processing, batch, analytics
                     Default: analytics
  --follow           Follow logs for the first matching resource when using `logs`.
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

stage_requires_stream_processing() {
  local stage="$1"
  case "$stage" in
    stream-processing|batch|analytics)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

run_cmd() {
  local description="$1"
  shift
  echo
  echo "==> ${description}"
  "$@"
}

prepare_connect_plugin_cache() {
  if ! stage_requires_stream_processing "$1"; then
    return 0
  fi

  run_cmd \
    "Preparing Kafka Connect Iceberg plugin cache before manifest apply" \
    bash tools/preload_connect_plugin_cache.sh
}

ensure_platform() {
  run_cmd "Applying Kubernetes platform" "$PYTHON_BIN" tools/manage_stack.py up "$1" || return 1
}

wait_for_job_completion() {
  local namespace="$1"
  local job_name="$2"
  local timeout="${3:-300s}"
  local progress_pid=""

  echo
  echo "==> Waiting for job/${job_name} in ${namespace}"
  if [[ "$namespace" == "data-platform-infra" && "$job_name" == "kafka-bootstrap" ]]; then
    stream_job_logs_until_completion "$namespace" "$job_name" &
    progress_pid=$!
  fi

  if kubectl -n "$namespace" wait --for=condition=complete "job/${job_name}" --timeout="$timeout"; then
    if [[ -n "$progress_pid" ]]; then
      wait "$progress_pid" >/dev/null 2>&1 || true
    fi
    return 0
  fi

  if [[ -n "$progress_pid" ]]; then
    kill "$progress_pid" >/dev/null 2>&1 || true
    wait "$progress_pid" >/dev/null 2>&1 || true
  fi

  echo "job/${job_name} did not complete in time. Recent logs:" >&2
  kubectl -n "$namespace" logs "job/${job_name}" --tail=120 >&2 || true
  return 1
}

stream_job_logs_until_completion() {
  local namespace="$1"
  local job_name="$2"
  local log_started=0

  while true; do
    if kubectl -n "$namespace" logs -f "job/${job_name}" 2>/dev/null; then
      return 0
    fi

    if kubectl -n "$namespace" get "job/${job_name}" -o jsonpath='{.status.completionTime}' 2>/dev/null | grep -q '.'; then
      return 0
    fi

    if [[ "$log_started" -eq 0 ]]; then
      echo "   Streaming kafka-bootstrap progress as topics are reconciled..."
      log_started=1
    fi
    sleep 2
  done
}

get_first_pod_creation_timestamp() {
  local namespace="$1"
  local label_selector="$2"
  kubectl -n "$namespace" get pods -l "$label_selector" -o jsonpath='{.items[0].metadata.creationTimestamp}' 2>/dev/null || true
}

get_job_completion_timestamp() {
  local namespace="$1"
  local job_name="$2"
  kubectl -n "$namespace" get "job/${job_name}" -o jsonpath='{.status.completionTime}' 2>/dev/null || true
}

ensure_kafka_bootstrap_is_fresh() {
  local namespace="data-platform-infra"
  local kafka_created_at
  local bootstrap_completed_at

  kafka_created_at="$(get_first_pod_creation_timestamp "$namespace" 'app.kubernetes.io/name=kafka')"
  bootstrap_completed_at="$(get_job_completion_timestamp "$namespace" 'kafka-bootstrap')"

  if [[ -z "$kafka_created_at" ]]; then
    echo "Kafka pod has not been created yet; leaving kafka-bootstrap as-is."
    return 0
  fi

  if [[ -z "$bootstrap_completed_at" ]]; then
    echo "kafka-bootstrap has not completed yet for the current cluster state."
    return 0
  fi

  if [[ "$bootstrap_completed_at" < "$kafka_created_at" ]]; then
    echo "kafka-bootstrap completed at ${bootstrap_completed_at}, but kafka pod was created at ${kafka_created_at}."
    echo "Recreating kafka-bootstrap so topics are initialized against the current broker."
    kubectl -n "$namespace" delete job kafka-bootstrap --ignore-not-found
    kubectl apply --validate=false -f "${ROOT_DIR}/k8s/platform.yaml"
  fi
}

port_is_listening() {
  local port="$1"
  if command -v nc >/dev/null 2>&1; then
    nc -z localhost "$port" >/dev/null 2>&1
  else
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
  fi
}

start_port_forward() {
  local service="$1"
  local local_port="$2"
  local remote_port="$3"
  local namespace
  namespace="$(namespace_for_service "$service")"
  local log_file="${ROOT_DIR}/.tmp-port-forward-${service}-${local_port}.log"

  if port_is_listening "$local_port"; then
    echo "==> Reusing existing localhost:${local_port} listener for ${service}"
    return 0
  fi

  if ! kubectl get "svc/${service}" -n "$namespace" >/dev/null 2>&1; then
    namespace="data-platform"
  fi

  kubectl -n "$namespace" port-forward "svc/${service}" "${local_port}:${remote_port}" >"$log_file" 2>&1 &
  local pid=$!
  local attempt
  for (( attempt=1; attempt<=20; attempt++ )); do
    if port_is_listening "$local_port"; then
      PORT_FORWARD_PIDS+=("$pid")
      echo "==> Port-forward ready: ${service} localhost:${local_port}"
      return 0
    fi
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      cat "$log_file" >&2 || true
      return 1
    fi
    sleep 1
  done

  kill "$pid" >/dev/null 2>&1 || true
  cat "$log_file" >&2 || true
  echo "Failed to establish port-forward for ${service} on localhost:${local_port}" >&2
  return 1
}

ensure_stage_access() {
  local stage="$1"

  echo
  echo "==> Ensuring workstation access for stage: ${stage}"

  case "$stage" in
    ingestion)
      ensure_kafka_bootstrap_is_fresh || return 1
      wait_for_job_completion "data-platform-infra" "kafka-bootstrap" "900s" || return 1
      start_port_forward "postgres" "5432" "5432" || return 1
      start_port_forward "kafka" "19092" "19092" || return 1
      start_port_forward "schema-registry" "8081" "8081" || return 1
      ;;
    stream-processing)
      start_port_forward "flink-jobmanager" "8082" "8081" || return 1
      start_port_forward "trino" "8080" "8080" || return 1
      ;;
    batch)
      start_port_forward "metadata" "9002" "9002" || return 1
      ;;
    analytics)
      start_port_forward "superset" "8088" "8088" || return 1
      ;;
  esac
}

validate_stage_runtime_ready() {
  local stage="$1"
  local max_attempts=30
  local interval_seconds=5
  local attempt
  local validation_cmd=("$PYTHON_BIN" tools/validate_pipeline.py --stack "$stage" --stability-seconds 0 "${READINESS_SKIPS[@]}")

  for (( attempt=1; attempt<=max_attempts; attempt++ )); do
    echo
    echo "[${stage}] Runtime readiness attempt ${attempt}/${max_attempts}"
    echo "[${stage}] Validation command: ${validation_cmd[*]}"

    if "${validation_cmd[@]}"; then
      echo "[${stage}] Runtime services are ready for port-forwarding."
      return 0
    fi

    if (( attempt < max_attempts )); then
      echo "[${stage}] Runtime services not ready yet. Retrying in ${interval_seconds}s..."
      sleep "$interval_seconds"
    fi
  done

  echo "[${stage}] Runtime services did not become ready for port-forwarding."
  return 1
}

validate_generator_preflight() {
  echo
  echo "==> Validating generator preflight"
  local max_attempts="$DEFAULT_GENERATOR_PREFLIGHT_ATTEMPTS"
  local interval_seconds="$DEFAULT_GENERATOR_PREFLIGHT_INTERVAL_SECONDS"
  local attempt
  local validation_cmd=("$PYTHON_BIN" tools/validate_pipeline.py --stack ingestion --stability-seconds 0 "${GENERATOR_PREFLIGHT_SKIPS[@]}")

  for (( attempt=1; attempt<=max_attempts; attempt++ )); do
    echo "[generator-preflight] Attempt ${attempt}/${max_attempts}"
    ensure_stage_access "ingestion" || true
    echo "[generator-preflight] Validation command: ${validation_cmd[*]}"
    if "${validation_cmd[@]}"; then
      echo "[generator-preflight] Generator dependencies and host access are ready."
      return 0
    fi

    if (( attempt < max_attempts )); then
      echo "[generator-preflight] Not ready yet. Retrying in ${interval_seconds}s..."
      sleep "$interval_seconds"
    fi
  done

  echo "[generator-preflight] Preflight failed."
  return 1
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
  local validation_cmd=("$PYTHON_BIN" tools/validate_pipeline.py --stack "$stage")

  if [[ "$stage" == "stream-processing" ]]; then
    max_attempts=120
  fi

  local attempt
  for (( attempt=1; attempt<=max_attempts; attempt++ )); do
    echo
    echo "[${stage}] Validation attempt ${attempt}/${max_attempts}"
    echo "[${stage}] Validation command: ${validation_cmd[*]}"

    ensure_stage_access "$stage" || return 1

    if "${validation_cmd[@]}"; then
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

run_workflow() {
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

  prepare_connect_plugin_cache "$stop_at" || return 1

  local stage
  local previous_stage=""
  for stage in "${STAGES[@]}"; do
    print_phase_state "$stage" "$previous_stage" "$stop_at"

    if [[ -n "$previous_stage" ]]; then
      echo
      echo "==> Reusing existing Kubernetes platform deployment for stage: ${stage}"
    else
      ensure_platform "$stage" || return 1
    fi

    validate_stage_runtime_ready "$stage" || return 1
    ensure_stage_access "$stage" || return 1

    if [[ "$stage" == "ingestion" ]]; then
      validate_generator_preflight || return 1
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

main() {
  local command="$DEFAULT_COMMAND"
  local follow_logs="false"

  if [[ $# -gt 0 ]]; then
    case "$1" in
      run|destroy|stop|status|logs)
        command="$1"
        shift
        ;;
      --stop-at|-h|--help)
        ;;
      *)
        echo "Error: Unknown command or option: $1"
        usage
        return 2
        ;;
    esac
  fi

  cd "$ROOT_DIR" || return 1

  case "$command" in
    run)
      run_workflow "$@"
      ;;
    destroy)
      if [[ $# -gt 0 ]]; then
        echo "Error: destroy does not accept additional arguments."
        usage
        return 2
      fi
      "$PYTHON_BIN" tools/manage_stack.py destroy
      ;;
    stop)
      if [[ $# -ne 1 ]]; then
        echo "Error: stop requires exactly one stack name."
        usage
        return 2
      fi
      "$PYTHON_BIN" tools/manage_stack.py stop "$1"
      ;;
    status)
      if [[ $# -ne 1 ]]; then
        echo "Error: status requires exactly one stack name."
        usage
        return 2
      fi
      "$PYTHON_BIN" tools/manage_stack.py ps "$1" --all
      ;;
    logs)
      if [[ $# -gt 0 && ( "$1" == "-f" || "$1" == "--follow" ) ]]; then
        follow_logs="true"
        shift
      fi
      if [[ $# -ne 1 ]]; then
        echo "Error: logs requires exactly one stack name."
        usage
        return 2
      fi
      if [[ "$follow_logs" == "true" ]]; then
        "$PYTHON_BIN" tools/manage_stack.py logs "$1" --follow
      else
        "$PYTHON_BIN" tools/manage_stack.py logs "$1"
      fi
      ;;
  esac
}

main "$@"
