#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="${KAFKA_NAMESPACE:-data-platform-infra}"
STATEFULSET="${KAFKA_STATEFULSET:-kafka}"
CONTAINER="${KAFKA_CONTAINER:-kafka}"
OUTPUT_ROOT="${KAFKA_CAPTURE_DIR:-${ROOT_DIR}/tmp/kafka-state-captures}"
SAMPLE_INTERVAL_SECONDS="${KAFKA_CAPTURE_INTERVAL_SECONDS:-5}"
WATCH_MODE="false"
MAX_SAMPLES="${KAFKA_CAPTURE_MAX_SAMPLES:-0}"
LABEL=""

usage() {
  cat <<'USAGE'
Usage:
  bash tools/capture_kafka_state.sh [--label <name>]
  bash tools/capture_kafka_state.sh --watch [--interval <seconds>] [--max-samples <count>] [--label <name>]

Options:
  --label <name>         Add a label to the capture directory name.
  --watch                Capture repeatedly until interrupted or max samples is reached.
  --interval <seconds>   Seconds between watch samples. Default: 5
  --max-samples <count>  Stop after this many samples in watch mode. 0 means unlimited.
  -h, --help             Show this help message.
USAGE
}

run_local() {
  local output_file="$1"
  shift
  {
    echo "\$ $*"
    "$@"
  } >"${output_file}" 2>&1 || true
}

run_kafka_exec() {
  local output_file="$1"
  local script="$2"
  {
    echo "\$ kubectl -n ${NAMESPACE} exec statefulset/${STATEFULSET} -c ${CONTAINER} -- sh -lc '${script}'"
    kubectl -n "${NAMESPACE}" exec "statefulset/${STATEFULSET}" -c "${CONTAINER}" -- sh -lc "${script}"
  } >"${output_file}" 2>&1 || true
}

capture_once() {
  local capture_dir="$1"
  mkdir -p "${capture_dir}"

  run_local "${capture_dir}/kafka_pod.txt" \
    kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-0" -o wide
  run_local "${capture_dir}/kafka_pod_describe.txt" \
    kubectl -n "${NAMESPACE}" describe pod "${STATEFULSET}-0"
  run_local "${capture_dir}/kafka_statefulset.txt" \
    kubectl -n "${NAMESPACE}" get statefulset "${STATEFULSET}" -o yaml
  run_local "${capture_dir}/kafka_pvc.txt" \
    kubectl -n "${NAMESPACE}" get pvc kafka-data -o wide
  run_local "${capture_dir}/kafka_pvc_describe.txt" \
    kubectl -n "${NAMESPACE}" describe pvc kafka-data
  run_local "${capture_dir}/kafka_bootstrap_job.txt" \
    kubectl -n "${NAMESPACE}" get job kafka-bootstrap -o yaml
  run_local "${capture_dir}/kafka_bootstrap_logs.txt" \
    kubectl -n "${NAMESPACE}" logs job/kafka-bootstrap --tail=400
  run_local "${capture_dir}/infra_events.txt" \
    kubectl -n "${NAMESPACE}" get events --sort-by=.lastTimestamp
  run_local "${capture_dir}/all_pods_wide.txt" \
    kubectl get pods -A -o wide
  run_local "${capture_dir}/nodes.txt" \
    kubectl get nodes -o wide

  run_kafka_exec "${capture_dir}/topic_list.txt" \
    "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list"
  run_kafka_exec "${capture_dir}/topic_describe.txt" \
    "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe"
  run_kafka_exec "${capture_dir}/metadata_quorum.txt" \
    "/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka:9092 describe --status"
  run_kafka_exec "${capture_dir}/meta_properties.txt" \
    "cat /var/lib/kafka/data/meta.properties"
  run_kafka_exec "${capture_dir}/data_dir_listing.txt" \
    "ls -lah /var/lib/kafka && echo --- && ls -lah /var/lib/kafka/data"
  run_kafka_exec "${capture_dir}/partition_dirs.txt" \
    "for d in /var/lib/kafka/data/*; do [ -d \"\$d\" ] && basename \"\$d\"; done | sort"
  run_kafka_exec "${capture_dir}/dir_timestamps.txt" \
    "for d in /var/lib/kafka/data/*; do [ -e \"\$d\" ] && ls -ld \"\$d\"; done | sort"
  run_kafka_exec "${capture_dir}/cluster_metadata_dir.txt" \
    "ls -lah /var/lib/kafka/data/__cluster_metadata-0 2>/dev/null || true"
  run_kafka_exec "${capture_dir}/bootstrap_checkpoint.txt" \
    "ls -lah /var/lib/kafka/data/bootstrap.checkpoint 2>/dev/null && od -An -tx1 -v /var/lib/kafka/data/bootstrap.checkpoint | sed -n '1,80p'"
  run_kafka_exec "${capture_dir}/kafka_processes.txt" \
    "ps -ef"
  run_local "${capture_dir}/kafka_logs.txt" \
    kubectl -n "${NAMESPACE}" logs "statefulset/${STATEFULSET}" -c "${CONTAINER}" --tail=1000

  cat >"${capture_dir}/README.txt" <<EOF
capture_dir=${capture_dir}
captured_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
namespace=${NAMESPACE}
statefulset=${STATEFULSET}
container=${CONTAINER}
EOF
}

timestamp_utc() {
  date -u +"%Y%m%dT%H%M%SZ"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --label)
      LABEL="$2"
      shift 2
      ;;
    --watch)
      WATCH_MODE="true"
      shift
      ;;
    --interval)
      SAMPLE_INTERVAL_SECONDS="$2"
      shift 2
      ;;
    --max-samples)
      MAX_SAMPLES="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

mkdir -p "${OUTPUT_ROOT}"

sample_count=0
while true; do
  sample_count=$((sample_count + 1))
  suffix="$(timestamp_utc)"
  if [[ -n "${LABEL}" ]]; then
    suffix="${suffix}-${LABEL}"
  fi
  capture_dir="${OUTPUT_ROOT}/${suffix}"
  echo "Capturing Kafka state into ${capture_dir}"
  capture_once "${capture_dir}"

  if [[ "${WATCH_MODE}" != "true" ]]; then
    break
  fi

  if [[ "${MAX_SAMPLES}" != "0" && "${sample_count}" -ge "${MAX_SAMPLES}" ]]; then
    break
  fi

  sleep "${SAMPLE_INTERVAL_SECONDS}"
done
