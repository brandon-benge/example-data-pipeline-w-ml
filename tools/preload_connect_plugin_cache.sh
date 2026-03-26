#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="data-platform-ingest"
PVC_NAME="kafka-connect-plugin-cache"
POD_NAME="kafka-connect-plugin-cache-loader"
DEFAULT_LOCAL_CACHE_ROOT="${ROOT_DIR}/.cache/iceberg-kafka-connect"
SOURCE_DIR="${1:-}"
ICEBERG_VERSION="$(sed -n 's/^ARG ICEBERG_VERSION=//p' "${ROOT_DIR}/config/debezium/Dockerfile" | head -1)"
LOCAL_PLUGIN_DIR="${DEFAULT_LOCAL_CACHE_ROOT}/${ICEBERG_VERSION}/lib"
REQUIRED_JAR_PATTERNS=(
  'iceberg-kafka-connect-*.jar'
  'iceberg-kafka-connect-events-*.jar'
  'iceberg-kafka-connect-transforms-*.jar'
  'iceberg-core-*.jar'
  'iceberg-api-*.jar'
  'iceberg-aws-*.jar'
)

usage() {
  cat <<'EOF'
Usage:
  bash tools/preload_connect_plugin_cache.sh [directory-of-plugin-jars]

If a directory is provided, the script validates that it contains the required
Apache Iceberg Kafka Connect runtime JARs and uploads them into
pvc/kafka-connect-plugin-cache.

If no directory is provided, the script validates the local cache under
.cache/iceberg-kafka-connect/<version>/lib. When the cache is missing or
incomplete, it builds the upstream runtime locally with Docker and then uploads
the resulting JARs into the PVC.
EOF
}

cleanup() {
  kubectl -n "$NAMESPACE" delete pod "$POD_NAME" --ignore-not-found >/dev/null 2>&1 || true
}

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
}

validate_plugin_dir() {
  local dir="$1"
  local pattern

  [[ -d "$dir" ]] || return 1

  for pattern in "${REQUIRED_JAR_PATTERNS[@]}"; do
    if ! find "$dir" -maxdepth 1 -name "$pattern" | grep -q .; then
      return 1
    fi
  done

  return 0
}

build_plugin_dir() {
  local target_dir="$1"
  local image_tag="local/iceberg-connect-builder:${ICEBERG_VERSION}"
  local container_name="iceberg-connect-builder-${ICEBERG_VERSION//./-}"

  require_command docker

  mkdir -p "$target_dir"

  echo "Building Apache Iceberg Kafka Connect runtime ${ICEBERG_VERSION} locally"
  docker build \
    --target iceberg_connect_builder \
    --tag "$image_tag" \
    -f "${ROOT_DIR}/config/debezium/Dockerfile" \
    "${ROOT_DIR}"

  docker rm -f "$container_name" >/dev/null 2>&1 || true
  docker create --name "$container_name" "$image_tag" >/dev/null

  rm -rf "${target_dir:?}/"*
  docker cp "${container_name}:/opt/iceberg-kafka-connect/." "$target_dir"
  docker rm -f "$container_name" >/dev/null
}

ensure_pvc() {
  echo "Ensuring namespace ${NAMESPACE} exists"
  kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

  echo "Ensuring pvc/${PVC_NAME} exists"
  kubectl apply -f - <<EOF >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${PVC_NAME}
  namespace: ${NAMESPACE}
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: do-block-storage
  resources:
    requests:
      storage: 100Mi
EOF
}

upload_plugin_dir() {
  local dir="$1"

  trap cleanup EXIT

  echo "Creating helper pod ${POD_NAME}"
  cleanup
  kubectl apply -f - <<EOF >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_NAME}
  namespace: ${NAMESPACE}
spec:
  restartPolicy: Never
  containers:
    - name: loader
      image: busybox:1.36.1
      command: ["/bin/sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: plugin-cache
          mountPath: /plugin
  volumes:
    - name: plugin-cache
      persistentVolumeClaim:
        claimName: ${PVC_NAME}
EOF

  kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/${POD_NAME}" --timeout=180s >/dev/null

  echo "Resetting cached plugin directory"
  kubectl -n "$NAMESPACE" exec "$POD_NAME" -- /bin/sh -c 'rm -rf /plugin/iceberg && mkdir -p /plugin/iceberg'

  echo "Copying plugin jars from ${dir}"
  kubectl -n "$NAMESPACE" cp "${dir}/." "${POD_NAME}:/plugin/iceberg"

  echo "Verifying cached plugin jars in pvc/${PVC_NAME}"
  kubectl -n "$NAMESPACE" exec "$POD_NAME" -- /bin/sh -c 'find /plugin/iceberg -maxdepth 1 -name "*.jar" | sort'
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  require_command kubectl

  if [[ -z "$ICEBERG_VERSION" ]]; then
    echo "Unable to determine ICEBERG_VERSION from config/debezium/Dockerfile" >&2
    exit 1
  fi

  if [[ -n "$SOURCE_DIR" ]]; then
    LOCAL_PLUGIN_DIR="$SOURCE_DIR"
  fi

  if validate_plugin_dir "$LOCAL_PLUGIN_DIR"; then
    echo "Validated local Iceberg Kafka Connect plugin jars at ${LOCAL_PLUGIN_DIR}"
  else
    if [[ -n "$SOURCE_DIR" ]]; then
      echo "Plugin directory is missing required jars: ${LOCAL_PLUGIN_DIR}" >&2
      exit 1
    fi

    echo "Local Iceberg Kafka Connect plugin cache is missing or incomplete at ${LOCAL_PLUGIN_DIR}"
    build_plugin_dir "$LOCAL_PLUGIN_DIR"

    if ! validate_plugin_dir "$LOCAL_PLUGIN_DIR"; then
      echo "Built plugin directory is still missing required jars: ${LOCAL_PLUGIN_DIR}" >&2
      exit 1
    fi
  fi

  ensure_pvc
  upload_plugin_dir "$LOCAL_PLUGIN_DIR"
  echo "Plugin cache populated in pvc/${PVC_NAME}"
}

main "$@"
