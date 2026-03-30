#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="data-platform-ingest"
PVC_NAME="kafka-connect-plugin-cache"
POD_NAME="kafka-connect-plugin-cache-loader"
STORAGE_CLASS_NAME="do-block-storage-retain"
DEFAULT_LOCAL_CACHE_ROOT="${ROOT_DIR}/.cache/iceberg-kafka-connect"
SOURCE_DIR="${1:-}"
ICEBERG_VERSION="${ICEBERG_VERSION:-1.10.1}"
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

find_running_sinks_pod() {
  kubectl -n "$NAMESPACE" get pods \
    -l app.kubernetes.io/name=kafka-connect-sinks \
    -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' 2>/dev/null | head -n 1
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
  local build_root
  local output_dir

  require_command docker

  mkdir -p "$target_dir"
  build_root="$(mktemp -d)"
  output_dir="${build_root}/output"
  mkdir -p "$output_dir"

  trap 'rm -rf "$build_root"' RETURN

  echo "Building Apache Iceberg Kafka Connect runtime ${ICEBERG_VERSION} locally"
  docker run --rm \
    -v "${build_root}:/workspace" \
    gradle:8.10.2-jdk17 \
    bash -lc "
      set -euo pipefail
      apt-get update
      apt-get install -y --no-install-recommends curl ca-certificates tar gzip unzip findutils
      rm -rf /var/lib/apt/lists/*
      mkdir -p /workspace/src /workspace/output
      curl --retry 5 --retry-all-errors --retry-delay 2 -fSL \
        -o /tmp/apache-iceberg-src.tar.gz \
        https://downloads.apache.org/iceberg/apache-iceberg-${ICEBERG_VERSION}/apache-iceberg-${ICEBERG_VERSION}.tar.gz
      tar -xzf /tmp/apache-iceberg-src.tar.gz --strip-components=1 -C /workspace/src
      rm -f /tmp/apache-iceberg-src.tar.gz
      cd /workspace/src
      ./gradlew --no-daemon -x test -x integrationTest clean build
      artifact=\$(find /workspace/src/kafka-connect/kafka-connect-runtime/build/distributions -maxdepth 1 \\( -name '*.zip' -o -name '*.tar' -o -name '*.tgz' \\) | head -1)
      test -n \"\$artifact\"
      mkdir -p /tmp/iceberg-connect
      case \"\$artifact\" in
        *.zip) unzip -q \"\$artifact\" -d /tmp/iceberg-connect ;;
        *.tar|*.tgz) tar -xf \"\$artifact\" -C /tmp/iceberg-connect ;;
      esac
      dist_dir=\$(find /tmp/iceberg-connect -maxdepth 2 -type d -name 'iceberg-kafka-connect-runtime*' | head -1)
      test -n \"\$dist_dir\"
      cp -R \"\$dist_dir\"/lib/* /workspace/output/
    "

  rm -rf "${target_dir:?}/"*
  cp -R "${output_dir}/." "$target_dir"
}

ensure_pvc() {
  echo "Ensuring storageclass/${STORAGE_CLASS_NAME} exists"
  if ! kubectl get storageclass "${STORAGE_CLASS_NAME}" >/dev/null 2>&1; then
    kubectl apply -f - <<EOF >/dev/null
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ${STORAGE_CLASS_NAME}
provisioner: dobs.csi.digitalocean.com
reclaimPolicy: Retain
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
parameters:
  csi.storage.k8s.io/fstype: ext4
EOF
  fi

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
  storageClassName: ${STORAGE_CLASS_NAME}
  resources:
    requests:
      storage: 100Mi
EOF
}

wait_for_pvc_binding() {
  local timeout_seconds="${1:-180}"
  local elapsed=0
  local phase=""

  while (( elapsed < timeout_seconds )); do
    phase="$(kubectl -n "$NAMESPACE" get pvc "$PVC_NAME" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [[ "$phase" == "Bound" ]]; then
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done

  echo "pvc/${PVC_NAME} did not bind within ${timeout_seconds}s (last phase: ${phase:-unknown})" >&2
  kubectl -n "$NAMESPACE" get pvc "$PVC_NAME" -o wide >&2 || true
  kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp | tail -n 20 >&2 || true
  return 1
}

storage_class_binding_mode() {
  kubectl get storageclass "${STORAGE_CLASS_NAME}" -o jsonpath='{.volumeBindingMode}' 2>/dev/null || true
}

diagnose_loader_failure() {
  kubectl -n "$NAMESPACE" get pvc "$PVC_NAME" -o wide >&2 || true
  kubectl -n "$NAMESPACE" get pod "$POD_NAME" -o wide >&2 || true
  kubectl -n "$NAMESPACE" logs "$POD_NAME" >&2 || true
  kubectl get nodes -o wide >&2 || true
  kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp | tail -n 20 >&2 || true
}

upload_plugin_dir() {
  local dir="$1"
  local binding_mode=""
  local target_pod=""
  local target_container=""

  trap cleanup EXIT

  target_pod="$(find_running_sinks_pod)"
  if [[ -n "$target_pod" ]]; then
    target_container="kafka-connect-sinks"
    echo "Using existing pod ${target_pod} because pvc/${PVC_NAME} is already attached there"
  else
    target_container="loader"
  fi

  binding_mode="$(storage_class_binding_mode)"
  if [[ -z "$target_pod" && "$binding_mode" != "WaitForFirstConsumer" ]]; then
    echo "Waiting for pvc/${PVC_NAME} to bind"
    wait_for_pvc_binding 180
  elif [[ -z "$target_pod" ]]; then
    echo "storageclass/${STORAGE_CLASS_NAME} uses WaitForFirstConsumer; creating helper pod before waiting on pvc/${PVC_NAME}"
  fi

  if [[ -z "$target_pod" ]]; then
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

    if ! kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/${POD_NAME}" --timeout=180s >/dev/null; then
      echo "Helper pod ${POD_NAME} did not become Ready" >&2
      diagnose_loader_failure
      return 1
    fi

    target_pod="$POD_NAME"
  fi

  echo "Resetting cached plugin directory"
  kubectl -n "$NAMESPACE" exec "$target_pod" -c "$target_container" -- /bin/sh -c 'rm -rf /plugin/iceberg && mkdir -p /plugin/iceberg'

  echo "Copying plugin jars from ${dir}"
  kubectl -n "$NAMESPACE" cp "${dir}/." "${target_pod}:/plugin/iceberg" -c "$target_container"

  echo "Verifying cached plugin jars in pvc/${PVC_NAME}"
  kubectl -n "$NAMESPACE" exec "$target_pod" -c "$target_container" -- /bin/sh -c 'find /plugin/iceberg -maxdepth 1 -name "*.jar" | sort'
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  require_command kubectl

  if [[ -z "$ICEBERG_VERSION" ]]; then
    echo "ICEBERG_VERSION is empty" >&2
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
