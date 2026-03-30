#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: [REPO_BUNDLE_IMAGE=<registry/repo>] [REPO_BUNDLE_TAG=<tag>] [REPO_BUNDLE_PLATFORMS=<platforms>] bash tools/push_repo_bundle.sh

Builds a tiny OCI image that contains dist/repo-bundle.tar.gz and pushes it to
the configured image repository. The default image repository is the configured
DigitalOcean registry repo bundle path, and the default tag is "latest", so each run
repoints the same tag to the newest bundle.

Examples:
  bash tools/push_repo_bundle.sh
  REPO_BUNDLE_IMAGE=registry.example.com/team/repo-bundle bash tools/push_repo_bundle.sh
  REPO_BUNDLE_PLATFORMS=linux/amd64 bash tools/push_repo_bundle.sh
  REPO_BUNDLE_IMAGE=registry.example.com/team/repo-bundle REPO_BUNDLE_TAG=dev bash tools/push_repo_bundle.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but was not found in PATH." >&2
  exit 1
fi

if ! docker buildx version >/dev/null 2>&1; then
  echo "docker buildx is required but was not found." >&2
  exit 1
fi

DEFAULT_REPO_BUNDLE_IMAGE="registry.digitalocean.com/registry-k8s-1-35-1-do-1-tor1-1774447535918/repo-bundle"
REPO_BUNDLE_IMAGE="${REPO_BUNDLE_IMAGE:-$DEFAULT_REPO_BUNDLE_IMAGE}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bundle_path="${repo_root}/dist/repo-bundle.tar.gz"
last_pushed_cksum_path="${repo_root}/dist/repo-bundle.last-pushed.cksum"
bundle_tag="${REPO_BUNDLE_TAG:-latest}"
bundle_platforms="${REPO_BUNDLE_PLATFORMS:-linux/amd64,linux/arm64}"
image_ref="${REPO_BUNDLE_IMAGE}:${bundle_tag}"

if [[ ! -f "$bundle_path" ]]; then
  echo "Bundle not found at $bundle_path. Run tools/package_repo.sh first." >&2
  exit 1
fi

bundle_sha="$(shasum -a 256 "$bundle_path" | awk '{print $1}')"
bundle_cksum="$(cksum "$bundle_path" | awk '{print $1 ":" $2}')"
bundle_size="$(stat -f '%z' "$bundle_path")"
created_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

if [[ -f "$last_pushed_cksum_path" ]]; then
  last_pushed_cksum="$(cat "$last_pushed_cksum_path")"
  if [[ "$last_pushed_cksum" == "$bundle_cksum" ]]; then
    echo "Bundle unchanged since last push; skipping push for ${image_ref}"
    echo "Bundle SHA-256: ${bundle_sha}"
    exit 0
  fi
fi

cat >"${tmp_dir}/Dockerfile" <<EOF
FROM alpine:3.20
COPY repo-bundle.tar.gz /repo-bundle.tar.gz
LABEL org.opencontainers.image.title="repo-bundle"
LABEL org.opencontainers.image.description="Packaged repository bundle for deployment bootstrap"
LABEL org.opencontainers.image.created="${created_at}"
LABEL org.opencontainers.image.source="example-data-pipeline-w-ml"
LABEL io.example.repo-bundle.sha256="${bundle_sha}"
LABEL io.example.repo-bundle.size="${bundle_size}"
EOF

cp "$bundle_path" "${tmp_dir}/repo-bundle.tar.gz"

echo "Building and pushing ${image_ref} for platforms ${bundle_platforms}"
docker buildx build \
  --platform "$bundle_platforms" \
  --tag "$image_ref" \
  --push \
  "$tmp_dir"

echo "Validating remote manifest for ${image_ref}"
docker manifest inspect "$image_ref" >/dev/null

printf '%s\n' "$bundle_cksum" > "$last_pushed_cksum_path"

echo "Pushed ${image_ref}"
echo "Bundle SHA-256: ${bundle_sha}"
