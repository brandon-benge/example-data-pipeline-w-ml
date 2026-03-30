#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: tools/package_repo.sh [output.tar.gz]

Creates a tar.gz archive of the current repository while excluding local-only
artifacts such as .git metadata, virtualenvs, caches, logs, and temp files.

Examples:
  tools/package_repo.sh
  tools/package_repo.sh /tmp/example-data-pipeline-w-ml.tar.gz
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if ! command -v tar >/dev/null 2>&1; then
  echo "tar is required but was not found in PATH." >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
default_output="$repo_root/dist/repo-bundle.tar.gz"
output_path="${1:-$default_output}"
tmp_output=""

mkdir -p "$(dirname "$output_path")"

output_abs="$(cd "$(dirname "$output_path")" && pwd)/$(basename "$output_path")"
tmp_output="${output_abs}.tmp"

exclude_patterns=(
  ".git/*"
  ".cache/*"
  ".venv/*"
  ".pytest_cache/*"
  ".mypy_cache/*"
  ".ruff_cache/*"
  "spark/jars/*"
  "flink/jars/*"
  ".DS_Store"
  "._*"
  "__pycache__/*"
  "*.pyc"
  "*.pyo"
  "*.log"
  ".tmp-*"
  "tmp/*"
  "dist/*"
)

cd "$repo_root"
rm -f "$tmp_output"

tar_excludes=()
for pattern in "${exclude_patterns[@]}"; do
  tar_excludes+=(--exclude="$pattern")
done

COPYFILE_DISABLE=1 tar -czf "$tmp_output" "${tar_excludes[@]}" .

new_cksum="$(cksum "$tmp_output" | awk '{print $1 ":" $2}')"
if [[ -f "$output_abs" ]]; then
  existing_cksum="$(cksum "$output_abs" | awk '{print $1 ":" $2}')"
  if [[ "$existing_cksum" == "$new_cksum" ]]; then
    rm -f "$tmp_output"
    archive_size="$(du -h "$output_abs" | awk '{print $1}')"
    echo "Bundle unchanged: $output_abs ($archive_size)"
    exit 0
  fi
fi

mv "$tmp_output" "$output_abs"
archive_size="$(du -h "$output_abs" | awk '{print $1}')"
echo "Created $output_abs ($archive_size)"
