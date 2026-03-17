#!/bin/sh
set -eu

export HOME="${HOME:-/app/superset_home}"
export PYTHONUSERBASE="${PYTHONUSERBASE:-/app/superset_home/.local}"

if ! python -c "from sqlalchemy.dialects import registry; registry.load('trino')" >/dev/null 2>&1; then
  python -m pip install --no-cache-dir --user "trino[sqlalchemy]"
fi
