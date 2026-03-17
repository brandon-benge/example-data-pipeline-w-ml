#!/bin/sh
set -eu

if find /app/bi/dashboards -maxdepth 1 -type f -name '*.zip' | grep -q .; then
  superset import-dashboards --path /app/bi/dashboards --username "$SUPERSET_ADMIN_USERNAME"
else
  echo "No Superset export archive found; using repository BI asset sync only."
fi
