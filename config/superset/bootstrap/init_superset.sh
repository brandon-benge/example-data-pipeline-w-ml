#!/bin/sh
set -eu

/bin/sh /app/bootstrap/bootstrap_python.sh

superset db upgrade
superset fab create-admin \
  --username "$SUPERSET_ADMIN_USERNAME" \
  --firstname "$SUPERSET_ADMIN_FIRSTNAME" \
  --lastname "$SUPERSET_ADMIN_LASTNAME" \
  --email "$SUPERSET_ADMIN_EMAIL" \
  --password "$SUPERSET_ADMIN_PASSWORD" || true
superset init

python /app/bootstrap/register_trino_connection.py
python /app/bootstrap/sync_bi_assets.py
/bin/sh /app/bootstrap/import_dashboards.sh

echo "Superset initialized."
