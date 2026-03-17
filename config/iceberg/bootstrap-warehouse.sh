#!/bin/sh
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb --ignore-existing local/warehouse
mc mb --ignore-existing local/ml-artifacts

echo "MinIO warehouse and model artifact buckets initialized."
