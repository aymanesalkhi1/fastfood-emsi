#!/bin/sh
set -eu
until mc alias set local http://minio:9000 "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin}"; do
  echo "attente de MinIO"
  sleep 2
done
mc mb -p local/lake || true
echo "bucket lake pret"
mc ls local
