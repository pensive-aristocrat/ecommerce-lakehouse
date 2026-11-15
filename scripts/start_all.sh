#!/usr/bin/env bash
# Bring the whole project up after a restart and rebuild every layer, in order.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python

./scripts/lineage_up.sh          # Marquez first, so every job below reports lineage
./scripts/sources_up.sh

echo "Waiting for Oracle to be healthy..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' src-oracle)" = "healthy" ]; do sleep 5; done

./scripts/lakehouse_up.sh

echo "Waiting for Trino to finish starting..."
until curl -s localhost:8080/v1/info | grep -q '"starting":false'; do sleep 5; done

$PY -m spark_jobs.bronze.ingest_sources
$PY -m spark_jobs.silver.build_silver
(cd dbt && OPENLINEAGE_URL=http://localhost:5000 OPENLINEAGE_NAMESPACE=lakehouse ../.venv/bin/dbt-ol build --profiles-dir .)
$PY scripts/validate_gold.py

docker stop src-oracle
echo "All layers rebuilt and checked."