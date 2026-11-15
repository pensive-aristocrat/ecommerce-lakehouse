#!/usr/bin/env bash
# Start Marquez (lineage server + UI). Its data survives restarts.
set -euo pipefail

cd "$(dirname "$0")/../docker"
docker compose -p lineage -f docker-compose.lineage.yml up -d
docker compose -p lineage -f docker-compose.lineage.yml ps