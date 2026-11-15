#!/usr/bin/env bash
# Build (first time) and start Superset on http://localhost:8088.
set -euo pipefail

cd "$(dirname "$0")/../docker"
docker compose -p bi -f docker-compose.bi.yml up -d --build
docker compose -p bi -f docker-compose.bi.yml ps