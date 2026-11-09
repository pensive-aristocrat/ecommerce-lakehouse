#!/usr/bin/env bash
# Start the source databases (PostgreSQL, MongoDB, Oracle) and show status.
set -euo pipefail

cd "$(dirname "$0")/../docker"
docker compose -f docker-compose.sources.yml up -d
docker compose -f docker-compose.sources.yml ps