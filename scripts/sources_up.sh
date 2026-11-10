#!/usr/bin/env bash
# Start the source databases (PostgreSQL, MongoDB, Oracle) and show status.
# "-p sources" keeps them in their own project, separate from the lakehouse.
set -euo pipefail

cd "$(dirname "$0")/../docker"
docker compose -p sources -f docker-compose.sources.yml up -d
docker compose -p sources -f docker-compose.sources.yml ps