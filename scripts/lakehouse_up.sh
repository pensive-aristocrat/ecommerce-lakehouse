#!/usr/bin/env bash
# Start the lakehouse storage layer (RustFS + Polaris) from a clean state.
# The quickstart keeps Polaris's catalog in memory and its setup jobs only
# succeed on a fresh start, so we reset first. This ERASES demo tables.
set -euo pipefail

cd "$(dirname "$0")/../docker"
docker compose -f docker-compose.lakehouse.yml down -v
docker compose -f docker-compose.lakehouse.yml up -d
docker compose -f docker-compose.lakehouse.yml ps -a