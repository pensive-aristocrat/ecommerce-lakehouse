#!/usr/bin/env bash
# Start the lakehouse storage layer (RustFS + Polaris) from a clean state,
# then give the catalog_admin role permission to create and write tables.
# The quickstart keeps Polaris's catalog in memory and its setup jobs only
# succeed on a fresh start, so we reset first. This ERASES lakehouse tables.
# "-p lakehouse" gives this setup its own project name, so the reset never
# touches the source databases.
set -euo pipefail

POLARIS=http://localhost:8181
CATALOG=quickstart_catalog
COMPOSE="docker compose -p lakehouse -f docker-compose.lakehouse.yml"

cd "$(dirname "$0")/../docker"
$COMPOSE down -v
$COMPOSE up -d

echo "Waiting for Polaris to accept logins..."
TOKEN=""
for i in $(seq 1 30); do
  TOKEN=$(curl -s -X POST "$POLARIS/api/catalog/v1/oauth/tokens" \
    -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
    -d scope=PRINCIPAL_ROLE:ALL | jq -r '.access_token // empty' || true)
  [ -n "$TOKEN" ] && break
  sleep 2
done
[ -n "$TOKEN" ] || { echo "Polaris did not start in time"; exit 1; }

echo "Waiting for the setup job to create $CATALOG..."
for i in $(seq 1 30); do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    "$POLARIS/api/management/v1/catalogs/$CATALOG" -H "Authorization: Bearer $TOKEN")
  [ "$CODE" = "200" ] && break
  sleep 2
done

# Allow catalog_admin to create tables and read/write their data.
curl -s -X PUT "$POLARIS/api/management/v1/catalogs/$CATALOG/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'
echo "Granted CATALOG_MANAGE_CONTENT on $CATALOG to catalog_admin."

$COMPOSE ps -a