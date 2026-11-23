#!/bin/sh
# Runs once, on first start of the postgres container (empty data volume).
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v owner_password="$RISKSTREAM_OWNER_PASSWORD" \
  -v app_password="$RISKSTREAM_APP_PASSWORD" \
  -v monitor_password="$RISKSTREAM_MONITOR_PASSWORD" \
  -f /riskstream/roles.sql
