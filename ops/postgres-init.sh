#!/bin/sh
set -eu

case "$BROKER_POSTGRES_PASSWORD" in
    *[!a-f0-9]*|'') echo 'Expected a hex PostgreSQL broker password' >&2; exit 1 ;;
esac
test "${#BROKER_POSTGRES_PASSWORD}" -ge 32
psql --username "$POSTGRES_USER" --dbname postgres --set ON_ERROR_STOP=1 \
    --set broker_password="$BROKER_POSTGRES_PASSWORD" <<'SQL'
CREATE ROLE broker LOGIN PASSWORD :'broker_password';
CREATE DATABASE broker OWNER broker;
REVOKE CONNECT ON DATABASE broker FROM PUBLIC;
SQL
