#!/bin/sh
set -eu
case "$CLICKHOUSE_BROKER_PASSWORD" in
    *[!a-f0-9]*|'') echo 'Expected a hex ClickHouse broker password' >&2; exit 1 ;;
esac
test "${#CLICKHOUSE_BROKER_PASSWORD}" -ge 32
broker_hash=$(printf %s "$CLICKHOUSE_BROKER_PASSWORD" | sha256sum | cut -d " " -f 1)
# The official image runs these scripts against its initialization server.
clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery <<SQL
CREATE USER IF NOT EXISTS broker IDENTIFIED WITH sha256_hash BY '$broker_hash';
GRANT SELECT, INSERT ON broker.history TO broker;
SQL
