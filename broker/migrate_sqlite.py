"""Copy a quiesced SQLite broker into an empty PostgreSQL database."""

import argparse
import os
import sqlite3
from contextlib import closing
from pathlib import Path

from .postgres import PostgresStore
from .store import backfill_metrics


TABLES = ("jobs", "nodes", "submissions", "queue_wait_buckets", "runtime_buckets")


def migrate(path, dsn):
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)) as source:
        source.row_factory = sqlite3.Row
        source.execute("BEGIN")
        clock = source.execute("SELECT now, attempts, retry_at FROM clock WHERE id = 1").fetchone()
        if clock is None:
            raise ValueError("SQLite source has no simulation clock")
        migration = PostgresStore(dsn, clock["now"])
        counts = {}
        with migration.session(write=True) as target:
            for table in TABLES:
                if target.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone():
                    raise ValueError("PostgreSQL target already has broker data")
            target.execute("UPDATE clock SET now = ?, attempts = ?, retry_at = ? WHERE id = 1", tuple(clock))
            for table in TABLES:
                columns = {row["name"] for row in source.execute(f"PRAGMA table_info({table})")}
                if not columns:
                    continue
                # Persist insertion order for deterministic scheduling of jobs submitted at the same time.
                rowid = "rowid, " if table in ("jobs", "nodes") else ""
                rows = source.execute(f"SELECT {rowid}* FROM {table}")
                names = ", ".join(field[0] for field in rows.description)
                placeholders = ", ".join("?" for _ in rows.description)
                count = 0
                while True:
                    batch = rows.fetchmany(500)
                    if not batch:
                        break
                    target.executemany(f"INSERT INTO {table} ({names}) VALUES ({placeholders})",
                                       [tuple(row) for row in batch])
                    count += len(batch)
                counts[table] = count
                if rowid:
                    target.execute(
                        f"""SELECT setval(pg_get_serial_sequence(?, 'rowid'),
                           coalesce((SELECT max(rowid) FROM {table}), 1),
                           EXISTS (SELECT 1 FROM {table}))""",
                        (table,),
                    )
            backfill_metrics(target)
        return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sqlite_path", help="Path to the existing SQLite broker database")
    args = parser.parse_args()
    dsn = os.environ.get("BROKER_DATABASE_URL")
    if not dsn:
        parser.error("BROKER_DATABASE_URL must point to PostgreSQL")
    counts = migrate(args.sqlite_path, dsn)
    print("Migrated " + ", ".join(f"{count} {table}" for table, count in counts.items()))


if __name__ == "__main__":
    main()
