import calendar
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .queue_metrics import record_timing


SCHEMA = """
CREATE TABLE IF NOT EXISTS clock (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    now INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    retry_at INTEGER
);
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    team TEXT NOT NULL,
    gpus INTEGER NOT NULL,
    estimate INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    status TEXT NOT NULL,
    submitted INTEGER NOT NULL,
    duration INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    started INTEGER,
    ended INTEGER,
    node_id TEXT,
    failure_reason TEXT
);
CREATE TABLE IF NOT EXISTS submissions (
    digest TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    team TEXT NOT NULL,
    gpus INTEGER NOT NULL,
    hourly_cents INTEGER NOT NULL,
    launched INTEGER NOT NULL,
    ready_at INTEGER NOT NULL,
    boot_failure INTEGER NOT NULL,
    state TEXT NOT NULL,
    reserved_job TEXT,
    idle_since INTEGER,
    ended INTEGER
);
CREATE TABLE IF NOT EXISTS queue_wait_buckets (
    team TEXT NOT NULL,
    node_gpus INTEGER NOT NULL,
    bucket INTEGER NOT NULL,
    observations INTEGER NOT NULL,
    seconds INTEGER NOT NULL,
    PRIMARY KEY (team, node_gpus, bucket)
);
CREATE TABLE IF NOT EXISTS runtime_buckets (
    team TEXT NOT NULL,
    node_gpus INTEGER NOT NULL,
    bucket INTEGER NOT NULL,
    observations INTEGER NOT NULL,
    seconds INTEGER NOT NULL,
    PRIMARY KEY (team, node_gpus, bucket)
);
CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(priority, submitted, id)
    WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_queue_deadline ON jobs(submitted) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_completion ON jobs(started + duration) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS nodes_reservation ON nodes(reserved_job) WHERE ended IS NULL;
CREATE INDEX IF NOT EXISTS jobs_running ON jobs(node_id, started + duration)
    WHERE status = 'running';
CREATE INDEX IF NOT EXISTS jobs_ended ON jobs(ended) WHERE ended IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_team_started ON jobs(team, started) WHERE started IS NOT NULL;
CREATE INDEX IF NOT EXISTS nodes_team_launched ON nodes(team, launched);
CREATE INDEX IF NOT EXISTS jobs_started ON jobs(started) WHERE started IS NOT NULL;
CREATE INDEX IF NOT EXISTS nodes_live ON nodes(state, team) WHERE ended IS NULL;
CREATE INDEX IF NOT EXISTS nodes_ended ON nodes(ended) WHERE ended IS NOT NULL;
CREATE INDEX IF NOT EXISTS nodes_launched ON nodes(launched);
"""


def retention_cutoff(now):
    current = datetime.fromtimestamp(now, timezone.utc)
    year, month = divmod(current.year * 12 + current.month - 4, 12)
    month += 1
    day = min(current.day, calendar.monthrange(year, month)[1])
    return int(current.replace(year=year, month=month, day=day).timestamp())


class Store:
    def __init__(self, path, initial_time):
        self.path = path
        with self.session() as db:
            db.commit()
            db.execute("PRAGMA auto_vacuum = INCREMENTAL")
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript(SCHEMA)
            db.execute("INSERT OR IGNORE INTO clock(id, now) VALUES (1, ?)", (initial_time,))
        with self.session(write=True) as db:
            columns = {row["name"] for row in db.execute("PRAGMA table_info(jobs)")}
            if "failure_reason" not in columns:
                db.execute("ALTER TABLE jobs ADD COLUMN failure_reason TEXT")
            if db.execute("SELECT 1 FROM queue_wait_buckets LIMIT 1").fetchone() is None:
                for job in db.execute(
                    """SELECT j.team, n.gpus, j.started - j.submitted AS wait_seconds
                       FROM jobs j JOIN nodes n ON n.id = j.node_id WHERE j.started IS NOT NULL"""
                ):
                    record_timing(db, "queue_wait_buckets", job["team"], job["gpus"], job["wait_seconds"])
            if db.execute("SELECT 1 FROM runtime_buckets LIMIT 1").fetchone() is None:
                for job in db.execute(
                    """SELECT j.team, n.gpus, j.ended - j.started AS runtime_seconds
                       FROM jobs j JOIN nodes n ON n.id = j.node_id WHERE j.status = 'succeeded'"""
                ):
                    record_timing(db, "runtime_buckets", job["team"], job["gpus"],
                                  job["runtime_seconds"])

    @contextmanager
    def session(self, write=False):
        db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA synchronous = FULL")
            if write:
                db.execute("BEGIN IMMEDIATE")
            else:
                db.execute("BEGIN")
            yield db
            if db.in_transaction:
                db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()
