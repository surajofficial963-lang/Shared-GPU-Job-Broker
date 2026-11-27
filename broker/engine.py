import hashlib
import json
import random
import uuid
from collections import defaultdict

from .store import retention_cutoff

PRIORITIES = {"high": 0, "normal": 1, "low": 2}
TYPES = {1: 100, 4: 450, 8: 850}
TERMINAL = {"succeeded", "failed", "cancelled"}


class KeyConflict(Exception):
    pass


class CapacityUnavailable(Exception):
    pass


class FakeProvider:
    def __init__(self, seed):
        self.seed = seed

    def launch_node(self, gpus, now, attempt):
        # Bucket outages so repeated attempts can fail during the same interval.
        if random.Random(f"{self.seed}:outage:{now // 300}").random() < 0.08:
            raise CapacityUnavailable
        random_launch = random.Random(f"{self.seed}:launch:{attempt}:{gpus}")
        failure = random_launch.random() < 0.03
        startup = 900 if failure else random_launch.randint(240, 480)
        return now + startup, failure

    @staticmethod
    def terminate_node(db, node_id, now):
        db.execute(
            "UPDATE nodes SET state = 'terminated', ended = ?, reserved_job = NULL WHERE id = ?",
            (now, node_id),
        )


class Broker:
    def __init__(self, store, idle_seconds=300, seed=23, archive=None):
        self.store = store
        self.idle_seconds = idle_seconds
        self.provider = FakeProvider(seed)
        self.archive = archive

    @staticmethod
    def now(db):
        return db.execute("SELECT now FROM clock WHERE id = 1").fetchone()[0]

    def submit(self, team, gpus, estimate, priority, key, duration=None, outcome="succeeded"):
        digest = hashlib.sha256(json.dumps([team, key]).encode()).hexdigest()
        fingerprint = json.dumps([gpus, estimate, priority])
        with self.store.session(write=True) as db:
            previous = db.execute("SELECT * FROM submissions WHERE digest = ?", (digest,)).fetchone()
            if previous:
                if previous["fingerprint"] != fingerprint:
                    raise KeyConflict
                return previous["job_id"], False
            job_id = uuid.uuid4().hex
            now = self.now(db)
            db.execute(
                """INSERT INTO jobs(id, team, gpus, estimate, priority, status, submitted, duration, outcome)
                   VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?)""",
                (job_id, team, gpus, estimate, PRIORITIES[priority], now, duration or estimate, outcome),
            )
            db.execute("INSERT INTO submissions VALUES (?, ?, ?)", (digest, job_id, fingerprint))
            self._settle(db, now)
            return job_id, True

    def finish(self, job_id, status):
        with self.store.session(write=True) as db:
            job = db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if job is None:
                archived = self.archive.get("jobs", job_id) if self.archive else None
                if archived:
                    return archived["status"] == status
                return None
            if job["status"] in TERMINAL:
                return job["status"] == status
            if job["status"] == "queued" and status != "cancelled":
                return False
            now = self.now(db)
            self._end_job(db, job, status, now)
            self._settle(db, now)
            return True

    @staticmethod
    def _end_job(db, job, status, now):
        db.execute("UPDATE jobs SET status = ?, ended = ? WHERE id = ?", (status, now, job["id"]))
        db.execute("UPDATE nodes SET reserved_job = NULL WHERE reserved_job = ?", (job["id"],))
        if job["node_id"]:
            db.execute(
                """UPDATE nodes SET idle_since = ? WHERE id = ?
                   AND NOT EXISTS (SELECT 1 FROM jobs WHERE status = 'running' AND node_id = ?)""",
                (now, job["node_id"], job["node_id"]),
            )

    def _settle(self, db, now):
        for job in db.execute(
            "SELECT * FROM jobs WHERE status = 'running' AND started + duration <= ?", (now,)
        ).fetchall():
            self._end_job(db, job, job["outcome"], now)
        for node in db.execute(
            "SELECT * FROM nodes WHERE ended IS NULL AND state = 'booting' AND ready_at <= ?", (now,)
        ).fetchall():
            if node["boot_failure"]:
                self.provider.terminate_node(db, node["id"], now)
            else:
                db.execute("UPDATE nodes SET state = 'ready', idle_since = ? WHERE id = ?", (now, node["id"]))

        self._place(db, now)
        for node in db.execute(
            """SELECT id FROM nodes WHERE ended IS NULL AND state = 'ready'
               AND idle_since <= ?""", (now - self.idle_seconds,)
        ).fetchall():
            self.provider.terminate_node(db, node["id"], now)
        self._launch(db, now)

    @staticmethod
    def _place(db, now):
        # Aging promotes a job one priority level every 30 minutes.
        for job in db.execute(
            """SELECT * FROM jobs WHERE status = 'queued'
               ORDER BY max(0, priority - (? - submitted) / 1800), submitted, rowid""", (now,)
        ).fetchall():
            # Keep an accepted launch's promised capacity available to its job.
            node = db.execute(
                """SELECT n.id, n.gpus - coalesce(sum(j.gpus), 0) AS free
                   FROM nodes n LEFT JOIN jobs j ON j.node_id = n.id AND j.status = 'running'
                   WHERE n.ended IS NULL AND n.state = 'ready' AND n.team = ?
                   AND (n.reserved_job IS NULL OR n.reserved_job = ?)
                   GROUP BY n.id HAVING free >= ?
                   ORDER BY n.reserved_job IS NULL, free, n.launched, n.rowid LIMIT 1""",
                (job["team"], job["id"], job["gpus"]),
            ).fetchone()
            if node:
                db.execute(
                    "UPDATE jobs SET status = 'running', started = ?, node_id = ? WHERE id = ?",
                    (now, node["id"], job["id"]),
                )
                db.execute("UPDATE nodes SET reserved_job = NULL WHERE reserved_job = ?", (job["id"],))
                db.execute("UPDATE nodes SET idle_since = NULL WHERE id = ?", (node["id"],))

    def _launch(self, db, now):
        clock = db.execute("SELECT attempts, retry_at FROM clock WHERE id = 1").fetchone()
        if clock["retry_at"] is not None and clock["retry_at"] > now:
            return
        db.execute("UPDATE clock SET retry_at = NULL WHERE id = 1")
        slots = 10 - db.execute("SELECT count(*) FROM nodes WHERE ended IS NULL").fetchone()[0]
        if slots <= 0:
            return
        attempts = clock["attempts"]
        for _ in range(slots):
            # Within a priority class, give the next node to the team holding fewer live nodes.
            job = db.execute(
                """WITH team_load AS (
                     SELECT team, count(*) AS live FROM nodes WHERE ended IS NULL GROUP BY team
                   )
                   SELECT j.* FROM jobs j LEFT JOIN team_load t ON t.team = j.team
                   WHERE j.status = 'queued' AND NOT EXISTS
                   (SELECT 1 FROM nodes n WHERE n.ended IS NULL AND n.reserved_job = j.id)
                   ORDER BY max(0, j.priority - (? - j.submitted) / 1800),
                            coalesce(t.live, 0), j.submitted, j.rowid LIMIT 1""", (now,)
            ).fetchone()
            if job is None:
                break
            gpus = next(size for size in TYPES if size >= job["gpus"])
            attempts += 1
            db.execute("UPDATE clock SET attempts = ? WHERE id = 1", (attempts,))
            try:
                ready_at, failure = self.provider.launch_node(gpus, now, attempts)
            except CapacityUnavailable:
                db.execute("UPDATE clock SET retry_at = ? WHERE id = 1", (now + 60,))
                break
            db.execute(
                """INSERT INTO nodes(id, team, gpus, hourly_cents, launched, ready_at,
                   boot_failure, state, reserved_job) VALUES (?, ?, ?, ?, ?, ?, ?, 'booting', ?)""",
                (uuid.uuid4().hex, job["team"], gpus, TYPES[gpus], now, ready_at, failure, job["id"]),
            )

    def _next_event(self, db):
        return db.execute(
            """SELECT min(event) FROM (
                 SELECT min(started + duration) AS event FROM jobs WHERE status = 'running'
                 UNION ALL SELECT min(ready_at) FROM nodes WHERE ended IS NULL AND state = 'booting'
                 UNION ALL SELECT min(idle_since) + ? FROM nodes WHERE ended IS NULL AND state = 'ready'
                 UNION ALL SELECT retry_at FROM clock
               )""", (self.idle_seconds,)
        ).fetchone()[0]

    def advance(self, target=None, *, seconds=None, maximum=None):
        with self.store.session(write=True) as db:
            now = self.now(db)
            if seconds is not None:
                target = now + seconds
                if maximum is not None and target > maximum:
                    raise ValueError("Simulation limit reached")
            if target is not None and target < now:
                raise ValueError("Time cannot move backwards")
            while True:
                event = self._next_event(db)
                if event is None or (target is not None and event > target):
                    break
                now = max(now, event)
                db.execute("UPDATE clock SET now = ? WHERE id = 1", (now,))
                self._settle(db, now)
            if target is not None:
                now = target
                db.execute("UPDATE clock SET now = ? WHERE id = 1", (now,))
            return now

    def advance_by(self, seconds, *, maximum=None):
        # Resolve relative time inside the same transaction as the update.
        return self.advance(seconds=seconds, maximum=maximum)

    def get(self, job_id):
        with self.store.session() as db:
            job = db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return dict(job) if job else self.archive.get("jobs", job_id) if self.archive else None

    def listing(self, kind, limit=100, offset=0, team=None):
        queries = {
            "jobs": "SELECT * FROM jobs WHERE status = 'queued' AND (? IS NULL OR team = ?) ORDER BY priority, submitted, rowid",
            "nodes": "SELECT * FROM nodes WHERE ended IS NULL AND (? IS NULL OR team = ?) ORDER BY launched, rowid",
        }
        with self.store.session() as db:
            return [dict(row) for row in db.execute(queries[kind] + " LIMIT ? OFFSET ?", (team, team, limit, offset))]

    def report(self, begin, end, team=None):
        totals = defaultdict(lambda: {"gpu_hours": 0.0, "cost_usd": 0.0})
        with self.store.session() as db:
            now = self.now(db)
            end = min(end, now)
            if end <= begin:
                return {"teams": {}, "total_cost_usd": 0}
            for table, start_column in (("jobs", "started"), ("nodes", "launched")):
                rows = [dict(row) for row in db.execute(
                    f"""SELECT * FROM {table} WHERE {start_column} < ? AND (ended IS NULL OR ended > ?)
                        AND (? IS NULL OR team = ?)""",
                    (end, begin, team, team),
                )]
                found = {row["id"]: row for row in rows}
                if self.archive and begin < retention_cutoff(now):
                    for row in self.archive.window(table, begin, end, team):
                        found.setdefault(row["id"], row)
                for row in found.values():
                    started = row[start_column]
                    ended = row["ended"] if row["ended"] is not None else now
                    if table == "jobs":
                        overlap = max(0, min(end, ended) - max(begin, started))
                        totals[row["team"]]["gpu_hours"] += overlap * row["gpus"] / 3600
                    else:
                        # Charge the minimum billing shortfall to the window containing termination.
                        seconds = max(0, min(end, ended) - max(begin, started))
                        if row["ended"] is not None and begin < ended <= end:
                            seconds += max(0, 600 - (ended - started))
                        totals[row["team"]]["cost_usd"] += seconds * row["hourly_cents"] / 360000
        return {"teams": dict(sorted(totals.items())), "total_cost_usd": sum(v["cost_usd"] for v in totals.values())}
