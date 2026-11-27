import sqlite3
import tempfile
import unittest
from pathlib import Path

from broker.archive import ArchiveUnavailable, archive_once
from broker.engine import Broker, CapacityUnavailable, KeyConflict
from broker.store import Store, retention_cutoff


class MemoryArchive:
    def __init__(self):
        self.rows = {}
        self.available = True

    def save(self, kind, rows):
        if not self.available:
            raise ArchiveUnavailable
        for row in rows:
            self.rows[(kind, row["id"])] = dict(row)

    def get(self, kind, record_id):
        return self.rows.get((kind, record_id))

    def window(self, kind, begin, end, team=None):
        start = "started" if kind == "jobs" else "launched"
        return [row for (table, _), row in self.rows.items()
                if table == kind and (team is None or team == row["team"]) and row[start] is not None and row[start] < end and row["ended"] > begin]


class BrokerTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / "test.db")
        self.start = 1740960000
        self.store = Store(self.path, self.start)
        self.broker = Broker(self.store)
        self.broker.provider.launch_node = lambda gpus, now, attempt: (now + 240, False)

    def test_submit_idempotency_and_capacity(self):
        job, created = self.broker.submit("vision", 2, 60, "normal", "j")
        self.assertTrue(created)
        self.assertEqual((job, False), self.broker.submit("vision", 2, 60, "normal", "j"))
        with self.assertRaises(KeyConflict):
            self.broker.submit("vision", 4, 60, "normal", "j")
        for i in range(12):
            self.broker.submit("speech", 8, 60, "normal", str(i))
        self.assertEqual(len(self.broker.listing("nodes")), 10)
        self.assertEqual(self.broker.listing("nodes")[0]["gpus"], 4)

    def test_same_team_sharing_and_cancel(self):
        a, _ = self.broker.submit("vision", 2, 3600, "normal", "a")
        self.broker.advance_by(240)
        b, _ = self.broker.submit("vision", 2, 3600, "normal", "b")
        c, _ = self.broker.submit("speech", 2, 3600, "normal", "c")
        self.assertEqual(self.broker.get(a)["node_id"], self.broker.get(b)["node_id"])
        self.assertEqual(self.broker.get(c)["status"], "queued")
        self.assertTrue(self.broker.finish(a, "cancelled"))
        self.assertTrue(self.broker.finish(c, "cancelled"))
        self.assertFalse(self.broker.finish(a, "succeeded"))
        self.assertEqual(self.broker.get(b)["status"], "running")

    def test_failure_retry_and_restart(self):
        def launch(gpus, now, attempt):
            if attempt == 1:
                raise CapacityUnavailable
            return (now + 900, True) if attempt == 2 else (now + 240, False)

        self.broker.provider.launch_node = launch
        job, _ = self.broker.submit("vision", 1, 60, "normal", "a")
        self.broker.advance_by(60)
        resumed = Broker(Store(self.path, 0))
        resumed.provider.launch_node = launch
        self.assertEqual(resumed.advance_by(900), self.start + 960)
        self.assertEqual(resumed.get(job)["status"], "queued")
        self.assertEqual(len(resumed.listing("nodes")), 1)
        resumed.advance_by(300)
        self.assertEqual(resumed.get(job)["status"], "succeeded")

    def test_provisioning_prefers_team_with_fewer_nodes(self):
        def unavailable(gpus, now, attempt):
            raise CapacityUnavailable

        self.broker.provider.launch_node = unavailable
        for i in range(10):
            self.broker.submit("vision", 1, 3600, "normal", str(i))
        job, _ = self.broker.submit("speech", 1, 3600, "normal", "a")
        self.broker.provider.launch_node = lambda gpus, now, attempt: (now + 240, False)
        self.broker.advance_by(60)
        self.assertEqual(self.broker.listing("nodes")[1]["reserved_job"], job)

    def test_billing_minimum_and_idle_reaping(self):
        engine = self.broker
        engine.idle_seconds = 1
        job, _ = engine.submit("vision", 2, 60, "normal", "a")
        end = engine.advance()
        self.assertEqual(end, self.start + 301)
        self.assertEqual(engine.get(job)["status"], "succeeded")
        self.assertEqual(engine.listing("nodes"), [])
        report = engine.report(self.start, end)
        self.assertAlmostEqual(report["teams"]["vision"]["gpu_hours"], 2 / 60)
        self.assertAlmostEqual(report["total_cost_usd"], 0.75)
        self.assertAlmostEqual(
            engine.report(self.start, end - 1)["total_cost_usd"]
            + engine.report(end - 1, end)["total_cost_usd"], 0.75
        )

    def test_archive_failure_preserves_data_and_success_preserves_reports(self):
        archive = MemoryArchive()
        self.broker.archive = archive
        job, _ = self.broker.submit("vision", 2, 60, "normal", "a")
        end = self.broker.advance()
        original = self.broker.report(self.start, end)
        self.broker.advance_by(100 * 86400)
        archive.available = False
        with self.assertRaises(ArchiveUnavailable):
            archive_once(self.store, archive)
        self.assertIsNotNone(self.broker.get(job))
        archive.available = True
        self.assertEqual(archive_once(self.store, archive), 2)
        self.assertEqual(archive_once(self.store, archive), 0)
        with self.store.session() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM jobs").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT count(*) FROM nodes").fetchone()[0], 0)
        self.assertEqual(self.broker.report(self.start, end), original)
        self.assertEqual(self.broker.get(job)["status"], "succeeded")
        self.assertEqual(self.broker.submit("vision", 2, 60, "normal", "a"), (job, False))

    def test_transactions_roll_back(self):
        with self.assertRaises(sqlite3.IntegrityError):
            with self.store.session(write=True) as db:
                db.execute("UPDATE clock SET now = 10")
                db.execute("INSERT INTO clock(id, now) VALUES (2, 0)")
        self.assertEqual(self.broker.advance_by(1), self.start + 1)

    def test_calendar_retention_clamps_end_of_month(self):
        self.assertEqual(retention_cutoff(1748649600), 1740700800)  # May 31 -> Feb 28.

    def test_queue_deadline_does_not_affect_running_job(self):
        self.broker.queue_timeout_seconds = 300
        job, _ = self.broker.submit("vision", 2, 1200, "normal", "a")
        self.broker.advance_by(300)
        self.assertEqual(self.broker.get(job)["status"], "running")
        self.broker.advance_by(1140)
        self.assertEqual(self.broker.get(job)["status"], "succeeded")

    def test_shorter_timeout_applies_to_persisted_queue_without_rewinding(self):
        job, _ = self.broker.submit("vision", 2, 60, "normal", "a")
        now = self.broker.advance_by(100)
        restarted = Broker(Store(self.path, 0), queue_timeout_seconds=60)
        self.assertEqual(restarted.advance_by(0), now)
        self.assertEqual(restarted.get(job)["ended"], now)
        self.assertEqual(restarted.get(job)["failure_reason"], "queue_timeout")
