"""Persist and export queue-wait observations when a job starts."""

from itertools import groupby

BUCKETS = (0, 30, 60, 120, 240, 300, 480, 600, 900, 1800, 3600, 7200, 14400, 86400)
NODE_TYPES = {1: "gpu-s", 4: "gpu-m", 8: "gpu-l"}


def record_wait(db, team, node_gpus, seconds):
    bucket = next((index for index, limit in enumerate(BUCKETS) if seconds <= limit), len(BUCKETS))
    db.execute(
        """INSERT INTO queue_wait_buckets(team, node_gpus, bucket, observations, seconds)
           VALUES (?, ?, ?, 1, ?)
           ON CONFLICT(team, node_gpus, bucket) DO UPDATE SET
             observations = observations + 1, seconds = seconds + excluded.seconds""",
        (team, node_gpus, bucket, seconds),
    )


class QueueWaitCollector:
    def __init__(self, store):
        self.store = store

    def collect(self):
        from prometheus_client.core import HistogramMetricFamily

        family = HistogramMetricFamily(
            "broker_job_queue_wait_seconds", "Simulated seconds between job submission and start",
            labels=["team", "gpu_type"],
        )
        with self.store.session() as db:
            rows = db.execute(
                "SELECT * FROM queue_wait_buckets ORDER BY team, node_gpus, bucket"
            ).fetchall()
        for (team, node_gpus), group in groupby(rows, key=lambda row: (row["team"], row["node_gpus"])):
            buckets = {}
            seconds = 0
            for row in group:
                buckets[row["bucket"]] = row["observations"]
                seconds += row["seconds"]
            cumulative = 0
            samples = []
            for index, bound in enumerate((*BUCKETS, "+Inf")):
                cumulative += buckets.get(index, 0)
                samples.append((str(bound), cumulative))
            family.add_metric([team, NODE_TYPES[node_gpus]], samples, seconds)
        yield family
