"""Persist and export job timing observations."""

from itertools import groupby

BUCKETS = (0, 30, 60, 120, 240, 300, 480, 600, 900, 1800, 3600, 7200, 14400, 86400)
TIMINGS = {
    "queue_wait_buckets": ("broker_job_queue_wait_seconds", "Simulated seconds between job submission and start"),
    "runtime_buckets": (
        "broker_job_runtime_seconds", "Simulated seconds between job start and successful completion",
    ),
}
NODE_TYPES = {1: "gpu-s", 4: "gpu-m", 8: "gpu-l"}


def record_timing(db, table, team, node_gpus, seconds):
    if table not in TIMINGS:
        raise ValueError("Unknown timing table")
    bucket = next((index for index, limit in enumerate(BUCKETS) if seconds <= limit), len(BUCKETS))
    db.execute(
        f"""INSERT INTO {table}(team, node_gpus, bucket, observations, seconds)
           VALUES (?, ?, ?, 1, ?)
           ON CONFLICT(team, node_gpus, bucket) DO UPDATE SET
             observations = observations + 1, seconds = seconds + excluded.seconds""",
        (team, node_gpus, bucket, seconds),
    )


class JobTimingCollector:
    def __init__(self, store):
        self.store = store

    def collect(self):
        from prometheus_client.core import HistogramMetricFamily

        with self.store.session() as db:
            for table, (metric, description) in TIMINGS.items():
                family = HistogramMetricFamily(metric, description, labels=["team", "gpu_type"])
                rows = db.execute(f"SELECT * FROM {table} ORDER BY team, node_gpus, bucket").fetchall()
                self._add_samples(family, rows)
                yield family

    @staticmethod
    def _add_samples(family, rows):
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
