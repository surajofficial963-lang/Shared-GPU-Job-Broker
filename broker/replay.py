import argparse
import csv
import json
import math
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .engine import PRIORITIES, TERMINAL, Broker
from .store import Store


def load(path):
    rejected = Counter()
    seen = set()
    jobs = []
    with open(path, newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            try:
                stamp = datetime.fromisoformat(row["submitted_at"].replace("Z", "+00:00"))
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=timezone.utc)
                gpus, estimate, actual = (int(row[key]) for key in (
                    "gpus", "estimated_minutes", "actual_minutes"
                ))
                if gpus not in (1, 2, 4, 8):
                    rejected["unsupported_gpu_request"] += 1
                    continue
                if min(estimate, actual) < 1 or not row["team"] or (
                    row["priority"] not in PRIORITIES or row["outcome"] not in TERMINAL
                ):
                    raise ValueError
                job = (int(stamp.timestamp()), row["team"], gpus, estimate, actual,
                       row["priority"], row["outcome"])
                if job in seen:
                    rejected["duplicate"] += 1
                    continue
                seen.add(job)
                jobs.append(job)
            except (ValueError, KeyError, AttributeError, TypeError, OverflowError):
                rejected["invalid_row"] += 1
    return sorted(jobs, key=lambda row: row[0]), rejected


def replay(jobs, idle_minutes, seed):
    start = jobs[0][0]
    with tempfile.TemporaryDirectory() as directory:
        store = Store(str(Path(directory) / "replay.db"), start)
        engine = Broker(store, idle_minutes * 60, seed=seed)
        for index, (submitted, team, gpus, estimate, actual, priority, outcome) in enumerate(jobs):
            engine.advance(submitted)
            engine.submit(team, gpus, estimate * 60, priority, str(index), actual * 60, outcome)
        end = engine.advance()
        waits = defaultdict(list)
        with store.session() as db:
            for row in db.execute("SELECT team, started, submitted FROM jobs"):
                if row["started"] is None:
                    raise RuntimeError("Replay ended with an unplaced job")
                waits[row["team"]].append((row["started"] - row["submitted"]) / 60)
        percentiles = {}
        for team, values in sorted(waits.items()):
            values.sort()
            percentiles[team] = {
                "jobs": len(values),
                **{f"p{q}": round(values[math.ceil(q * len(values) / 100) - 1], 2)
                   for q in (50, 90, 95, 99)},
            }
        report = engine.report(start, end)
        return {"idle_minutes": idle_minutes, "wait_minutes": percentiles, **report}


def main():
    parser = argparse.ArgumentParser(description="Replay GPU submissions")
    parser.add_argument("trace", nargs="?", default="data/01a_trace.csv")
    parser.add_argument("--idle-minutes", type=int, nargs="+", default=[5, 15])
    parser.add_argument("--seed", type=int, default=23)
    args = parser.parse_args()
    jobs, rejected = load(args.trace)
    if not jobs or min(args.idle_minutes) < 1:
        parser.error("A nonempty valid trace and positive idle timeout are required")
    print(json.dumps({
        "seed": args.seed, "accepted": len(jobs), "skipped": rejected,
        "policies": [replay(jobs, idle, args.seed) for idle in args.idle_minutes],
    }, indent=2))


if __name__ == "__main__":
    main()
