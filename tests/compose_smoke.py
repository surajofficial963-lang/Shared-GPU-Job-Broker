"""Run against a disposable Compose stack. Verifies HTTP, archive, and metrics."""
import json
import os
import time
from urllib.request import Request, urlopen


def request(path, payload=None, *, metrics=False):
    token = os.environ["BROKER_METRICS_TOKEN" if metrics else "BROKER_API_TOKEN"]
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Idempotency-Key": "compose-archive-smoke",
    }
    data = None if payload is None else json.dumps(payload).encode()
    with urlopen(Request(f"http://127.0.0.1:8080{path}", data=data, headers=headers), timeout=10) as response:
        return response.read().decode() if metrics else json.load(response)


def main():
    job = request("/jobs", {"team": "smoke", "gpus": 2, "estimated_seconds": 60, "priority": "normal"})
    path = "/jobs/" + job["id"]
    start = request(path)["submitted"]
    end = request("/simulation/advance", {"seconds": 86400})["now"]
    assert request(path)["status"] == "succeeded"
    report_path = f"/cost?start={start}&end={end}"
    original = request(report_path)
    assert original["total_cost_usd"] > 0
    for _ in range(95):
        request("/simulation/advance", {"seconds": 86400})
    for _ in range(50):
        metrics = request("/metrics", metrics=True)
        values = dict(line.split() for line in metrics.splitlines()
                      if line.startswith(("broker_archived_records_total ", "broker_archive_pending_records ")))
        if (float(values["broker_archived_records_total"]) >= 2
                and float(values["broker_archive_pending_records"]) == 0):
            break
        time.sleep(1)
    else:
        raise AssertionError("Expected one job and one node to be archived")
    assert request(path)["status"] == "succeeded"
    assert request(report_path) == original
    print("Compose smoke passed: archived job retrieval, consistent costs, and metrics")


if __name__ == "__main__":
    main()
