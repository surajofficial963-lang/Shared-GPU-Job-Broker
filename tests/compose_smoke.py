"""Run against a disposable Compose stack. Verifies HTTP, archive, and metrics."""
import base64
import json
import os
import subprocess
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
    base_url = os.getenv("BROKER_SMOKE_URL", "http://127.0.0.1:8080")
    with urlopen(Request(base_url + path, data=data, headers=headers), timeout=10) as response:
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
    # Jump directly to the retention boundary in this disposable test stack.
    subprocess.run([
        "docker", "compose", "exec", "-T", "broker", "python", "-c",
        "import os; from broker.engine import Broker; from broker.postgres import PostgresStore; "
        "Broker(PostgresStore(os.environ['BROKER_DATABASE_URL'], 0)).advance_by(95 * 86400)",
    ], check=True)
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


def check_grafana(password):
    credentials = base64.b64encode(f"admin:{password}".encode()).decode()
    base_url = os.getenv("GRAFANA_SMOKE_URL", "http://127.0.0.1:3000")
    req = Request(base_url + "/api/dashboards/uid/shared-gpu-broker",
                  headers={"Authorization": f"Basic {credentials}"})
    for _ in range(30):
        try:
            with urlopen(req, timeout=2) as response:
                dashboard = json.load(response)["dashboard"]
                titles = {panel["title"] for panel in dashboard["panels"]}
                for timing in ("Job completion time", "Job queue wait", "HTTP request latency"):
                    assert {f"{timing} p90", f"{timing} p95", f"{timing} (${{percentile:text}})"} <= titles
                print("Grafana dashboard provisioned")
                return
        except OSError:
            time.sleep(1)
    raise AssertionError("Grafana dashboard was not available")


if __name__ == "__main__":
    main()
    if os.getenv("GRAFANA_ADMIN_PASSWORD"):
        check_grafana(os.environ["GRAFANA_ADMIN_PASSWORD"])
