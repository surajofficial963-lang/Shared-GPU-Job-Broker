import tempfile
import unittest
from pathlib import Path

from broker.api import create_app


class ApiTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.operator = {"Authorization": "Bearer " + "a" * 32, "Idempotency-Key": "example"}
        self.metrics = {"Authorization": "Bearer " + "m" * 32}
        self.team = {"Authorization": "Bearer " + "t" * 32, "Idempotency-Key": "example"}
        self.app = create_app({
            "TESTING": True, "API_TOKEN": "a" * 32, "METRICS_TOKEN": "m" * 32,
            "TEAM_TOKENS": '{"vision":"' + "t" * 32 + '"}',
            "DB_PATH": str(Path(directory.name) / "api.db"), "INITIAL_TIME": 1740960000,
            "ARCHIVE_URL": "",
        })
        self.client = self.app.test_client()
        self.job = {"team": "speech", "gpus": 2, "estimated_seconds": 60, "priority": "normal"}

    def test_security_and_team_isolation(self):
        self.assertEqual(self.client.get("/readyz").status_code, 200)
        self.assertEqual(self.client.get("/nodes").status_code, 401)
        self.assertEqual(self.client.get("/metrics", headers=self.operator).status_code, 401)
        self.assertEqual(self.client.get("/nodes", headers=self.metrics).status_code, 401)
        self.assertEqual(self.client.post("/jobs", headers=self.team, json=self.job).status_code, 403)
        result = self.client.post("/jobs", headers=self.operator, json=self.job)
        self.assertEqual(result.status_code, 201)
        url = result.headers["Location"]
        self.assertEqual(self.client.get(url, headers=self.team).status_code, 404)
        self.assertEqual(self.client.post(url + "/cancel", headers=self.team).status_code, 404)
        self.assertEqual(self.client.get("/nodes", headers=self.team).json["items"], [])
        self.assertEqual(self.client.post("/simulation/advance", headers=self.team, json={"seconds": 1}).status_code, 403)

    def test_validation_duplicate_and_metrics(self):
        bad = dict(self.job, gpus=True)
        self.assertEqual(self.client.post("/jobs", headers=self.operator, json=bad).status_code, 400)
        bad = dict(self.job, priority=[])
        self.assertEqual(self.client.post("/jobs", headers=self.operator, json=bad).status_code, 400)
        self.assertEqual(self.client.post("/jobs", headers=self.operator, json=self.job).status_code, 201)
        self.assertEqual(self.client.post("/jobs", headers=self.operator, json=self.job).status_code, 200)
        changed = dict(self.job, gpus=4)
        self.assertEqual(self.client.post("/jobs", headers=self.operator, json=changed).status_code, 409)
        metrics = self.client.get("/metrics", headers=self.metrics)
        self.assertEqual(metrics.status_code, 200)
        self.assertIn(b'broker_http_requests_total{method="POST",route="/jobs",status="409"} 1', metrics.data)

    def test_configured_queue_timeout_is_exposed_by_api(self):
        from unittest.mock import patch

        settings = {
            key: self.app.config[key]
            for key in ("TESTING", "API_TOKEN", "METRICS_TOKEN", "TEAM_TOKENS", "DB_PATH", "INITIAL_TIME")
        }
        settings["ARCHIVE_URL"] = ""
        with patch.dict("os.environ", {"BROKER_QUEUE_TIMEOUT_SECONDS": "120"}):
            app = create_app(settings)

        client = app.test_client()
        result = client.post("/jobs", headers=self.operator, json=self.job)
        url = result.headers["Location"]
        client.post("/simulation/advance", headers=self.operator, json={"seconds": 119})
        self.assertEqual(client.get(url, headers=self.operator).json["status"], "queued")
        client.post("/simulation/advance", headers=self.operator, json={"seconds": 1})
        job = client.get(url, headers=self.operator).json
        self.assertEqual(job["status"], "failed")
        self.assertEqual(job["failure_reason"], "queue_timeout")
        self.assertIsNone(job["started"])
        self.assertEqual(job["ended"] - job["submitted"], 120)
        self.assertEqual(client.get("/queue", headers=self.operator).json["items"], [])
        broker = app.extensions["broker"]
        self.assertTrue(all(node["reserved_job"] is None for node in broker.listing("nodes")))
