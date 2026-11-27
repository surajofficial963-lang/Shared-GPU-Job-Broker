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

    def test_swagger_documents_each_api_route_without_disabling_auth(self):
        result = self.client.get("/openapi.json")
        self.assertEqual(result.status_code, 200)
        spec = result.json
        actual_paths = {
            rule.rule.replace("<job_id>", "{job_id}")
            for rule in self.app.url_map.iter_rules()
            if rule.endpoint != "static" and not rule.endpoint.startswith("swagger_ui.")
            and rule.endpoint != "openapi"
        }
        self.assertEqual(set(spec["paths"]), actual_paths)
        self.assertEqual(spec["paths"]["/metrics"]["get"]["security"], [{"metricsToken": []}])
        page = self.client.get("/docs/")
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"../openapi.json", page.data)
        with self.client.get("/docs/swagger-ui-bundle.js") as asset:
            self.assertEqual(asset.status_code, 200)
        self.assertEqual(self.client.get("/queue").status_code, 401)

    def test_queue_wait_metrics_use_allocated_node_type_and_survive_restart(self):
        broker = self.app.extensions["broker"]
        broker.provider.launch_node = lambda gpus, now, attempt: (now + 330, False)
        result = self.client.post("/jobs", headers=self.operator, json=self.job)
        self.assertEqual(result.status_code, 201)
        broker.advance_by(330)
        self.client.post("/jobs", headers=dict(self.operator, **{"Idempotency-Key": "second"}), json=self.job)
        labels = b'{gpu_type="gpu-m",team="speech"}'
        metrics = self.client.get("/metrics", headers=self.metrics).data
        self.assertIn(b"broker_job_queue_wait_seconds_count" + labels + b" 2", metrics)
        self.assertIn(b"broker_job_queue_wait_seconds_sum" + labels + b" 330", metrics)

        # Simulate upgrade from a database without stored queue-wait aggregates.
        with broker.store.session(write=True) as db:
            db.execute("DROP TABLE queue_wait_buckets")
        settings = {
            key: self.app.config[key]
            for key in ("TESTING", "API_TOKEN", "METRICS_TOKEN", "TEAM_TOKENS", "DB_PATH", "INITIAL_TIME")
        }
        settings["ARCHIVE_URL"] = ""
        for _ in range(2):
            client = create_app(settings).test_client()
            updated = client.get("/metrics", headers=self.metrics).data
            self.assertIn(b"broker_job_queue_wait_seconds_count" + labels + b" 2", updated)
            self.assertIn(b"broker_job_queue_wait_seconds_sum" + labels + b" 330", updated)

    def test_api_rate_limit_and_cooldown_are_per_credential(self):
        from unittest.mock import patch

        with patch("broker.rate_limit.monotonic", return_value=10) as clock:
            for _ in range(30):
                self.assertEqual(self.client.get("/queue", headers=self.operator).status_code, 200)
                self.assertEqual(self.client.get("/nodes", headers=self.operator).status_code, 200)
            blocked = self.client.get("/queue", headers=self.operator)
            self.assertEqual(blocked.status_code, 429)
            self.assertEqual(blocked.headers["Retry-After"], "120")
            self.assertEqual(blocked.json["retry_after_seconds"], 120)
            self.assertEqual(self.client.get("/queue", headers=self.team).status_code, 200)
            self.assertEqual(self.client.get("/healthz").status_code, 200)
            self.assertEqual(self.client.get("/metrics", headers=self.metrics).status_code, 200)

            clock.return_value = 129.1
            self.assertEqual(self.client.get("/nodes", headers=self.operator).headers["Retry-After"], "1")
            clock.return_value = 130
            self.assertEqual(self.client.get("/queue", headers=self.operator).status_code, 200)

    def test_completion_histogram_counts_success_once_and_excludes_queue_wait(self):
        broker = self.app.extensions["broker"]
        broker.provider.launch_node = lambda gpus, now, attempt: (now + 330, False)
        first, _ = broker.submit("speech", 2, 60, "normal", "first")
        broker.advance_by(390)
        self.assertTrue(broker.finish(first, "succeeded"))
        second, _ = broker.submit("speech", 2, 60, "normal", "second")
        self.assertTrue(broker.finish(second, "cancelled"))

        metrics = self.client.get("/metrics", headers=self.metrics).data
        labels = b'{gpu_type="gpu-m",team="speech"}'
        self.assertIn(b"broker_job_runtime_seconds_count" + labels + b" 1", metrics)
        self.assertIn(b"broker_job_runtime_seconds_sum" + labels + b" 60", metrics)

    def test_rate_limit_reads_all_three_environment_settings(self):
        from unittest.mock import patch

        settings = {
            key: self.app.config[key]
            for key in ("TESTING", "API_TOKEN", "METRICS_TOKEN", "TEAM_TOKENS", "DB_PATH", "INITIAL_TIME")
        }
        settings["ARCHIVE_URL"] = ""
        with patch.dict("os.environ", {
            "BROKER_RATE_LIMIT_REQUESTS": "2",
            "BROKER_RATE_LIMIT_WINDOW_SECONDS": "10",
            "BROKER_RATE_LIMIT_COOLDOWN_SECONDS": "25",
        }):
            client = create_app(settings).test_client()

        with patch("broker.rate_limit.monotonic", return_value=100) as clock:
            for _ in range(2):
                self.assertEqual(client.get("/queue", headers=self.operator).status_code, 200)
            clock.return_value = 110
            for _ in range(2):
                self.assertEqual(client.get("/queue", headers=self.operator).status_code, 200)
            response = client.get("/queue", headers=self.operator)
            self.assertEqual(response.status_code, 429)
            self.assertEqual(response.headers["Retry-After"], "25")
            clock.return_value = 135
            self.assertEqual(client.get("/queue", headers=self.operator).status_code, 200)

        throttled = client.get("/openapi.json").json["paths"]["/queue"]["get"]["responses"]["429"]
        self.assertEqual(throttled["content"]["application/json"]["example"]["retry_after_seconds"], 25)
