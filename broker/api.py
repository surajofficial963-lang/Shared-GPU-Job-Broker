import hmac
import json
import os
import re
import sqlite3
import threading
import time
from pathlib import Path

from flask import Flask, abort, g, request
from flask_swagger_ui import get_swaggerui_blueprint
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from werkzeug.exceptions import HTTPException

from .archive import ArchiveUnavailable, ClickHouseArchive, archive_once
from .engine import PRIORITIES, Broker, KeyConflict
from .openapi import specification
from .store import Store, retention_cutoff


TEAM = re.compile(r"[a-z0-9][a-z0-9_-]{0,39}")
KEY = re.compile(r"[A-Za-z0-9._:-]{1,128}")
JOB_ID = re.compile(r"[a-f0-9]{32}")
MAX_SECONDS = 90 * 86400
MAX_EPOCH = 4102444800  # 2100-01-01, within the archive partition type.


def create_app(config=None):
    app = Flask(__name__)
    app.config.update(
        MAX_CONTENT_LENGTH=8192,
        DB_PATH=os.getenv("BROKER_DB", "broker.db"),
        API_TOKEN=os.getenv("BROKER_API_TOKEN", ""),
        METRICS_TOKEN=os.getenv("BROKER_METRICS_TOKEN", ""),
        TEAM_TOKENS=os.getenv("BROKER_TEAM_TOKENS", "{}"),
        IDLE_SECONDS=int(os.getenv("BROKER_IDLE_SECONDS", "300")),
        QUEUE_TIMEOUT_SECONDS=int(os.getenv("BROKER_QUEUE_TIMEOUT_SECONDS", "1800")),
        ARCHIVE_INTERVAL=int(os.getenv("BROKER_ARCHIVE_INTERVAL", "30")),
        ARCHIVE_URL=os.getenv("CLICKHOUSE_URL", ""),
        ARCHIVE_USER=os.getenv("CLICKHOUSE_USER", ""),
        ARCHIVE_PASSWORD=os.getenv("CLICKHOUSE_PASSWORD", ""),
        ARCHIVE_ALLOW_HTTP=os.getenv("CLICKHOUSE_ALLOW_HTTP") == "1",
        INITIAL_TIME=int(time.time()),
    )
    app.config.update(config or {})
    settings = app.config
    tokens = json.loads(settings["TEAM_TOKENS"])
    if not isinstance(tokens, dict) or any(not TEAM.fullmatch(team) for team in tokens):
        raise ValueError("Invalid team token configuration")
    all_tokens = [settings["API_TOKEN"], settings["METRICS_TOKEN"], *tokens.values()]
    if any(not isinstance(token, str) or not re.fullmatch(r"[!-~]{32,128}", token) for token in all_tokens):
        raise ValueError("Configure separate credentials of 32–128 visible ASCII characters")
    if len(set(all_tokens)) != len(all_tokens):
        raise ValueError("Credentials must be distinct")
    if settings["IDLE_SECONDS"] < 1 or settings["ARCHIVE_INTERVAL"] < 1:
        raise ValueError("Intervals must be positive")
    if not 1 <= settings["QUEUE_TIMEOUT_SECONDS"] <= MAX_SECONDS:
        raise ValueError(f"BROKER_QUEUE_TIMEOUT_SECONDS must be between 1 and {MAX_SECONDS}")
    if settings["ARCHIVE_URL"] and not (settings["ARCHIVE_USER"] and settings["ARCHIVE_PASSWORD"]):
        raise ValueError("Configure a dedicated ClickHouse credential")
    archive = ClickHouseArchive(
        settings["ARCHIVE_URL"], settings["ARCHIVE_USER"], settings["ARCHIVE_PASSWORD"],
        allow_http=settings["ARCHIVE_ALLOW_HTTP"],
    ) if settings["ARCHIVE_URL"] else None
    store = Store(settings["DB_PATH"], settings["INITIAL_TIME"])
    broker = Broker(
        store, settings["IDLE_SECONDS"], archive=archive,
        queue_timeout_seconds=settings["QUEUE_TIMEOUT_SECONDS"],
    )
    broker.advance_by(0)
    app.extensions["broker"] = broker

    registry = CollectorRegistry()
    calls = Counter("broker_http_requests", "HTTP responses", ["method", "route", "status"], registry=registry)
    duration = Histogram("broker_http_request_duration_seconds", "HTTP handler duration", ["route"], registry=registry)
    cost = Gauge("broker_cost_usd_24h", "Estimated node charges in the last 24 simulated hours", registry=registry)
    queued = Gauge("broker_queued_jobs", "Jobs awaiting placement", registry=registry)
    nodes = Gauge("broker_live_nodes", "Booting and ready nodes", registry=registry)
    archive_errors = Counter("broker_archive_errors", "Failed archival passes", registry=registry)
    archive_count = Counter("broker_archived_records", "Records moved to ClickHouse", registry=registry)
    archive_backlog = Gauge("broker_archive_pending_records", "Records older than hot retention", registry=registry)

    if archive and not settings.get("DISABLE_ARCHIVE_WORKER"):
        def archive_loop():
            while True:
                try:
                    archive_count.inc(archive_once(store, archive))
                except (ArchiveUnavailable, sqlite3.Error):
                    archive_errors.inc()
                    app.logger.warning("Archive pass failed; pending data will be retried")
                time.sleep(settings["ARCHIVE_INTERVAL"])

        threading.Thread(target=archive_loop, name="broker-archive", daemon=True).start()

    app.register_blueprint(get_swaggerui_blueprint(
        "/docs", "../openapi.json",
        config={"app_name": "Shared GPU Job Broker", "validatorUrl": None, "persistAuthorization": False},
    ))

    @app.get("/openapi.json")
    def openapi():
        return specification()

    @app.before_request
    def authenticate():
        g.started = time.monotonic()
        if request.endpoint in {"health", "ready", "openapi"} or request.blueprint == "swagger_ui" or request.url_rule is None:
            return
        header = request.headers.get("Authorization", "")
        scheme, _, credential = header.partition(" ")
        if scheme.lower() != "bearer" or not credential.isascii() or len(credential) > 512:
            abort(401)
        if request.endpoint == "metrics":
            if not hmac.compare_digest(credential, settings["METRICS_TOKEN"]):
                abort(401)
            return
        g.team = None
        if hmac.compare_digest(credential, settings["API_TOKEN"]):
            return
        for team, token in tokens.items():
            if hmac.compare_digest(credential, token):
                g.team = team
                return
        abort(401)

    @app.after_request
    def record(response):
        if request.endpoint not in {"health", "ready", "metrics"}:
            route = request.url_rule.rule if request.url_rule else "unmatched"
            method = request.method if request.method in {"GET", "POST", "HEAD", "OPTIONS"} else "other"
            calls.labels(method, route, response.status_code).inc()
            duration.labels(route).observe(time.monotonic() - g.started)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        if response.status_code == 401:
            response.headers["WWW-Authenticate"] = "Bearer"
        return response

    @app.errorhandler(HTTPException)
    def http_error(error):
        response = error.get_response()
        response.set_data(app.json.dumps({"error": error.description}))
        response.content_type = "application/json"
        return response

    @app.errorhandler(KeyConflict)
    def duplicate_conflict(_):
        return {"error": "Idempotency key already used with a different job"}, 409

    @app.errorhandler(ArchiveUnavailable)
    @app.errorhandler(sqlite3.OperationalError)
    def unavailable(_):
        return {"error": "Storage temporarily unavailable"}, 503

    def object_body(expected):
        payload = request.get_json()
        if not isinstance(payload, dict) or set(payload) != expected:
            abort(400, description="Expected exactly: " + ", ".join(sorted(expected)))
        return payload

    def integer(value, low, high):
        if type(value) is not int or not low <= value <= high:
            abort(400, description=f"Expected an integer between {low} and {high}")
        return value

    def query_int(name, default, low, high):
        value = request.args.get(name, default)
        try:
            return integer(int(value), low, high)
        except (ValueError, TypeError):
            abort(400, description=f"Invalid {name}")

    def operator_only():
        if g.team is not None:
            abort(403)

    def job_view(job):
        public = {key: job[key] for key in (
            "id", "team", "gpus", "estimate", "status", "submitted", "started", "ended", "node_id"
        )}
        public["failure_reason"] = job.get("failure_reason")
        public["priority"] = next(name for name, value in PRIORITIES.items() if value == job["priority"])
        return public

    def read_job(job_id):
        if not JOB_ID.fullmatch(job_id):
            abort(404)
        job = broker.get(job_id)
        if job is None or (g.team is not None and job["team"] != g.team):
            abort(404)
        return job

    @app.get("/healthz")
    def health():
        return {"status": "ok"}

    @app.get("/readyz")
    def ready():
        with store.session() as db:
            broker.now(db)
            if not os.access(Path(store.path).parent, os.W_OK):
                abort(503)
        return {"status": "ready"}

    @app.post("/jobs")
    def submit():
        body = object_body({"team", "gpus", "estimated_seconds", "priority"})
        team, gpus, priority = body["team"], body["gpus"], body["priority"]
        key = request.headers.get("Idempotency-Key", "")
        if not isinstance(team, str) or not TEAM.fullmatch(team):
            abort(400, description="Invalid team")
        if g.team is not None and team != g.team:
            abort(403)
        if type(gpus) is not int or gpus not in (1, 2, 4, 8):
            abort(400, description="GPUs must be 1, 2, 4 or 8")
        if not isinstance(priority, str) or priority not in PRIORITIES or not KEY.fullmatch(key):
            abort(400, description="Invalid priority or Idempotency-Key")
        estimate = integer(body["estimated_seconds"], 1, MAX_SECONDS)
        job_id, created = broker.submit(team, gpus, estimate, priority, key)
        return {"id": job_id}, 201 if created else 200, {"Location": f"/jobs/{job_id}"}

    @app.get("/jobs/<job_id>")
    def get_job(job_id):
        return job_view(read_job(job_id))

    @app.post("/jobs/<job_id>/cancel")
    def cancel(job_id):
        read_job(job_id)
        if not broker.finish(job_id, "cancelled"):
            abort(409, description="Job has already reached a different terminal state")
        return {"id": job_id, "status": "cancelled"}

    @app.post("/jobs/<job_id>/complete")
    def complete(job_id):
        operator_only()
        read_job(job_id)
        status = object_body({"status"})["status"]
        if status not in ("succeeded", "failed"):
            abort(400, description="Expected succeeded or failed")
        if not broker.finish(job_id, status):
            abort(409, description="Job cannot transition to the requested state")
        return {"id": job_id, "status": status}

    @app.get("/queue")
    @app.get("/nodes")
    def listing():
        limit = query_int("limit", 100, 1, 500)
        offset = query_int("offset", 0, 0, 1_000_000)
        if request.path == "/queue":
            rows = [job_view(row) for row in broker.listing("jobs", limit, offset, g.team)]
        else:
            rows = [{key: row[key] for key in ("id", "team", "gpus", "state", "launched", "ended")}
                    for row in broker.listing("nodes", limit, offset, g.team)]
        return {"items": rows, "offset": offset, "limit": limit}

    @app.get("/cost")
    def cost_report():
        begin = query_int("start", None, 0, MAX_EPOCH)
        end = query_int("end", None, 1, MAX_EPOCH)
        if end <= begin:
            abort(400, description="End must be after start")
        return broker.report(begin, end, g.team)

    @app.post("/simulation/advance")
    def advance():
        operator_only()
        seconds = integer(object_body({"seconds"})["seconds"], 1, 86400)
        try:
            return {"now": broker.advance_by(seconds, maximum=MAX_EPOCH)}
        except ValueError as exc:
            abort(400, description=str(exc))

    @app.get("/metrics")
    def metrics():
        with store.session() as db:
            now = broker.now(db)
            cutoff = retention_cutoff(now)
            queued.set(db.execute("SELECT count(*) FROM jobs WHERE status = 'queued'").fetchone()[0])
            nodes.set(db.execute("SELECT count(*) FROM nodes WHERE ended IS NULL").fetchone()[0])
            archive_backlog.set(sum(db.execute(
                f"SELECT count(*) FROM {table} WHERE ended <= ?", (cutoff,)
            ).fetchone()[0] for table in ("jobs", "nodes")))
        cost.set(broker.report(now - 86400, now)["total_cost_usd"])
        return generate_latest(registry), 200, {"Content-Type": "text/plain; version=0.0.4; charset=utf-8"}

    return app
