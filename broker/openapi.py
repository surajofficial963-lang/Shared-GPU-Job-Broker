"""OpenAPI contract for the Flask broker."""

JSON = "application/json"


def response(description, example=None):
    result = {"description": description}
    if example is not None:
        result["content"] = {JSON: {"example": example}}
    return result


def body(properties, example):
    return {
        "required": True,
        "content": {JSON: {
            "schema": {"type": "object", "properties": properties,
                       "required": list(properties), "additionalProperties": False},
            "example": example,
        }},
    }


def parameter(name, location, schema, description, required=True):
    return {"name": name, "in": location, "schema": schema, "description": description, "required": required}


def operation(summary, responses, **fields):
    return {"summary": summary, "responses": responses, **fields}


def specification():
    identifier = "d075f9c27ec84418ad77fc204de849fe"
    job_id = parameter("job_id", "path", {"type": "string", "pattern": "^[0-9a-f]{32}$"},
                       "ID returned by POST /jobs")
    key = parameter("Idempotency-Key", "header", {"type": "string", "example": "vision-demo-1"},
                    "Client key unique to this team and job submission")
    page = [
        parameter("limit", "query", {"type": "integer", "minimum": 1, "maximum": 500, "default": 100},
                  "Page size", required=False),
        parameter("offset", "query", {"type": "integer", "minimum": 0, "maximum": 1_000_000, "default": 0},
                  "Page offset", required=False),
    ]
    epoch = {"type": "integer", "format": "int64"}
    job = {"id": identifier, "status": "queued", "team": "vision", "gpus": 2, "estimate": 1800,
           "priority": "normal", "submitted": 1740960000, "started": None, "ended": None,
           "node_id": None, "failure_reason": None}
    denied = response("Missing or invalid bearer token")
    missing = response("Job not found")
    invalid = response("Invalid input")
    conflict = response("Request conflicts with existing state")
    forbidden = response("Credential lacks permission for this operation")
    page_example = response("Page of results", {"items": [], "offset": 0, "limit": 100})
    return {
        "openapi": "3.0.3",
        "info": {"title": "Shared GPU Job Broker", "version": "0.1.0"},
        "components": {"securitySchemes": {
            "apiToken": {"type": "http", "scheme": "bearer", "description": "Operator or team token."},
            "metricsToken": {"type": "http", "scheme": "bearer", "description": "Dedicated metrics token."},
        }},
        "security": [{"apiToken": []}],
        "paths": {
            "/jobs": {"post": operation(
                "Submit a job",
                {"201": response("Job created", {"id": identifier}), "200": response("Existing job", {"id": identifier}),
                 "400": invalid, "401": denied, "403": forbidden, "409": conflict, "415": response("Expected JSON")},
                parameters=[key],
                requestBody=body({
                    "team": {"type": "string", "pattern": "^[a-z0-9][a-z0-9_-]{0,39}$"},
                    "gpus": {"type": "integer", "enum": [1, 2, 4, 8]},
                    "estimated_seconds": {"type": "integer", "minimum": 1, "maximum": 7_776_000},
                    "priority": {"type": "string", "enum": ["low", "normal", "high"]},
                }, {"team": "vision", "gpus": 2, "estimated_seconds": 1800, "priority": "normal"}),
            )},
            "/jobs/{job_id}": {"get": operation("Get job status",
                {"200": response("Job found", job), "401": denied, "404": missing}, parameters=[job_id])},
            "/jobs/{job_id}/cancel": {"post": operation("Cancel a queued or running job",
                {"200": response("Job cancelled", {"id": identifier, "status": "cancelled"}),
                 "401": denied, "404": missing, "409": conflict}, parameters=[job_id])},
            "/jobs/{job_id}/complete": {"post": operation("Complete a running job (operator)",
                {"200": response("Job completed", {"id": identifier, "status": "succeeded"}),
                 "400": invalid, "401": denied, "403": forbidden, "404": missing, "409": conflict},
                parameters=[job_id],
                requestBody=body({"status": {"type": "string", "enum": ["succeeded", "failed"]}},
                                 {"status": "succeeded"}))},
            "/queue": {"get": operation("List queued jobs", {"200": page_example, "400": invalid, "401": denied},
                                        parameters=page)},
            "/nodes": {"get": operation("List live nodes", {"200": page_example, "400": invalid, "401": denied},
                                        parameters=page)},
            "/cost": {"get": operation("Report usage and cost by team",
                {"200": response("Cost report", {"teams": {}, "total_cost_usd": 0}),
                 "400": invalid, "401": denied, "503": response("Storage temporarily unavailable")},
                parameters=[parameter("start", "query", epoch, "Start time in Unix seconds UTC"),
                            parameter("end", "query", epoch, "End time in Unix seconds UTC")])},
            "/simulation/advance": {"post": operation("Advance the simulated clock (operator)",
                {"200": response("Simulated time advanced", {"now": 1740960600}),
                 "400": invalid, "401": denied, "403": forbidden},
                requestBody=body({"seconds": {"type": "integer", "minimum": 1, "maximum": 86400}}, {"seconds": 600}))},
            "/healthz": {"get": operation("Check liveness", {"200": response("OK", {"status": "ok"})}, security=[])},
            "/readyz": {"get": operation("Check readiness",
                {"200": response("Ready", {"status": "ready"}), "503": response("Storage temporarily unavailable")},
                security=[])},
            "/metrics": {"get": operation("Scrape Prometheus metrics",
                {"200": {"description": "Prometheus exposition", "content": {"text/plain": {"schema": {"type": "string"}}}},
                 "401": denied}, security=[{"metricsToken": []}])},
        },
    }
