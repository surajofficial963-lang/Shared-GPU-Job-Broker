# Shared GPU Job Broker

A Python HTTP service that schedules simulated GPU jobs, persists state in SQLite, and archives terminal records to ClickHouse. Node allocation and billing use a persisted simulated clock. All inputs and infrastructure in the trace are fictional.

## Run locally

Requires Python 3.10+ for tests and replay. The full stack requires Docker with Compose v2.

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python -m broker.replay
```

For a disposable local stack, generate credentials into a new ignored `.env` file. Preserve your existing `.env` and volumes if you have already initialized the stack.

```sh
(
  umask 077
  set -C
  for name in BROKER_API_TOKEN BROKER_METRICS_TOKEN CLICKHOUSE_ADMIN_PASSWORD CLICKHOUSE_BROKER_PASSWORD GRAFANA_ADMIN_PASSWORD; do
    printf '%s=%s\n' "$name" "$(openssl rand -hex 32)"
  done > .env
)
docker compose up --build --wait
set -a
. ./.env
set +a
```

The API is at `http://127.0.0.1:8080`. Open Grafana at `http://127.0.0.1:3000`, sign in as `admin` with the generated password, and open **GPU Broker → Shared GPU Job Broker**. The dashboard shows per-route request p90/p95, requests per second, HTTP 5xx, the last 24 simulated hours of node costs, queue depth, live nodes, and archival backlog/errors. Prometheus needs multiple scrapes and request traffic before rate and percentile panels populate.

```sh
curl -fsS http://127.0.0.1:8080/jobs \
  -H "Authorization: Bearer $BROKER_API_TOKEN" \
  -H "Idempotency-Key: vision-example-1" \
  -H "Content-Type: application/json" \
  -d '{"team":"vision","gpus":2,"estimated_seconds":1800,"priority":"normal"}'

curl -fsS http://127.0.0.1:8080/simulation/advance \
  -H "Authorization: Bearer $BROKER_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"seconds":3600}'
```

Use the returned job ID with `GET /jobs/{id}`. Use the job's `submitted` time and the clock response's `now` as `start` and `end` on `GET /cost`. Run `docker compose restart broker` to verify that the clock and IDs survive restart. The live broker uses estimated duration as the default completion time. Operators can report completion earlier using the API.

## HTTP API

| Request | Behavior |
|---|---|
| `POST /jobs` | Requires an `Idempotency-Key`. Returns `201` for a new job, `200` with the original ID for a retry, or `409` when the same team/key has a different payload. |
| `GET /jobs/{id}` | Returns job status and timestamps, including archived jobs. |
| `POST /jobs/{id}/cancel` | Cancels a queued or running job. Repeated cancellation succeeds. |
| `POST /jobs/{id}/complete` | Operator reports `{"status":"succeeded"}` or `{"status":"failed"}` for a running job. |
| `GET /queue`, `GET /nodes` | Lists queued jobs or live nodes. Accepts `limit` and `offset`. |
| `GET /cost?start=…&end=…` | Returns GPU-hours and USD charges by team and total node cost over the window. All times are Unix seconds in UTC. End is capped at the simulated clock. |
| `POST /simulation/advance` | Operator advances by 1 to 86,400 seconds per call. |
| `GET /healthz`, `GET /readyz`, `GET /metrics` | Liveness, SQLite readiness, and Prometheus scrape. Metrics require a dedicated bearer token. |

The operator token has full broker access. Optional `BROKER_TEAM_TOKENS` is a JSON object mapping team names to distinct random tokens. Team tokens can submit for their team, look up and cancel their jobs, list their queue and nodes, and retrieve their costs. The service requires separate operator and metrics tokens of at least 32 ASCII characters. Store credentials in your secret manager and use TLS at the API gateway for any remote traffic. The Compose HTTP ports bind to loopback, and the private archive connection uses HTTP only in that local stack.

## Scheduling, cost, and retention

- The scheduler packs same-team jobs onto the smallest available fit, launches the smallest suitable node when needed, and reserves accepted launches for a job. A booting node counts toward the 10-node limit. Capacity errors retry every simulated minute and can occur in bursts. A failed boot costs 15 minutes and releases its reservation.
- Priority controls ordering without preemption. Aging promotes a waiting job one priority level every 30 minutes. Within a priority class, the scheduler provisions the next node for a team with fewer live nodes, then breaks ties in FIFO order. This balances node counts while queued teams compete for capacity. It does not equalize GPU counts or preempt nodes that are already running. Idle nodes terminate at the configured timeout. Boot, failed launch, and idle charges go to the team assigned to that node.
- Reports count only overlapping running time as GPU-hours. The difference between billed uptime and the 10-minute minimum is charged to the window ending at or containing termination. Active node cost accrues through the simulated present.
- SQLite uses WAL, transactions, and indexes for queue and completion queries, reservations, team/time windows, and expiry scans. The archive worker moves completed jobs and terminated nodes after **three UTC calendar months of simulated time**. It writes a batch synchronously to ClickHouse, removes the hot rows after acknowledgment, and reclaims SQLite pages incrementally. Failed writes leave rows available in SQLite and are retried. Compact hashed idempotency records stay in SQLite so retries still resolve after archival.
- ClickHouse uses monthly partitions, an ID skipping index, and replacement on stable record IDs within a partition. Historical reads deduplicate overlapping copies while archival is in progress. ClickHouse has no automatic expiry configured. Configure backups and a long-term retention policy at the archive service as appropriate. A storage failure on an archive-dependent read returns `503`. Without a ClickHouse URL, expiration pauses and all records remain in SQLite to preserve history.

## Kubernetes

Build and push the Docker image to your registry. Apply [the archive schema](ops/clickhouse.sql) to your TLS-enabled ClickHouse instance. Provision a dedicated account with `SELECT, INSERT` on `broker.history`. Create the existing Kubernetes Secret via your secret manager with `BROKER_API_TOKEN`, `BROKER_METRICS_TOKEN`, and, when archiving is enabled, `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD`. `BROKER_TEAM_TOKENS` is optional.

```sh
helm upgrade --install broker charts/gpu-broker -n gpu --create-namespace \
  --set image.repository=registry.example/gpu-broker \
  --set image.tag=0.1.0 \
  --set existingSecret=gpu-broker-credentials \
  --set clickhouse.url=https://archive.example:8443 \
  --set clickhouse.egressCIDR=10.10.20.0/24 \
  --set serviceMonitor.enabled=true \
  --set grafanaDashboard.enabled=true
```

Set `image.digest` for an immutable release. The chart uses a non-root container, read-only root filesystem, dropped capabilities, seccomp, no service account token mount, and a NetworkPolicy. Label the gateway and Prometheus namespaces with `gpu-broker-access=true`. Adapt the archive CIDR, port and DNS policy to your CNI and DNS setup. The ClusterIP service has no public ingress. Terminate HTTPS at your gateway and use encrypted storage classes and periodic backups for SQLite and ClickHouse.

The optional ServiceMonitor requires Prometheus Operator and discovery of the chart namespace. The optional Grafana ConfigMap uses the `grafana_dashboard: "1"` sidecar label. Configure the Grafana Prometheus data source UID as `broker-prometheus`, or import the included [dashboard JSON](charts/gpu-broker/dashboards/broker.json). ClickHouse, Prometheus and Grafana are provisioned together by Compose. The Helm chart expects those services to be managed separately.

Keep the broker at one replica with one Gunicorn worker and a durable PVC. The rollout briefly interrupts the API. This preserves one scheduler and a single SQLite writer across deploys. Check readiness and archive backlog after an upgrade. Back up SQLite using its backup API or a quiesced volume snapshot, and back up ClickHouse separately.

## Trace replay and scope

The input contains 494 rows. The replay normalizes explicit offsets to UTC, assumes UTC for timestamps without offsets, then sorts stably by timestamp. It skips two unsupported GPU requests and 14 duplicates after timestamp normalization. Without submission IDs, rows identical after normalization are assumed to be repeats. The simulator processes all 478 accepted jobs, including failures and cancellations, then drains jobs and terminates idle nodes before reporting. The scheduler uses GPU size and priority for placement. Actual duration is used only to complete the simulated job.

| Team | 5 min idle p50 / p95 wait (min) | 15 min idle p50 / p95 wait (min) |
|---|---:|---:|
| forecast | 5.53 / 26.58 | 5.08 / 35.63 |
| nlp | 5.48 / 12.38 | 5.05 / 21.43 |
| platform | 0.42 / 113.87 | 2.83 / 124.48 |
| speech | 5.53 / 16.47 | 6.17 / 20.55 |
| vision | 74.08 / 222.58 | 100.02 / 255.87 |
| **Total cost** | **$2,898.50** | **$3,093.08** |

For seed 23, the 5-minute policy cost about **$194.58 less** and had lower p95 waits for every team. Shorter idle time frees scarce node slots when another team is waiting. The very long tails for `vision` and `platform` show the impact of queue bursts, team isolation, and the shared 10-node cap. This is one seeded comparison. The replay prints p90, p99, GPU-hours, and team costs as well.

The implementation excludes interruptible nodes, job preemption, real GPU execution, and multi-replica scheduling. A next scheduling experiment would compare balancing by live node count with balancing by allocated GPUs at each effective priority. The fake provider and manually advanced clock are intentional exercise constraints. Codex assisted with implementation, tests, delivery configuration, and trace analysis.
