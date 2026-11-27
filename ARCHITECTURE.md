# Shared GPU Job Broker architecture

```mermaid
flowchart LR
    Client["Postman, Swagger UI, or API client"]

    subgraph Broker["Broker service · Flask + Gunicorn"]
        direction TB
        API["HTTP API · bearer auth and rate limiting"]
        Scheduler["Scheduler · simulated clock and job lifecycle"]
        Provider["Fake GPU provider · boot delays, capacity failures, teardown"]
        Archive["Archive worker · three simulated calendar months"]
        Metrics["Authenticated /metrics endpoint"]
    end

    PostgreSQL[("PostgreSQL · jobs, nodes, clock, idempotency and timing aggregates")]
    ClickHouse[("ClickHouse · archived job and node records")]
    Prometheus[("Prometheus · scraped metrics")]
    Grafana["Grafana · filtered latency, cost and operations dashboards"]

    Client -->|"Submit, inspect, cancel, complete, advance time"| API
    API -->|"Mutations and scheduling"| Scheduler
    Scheduler -->|"Launch or terminate simulated nodes"| Provider
    Scheduler -->|"Persist state in one transaction"| PostgreSQL
    API -->|"Read current jobs, queue, nodes and costs"| PostgreSQL
    API -->|"Read archived jobs and costs"| ClickHouse
    Archive -->|"Read eligible terminal records"| PostgreSQL
    Archive -->|"Write archive batch and await acknowledgment"| ClickHouse
    Archive -->|"After acknowledgment, delete copied rows"| PostgreSQL
    Metrics -->|"Read scheduler and timing aggregates"| PostgreSQL
    Prometheus -->|"Scrape with a dedicated bearer token"| Metrics
    Grafana -->|"Query time series"| Prometheus
```

- **Scheduling.** A PostgreSQL transaction-scoped advisory lock serializes state changes. The scheduler checks same-team GPU capacity, applies queue timeouts, and terminates idle nodes as simulated time advances.
- **Storage.** PostgreSQL holds operational state and derived timing aggregates. The archive worker copies terminal jobs and nodes to ClickHouse before deleting their PostgreSQL rows. Historical API reads combine the two stores when a report spans the retention cutoff.
- **Deployment.** Docker Compose runs the broker and all four dependencies with named volumes for data. The Helm chart deploys the broker and connects it to separately managed data and monitoring services.
- **Trace replay.** The synthetic CSV runs through the same scheduling logic with an isolated SQLite database. It does not write to the live PostgreSQL database.

See the [README](README.md) for deployment commands, configuration, and trace results.
