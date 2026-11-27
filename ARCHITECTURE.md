# Shared GPU Job Broker architecture

## System components

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

## A job from submission to archive

This example follows a two-GPU job that needs a new four-GPU node.

```mermaid
sequenceDiagram
    autonumber
    actor User as User via Postman or Swagger
    participant API as Broker API
    participant Scheduler as Scheduler
    participant GPU as Simulated GPU provider
    participant PG as PostgreSQL
    participant Archive as Archive worker
    participant CH as ClickHouse

    User->>API: Submit a job with an idempotency key
    API->>Scheduler: Submit the validated job
    Scheduler->>PG: Save job as queued and check same-team capacity
    Scheduler->>GPU: Request a four-GPU node
    Scheduler->>PG: Save booting node and its job reservation
    API-->>User: Return the job ID

    User->>API: Advance the simulated clock
    API->>Scheduler: Process due events
    Scheduler->>GPU: Check the simulated boot event
    Scheduler->>PG: Mark node ready and job running
    User->>API: Look up the job by ID
    API->>PG: Read job status and assigned node
    API-->>User: Return running and the node ID

    User->>API: Advance the clock through job completion
    API->>Scheduler: Process due events
    Scheduler->>PG: Mark job succeeded and record execution time
    Note over Scheduler,PG: The node becomes idle if its last job finished
    Scheduler->>GPU: Release the node when the idle timeout expires
    Scheduler->>PG: Save node termination time for billing

    User->>API: Request a cost report for a time window
    API->>PG: Read job and node timestamps
    API-->>User: Return team GPU-hours and cost

    Note over PG,Archive: Terminal records become eligible after three simulated calendar months
    Archive->>PG: Read eligible jobs and nodes
    Archive->>CH: Copy terminal records
    CH-->>Archive: Acknowledge the write
    Archive->>PG: Delete the copied rows
    User->>API: Look up the same job ID later
    API->>PG: Check live storage
    API->>CH: Read the archived job
    API-->>User: Return succeeded and the saved timestamps
```

If a ready node already has enough capacity for the team, the scheduler can start the job at submission. If it cannot start the job before its queue deadline, it records a failure with `queue_timeout`.

## Design notes

- **Scheduling.** A PostgreSQL transaction-scoped advisory lock serializes state changes. The scheduler checks same-team GPU capacity, applies queue timeouts, and terminates idle nodes as simulated time advances.
- **Storage.** PostgreSQL holds operational state and derived timing aggregates. The archive worker copies terminal jobs and nodes to ClickHouse before deleting their PostgreSQL rows. Historical API reads combine the two stores when a report spans the retention cutoff.
- **Deployment.** Docker Compose runs the broker and all four dependencies with named volumes for data. The Helm chart deploys the broker and connects it to separately managed data and monitoring services.
- **Trace replay.** The synthetic CSV runs through the same scheduling logic with an isolated SQLite database. It does not write to the live PostgreSQL database.

See the [README](README.md) for deployment commands, configuration, and trace results.
