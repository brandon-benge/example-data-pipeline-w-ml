# Example Data Pipeline with ML

Architecture overview:

![Platform Architecture](./docs/image.png)

This repository demonstrates how a modern data platform can support **real-time ML decisions** using streaming pipelines, a lakehouse architecture, and an inference service.

📖 **Blog walkthrough**  
Full architecture explanation and design discussion:  
https://github.com/brandon-benge/example-data-pipeline-w-ml/blob/main/docs/blog.md

📐 **Detailed architecture specification**  
See the full architecture definition here:  
https://github.com/brandon-benge/example-data-pipeline-w-ml/blob/main/ARCHITECTURE.md

## Platform Overview

This repository contains a **local laptop-scale end-to-end platform** that demonstrates how streaming, batch processing, analytics, and ML inference systems can work together to produce real operational decisions.

Key components include:

- Postgres source tables with Debezium CDC into Kafka
- direct behavioral events on `events.session_event` with Schema Registry
- Bronze ingestion and online Redis feature serving in Flink
- Silver, governance, and offline feature parity logic in Spark
- Gold dimensions, facts, marts, and semantic views in dbt
- Trino and Superset for curated BI access
- local ML training code plus a containerized inference service that reads dbt-built Iceberg feature tables and publishes artifacts to MinIO-backed object storage

## Architecture Layers

- Ingestion: Postgres, Debezium, Kafka Connect, Kafka, Schema Registry
- Stream Processing: Kafka Connect Iceberg sinks, Flink, Redis
- Lakehouse: Apache Iceberg, Iceberg REST Catalog, MinIO
- Batch Processing: Spark, dbt
- Analytics: Trino, Apache Superset
- ML: scikit-learn, MinIO, FastAPI, Redis

## Setup

Prerequisites:

- Docker and Docker Compose
- Python 3.11+ for the synthetic generator and local tests

## Startup

Bring up the full local platform:

```bash
docker compose up -d
```

This is the canonical local startup path. Kafka topic creation, Schema Registry subject registration, Debezium source registration, Iceberg sink registration, MinIO/Iceberg bootstrap, Trino, and Superset initialization all happen through Compose-managed services.
Flink streaming job submission and Spark batch job execution are also started through Compose-managed services.
The REST-catalog namespaces and Bronze CDC table DDL are also initialized by a dedicated one-shot bootstrap service before the per-table Kafka Connect Iceberg sinks are registered. The Debezium source connector and the Iceberg sink connectors run on separate Kafka Connect workers in local mode.

For local simplicity, the Iceberg REST catalog now uses the existing Postgres service as its JDBC metadata backend. That shared database is a demo shortcut. In a more realistic deployment, source application tables, Iceberg catalog metadata, Superset metadata, and any other control-plane state should live in separate databases.

## Synthetic data generation

The synthetic generator is the only repository component intended to be run manually from the command line:

```bash
python generator/app.py --config params.yaml --mode both
```

That seeds:

- mutable business tables in Postgres for CDC capture
- `events.session_event` behavioral events in Kafka

## Architecture-aligned workflow

1. Start the platform with `docker compose up -d`.
2. Run the synthetic generator manually.
3. Let the compose-managed Kafka Connect, Flink, and Spark services process CDC and event data into Bronze, Silver, Gold-supporting Silver datasets, Redis, and Superset.
4. Inspect curated outputs through Superset, Trino, Redis, and repository-managed metadata/artifacts.

## Example ML Decisions

The platform demonstrates three operational ML decision patterns:

- **Customer purchase propensity** – predicts likelihood of a near-term purchase
- **Campaign success propensity** – predicts whether a campaign will perform well
- **Advertiser budget expansion** – predicts likelihood of increased advertising spend

These predictions are exposed through the containerized inference service.

## Key paths

- platform config: [docker-compose.yml](./docker-compose.yml)
- source generator: [generator/](./generator)
- Flink jobs: [flink/jobs](./flink/jobs)
- Spark jobs: [spark/jobs](./spark/jobs)
- dbt Gold project: [dbt/](./dbt)
- BI assets: [bi/](./bi)
- ML code and artifacts: [ml/](./ml)
- governance and metadata: [config/governance](./config/governance), [metadata/](./metadata)
- runbooks: [docs/runbooks](./docs/runbooks)

## BI and access

- Trino: `http://localhost:8080`
- Superset: `http://localhost:8088`
- Schema Registry: `http://localhost:8081`
- metadata HTTP server: `http://localhost:9002`
- ML inference API: `http://localhost:8010`

The inference surface exposes separate use-case endpoints:
- `POST /score/customer_purchase`
- `POST /score/campaign_success`
- `POST /score/advertiser_budget_expansion`

Superset is bootstrapped during `docker compose up` and creates a Trino connection using the local pattern `trino://trino:8080/iceberg`, defaulting BI assets to the curated `gold` schema.

## ML implementation

The ML layer is intentionally lightweight and local-demo-friendly:

- training features are assembled only from Silver-derived feature dataset snapshots
- example feature groups are supported for customer, campaign, and advertiser use cases
- example labels are supported for `customer_purchase_next_7d`, `campaign_success_flag`, and `advertiser_budget_increase_next_30d`
- model artifacts are cached under [ml/artifacts/](./ml/artifacts), published to MinIO, and versioned in an Iceberg model registry table

## Tests

The repository includes unit and integration tests for critical local behaviors under [tests/unit](./tests/unit) and [tests/integration](./tests/integration).

## Runbooks

- [Platform Runbook](./docs/runbooks/platform_runbook.md)
- [Demo Workflow](./docs/runbooks/demo_workflow.md)

## Documentation Index

Operational docs:

- [Ad Hoc Queries](./docs/ad_hoc_queries.md)

Governance and quality docs:

- [Governance Notes](./docs/governance.md)
- [DQ Policy](./docs/dq_policy.md)

## Reference Material

- [Architecture Blog](./docs/blog.md)
- [Architecture](./ARCHITECTURE.md)
- [Architecture Rationale](./docs/architecture_rationale.md)
- [Real-Time Scoring Use Case](./docs/realtime_scoring_use_case.md)
