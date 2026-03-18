# Example Data Pipeline with ML

Architecture overview:

![Platform Architecture](./docs/image.png)

This repository demonstrates how a modern data platform can support real-time ML decisions using streaming pipelines, a lakehouse architecture, and an inference service.

Blog walkthrough:

Full architecture explanation and design discussion:
https://github.com/brandon-benge/example-data-pipeline-w-ml/blob/main/docs/blog.md

Detailed architecture specification:

See the full architecture definition here:
https://github.com/brandon-benge/example-data-pipeline-w-ml/blob/main/ARCHITECTURE.md

## Platform Overview

This repository contains a local laptop-scale end-to-end platform that demonstrates how streaming, batch processing, analytics, and ML inference systems can work together to produce real operational decisions.

Key components include:

- Postgres source tables with Debezium CDC into Kafka
- direct behavioral events on `events.session_event` with Schema Registry
- Bronze ingestion and online Redis feature serving in Flink
- Silver, governance, and offline feature parity logic in Spark
- Gold dimensions, facts, marts, and semantic views in dbt
- Trino and Superset for curated BI access
- local ML training code that publishes artifacts to MinIO-backed object storage plus a containerized inference service that reads governed features and serves request-time predictions

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

There are two ways to run the local platform:

Recommended for normal local use:

- use the staged wrapper flow in [Demo Workflow](./docs/runbooks/demo_workflow.md)
- the wrapper starts the platform in phases, validates each stage, and monitors data as it moves through the system

Full-stack option:

```bash
docker compose up -d
```

This brings up the whole platform at once, but it is heavy for a laptop and can consume substantial CPU and memory.

The staged workflow is usually the better local experience:

```bash
bash tools/run_stack_workflow.sh --stop-at ingestion
bash tools/run_stack_workflow.sh --stop-at stream-processing
bash tools/run_stack_workflow.sh --stop-at batch
bash tools/run_stack_workflow.sh --stop-at analytics
bash tools/run_stack_workflow.sh --stop-at ml
```

If you do use `docker compose up -d`, Kafka topic creation, Schema Registry subject registration, Debezium source registration, Iceberg sink registration, MinIO/Iceberg bootstrap, Trino, and Superset initialization all happen through Compose-managed services.
Flink streaming job submission and Spark batch job execution are also started through Compose-managed services.
The REST-catalog namespaces and Bronze CDC table DDL are also initialized by a dedicated one-shot bootstrap service before the per-table Kafka Connect Iceberg sinks are registered. The Debezium source connector and the Iceberg sink connectors run on separate Kafka Connect workers in local mode.

For local simplicity, the Iceberg REST catalog now uses the existing Postgres service as its JDBC metadata backend. That shared database is a demo shortcut. In a more realistic deployment, source application tables, Iceberg catalog metadata, Superset metadata, and any other control-plane state should live in separate databases.

For backend A/B testing, you can override the Iceberg REST catalog JDBC URI before recreating dependent services:

```bash
export ICEBERG_CATALOG_URI='jdbc:sqlite:/tmp/iceberg_rest_mode.db'
docker compose up -d --force-recreate iceberg-rest trino kafka-connect-sinks kafka-connect-sinks-bootstrap
```

Unset `ICEBERG_CATALOG_URI` to return to the default Postgres-backed catalog.

## Synthetic data generation

The synthetic generator is the only repository component intended to be run manually from the command line:

```bash
python generator/app.py --config params.yaml --mode both
```

That seeds:

- mutable business tables in Postgres for CDC capture
- `events.session_event` behavioral events in Kafka

The staged wrapper in [Demo Workflow](./docs/runbooks/demo_workflow.md) runs this generation step at the right point in the flow and then validates downstream stages as data lands in Bronze, Silver, Gold, Redis, and the ML serving path.

## Architecture-aligned workflow

1. Prefer the staged wrapper flow in [Demo Workflow](./docs/runbooks/demo_workflow.md) over bringing up the whole stack at once.
2. The wrapper starts ingestion, stream-processing, batch, analytics, and ML in order.
3. It runs validation after each stage so you can see when data has actually moved into Bronze, Silver, Gold, Redis, and the model registry.
4. The ML stage ends with inference checks against the API, including `curl` examples for customer, campaign, and advertiser scoring.

## Example ML Decisions

The platform demonstrates three operational ML decision patterns:

- **Customer purchase propensity** – predicts likelihood of a near-term purchase
- **Campaign success propensity** – predicts whether a campaign will perform well
- **Advertiser budget expansion** – predicts likelihood of increased advertising spend

These predictions are exposed through the containerized inference service.

The practical way to exercise them is documented in [Demo Workflow](./docs/runbooks/demo_workflow.md), including the `curl` commands for:

- `POST /score/customer_purchase`
- `POST /score/campaign_success`
- `POST /score/advertiser_budget_expansion`

Training remains lightweight and local-demo-friendly:

- training features are assembled from Silver-derived feature tables
- local training artifacts are written under [ml/artifacts/](./ml/artifacts)
- canonical artifact copies are published to MinIO
- model metadata is recorded in `iceberg.silver.ml_model_registry`
- runtime inference resolves the latest manifest from the registry and downloads the model from MinIO in memory at request time

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

## Author

Brandon Benge  
LinkedIn: https://www.linkedin.com/in/brandon-benge-3b57a547/

If you're interested in discussing data platforms, streaming infrastructure, or AI/ML architecture, feel free to connect.
