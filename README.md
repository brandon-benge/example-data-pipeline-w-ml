# Example Data Pipeline

Architecture overview:

![Platform Architecture](./docs/image.png)

This repository demonstrates a modern data platform built around streaming ingestion, lakehouse storage, governed batch transformation, and BI consumption.

Blog walkthrough:

Full architecture explanation and design discussion:
https://github.com/brandon-benge/example-data-pipeline-w-ml/blob/main/docs/blog.md

Detailed architecture specification:

See the full architecture definition here:
https://github.com/brandon-benge/example-data-pipeline-w-ml/blob/main/SpecRepo/ARCHITECTURE.md

## Platform Overview

This repository contains a local laptop-scale end-to-end platform for ingestion, Bronze and Silver processing, Gold analytics, and governed metadata.

Key components include:

- Postgres source tables with Debezium CDC into Kafka
- direct behavioral events on `events.session_event` with Schema Registry
- Bronze ingestion in Flink
- Silver and governance logic in Spark
- Gold dimensions, facts, marts, and semantic views in dbt
- Trino and Superset for curated BI access
- published offline feature datasets consumed by the ML platform repo [`example-model-routing`](https://github.com/brandon-benge/example-model-routing)

## Architecture Layers

- Ingestion: Postgres, Debezium, Kafka Connect, Kafka, Schema Registry
- Stream Processing: Kafka Connect Iceberg sinks, Flink
- Lakehouse: Apache Iceberg, Iceberg REST Catalog, MinIO
- Batch Processing: Spark, dbt
- Analytics: Trino, Apache Superset

## Setup

Prerequisites:

- `kubectl` configured for your DigitalOcean Kubernetes cluster
- Python 3.11+ for the synthetic generator and local tests

## Startup

The repo now runs on Kubernetes instead of `docker compose`.

Primary deployment path:

```bash
kubectl apply -f k8s/platform.yaml
```

Recommended operator flow:

- use the staged wrapper flow in [Demo Workflow](./docs/runbooks/demo_workflow.md)
- the wrapper applies the Kubernetes platform once, then validates each logical stage against the cluster

The staged workflow is usually the better local experience:

```bash
bash tools/run_stack_workflow.sh --stop-at ingestion
bash tools/run_stack_workflow.sh --stop-at stream-processing
bash tools/run_stack_workflow.sh --stop-at batch
bash tools/run_stack_workflow.sh --stop-at analytics
```

The Kubernetes manifest preserves the same demo topology:

- bootstrap tasks run as Kubernetes `Job` resources
- long-running services run as `Deployment` or `StatefulSet` resources
- the Debezium source connector and the Iceberg sink connectors still run on separate Kafka Connect workers

For local simplicity, the Iceberg REST catalog now uses the existing Postgres service as its JDBC metadata backend. That shared database is a demo shortcut. In a more realistic deployment, source application tables, Iceberg catalog metadata, Superset metadata, and any other control-plane state should live in separate databases.

## Synthetic data generation

The synthetic generator is the only repository component intended to be run manually from the command line:

```bash
python generator/app.py --config params.yaml --mode both
```

That seeds:

- mutable business tables in Postgres for CDC capture
- `events.session_event` behavioral events in Kafka

The staged wrapper in [Demo Workflow](./docs/runbooks/demo_workflow.md) runs this generation step at the right point in the flow and then validates downstream stages as data lands in Bronze, Silver, and Gold.

## Architecture-aligned workflow

1. Prefer the staged wrapper flow in [Demo Workflow](./docs/runbooks/demo_workflow.md) over bringing up the whole stack at once.
2. The wrapper starts ingestion, stream-processing, batch, and analytics in order.
3. It runs validation after each stage so you can see when data has actually moved into Bronze, Silver, and Gold.

## ML Split

ML training, model storage and registry behavior, hot feature serving, and model hosting have been moved to [`example-model-routing`](https://github.com/brandon-benge/example-model-routing).

That external repo now owns:

- model training
- model storage and registry workflows
- hot Redis-backed feature serving
- inference and model hosting
- experimentation and rollout logic

The current repo remains responsible for:

- governed source ingestion and replayable Bronze history
- deterministic Silver datasets
- Gold analytics models and published offline feature datasets
- metadata, ownership, lineage, and BI access

See [ML Platform Split](./docs/ml_platform_split.md) for the move plan and the list of assets that should leave this repo.

## Key paths

- platform config: [k8s/platform.yaml](./k8s/platform.yaml)
- source generator: [generator/](./generator)
- Flink jobs: [flink/jobs](./flink/jobs)
- Spark jobs: [spark/jobs](./spark/jobs)
- dbt Gold project: [dbt/](./dbt)
- BI assets: [bi/](./bi)
- governance and metadata: [config/governance](./config/governance), [metadata/](./metadata)
- runbooks: [docs/runbooks](./docs/runbooks)

## BI and access

Use Kubernetes port-forwarding from your workstation for the supported local interfaces. Do not assume other host ports are available.

When you use `bash tools/run_stack_workflow.sh --stop-at <stage>`, the workflow establishes these workstation port-forwards automatically after the platform is applied. Run them manually only when you are not using the staged workflow.

Primary interfaces:

```bash
kubectl -n data-platform-serve port-forward svc/trino 8080:8080
kubectl -n data-platform-process port-forward svc/flink-jobmanager 8082:8081
kubectl -n data-platform-serve port-forward svc/superset 8088:8088
```

Optional debugging endpoints:

```bash
kubectl -n data-platform-infra port-forward svc/schema-registry 8081:8081
kubectl -n data-platform-govern port-forward svc/metadata 9002:9002
```

Generator access from your workstation:

```bash
kubectl -n data-platform-infra port-forward svc/postgres 5432:5432
kubectl -n data-platform-infra port-forward svc/kafka 19092:19092
kubectl -n data-platform-infra port-forward svc/schema-registry 8081:8081
```

Run those in separate terminals only when you seed data manually from the host without `tools/run_stack_workflow.sh`.

Superset is bootstrapped through a Kubernetes job and creates a Trino connection using the cluster-local pattern `trino://trino.data-platform-serve:8080/iceberg`, defaulting BI assets to the curated `gold` schema.

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
- [Architecture](./SpecRepo/ARCHITECTURE.md)
- [Architecture Rationale](./docs/architecture_rationale.md)
- [ML Platform Split](./docs/ml_platform_split.md)

# Templates

This folder contains the reusable `SpecRepo` template set.

Use these files when you want a working starting point for a repository-based specification system.

## Template Set

- [SpecRepo README](./SpecRepo/README.md)
- [PROBLEM.md](./SpecRepo/PROBLEM.md)
- [INVARIANTS.md](./SpecRepo/INVARIANTS.md)
- [REQUIREMENTS.md](./SpecRepo/REQUIREMENTS.md)
- [DATA_MODEL.md](./SpecRepo/DATA_MODEL.md)
- [CONSISTENCY.md](./SpecRepo/CONSISTENCY.md)
- [ARCHITECTURE.md](./SpecRepo/ARCHITECTURE.md)
- [FAILURE_MODES.md](./SpecRepo/FAILURE_MODES.md)
- [SCALING.md](./SpecRepo/SCALING.md)
- [OBSERVABILITY.md](./SpecRepo/OBSERVABILITY.md)
- [SECURITY.md](./SpecRepo/SECURITY.md)
- [TEST_PLAN.md](./SpecRepo/TEST_PLAN.md)
- [API_CONTRACTS.yaml](./SpecRepo/API_CONTRACTS.yaml)
- [CHANGELOG.md](./SpecRepo/CHANGELOG.md)

## How To Use

1. Start with the required files.
2. Add optional files when the system matures.
3. Treat the templates as a baseline, not as immutable law.
4. Keep the files specific enough that humans and artificial intelligence can both act from them.

# Author

Brandon Benge  
LinkedIn: https://www.linkedin.com/in/brandon-benge-3b57a547/

If you're interested in discussing data platforms, streaming infrastructure, or AI/ML architecture, feel free to connect.
