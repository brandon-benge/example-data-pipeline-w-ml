# Problem Definition

> Note
> This file still reflects the pre-split combined data-and-ML platform. The active boundary change is documented in [../docs/ml_platform_split.md](../docs/ml_platform_split.md).

## 1. One-Sentence Mission

We are building a laptop-scale but production-shaped data and ML platform so that engineers, analysts, and data practitioners can generate, ingest, govern, transform, analyze, and score synthetic retail advertising and commerce data under constraints of modest local resources, explicit governance, replayable pipelines, and minimal infrastructure footprint.

## 2. Users / Actors

Primary:
- Data engineers operating CDC, streaming, lakehouse, dbt, and Spark pipelines.
- Analytics engineers and analysts querying curated Gold datasets through Trino and BI tooling.
- ML practitioners training models from governed Silver feature data and serving approved low-latency features through Redis-backed inference paths.

Secondary:
- Platform engineers running the stack locally or on a developer-scale DigitalOcean Kubernetes cluster.
- Governance stewards reviewing ownership, lineage, classification, masking, tokenization, and certification metadata.

## 3. Scope Boundaries

### In Scope
- Synthetic source generation into Postgres and Kafka.
- Debezium CDC capture from Postgres into Kafka.
- Schema-managed direct event ingestion.
- Bronze, Silver, and Gold Iceberg layers on MinIO with Iceberg REST catalog.
- Streaming transformation with Flink and batch transformation with Spark.
- dbt transformation of Silver into Gold dimensions, facts, marts, and feature tables.
- Redis-backed online feature serving for approved low-latency features.
- Local ML training, feature reuse, model artifact publishing, and model registry metadata.
- File-backed metadata for ownership, lineage, data quality, discoverability, certification, masking, tokenization, and access intent.
- BI and ad hoc SQL consumption through a SQL query engine and at least one open-source dashboard application.

### Out of Scope
- Full enterprise IAM and secret-management platforms.
- Internet-scale ingestion, multi-region recovery, or high-scale dashboard hosting.
- Large orchestration systems beyond lightweight local scheduling and runnable services.
- Arbitrary direct Gold reads from raw topics or Bronze.

### Deferred / Assumed
- The local implementation is intentionally single-node or light-cluster, but the logical architecture should scale beyond laptop mode.
- Dataset sizes are intentionally constrained to fit modest laptop memory.
- Broad-access Silver and Gold access paths are assumed to be governed views or variants rather than unrestricted raw table exposure.

## 4. Inputs / Outputs

Inputs:
- Synthetic OLTP entities and transactions written into Postgres.
- Synthetic behavioral events written directly to Kafka.
- Debezium CDC records, Kafka topic metadata, schema registry subjects, and governance configuration files.
- Bronze data consumed by Spark and direct event streams consumed by Flink.

Outputs:
- Bronze append-preserving Iceberg tables.
- Silver current-state dimensions, clean fact tables, and replayable feature snapshots.
- Gold dimensions, facts, marts, BI-ready datasets, and ML feature tables.
- Redis online feature state for approved low-latency use cases.
- Queryable metadata for ownership, lineage, certification, classification, and discoverability.
- Model artifacts in MinIO and registry entries in `iceberg.silver.ml_model_registry`.

## 5. Success Definition

### Business Outcome
- The repository demonstrates an end-to-end, realistic data-platform architecture that combines CDC, streaming, governance, BI, and ML in one coherent implementation.

### User / Platform Outcome
- Engineers can rebuild Bronze from Kafka, rebuild Silver from Bronze, and rebuild Gold from Silver without ambiguity.
- Analysts can query certified Gold datasets through a SQL engine and dashboarding layer.
- ML users can train from point-in-time-correct Silver feature data and validate online Redis features against offline recomputation.

### Safety Outcome
- PII is not exposed unmasked in broad-access Silver or Gold paths.
- Governance metadata is explicit for ownership, classification, access intent, lineage, and certification.
- Data quality outcomes are auditable, and invalid or unreconcilable records are surfaced rather than silently accepted.

## 6. Constraints

- Latency ceiling: online feature updates and inference-facing reads must support low-latency access patterns, while batch analytics may tolerate slower runtimes.
- Scale floor / peak load: low-to-moderate event rates only; no cluster tuning should be required for default workloads.
- Cost cap: single-node or lightweight deployment only, with no unnecessary infrastructure.
- Compliance / regulatory limits: governance controls must support masking, tokenization, access policies, and auditable metadata.
- Data sensitivity / residency: restricted identifiers must support deterministic tokenization for safe joins and must not appear alongside raw values in broad-access paths.
- Time / staffing / dependency limits: the repository should remain operable by a small engineering team on a developer machine or small Kubernetes cluster.

## 7. Tradeoff Authority Line

When production realism conflicts with local simplicity, choose correctness, replayability, and governance fidelity over scale or polish because this repository is meant to be a trustworthy build contract for a small but realistic platform.

Escalation path:
- Human maintainers revise the spec intentionally when the implementation requires a different guarantee or documented tradeoff.

## 8. Explicit Assumptions

1. Kafka retention is sufficient to rebuild Bronze from source streams when needed.
2. Bronze retention is sufficient to rebuild retained Silver and Gold datasets.
3. Shared feature definitions can be versioned once and reused across streaming and batch paths.
4. Control metadata may be file-backed locally as long as it remains queryable and explicit.
5. The synthetic domain is intentionally narrow: advertisers, campaigns, products, customers, orders, sessions, sales activity, and derived performance analytics.
