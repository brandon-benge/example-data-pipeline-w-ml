# Architecture

> Note
> This architecture file still describes the pre-split combined platform. The current repo is being narrowed to the data platform boundary described in [../docs/ml_platform_split.md](../docs/ml_platform_split.md).

## Current Runtime Delivery Model

- All runtime images are upstream images. The repo does not maintain or publish repo-built Kafka, Flink, or similar core runtime images to a paid registry.
- Flink runs on the upstream `flink:1.20.3-scala_2.12-java17` image.
- Kafka runs on the upstream `apache/kafka:4.2.0` image.
- The platform relies on runtime init/bootstrap steps and repo-bundle or repo-sync style delivery rather than baking repo behavior into custom service images.
- Kafka Connect sink runtime depends on a shared plugin-cache PVC populated with repo-preloaded runtime JARs rather than plugin installation on every startup.
- Repo bundle distribution is a first-class deployment artifact and should behave like a content-addressed artifact even when operationally published behind a stable tag.

## 1. Overview

The system is a laptop-scale but production-shaped data platform for a synthetic retail advertising and commerce domain. It generates source data into Postgres and Kafka, captures CDC with Debezium, lands replayable Bronze data in Iceberg on MinIO, builds governed Silver data with Spark, curates Gold analytics and feature tables with dbt, serves BI through Trino and Apache Superset, and publishes governed offline feature datasets for downstream ML consumers. The design preserves replayability, explicit governance, deterministic derived layers, and a lightweight operational footprint.

```mermaid
flowchart TB
    subgraph DP[Data Plane]
        subgraph SRC[Source Generation]
            PG[(Postgres OLTP)]
            PY[Python Synthetic Data Generator]
            EV[Kafka Event Module]
        end

        subgraph ING[Ingestion]
            DBZ[Debezium CDC]
            KFK[(Kafka)]
            SR[Schema Registry]
        end

        subgraph PROC[Processing]
            KC[Kafka Connect Iceberg Sink]
            FL[Flink Streaming Jobs]
            SP[Spark Batch Jobs]
        end

        subgraph LAKE[Lakehouse Storage]
            BR[Bronze Iceberg Tables]
            SV[Silver Iceberg Tables]
            GD[Gold Iceberg Tables]
            MINIO[(MinIO / S3 API)]
            REST[Iceberg REST Catalog]
        end

        subgraph CONS[Consumption]
            DBT[dbt on Spark SQL]
            BI[BI / Ad Hoc SQL]
            OF[Published Offline Feature Datasets]
            EXTML[External ML Platform]
        end
    end

    subgraph CP[Control Plane]
        META[File-backed metadata catalog]
        ACL[Access Policies / Masked & Tokenized Views]
        OWN[Ownership / Lineage]
        DQ[Data Quality Rules]
        FSPEC[Shared Feature Definitions]
    end

    PY --> PG
    PY --> EV
    PG --> DBZ --> KFK
    EV -. schema lookup / register .-> SR
    EV --> KFK
    KFK --> KC
    KFK --> FL
    KC --> BR
    FL -. event schema lookup .-> SR
    FL --> BR
    BR --> SP
    SP --> SV
    SV --> DBT --> GD
    SV --> OF
    FSPEC --> FL
    FSPEC --> SP
    GD --> BI
    OF --> EXTML
    REST --- BR
    REST --- SV
    REST --- GD
    MINIO --- BR
    MINIO --- SV
    MINIO --- GD
    SP --> DQ
    SP --> ACL
    SP --> META
    META --> OWN
    META --> BI
```

### Primary Design Drivers
- Replayable Bronze, Silver, Gold, and online feature layers.
- Deterministic transformation behavior.
- Explicit governance for masking, tokenization, ownership, lineage, classification, and certification.
- Shared versioned feature definitions across offline and online paths.
- Low infrastructure footprint suitable for local or developer-scale Kubernetes execution.
- Clean boundary between the data platform and downstream ML-platform responsibilities.

### Architecture Style
- Hybrid batch and streaming lakehouse architecture with a separate metadata and governance control plane.

## 2. Component Design

### Synthetic source generation
- Two output paths: Postgres writer for mutable business tables and Kafka producer for behavioral `session_event`.
- The generator is the only component intended to run manually from the command line in local development.
- It generates baseline dimensions, mutable entity updates, sessions, orders, sales activities, and direct event streams with controlled duplicates and lateness.

### Postgres
- Acts as the OLTP simulation layer for CDC-managed source-of-truth business tables.

### Debezium and Kafka Connect
- Debezium reads Postgres WAL changes and emits Kafka CDC topics.
- CDC is used for business and transactional state rather than the high-volume clickstream.
- Kafka Connect writes Bronze CDC Iceberg tables through one Apache Iceberg sink connector per source table.
- This removes Flink CDC handling from the operational CDC path while keeping Kafka as the transport backbone.
- Kafka Connect sink runtime depends on a shared plugin-cache PVC populated ahead of startup with the required Iceberg connector JAR set.
- The design intentionally avoids rebuilding or reinstalling sink plugin artifacts during every sink pod startup.

### Kafka
- Holds both CDC topics and direct event topics.
- Local deployment uses a single-broker Kafka cluster in KRaft mode.
- Runtime uses the upstream `apache/kafka:4.2.0` image with repo-owned configuration and bootstrap behavior layered in at deploy time rather than through a custom Kafka image.

### Flink
- Implemented in PyFlink.
- Consumes direct event streams, lands raw direct-event history to Bronze, resolves direct-event schemas through Schema Registry, and routes schema failures to DLQ.
- Flink responsibilities also include lightweight streaming normalization where helpful.
- Runtime uses the upstream `flink:1.20.3-scala_2.12-java17` image with init-time dependency/bootstrap setup rather than a repo-built Flink image.

### Spark
- Runs via `spark-submit` in the Spark container in `local[*]` mode.
- Performs Bronze validation, dedupe, current-state resolution, delete handling, masking, quarantine handling, aggregate construction, backfills, and offline feature creation.
- Spark batch responsibilities also include canonical offline feature dataset creation for downstream consumers.

### Iceberg, MinIO, and REST catalog
- MinIO stores Iceberg data files.
- Iceberg provides schema evolution, partitioning, and ACID table management.
- The Iceberg REST catalog is used for namespace and table access.
- Local mode keeps the catalog single-node and lightweight.
- The current local implementation uses the existing Postgres service as the JDBC metadata backend for the REST catalog.
- That shared Postgres usage is a demo shortcut; more realistic deployments would isolate OLTP, Iceberg catalog, Superset metadata, and related control-plane state.
- Bronze CDC namespaces and table DDL are bootstrapped by a dedicated Kubernetes job when the REST catalog comes online rather than owned by Spark or Flink.

### Schema Registry
- Used only for `events.session_event`.
- Direct event producers and consumers use it for contract and evolution management.
- Local endpoints are `http://schema-registry:8081` from containers and `http://localhost:8081` from the host.
- CDC topics intentionally remain on Debezium’s native envelope without Schema Registry registration.
- Debezium can integrate with Schema Registry, but that integration is intentionally not used for CDC topics in this design.
- In local mode Schema Registry remains useful for schema evolution demos, serializer compatibility checks, consumer contract validation, and topic contract visibility.
- The direct-event subject strategy is topic-based subjects for key and value with `BACKWARD` compatibility.
- Producers must register or validate the subject before publishing.
- Consumers must reject payloads that do not match the required registered schema contract.
- If a direct event cannot be deserialized with the required subject or compatible schema version, it must go to DLQ with raw payload, Kafka metadata, schema subject/version if present, and failure reason.

### dbt
- Executes Spark SQL transformations against Silver Iceberg tables.
- Produces conformed dimensions, reusable facts, and business-facing marts.
- Responsibilities include stable metric definitions and join paths, curated column naming for dashboard authors, dimensional consistency across marts, tests, and lineage for trusted BI consumption.

### BI
- Apache Superset is the standard BI path in local deployment.
- Superset connects only to curated Gold schemas by default.
- Certified Gold marts are promoted in the catalog and mirrored as approved Superset datasets.
- Ad hoc SQL access can share the same Trino endpoint while dashboards remain the primary BI surface.
- Required prebuilt BI assets include datasets, SQL queries, charts, and dashboards for campaign performance, advertiser engagement, customer browse-to-purchase funnel analysis, and category/product/channel contribution.

### Published offline features and ML boundary
- This repo publishes governed offline feature datasets for downstream ML consumers.
- Feature tables are built from Bronze, Silver, and Gold-adjacent governed data products and are intended to be consumed by a sibling ML platform.
- Training workloads, inference APIs, model registry ownership, rollout logic, and online serving are outside the steady-state ownership boundary of this repo.
- Shared feature definitions may still be versioned here where they are required for data-platform transforms, but downstream ML execution is not the responsibility of this architecture.

## 3. Interaction and Data Flow

### Primary flow
1. Synthetic generators write operational entities into Postgres and behavioral events into Kafka.
2. Debezium captures source-table changes into Kafka; direct events are validated against Schema Registry subjects.
3. Kafka Connect sinks CDC topics into Bronze Iceberg tables, and Flink lands valid direct events into Bronze while routing schema failures to DLQ.
4. Spark reads Bronze, runs quality checks, applies deterministic deduplication plus governance controls, and publishes Silver.
5. dbt reads Silver only and builds Gold dimensions, facts, marts, and feature tables.
6. Trino and Superset consume curated outputs, and governed offline feature datasets are published for downstream ML-platform use.

### Sequence diagrams

#### CDC ingestion to Bronze
```mermaid
sequenceDiagram
    participant GEN as Python Generator
    participant PG as Postgres
    participant DBZ as Debezium
    participant K as Kafka
    participant KC as Kafka Connect Iceberg Sink
    participant BR as Bronze Iceberg

    GEN->>PG: Insert / update business rows
    PG-->>DBZ: WAL changes
    DBZ->>K: Publish CDC messages
    KC->>K: Consume CDC topics
    KC->>BR: Write Bronze CDC rows
```

#### Direct event stream to Bronze
```mermaid
sequenceDiagram
    participant GEN as Python Event Producer
    participant SR as Schema Registry
    participant K as Kafka
    participant FL as Flink
    participant BR as Bronze Iceberg
    participant DLQ as Event DLQ

    GEN->>SR: Register / validate event schema
    GEN->>K: Publish events.session_event
    FL->>SR: Fetch event schema
    FL->>K: Consume event topic
    alt Schema valid
        FL->>BR: Write raw Bronze event rows
    else Schema missing / invalid
        FL->>DLQ: Publish raw payload + schema error
    end
```

#### Bronze to Silver batch processing
```mermaid
sequenceDiagram
    participant SP as Spark Batch Job
    participant BR as Bronze Iceberg
    participant GOV as Governance Rules
    participant SV as Silver Iceberg

    SP->>BR: Read Bronze partitions
    SP->>GOV: Apply DQ, masking, ownership, lineage rules
    GOV-->>SP: Validation and policy results
    SP->>SV: Write deduped Silver tables
```

#### Silver to Gold with dbt
```mermaid
sequenceDiagram
    participant DBT as dbt
    participant SV as Silver Iceberg
    participant GD as Gold Iceberg
    participant BI as BI / SQL Consumer

    DBT->>SV: Read Silver models
    DBT->>GD: Build dimensions, facts, marts
    BI->>GD: Query curated tables
```

#### Silver to ML training
```mermaid
sequenceDiagram
    participant FE as Feature Builder
    participant SV as Silver Iceberg
    participant TR as Training Job
    participant AR as Local Artifacts

    FE->>SV: Read Silver feature inputs
    FE->>TR: Build point-in-time training dataset
    TR->>AR: Write model, metrics, metadata
```

#### Kafka stream to Redis online store
```mermaid
sequenceDiagram
    participant SPEC as Shared Feature Definitions
    participant K as Kafka
    participant FL as Flink
    participant RD as Redis
    participant INF as Online Inference / App

    SPEC->>FL: Load approved online feature definitions
    FL->>K: Consume event and CDC streams
    FL->>RD: Upsert entity-keyed feature records
    INF->>RD: Read low-latency features at request time
```

## 4. Trust Boundaries

### External boundary
- External actors: engineers, analysts, ML practitioners, and platform operators using local tools, Kubernetes, SQL engines, dashboards, and inference workflows.
- Entry points: source generators, Kafka topics, Trino queries, Superset dashboards, and ML or inference services.
- Authentication mechanism: repository- and environment-specific local credentials rather than full enterprise IAM.

### Internal boundary
- Explicit component boundaries exist across ingestion, processing, storage, metadata, and consumption layers.
- Authorization for sensitive data paths relies on governed dataset access intent supporting RBAC and ABAC-aligned restrictions.

### Sensitive data boundary
- Sensitive data includes PII and restricted joinable identifiers from customer, advertiser, and related domains.
- Sensitive handling rules are enforced primarily through masking, tokenization, and access-path separation.
- Ownership, classification, lineage, quality results, certification, and access intent remain queryable and reviewable.

## 5. Control Plane and Data Plane

### Control plane
- Owns governance policy definition, ownership registration, lineage metadata, classification, certification, masking, tokenization, discoverability metadata, and shared feature definitions.
- State lives in the file-backed metadata catalog, policy files, and versioned definitions.

### Data plane
- Owns source generation, CDC capture, streaming ingestion, Bronze storage, Spark Silver transformations, dbt Gold transformations, Redis online features, BI queries, and ML training and inference.
- State includes source streams, Iceberg data layers, Redis serving state, BI assets, and model artifacts.

### Separation rule
- Data-plane execution must not bypass control-plane governance definitions.
- Sensitive data publication, masking, tokenization, ownership, classification, and certification status are defined through governed metadata and transformation logic rather than ad hoc downstream use.

## 6. Local Deployment and Execution Model

### Local deployment design
- All infrastructure and bootstrap behavior should happen through Kubernetes manifests applied with `kubectl`.
- Topic creation, connector registration, catalog initialization, Schema Registry subject registration, and Superset bootstrap should be handled by container entrypoints, helper `Job` resources, or Kubernetes-managed bootstrap flows.
- The synthetic generator is the only repository component expected to be run manually from the command line.
- Local Kubernetes deployment must use workload-based namespaces rather than a single shared namespace.

### Namespace model
- Namespace grouping is based on workload responsibility so operators can isolate infrastructure, ingestion, processing, serving, and governance concerns.
- The canonical local namespace set is:
  `data-platform-infra` for shared stateful platform infrastructure,
  `data-platform-ingest` for CDC and ingestion workloads,
  `data-platform-process` for batch and streaming transformation workloads,
  `data-platform-serve` for query, BI, inference, and low-latency serving workloads,
  and `data-platform-govern` for metadata and governance-facing services.
- New workloads must be assigned to the namespace matching their primary operational responsibility. A workload that spans concerns should be placed by its primary write authority and cross-namespace access should be explicit.
- Bootstrap jobs should run in the namespace of the workload they initialize unless they are creating shared infrastructure primitives, in which case they belong in `data-platform-infra`.

### Kubernetes workloads
- `data-platform-infra`: `postgres`, `kafka`, `schema-registry`, `minio`, `iceberg-rest`
- `data-platform-ingest`: `generator`, `kafka-connect-source`, `kafka-connect-sinks`
- `data-platform-process`: `flink-jobmanager`, `flink-taskmanager`, `spark`, `dbt`, ML training and layer-bootstrap jobs
- `data-platform-serve`: `redis`, `trino`, `superset`, `ml-inference`
- `data-platform-govern`: `metadata`

### Networking and footprint
- Namespaces are separate, but service DNS and policies must preserve the same logical connectivity between producers, storage, processing, and serving layers.
- Exposed service endpoints include `kafka.data-platform-infra:9092`, `schema-registry.data-platform-infra:8081`, `minio.data-platform-infra:9000`, `trino.data-platform-serve:8080`, and `superset.data-platform-serve:8088`
- Desktop guidance is single Kafka broker, single Flink JM/TM, single Spark container in `local[*]`, single Redis, single Trino coordinator, single Superset container, single-node MinIO, tiny topic partitions, and constrained default row counts

### Execution model
- `kubectl apply -f k8s/platform.yaml` is the canonical startup path.
- Kubernetes-managed bootstrap jobs initialize Kafka topics, Kafka Connect connectors, Schema Registry subjects, catalog settings, and Superset across the workload-aligned namespaces.
- The synthetic data generator is the only component intended to be run manually from the command line.
- Flink continuously lands Bronze data and maintains approved online features.
- Spark batch jobs produce deterministic Silver datasets and offline ML features.
- Spark jobs may accept date or window parameters for rebuild and backfill operation.
- dbt builds curated Gold models for BI consumption.
- Metadata, governance, and quality artifacts are repository-managed and version-controlled.

### Implementation notes
- Some existing repository assets still reflect older local-bootstrap helpers, including `config/iceberg/bootstrap-cdc-rest-catalog.sh` and `config/debezium/register-connector.sh`, but the canonical runtime model is Kubernetes-managed bootstrap jobs.
- Superset bootstrap expectations include `superset import-dashboards --path /app/bi/dashboards --username admin`.
- A local Trino connection URI such as `trino://trino:8080/iceberg` is acceptable for Superset bootstrap.
- Bootstrap references are expected to use `kubectl apply -f k8s/platform.yaml` rather than any Docker Compose-era startup commands.
- Existing bootstrap documentation may refer to Superset bootstrap running from `config/superset/bootstrap/` and local topic definitions under `config/kafka/topics/`.
- Existing manifests and runbooks may still show `data-platform` as a flat namespace; that is an implementation lag and not the target deployment contract.
- Repository structure is expected to center around `config/`, `generator/`, `flink/`, `spark/`, `dbt/`, `bi/`, `ml/`, `tools/`, `metadata/`, `docs/`, and `tests/`.

## 7. Tradeoffs and Notes

### Decision: `hybrid streaming and batch`
- Chosen approach: Flink handles direct-event streaming ingestion; Spark and dbt handle deterministic Silver and Gold batch layers plus offline feature publication.
- Alternative considered: one engine for all transformations.
- Why chosen: matches realistic platform patterns while keeping the local implementation understandable.
- Cost of this choice: more moving parts and cross-engine coordination.

### Decision: `Iceberg + MinIO + REST catalog`
- Chosen approach: lightweight lakehouse stack with Iceberg tables on MinIO and Iceberg REST catalog.
- Alternative considered: heavier local metastore stacks or managed services.
- Why chosen: preserves realistic table semantics with low local operational weight.
- Cost of this choice: fewer enterprise conveniences and more repo-driven configuration.

### Decision: `file-backed governance metadata`
- Chosen approach: repository-managed metadata and governance definitions.
- Alternative considered: dedicated enterprise catalog and policy systems.
- Why chosen: keeps the platform lightweight while making governance explicit and reviewable in code.
- Cost of this choice: stronger process discipline is required to keep metadata aligned with datasets.

### Final note
- High-level rationale may live in `docs/architecture_rationale.md`, but implementation-defining architecture detail belongs under `SpecRepo`.
