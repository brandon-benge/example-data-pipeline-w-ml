# System Requirements

> Note
> This file still includes training, inference, and online-feature requirements from the combined platform. The current repo boundary change is tracked in [../docs/ml_platform_split.md](../docs/ml_platform_split.md).

## 1. Functional Requirements

### FR-1: Generate source data
- Actor: synthetic data generator.
- Trigger: local run or scheduled pipeline execution.
- The system must: generate baseline dimensions plus mutable campaign, product, customer, session, order, and sales-activity records into Postgres, and generate behavioral `session_event` records into Kafka.
- Output / side effect: source tables and direct-event topics populated with explicit schemas, including controlled duplicates and lateness for testing.
- Failure behavior: generation failures stop new downstream work and surface actionable errors.
- Related invariants: 5, 8, 9, 11.

### FR-2: Capture CDC and publish source streams
- Actor: Debezium and Kafka platform components.
- Trigger: changes committed to Postgres source tables.
- The system must: capture row-level CDC into Kafka topics using Debezium envelopes and stable keys, with one Kafka Connect Iceberg sink connector per source table.
- Output / side effect: CDC topics available for Bronze ingestion.
- Failure behavior: connector failures are surfaced; CDC semantics are not silently changed or dropped.
- Related invariants: 6, 7, 8.

### FR-3: Land Bronze data
- Actor: Kafka Connect Iceberg sink and Flink streaming jobs.
- Trigger: new CDC records or direct events on Kafka topics.
- The system must: write raw append-preserving Bronze history with event-time and ingest-time metadata, preserving CDC envelope meaning and direct-event payload metadata.
- Output / side effect: Bronze Iceberg tables on MinIO through Iceberg REST catalog.
- Failure behavior: unresolved event schemas are routed to DLQ rather than written to Bronze.
- Related invariants: 1, 5, 9, 12.

### FR-4: Build low-latency online features
- Actor: Flink streaming jobs.
- Trigger: approved streaming inputs arrive.
- The system must: compute approved online features from shared versioned feature definitions and publish the serving subset to Redis keyed by entity identifier.
- Output / side effect: Redis feature state suitable for request-time inference and application reads.
- Failure behavior: online state remains rebuildable from retained Silver snapshots or replayable streams and is validated against offline recomputation.
- Related invariants: 29, 30, 31, 32.

### FR-5: Validate and transform Bronze to Silver
- Actor: Spark batch pipelines.
- Trigger: scheduled run, manual run, or backfill request.
- The system must: validate schema, null, uniqueness, volume, drift, enum, critical foreign-key, and reconciliation rules; enforce checks such as `line_amount = quantity * unit_price` within tolerance, `session_start_ts <= session_end_ts` when an end exists, and `start_date <= end_date`; deduplicate deterministically; handle deletes and quarantines; and publish current-state, clean fact, and aggregate-friendly Silver tables.
- Output / side effect: governed Silver datasets plus auditable check results and exception outputs.
- Failure behavior: invalid records are quarantined or rejected per rule; invariant violations block publication of affected outputs.
- Related invariants: 2, 6, 10, 19, 20, 21, 22, 23, 24, 25, 26.

### FR-6: Apply governance controls
- Actor: Spark jobs and metadata/governance layer.
- Trigger: Silver publication and dataset registration.
- The system must: apply masking, deterministic tokenization where required, RBAC and ABAC-aligned views or variants, ownership registration, lineage capture, classification tagging, certification metadata, discoverability metadata, and access expectations.
- Output / side effect: governed datasets and queryable metadata.
- Failure behavior: broad-access paths must not publish restricted data without required controls.
- Related invariants: 13, 14, 15, 16, 17, 18.

### FR-7: Build Gold analytics and feature tables
- Actor: dbt on Spark SQL.
- Trigger: Silver refresh completion or manual dbt invocation.
- The system must: build conformed dimensions, reusable facts, semantic marts, and reusable ML feature tables from Silver only.
- Output / side effect: business-friendly Gold datasets with stable metrics, join paths, curated names, tests, and lineage.
- Failure behavior: failed models do not partially redefine Gold correctness guarantees.
- Related invariants: 3, 4, 20, 26, 28.

### FR-8: Serve analytics consumption
- Actor: Trino and BI application.
- Trigger: analyst query or dashboard load.
- The system must: expose Gold datasets for dashboarding and ad hoc SQL, with Apache Superset as the standard BI path in local deployment.
- Output / side effect: curated query results plus prebuilt datasets, SQL queries, charts, and dashboards for campaign performance, advertiser engagement, customer funnel, and category/product/channel contribution analysis.
- Failure behavior: uncataloged or uncertified datasets must not appear as approved published assets.
- Related invariants: 14, 16, 18.

### FR-9: Support ML training, inference, and registry updates
- Actor: ML training and inference services.
- Trigger: batch training, retraining, or request-time scoring invocation.
- The system must: train models from Silver-backed feature tables, cache artifacts locally, publish canonical copies to MinIO, register metadata in `iceberg.silver.ml_model_registry`, and serve separate inference endpoints for customer purchase, campaign success, and advertiser budget expansion use cases.
- Output / side effect: trained models, evaluation artifacts, registry metadata, and request-time scoring outputs.
- Failure behavior: training or inference paths that violate point-in-time or governance rules are rejected.
- Related invariants: 27, 28, 29, 30, 31, 32.
- Example ML uses include promotion and recommendation prioritization for customers, intervention and ranking decisions for campaigns, and outreach or upsell prioritization for advertisers.

### FR-10: Support rebuilds and backfills
- Actor: data engineer or scheduled pipeline.
- Trigger: backfill request, replay request, or recovery workflow.
- The system must: rebuild Bronze from Kafka, Silver from Bronze, Gold from Silver, and online feature state from retained snapshots or replayed streams by date or window.
- Output / side effect: reproducible downstream state after replay.
- Failure behavior: rebuilds must not rely on ad hoc manual mutation of downstream state.
- Related invariants: 1, 2, 3, 4, 32.

### FR-11: Support governed local deployment
- Actor: platform operator.
- Trigger: local or developer-cluster startup.
- The system must: start through Kubernetes-managed bootstrap using `kubectl`, initialize topics, connectors, catalog settings, Schema Registry subjects, and Superset, place workloads into Kubernetes namespaces aligned to workload responsibility rather than one shared namespace, and keep the synthetic generator as the only component intended to run manually from the command line.
- Output / side effect: fully bootstrapped local platform with minimal manual steps.
- Failure behavior: platform bootstrap failures are surfaced as operational errors rather than hidden post-start drift.
- Related invariants: 14, 15, 16, 18.

## 2. Non-Functional Requirements

### Performance
- Platform runtime target: runs on a developer-scale DigitalOcean Kubernetes cluster through `kubectl`.
- Memory target: default datasets fit within modest laptop memory.
- Streaming target: low-to-moderate event rates are supported without cluster tuning.
- Query target: Gold datasets are practical for local BI and ad hoc SQL use.

### Reliability
- Bronze replayability: Bronze is rebuildable from retained Kafka source streams.
- Silver rebuildability: Silver is rebuildable from Bronze.
- Gold rebuildability: Gold is rebuildable from Silver.
- Online feature rebuildability: Redis state is rebuildable from retained Silver snapshots or replayable events.

### Scale
- Deployment shape: single-node or otherwise light deployment model by default.
- Logical scalability: partitioning and table design must remain aligned with realistic production patterns.
- Storage design: Iceberg layout must support partitioned tables on MinIO.

### Cost
- Infrastructure guardrail: avoid unnecessary components and prefer the lightest stack that satisfies the architecture.
- Catalog guardrail: prefer Iceberg REST catalog plus MinIO rather than heavier local metastore stacks.

### Security / Compliance
- Broad-access Silver and Gold views must not expose unmasked PII.
- Access policies must support RBAC and ABAC-aligned restrictions for sensitive datasets.
- Sensitive joinable identifiers must use deterministic tokenization when masking alone is insufficient.

### Operability
- Data quality controls must emit auditable results and exception outputs.
- Catalog metadata must be queryable for ownership, classification, lineage, certification, and discoverability.
- Services must be startable independently.
- Kubernetes workloads must use workload-aligned namespaces so operators can reason about infrastructure, processing, serving, and governance boundaries without a flat shared namespace.

### Maintainability
- Schemas are defined explicitly.
- Transformation contracts are documented.
- Shared feature definitions are versioned once and reused across offline and online paths.

## 3. Acceptance Criteria

### Capability Acceptance
- Synthetic data can be generated into Postgres and Kafka, captured into Bronze, transformed into Silver and Gold, queried through BI, and used for ML training and online feature serving.

### Correctness Acceptance
- Silver and Gold rebuilds from retained upstream layers produce deterministic results for the same input and rule set.
- Governance controls, CDC semantics, deduplication, and point-in-time feature correctness hold under replay and backfill.

### Performance Acceptance
- Default workloads run successfully on a modest developer machine or developer-scale Kubernetes cluster without bespoke tuning.

### Safety Acceptance
- Broad-access analytic paths do not expose raw sensitive identifiers or unmasked PII.
- Published datasets are cataloged with ownership, classification, lineage, and certification metadata.

### Operational Acceptance
- Data quality failures, schema failures, and reconciliation issues produce auditable outputs rather than silent corruption.
- Operators can rebuild each layer from its retained upstream source.

## 4. Out of Scope

- Full enterprise IAM and policy enforcement infrastructure.
- Large-scale multi-region recovery and internet-scale traffic.
- Heavy orchestration or large-hosted dashboard platforms.

## 5. Open Questions

1. What watermark and late-arrival policy should be treated as canonical for direct event processing in v1?
2. Which datasets are considered certified and broadly published at initial release versus merely queryable?
3. Which exact low-latency features are approved for Redis publication in v1, and what validation thresholds govern parity against offline recomputation?
