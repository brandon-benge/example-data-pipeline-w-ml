# Test Plan

## 1. Test Objectives

- What must be proven: the platform can generate, ingest, govern, transform, publish, query, and use data for ML while preserving replayability and deterministic correctness.
- Highest-risk behaviors: CDC semantics, direct-event schema handling, Silver data quality enforcement, Gold derivation from Silver only, and online-feature parity versus offline recomputation.
- Invariants that require direct validation: Bronze append preservation, Silver determinism, Gold-from-Silver-only derivation, masking and tokenization rules, lineage and ownership completeness, and point-in-time feature correctness.
- Release-blocking failures: silent data loss, unmasked PII in broad-access paths, broken rebuild guarantees, invalid Silver or Gold publication after failed checks, and approved feature drift beyond policy.

## 2. Test Layers

### Unit Tests
- Scope: transformation helpers, deduplication logic, feature logic, schema handling, and masking or tokenization helpers.
- Target components: Spark or ML utility code, Flink helper logic, and metadata-validation utilities.
- Main purpose: prove deterministic low-level behavior and rule implementation.

### Integration Tests
- Scope: Debezium to Kafka to Bronze, direct-event ingestion with Schema Registry and DLQ handling, Spark Silver publication, dbt Gold builds, Redis feature updates, and model registry writes.
- Target boundaries: connectors, storage systems, schema contracts, governance metadata inputs, and online-store interactions.
- Main purpose: prove component interfaces and layer-to-layer contracts.

### End-to-End Tests
- Scope: source generation through Bronze, Silver, Gold, BI-readiness, model training, and online-feature validation.
- Critical workflows: CDC ingestion, direct-event ingestion, deterministic backfill, curated dataset publication, and offline-to-online feature parity validation.
- Main purpose: prove the full platform satisfies the spec as an integrated system.

### Failure / Resilience Tests
- Scope: connector failures, schema failures, DLQ routing, quality-check failures, Gold build failures, Redis loss, and rebuild workflows.
- Fault injection targets: Debezium, Kafka Connect sinks, Schema Registry interactions, Spark quality rules, dbt runs, and Redis.
- Main purpose: prove bounded degradation, safe failure behavior, and recoverability from retained upstream sources.

## 3. Invariant-to-Test Mapping

### Invariant: `Bronze is append-preserving`
- Validation strategy: ingest source CDC and direct events, then verify Bronze retains raw history plus event-time and ingest-time metadata without destructive mutation.
- Test layer: integration and end-to-end.
- Pass condition: replayable Bronze history exists and matches retained source-stream semantics.

### Invariant: `Silver is deterministic`
- Validation strategy: rerun the same Bronze input and rule set through Spark, including backfill paths.
- Test layer: integration and end-to-end.
- Pass condition: Silver outputs are reproducible for the same retained Bronze input.

### Invariant: `Gold derives only from Silver`
- Validation strategy: inspect model dependencies and run builds that only use Silver inputs.
- Test layer: integration and end-to-end.
- Pass condition: no Gold model reads raw topics or Bronze directly.

### Invariant: `Invalid direct events go to DLQ`
- Validation strategy: submit events with unresolved schema subject or version.
- Test layer: integration.
- Pass condition: events are not written to Bronze and are published to DLQ with failure reason and raw payload.

### Invariant: `Governance controls are enforced`
- Validation strategy: publish restricted datasets and verify masking, tokenization, ownership, classification, certification, and lineage metadata.
- Test layer: integration and end-to-end.
- Pass condition: broad-access paths never expose raw restricted values and published datasets are metadata-complete.

### Invariant: `Online features match approved offline logic`
- Validation strategy: compare Redis feature values to offline Silver feature recomputation for approved feature definitions.
- Test layer: integration and end-to-end.
- Pass condition: parity remains within approved thresholds and drift is surfaced when exceeded.

### Invariant: `Training is point-in-time correct`
- Validation strategy: build training datasets and verify no future information appears in the feature set relative to the label timestamp.
- Test layer: unit, integration, and end-to-end.
- Pass condition: leakage checks pass for all approved training features.

## 4. Test Data Strategy

- Synthetic vs production-like data: use synthetic domain data that is intentionally production-shaped for advertisers, campaigns, customers, products, sessions, and orders.
- Sensitive data handling: no real PII is required; synthetic data still exercises masking and tokenization policies.
- Deterministic fixtures: retain seeded inputs and scenario fixtures for CDC, direct events, and ML parity checks where repeatability is required.
- Data reset strategy: reset local state and replay from known retained inputs between suites where necessary.

## 5. Environment Strategy

- Local expectations: a developer can run representative unit, integration, and targeted end-to-end flows on a modest machine.
- CI expectations: automate release-blocking logic, governance, and rebuild tests as much as practical.
- Staging expectations: for developer-scale Kubernetes, validate full-stack workflows including ingestion, transformation, BI-readiness, and ML parity.
- Production verification expectations: not applicable as a hard requirement for this repository, but any non-local deployment should still prove rebuildability and governance enforcement before use.

## 6. Exit Criteria

- Required pass rate: 100% for release-blocking suites tied to invariants and governed publication.
- Required coverage areas: CDC correctness, schema handling, DLQ routing, Silver quality checks, Gold derivation, governance metadata completeness, rebuild paths, and feature parity.
- Performance gates: default platform workloads complete successfully on modest hardware or a developer-scale Kubernetes cluster without bespoke tuning.
- Security / correctness gates: no unmasked PII in broad-access outputs, no accepted publication after blocking quality failures, and no point-in-time leakage in training data.

## 7. Known Gaps

1. Exact numeric parity thresholds for online versus offline feature validation still need explicit definition.
2. The full replay and recovery test matrix for DLQ reprocessing and late-arriving events is not yet complete.
3. Benchmark-based scaling tests on DigitalOcean Kubernetes versus laptop mode still need to be formalized.
