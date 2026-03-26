# Observability

## 1. Observability Goals

- What operators must know: whether CDC, Bronze, Silver, Gold, Redis features, and metadata publication are fresh, healthy, and rebuildable.
- What developers must debug: schema failures, DLQ causes, Spark quality failures, dbt model issues, and offline-to-online feature mismatches.
- What business owners must measure: dataset freshness, dashboard readiness, and whether certified published datasets are available for use.
- What auditors must reconstruct: ownership, lineage, classification, certification, quality outcomes, and access-intent changes over time.

## 2. Logs

- Required structured fields: `timestamp`, `component`, `dataset_or_topic`, `run_id` or `job_id`, `status`, and failure reason where relevant.
- Sensitive fields that must be excluded or masked: raw PII, restricted joinable identifiers, secrets, and unmasked values from governed columns.
- Correlation identifiers: connector task IDs, topic names, table names, job run IDs, and model or feature version identifiers.
- Retention policy: long enough to support operational debugging and auditable reconstruction of failures and publication decisions.

### Required Log Events
- CDC connector start, stop, failure, restart, and lag warnings.
- Schema resolution failures and DLQ publications for direct events.
- Spark quality-check outcomes, exception-output publication, and Silver publish decisions.
- dbt Gold build success or failure with model identity.
- Redis online feature replay, rebuild, or parity-failure events.
- Governance metadata changes affecting ownership, classification, masking, tokenization, access intent, or certification.

## 3. Metrics

### Golden Signals
- Latency: connector lag, stream-processing lag, Spark and dbt run durations, Redis feature update latency, and query freshness delay.
- Traffic: source record volume, event throughput, Bronze write rate, Silver and Gold publication counts, and feature update counts.
- Errors: connector failures, schema mismatches, DLQ volume, quality-check failures, reconciliation failures, dbt errors, and parity failures.
- Saturation: CPU, memory, storage pressure, Kafka backlog, Redis memory usage, and object-store or catalog contention.

### Business / Domain Metrics
- Certified Gold dataset freshness.
- Dashboard dataset readiness and recent refresh success.
- Model training runs completed and feature version adoption.

### Invariant / Safety Metrics
- Null-key, uniqueness, and reconciliation failure counts.
- Masking or tokenization policy violation attempts.
- Lineage and ownership completeness for published datasets.
- Offline-to-online feature parity variance and drift.
- Catalog-visible warning quality status for non-blocking checks.

## 4. Traces

- Critical flows to trace: direct-event ingestion to Bronze, Bronze-to-Silver publication, Silver-to-Gold publication, and online-feature update or rebuild workflows.
- Required span attributes: dataset or topic identity, job or run ID, feature or model version where applicable, and outcome status.
- Sampling strategy: traces are useful for targeted debugging but are optional if the local stack relies primarily on logs and metrics.
- Cross-service propagation mechanism: use shared run or correlation IDs across components even where full distributed tracing is absent.

## 5. Alerts

### Page-Worthy Alerts
- Condition: CDC or Bronze ingestion freshness exceeds the operational threshold.
- Threshold: enough lag to threaten downstream rebuild or publication expectations.
- Runbook target: ingestion and platform operator.

### Page-Worthy Alerts
- Condition: Silver quality, reconciliation, or schema validation failures block publication of critical datasets.
- Threshold: any release-blocking dataset failure.
- Runbook target: data engineering owner.

### Ticket / Investigation Alerts
- Condition: DLQ volume rises above normal background levels.
- Threshold: sustained schema-failure or malformed-event activity.
- Owner: streaming or producer owner.

### Ticket / Investigation Alerts
- Condition: offline-to-online feature parity variance exceeds warning levels but not the hard fail threshold.
- Threshold: sustained drift requiring investigation.
- Owner: ML and data platform owners.

## 6. Dashboards and Reporting

- Operational dashboard: connector health, topic lag, Bronze freshness, Silver and Gold run status, Redis health, and quality-check summaries.
- Executive / product dashboard: published dataset readiness and dashboard availability for the synthetic retail advertising and commerce domain.
- Compliance / audit reporting: ownership, classification, masking, tokenization, certification status, lineage completeness, quality check summary, and material metadata changes.
- Required catalog metadata for published datasets includes business description, owner and steward, freshness or SLA expectation, quality check summary, and discoverability status.
- Discoverability expectations are that Bronze datasets are hidden by default except for platform operators, Silver datasets are searchable to engineering and data producer roles, and certified Gold marts plus approved feature datasets are promoted.

## 7. Gaps

1. Exact alert thresholds and freshness SLOs still need numeric definitions by dataset and service.
2. It is not yet decided whether full distributed tracing is worth the footprint in laptop mode.
3. A standard sink or storage location for long-lived quality-check and exception-output history should be made explicit.
