# Failure Modes

## 1. Failure Scenarios

### Scenario: `CDC connector outage`
- Trigger: Debezium connector stops, loses connectivity, or falls behind.
- Affected components: Postgres CDC capture, Kafka CDC topics, downstream Bronze CDC ingestion.
- User-visible impact: Bronze freshness for source-table history degrades; downstream Silver and Gold refreshes become stale.
- Data risk: retained source changes may be delayed, but must remain replayable from Kafka once capture resumes.
- Detection mechanism: connector health checks, lag metrics, and missing-ingestion freshness alerts.

### Scenario: `Kafka Connect Iceberg sink failure`
- Trigger: a per-table Iceberg sink connector fails, misconfigures, or cannot write to Iceberg.
- Affected components: Bronze CDC tables for the impacted source table.
- User-visible impact: affected domains stop refreshing downstream.
- Data risk: raw CDC history is not materialized into Bronze even though Kafka may still retain it.
- Detection mechanism: connector task failures, Bronze freshness lag, and row-count reconciliation gaps.

### Scenario: `Schema resolution failure for direct events`
- Trigger: event payload cannot resolve against the required Schema Registry subject or version.
- Affected components: Flink event ingestion, Bronze direct-event tables, DLQ topics.
- User-visible impact: invalid events are absent from Bronze-derived analytics.
- Data risk: event loss is prohibited; invalid payloads must be preserved in DLQ with failure reason.
- Detection mechanism: DLQ volume, schema lookup failures, and Bronze-to-topic count discrepancies.

### Scenario: `Silver quality or reconciliation failure`
- Trigger: schema, null, uniqueness, volume, drift, or reconciliation checks fail during Spark processing.
- Affected components: Silver publication and all dependent Gold and ML outputs.
- User-visible impact: affected curated datasets remain stale or unavailable.
- Data risk: publishing corrupted derived state would violate invariants.
- Detection mechanism: failed quality jobs, exception outputs, and control-total alerts.

### Scenario: `Gold model build failure`
- Trigger: dbt model, dependency, or contract failure.
- Affected components: Gold dimensions, facts, marts, BI outputs, and feature tables derived from Silver.
- User-visible impact: dashboards and ad hoc SQL may show stale or incomplete curated data.
- Data risk: no direct bypass to Bronze or raw topics is allowed to restore Gold.
- Detection mechanism: dbt run failures, freshness checks, and downstream dashboard or query validation failures.

### Scenario: `Redis online feature drift or loss`
- Trigger: streaming update failure, Redis state loss, or parity drift versus offline recomputation.
- Affected components: online feature store and inference-facing reads.
- User-visible impact: online scoring features may be stale or unavailable.
- Data risk: online values may diverge from approved offline feature definitions.
- Detection mechanism: parity validation jobs, Redis health checks, and feature freshness indicators.

## 2. Expected System Behavior

### `CDC connector outage`
- Fail open or fail closed: fail closed for freshness-sensitive downstream publication; retain existing published downstream data as last accepted state.
- Retry behavior: connector resumes or is restarted; no manual downstream patching.
- Degradation behavior: downstream consumers operate on last successful publication until rebuild.
- Manual intervention required or not: manual intervention if the connector does not recover automatically.
- Recovery condition: CDC capture catches up and Bronze materialization is reconciled.

### `Kafka Connect Iceberg sink failure`
- Fail open or fail closed: fail closed for affected Bronze publication.
- Retry behavior: restart connector and replay from retained Kafka offsets.
- Degradation behavior: unaffected source tables continue independently.
- Manual intervention required or not: often required for connector or storage issues.
- Recovery condition: Bronze table is rebuilt or caught up from Kafka and passes reconciliation.

### `Schema resolution failure for direct events`
- Fail open or fail closed: fail closed for Bronze writes of invalid events.
- Retry behavior: corrected schemas or producers may allow replay from retained raw events where available.
- Degradation behavior: valid events continue; invalid events are routed to DLQ.
- Manual intervention required or not: required if subject registration or producer contract is broken.
- Recovery condition: schema mismatch is resolved and DLQ events are replayed or dispositioned.

### `Silver quality or reconciliation failure`
- Fail open or fail closed: fail closed for affected Silver outputs.
- Retry behavior: rerun deterministic Spark jobs after source or rule correction.
- Degradation behavior: prior accepted Silver and Gold outputs remain the last valid publication.
- Manual intervention required or not: required for rule violations that are not transient.
- Recovery condition: rerun completes with auditable check results and no release-blocking failures.
- Warning-level failures may still publish with exception records and visible degraded quality status; critical failures do not publish affected outputs.
- Representative critical checks include row-count volume checks against expected daily or hourly baselines, drift checks for event type, channel, order status, payment type, and key ML features, plus critical foreign-key validation where feasible.

### `Gold model build failure`
- Fail open or fail closed: fail closed for the failed models.
- Retry behavior: rerun dbt after fixing model logic or upstream dependencies.
- Degradation behavior: previously accepted Gold outputs may remain queryable but are marked stale operationally.
- Manual intervention required or not: usually required.
- Recovery condition: Gold models rebuild successfully from accepted Silver inputs.

### `Redis online feature drift or loss`
- Fail open or fail closed: fail closed for parity-sensitive online serving if governed thresholds are breached.
- Retry behavior: replay event streams or rebuild from retained Silver feature snapshots.
- Degradation behavior: inference paths may use only the approved remaining features or suspend affected use cases.
- Manual intervention required or not: required when parity failures persist.
- Recovery condition: Redis state matches approved offline recomputation within the defined thresholds.

## 3. Containment Strategy

- Isolation boundary: per-source-table CDC connectors, per-topic event streams, per-dataset Silver and Gold publication units, and Redis as a serving-only layer.
- Blast radius expectation: failures should stay scoped to the affected connector, topic, dataset, or feature family rather than corrupting all layers.
- Circuit breaker / backpressure behavior: invalid events are diverted to DLQ, bad Silver outputs are withheld, and downstream publication does not proceed from failed upstream layers.
- Escalation path: operator investigates the failing component, restores retained-source replayability, and reruns the deterministic downstream layer.

## 4. Recovery Strategy

- Automatic recovery path: restart or resume connectors and jobs where safe, then replay from retained upstream layers.
- Manual recovery path: repair schema contracts, quality rules, or storage issues, then rebuild Bronze, Silver, Gold, or Redis state from the retained canonical upstream source.
- Reconciliation required: yes; source-to-Bronze, Bronze-to-Silver, Silver-to-Gold, and offline-to-online feature parity checks are required after recovery.
- Recovery validation: freshness restored, row-count or control-total checks pass, and quality or parity outputs show the recovered layer is back within policy.

## 5. Open Risks

1. The exact operational policy for replaying DLQ events is still not fully specified.
2. Watermark and late-arrival handling for direct events need tighter operational thresholds to make failure recovery fully deterministic.
3. Online-feature degradation behavior during parity failures needs an explicit serving policy for each approved use case.
