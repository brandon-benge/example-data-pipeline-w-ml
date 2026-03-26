# Consistency Model

> Note
> Consistency rules for Redis-backed online features and ML serving are being split out with the ML platform. See [../docs/ml_platform_split.md](../docs/ml_platform_split.md) for the active ownership change.

## 1. Consistency Goals

### Workflow: `CDC to Bronze ingestion`
- Business importance: foundational; all downstream rebuild guarantees depend on it.
- Required guarantee: append-preserving raw history with explicit source semantics and preserved timestamps.
- Acceptable staleness: asynchronous ingestion is acceptable; silent loss or mutation is not.
- Failure tolerance: ingestion may lag or stop, but replay must remain possible from retained Kafka streams.

### Workflow: `Bronze to Silver publication`
- Business importance: high; Silver is the governed operational analytics layer.
- Required guarantee: deterministic outputs for the same Bronze input and rule set.
- Acceptable staleness: batch latency is acceptable within scheduled or manual run windows.
- Failure tolerance: invalid outputs must be quarantined or withheld rather than published silently.

### Workflow: `Silver to Gold publication`
- Business importance: high; Gold is the published analytics and feature layer.
- Required guarantee: Gold is derived only from Silver and remains rebuildable from Silver.
- Acceptable staleness: bounded batch staleness is acceptable.
- Failure tolerance: failed models do not justify bypassing Silver or mixing layers.

### Workflow: `Online feature serving`
- Business importance: high for inference paths.
- Required guarantee: online feature definitions remain consistent with approved offline definitions.
- Acceptable staleness: low-latency state may lag slightly but must remain rebuildable and auditable.
- Failure tolerance: online state may be replayed or rebuilt; it must not become the only authoritative copy.

## 2. Read Semantics

### Strong Reads
- Required for: source table writes at the source system of record, governed metadata files in the repository, and deterministic batch outputs once committed.
- Source of truth: Postgres for source state, repository-backed metadata for governance definitions, Iceberg tables for published batch layers.
- Cost / latency tradeoff: stronger correctness takes priority over low-latency reads on control and metadata paths.

### Stale-Tolerant Reads
- Allowed for: asynchronous Bronze ingestion visibility, batch publication intervals, BI refreshes, and Redis online state relative to offline recomputation.
- Maximum staleness: bounded by job cadence, replay policy, and local operational expectations rather than strict real-time SLAs.
- How staleness is surfaced: run metadata, data quality outputs, lineage metadata, and validation workflows.

### Read-Your-Writes
- Required or not: required within each committed transformation layer.
- Scope: once a Bronze, Silver, Gold, or metadata publication completes, later readers of that published version observe the committed state.

### Monotonic Reads
- Required or not: required for latest-state Silver outputs and governed dataset publication.
- Scope: readers must not observe a newer committed latest-state output and then regress to an older committed version within the same publication context.

## 3. Write Semantics

### Commit Point
- Bronze writes are committed when records are durably materialized into Iceberg with preserved metadata.
- Silver writes are committed when deterministic transforms and required quality controls complete for the published dataset version.
- Gold writes are committed when dbt-built models from Silver complete successfully.
- Online feature writes are committed when approved feature state is durably updated in Redis and remains reproducible from retained sources.

### Durability
- No acknowledged Bronze, Silver, Gold, governance metadata, or model registry publication may rely on ephemeral state alone.
- Rebuildable upstream retention is part of the durability contract for derived layers.

### Idempotency
- Idempotency key: stable CDC business keys plus source ordering fields for CDC, event identity for direct events, and deterministic model/version identifiers for registry writes.
- Duplicate write behavior: duplicates collapse in Silver or are otherwise handled without changing authoritative logical meaning.

### Partial Failure Handling
- If downstream work fails after Bronze commit: Bronze remains authoritative raw history and downstream jobs are retried or rebuilt from it.
- If Silver or Gold commit is uncertain: affected datasets are not treated as valid published outputs until rerun or reconciled.

## 4. Ordering Guarantees

### Per-Entity Ordering
- Required or not: required for latest-state CDC interpretation and Silver current-state derivation.
- Ordering key: stable business key with source time first and ingest tie-breakers second.
- Enforcement mechanism: deterministic Spark logic and explicit CDC semantics.

### Cross-Entity Ordering
- Required or not: not required globally across unrelated entities.
- Scope: independent source tables and event streams may advance independently.
- Justification: the platform is layer-consistent, not globally serialized.

### Version Monotonicity
- Version source: schema definitions, shared feature definition versions, model versions, and dataset publication revisions.
- Conflict behavior: newer accepted versions supersede older ones intentionally; regressions require explicit rollback or rebuild.

## 5. Concurrency Strategy

### Concurrency Control
- Strategy: partitioned ownership plus deterministic batch recomputation.
- Protected resource: latest-state Silver tables, Gold models, shared feature definitions, and governance metadata.
- Why this strategy is sufficient: source ingestion is append-driven, while derived layers are rebuilt from retained inputs instead of patched manually.

### Duplicate Requests
- Detection mechanism: stable CDC keys, event identity, and deterministic Silver dedup rules.
- Resolution behavior: duplicate direct events are handled idempotently; duplicate CDC observations do not create multiple latest-state rows.

### Concurrent Updates
- Winner selection: source ordering and accepted publication versions determine the effective state.
- Retry behavior: rerun the relevant deterministic pipeline from retained upstream data.

## 6. Conflict Resolution

- Conflict types: duplicate direct events, late-arriving events, CDC delete/update races, schema mismatches, and reconciliation mismatches across layers.
- Detection mechanism: explicit quality checks, schema validation, ordering rules, drift checks, and control-total reconciliation.
- Resolution rule: Bronze keeps raw history, Silver applies deterministic ordering and quality policy, Gold rebuilds only from accepted Silver outputs.
- Whether resolution is automatic or manual: duplicate and ordering conflicts are automatic; broken contracts or unreconcilable quality failures require operator action.
- Audit requirements: check results, exception outputs, and metadata changes must remain auditable.
- Minimum lineage chain expected across the platform: Postgres table to CDC topic to Bronze Iceberg, Kafka direct topic to Bronze Iceberg, Bronze to Silver batch job, Silver to Gold dbt model, and Silver to ML feature dataset.
- Lineage metadata captures upstream dataset identifiers, transformation job or model name, run timestamp or version, and downstream published dataset.

## 7. Replication

- Replication topology: source systems and Kafka feed Iceberg Bronze; Spark feeds Silver; dbt feeds Gold; Flink feeds Redis online state; metadata is file-backed and queryable.
- Sync vs async: ingestion and derived layer refreshes are largely asynchronous.
- Expected lag: bounded by local job cadence and streaming throughput rather than strict production SLOs.
- Lag visibility: visible through quality outputs, lineage, metadata, and job results.
- Reconciliation behavior: retained upstream layers are replayed or recomputed to converge downstream state.
- Failover implications: if a derived layer is lost or corrupted, it is rebuilt from the retained upstream layer rather than manually repaired.

## 8. Explicit Non-Guarantees

- The platform does not guarantee global ordering across unrelated source tables and event streams.
- The platform does not guarantee exactly-once delivery semantics for every transport hop; consumers rely on explicit dedup and deterministic rebuild behavior.
- The platform does not guarantee that Redis online feature state is the authoritative source of feature truth.
