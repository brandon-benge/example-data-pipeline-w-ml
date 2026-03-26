# Scaling

## 1. Growth Assumptions

- Current load: laptop-scale synthetic data volumes and low-to-moderate event rates.
- Expected steady-state growth: increase data volume and topic throughput without changing the logical architecture.
- Peak growth pattern: bursts are expected around generation runs, backfills, and rebuilds rather than internet-scale user spikes.
- Tenant / user growth: the domain remains small in v1, but table design should support more entities and consumers without redesign.
- Data growth: Bronze history, Silver snapshots, Gold marts, and model artifacts all grow over time and must remain partition-friendly.

## 2. Bottleneck Hypotheses

### Suspected Bottleneck: `local compute and memory`
- Why it may bottleneck: Spark, Flink, dbt, Trino, and supporting services share a modest developer machine or small cluster.
- Leading indicator: job runtimes increase sharply, memory pressure rises, and concurrent services become unstable.
- Breaking point estimate: default workloads should fit comfortably; large backfills or excessive concurrency will exceed laptop limits first.
- Mitigation options: reduce default dataset sizes, stagger workloads, scale vertically modestly, or move to developer-scale Kubernetes resources.

### Suspected Bottleneck: `Bronze and Iceberg table layout`
- Why it may bottleneck: poor partitioning or file sizing will slow rebuilds and queries as history accumulates.
- Leading indicator: increasing query scan cost, slower compaction or backfills, and degraded freshness for downstream jobs.
- Breaking point estimate: history-heavy Bronze tables will degrade first if partitions do not align with realistic access patterns.
- Mitigation options: production-shaped partitioning, compaction, retention discipline, and careful file-layout tuning.

### Suspected Bottleneck: `Kafka and connector throughput`
- Why it may bottleneck: CDC topics, direct events, Debezium, and Kafka Connect sinks all share the same small local messaging layer.
- Leading indicator: connector lag, topic backlog, and Bronze freshness delays.
- Breaking point estimate: moderate sustained event rates with multiple simultaneous backfills or connector restarts.
- Mitigation options: tune partition counts judiciously, isolate heavy topics, and move beyond single-node development resources when needed.

### Suspected Bottleneck: `Redis online feature state`
- Why it may bottleneck: online serving depends on low-latency updates and lookups, but the local deployment is intentionally light.
- Leading indicator: increased read or write latency and parity drift caused by lagged updates.
- Breaking point estimate: feature families or key cardinality expand beyond local memory and update throughput.
- Mitigation options: limit approved low-latency features, prune unused state, and size Redis independently in non-laptop environments.

## 3. Scaling Strategy

### Compute
- Horizontal or vertical: vertical-first in laptop mode, then horizontal where the platform or deployment target supports it.
- Autoscaling signal: not required for local mode; future scaling should consider job backlog, topic lag, and resource saturation.
- Warm-up concerns: streaming and query services need stable connectivity to Kafka, Iceberg catalog, and storage before they can take useful work.

### Storage
- Partitioning strategy: use production-shaped Iceberg partitioning for Bronze, Silver, and Gold where practical.
- Retention strategy: retain Bronze long enough to satisfy rebuild guarantees; retain Silver long enough to rebuild Gold and validate feature parity.
- Archival strategy: keep the local footprint light while allowing canonical copies, especially model artifacts, to live in MinIO-backed storage.

### Network / Dependency Limits
- External dependency ceilings: Kafka, MinIO, Redis, Trino, and the Iceberg REST catalog are the primary local service ceilings.
- Rate limit strategy: keep source generation and rebuild jobs within modest local operating envelopes.
- Queueing / buffering strategy: Kafka retention and topic backlogs absorb transient failures, but not indefinite underprovisioning.

## 4. Capacity Guardrails

- Maximum safe QPS: not expressed as a fixed numeric SLO in v1; the guardrail is successful operation at low-to-moderate event rates on modest hardware.
- Maximum safe queue depth: enough backlog to recover transient outages without losing Bronze replayability.
- Maximum tenant concentration: no single dataset, topic, or feature family should dominate local resources so heavily that the rest of the platform becomes unusable.
- Cost ceiling at peak: remain within a single-node or light developer-cluster footprint and avoid adding heavyweight infrastructure purely to satisfy local demos.

## 5. Validation Plan

- Load test scope: generate enough CDC and direct-event volume to validate that default laptop-scale and developer-cluster configurations remain stable.
- Soak test scope: run sustained ingestion, Silver refreshes, Gold builds, and feature updates long enough to expose memory, lag, and storage-layout issues.
- Failure-under-load test: combine moderate traffic with connector restarts, schema failures, and replay workflows.
- Success criteria: the platform remains operable without bespoke tuning, retained layers stay rebuildable, and table layout does not collapse under normal history growth.

## 6. Unknowns

1. The exact partition strategy and file-management rules that best balance local simplicity with future production realism still need validation.
2. The point at which Redis state or Kafka backlog becomes the first practical bottleneck in developer environments is not yet benchmarked.
3. The resource profile for full-stack execution on DigitalOcean Kubernetes versus a single developer laptop needs explicit comparative measurements.
