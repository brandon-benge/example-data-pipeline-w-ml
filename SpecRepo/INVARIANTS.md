# System Invariants

> Note
> ML-platform responsibilities are being extracted to a sibling repository. Treat [../docs/ml_platform_split.md](../docs/ml_platform_split.md) as the current ownership boundary while this file is being updated.

These are mandatory platform correctness rules.

## Platform Invariants

1. Bronze is append-preserving. Raw Bronze tables retain source history and ingestion metadata.
2. Silver is deterministic. Given the same Bronze input and rule set, Silver outputs are reproducible.
3. Gold is derived only from Silver. Gold models do not read raw topics or Bronze directly.
4. Backfills are lossless relative to retained Bronze. Retained Silver and Gold datasets remain rebuildable from Bronze.
5. Event time and ingest time are both preserved through processing.

## CDC Invariants

6. Latest-state tables are ordered by source time first, then ingest tie-breakers.
7. CDC delete semantics are explicit and must not be silently dropped.
8. Every CDC table has a stable durable business key.

## Streaming Invariants

9. Direct behavioral event topics are append-only after publication.
10. Duplicate direct events are handled idempotently in Silver.
11. Late-arriving events are accepted within the configured watermark or backfill policy.
12. Direct events that cannot resolve against the required Schema Registry subject or version are not written to Bronze and are instead published to DLQ with raw payload and failure reason.

## Governance Invariants

13. Masked fields remain masked outside restricted access paths.
14. Every Iceberg table has defined ownership.
15. Lineage exists for Bronze to Silver to Gold transformations.
16. Every published dataset declares classification and permitted access policy.
17. Deterministically tokenized identifiers stay stable for approved joins and are never exposed with raw sensitive values in broad-access paths.
18. Certified datasets declare certification tier, steward approval, and last review timestamp.

## Data Quality Invariants

19. Primary keys are never null in Silver current-state tables.
20. Defined business keys are unique in Silver current-state tables and curated Gold models.
21. Fact records with invalid critical references are quarantined or rejected by rule.
22. Amounts are non-negative unless an explicit business rule allows otherwise.
23. Session event types are limited to the documented enum.
24. Volume checks compare observed row counts to expected baselines and flag threshold breaches.
25. Drift checks detect statistically meaningful changes in key field distributions and feature populations.
26. Reconciliation checks compare source, Bronze, Silver, and Gold control totals for selected entities and measures.

## ML Invariants

27. Training features are point-in-time correct and do not leak future information.
28. ML training reads from Silver only.
29. Feature definitions are versioned.
30. Online feature definitions derive from approved versioned offline feature logic and remain consistent with training features.
31. Online serving paths expose only approved low-latency features and do not bypass governance controls on restricted attributes.
32. Offline Silver feature recomputation is the canonical reference for validating Redis online feature values.
