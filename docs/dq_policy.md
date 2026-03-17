# DQ Policy

This repository applies data quality controls as repository-managed rules across Bronze normalization, Silver curation, Gold modeling, and ML/BI validation.

## Principles

- DQ rules are versioned with the repository.
- Silver is the primary enforcement layer for validity, deduplication, and governance-aware shaping.
- Gold adds business-facing consistency tests and control-total checks.
- DQ outputs are file-backed for local demo use.

## Rule categories

Current-state integrity:

- primary key not null for Silver current-state tables
- business key uniqueness for Silver current-state tables

Behavioral event quality:

- accepted `event_type` values only in `silver_session_event_clean`
- schema and deserialization failures routed to `dlq.events.session_event_schema`
- direct events deduped by `event_uuid`

Operational fact quality:

- non-negative order monetary fields
- order line `line_amount` consistent with `quantity * unit_price`
- valid customer session windows where `session_start_ts <= session_end_ts` when an end timestamp exists
- valid campaign date ranges

Gold and reconciliation quality:

- curated Gold models maintain relationship and uniqueness tests in dbt
- control-total comparisons validate campaign metrics and purchase totals between Gold marts and their underlying Gold facts
- parity monitoring flags offline Silver reference outputs for comparison with Redis-served customer online features

## Implemented rule locations

Repository-managed rule/config sources:

- [config/governance/dq_rules.yaml](../config/governance/dq_rules.yaml)
- [spark/sql/dq_checks.sql](../spark/sql/dq_checks.sql)
- [dbt/models/staging/schema.yml](../dbt/models/staging/schema.yml)
- [dbt/models/marts/schema.yml](../dbt/models/marts/schema.yml)
- [dbt/tests](../dbt/tests)

## Result handling

Spark DQ results are appended to:

- [metadata/table_contracts/dq_results.jsonl](../metadata/table_contracts)

Gold/dbt test failures surface through dbt execution rather than a separate metadata file in this local demo.

## Severity guidance

- `critical`: key integrity, uniqueness, schema validity, and financial consistency checks
- `warning`: volume baselines, reconciliation readiness, and parity-oriented monitoring checks

## Expectations by layer

Bronze:

- preserve append history and source semantics
- do not silently discard deletes or malformed envelopes

Silver:

- normalize CDC and event records
- enforce deterministic dedupe rules
- apply masking, tokenization, ownership, and sensitivity metadata
- produce stable curated aggregates and feature-ready datasets

Gold:

- expose business-friendly names and stable metric definitions
- maintain relationship and control-total consistency for BI consumption

## Local validation commands

Run unit and integration tests:

```bash
python3 -m unittest discover -s tests/unit -p 'test_*.py'
python3 -m unittest discover -s tests/integration -p 'test_*.py'
```

Inspect Spark DQ results:

```bash
cat metadata/table_contracts/dq_results.jsonl
```
