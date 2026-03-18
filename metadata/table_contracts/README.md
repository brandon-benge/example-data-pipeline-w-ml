This directory stores file-backed data quality and table contract results produced by the Spark batch jobs.

What is here
- `dq_results.jsonl`: append-only history of individual DQ checks.
- `dbt_test_results.jsonl`: append-only history of dbt test execution exported from dbt artifacts.
- `latest_results.json`: current-state snapshot keyed by `dataset:rule_name`. This is the fastest way to see the latest outcome for each rule.

Record format
- Each DQ record includes:
  - `rule_name`
  - `severity`
  - `dataset`
  - `passed`
  - `details`
  - `recorded_at`
  - `run_id`

Where it comes from
- Records are written by `append_dq_result(...)` in [spark/utils/common.py](../../spark/utils/common.py).
- The Spark jobs call that helper while building dimensions, facts, aggregates, and feature tables.
- dbt test results are exported from `run_results.json` and `manifest.json` by [export_metadata.py](../../config/dbt/export_metadata.py), which runs after each successful dbt scheduler chunk.

Why it is useful
- It gives you both a history of checks and a current-state view without needing a separate metadata system.
- It is useful for answering:
  - "Did this rule run?"
  - "What was the latest outcome for this dataset?"
  - "What threshold or failure count was evaluated?"
- The `details` field now carries operational context such as:
  - `row_count`
  - `min_expected_row_count`
  - `failure_count`
  - `expected_failure_count`

What improved
- `volume_baseline` is no longer effectively meaningless. It now fails when a dataset with an expected positive volume has `0` rows.
- `latest_results.json` makes it much easier to inspect current health than scanning the full JSONL history.

Current limitations
- This is still event logging, not full contract enforcement. A bad result is recorded, but nothing in this directory blocks downstream consumers by itself.
- Thresholds are currently hard-coded in the Spark jobs instead of being managed from a central contract definition.
- dbt tests are now included, but only after successful dbt chunks. A failed chunk can still prevent the metadata from being refreshed.

Practical interpretation
- Use `latest_results.json` for the present state of each rule.
- Use `dq_results.jsonl` for trend/history.
- Treat these files as useful operational metadata, but not as a complete data quality platform.
