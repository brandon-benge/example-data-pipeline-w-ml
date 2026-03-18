This directory stores file-backed lineage artifacts produced by the Spark batch jobs.

What is here
- `bronze_to_silver_dimensions.jsonl`: append-only lineage history for the `bronze_to_silver_dimensions` job.
- `bronze_to_silver_facts.jsonl`: append-only lineage history for the `bronze_to_silver_facts` job.
- `silver_aggregates.jsonl`: append-only lineage history for the `silver_aggregates` job.
- `build_ml_features.jsonl`: append-only lineage history for the `build_ml_features` job.
- `dbt_runs.jsonl`: append-only lineage history for dbt model execution across staging, marts, features, and semantic layers.
- `latest_runs.json`: current-state snapshot keyed by downstream dataset. This is the quickest file to inspect if you want the latest row count and most recent run metadata.

Record format
- Each lineage record now includes:
  - `job_name`
  - `upstream_datasets`
  - `downstream_dataset`
  - `run_timestamp`
  - `run_id`
  - `status`
  - `write_mode`
  - `row_count`
  - `duration_ms`
  - `metadata`

Where it comes from
- The files are written by [spark/utils/lineage.py](../../spark/utils/lineage.py).
- Spark pipeline jobs call `record_lineage(...)` from [spark/jobs/_pipelines.py](../../spark/jobs/_pipelines.py).
- dbt execution metadata is exported from `manifest.json` and `run_results.json` by [export_metadata.py](../../config/dbt/export_metadata.py), which is invoked by [run-scheduler.sh](../../config/dbt/run-scheduler.sh).

Why it is useful
- It gives you both history and a current-state view of the Spark-produced datasets.
- It is now useful for questions such as:
  - "Which upstream datasets feed this output table?"
  - "How many rows were written on the latest successful run?"
  - "Which job last touched this dataset?"
  - "Did this write finish quickly or slowly?"

Execution boundary in this repo
- Spark is the intended Bronze-to-Silver engine. That is normal here because Bronze inputs are CDC- and event-shaped and need deduplication, current-state logic, masking, DQ checks, and other dataframe-friendly processing.
- dbt is the intended Silver-and-above SQL modeling engine. In practice that means dbt is responsible for curated Gold outputs and SQL-managed feature tables built from Silver inputs.
- Treat that split as the designed contract for this repository, not as an inconsistency.

Current limitations
- It is still dataset-level lineage only. There is no column-level lineage, SQL text capture, or join/filter detail.
- dbt lineage is based on dbt artifacts after a successful chunk. Failed dbt chunks do not currently emit lineage records.
- There is no retention or compaction policy for the JSONL history files.
- Spark records currently carry richer write metadata than dbt records because Spark can observe the table write directly.

Practical interpretation
- Use `latest_runs.json` first for a quick answer.
- Use the job-specific JSONL files when you want run history over time.
- Treat this as lightweight operational lineage, not a full metadata graph.
