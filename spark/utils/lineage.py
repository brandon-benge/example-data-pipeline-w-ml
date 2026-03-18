from __future__ import annotations

from typing import Any

from spark.utils.common import METADATA_ROOT, append_jsonl, upsert_json_record, utc_now


def record_lineage(
    job_name: str,
    upstream_datasets: list[str],
    downstream_dataset: str,
    *,
    run_id: str | None = None,
    row_count: int | None = None,
    status: str = "success",
    duration_ms: int | None = None,
    write_mode: str = "overwrite",
    metadata: dict[str, Any] | None = None,
) -> None:
    record: dict[str, Any] = {
        "job_name": job_name,
        "upstream_datasets": upstream_datasets,
        "downstream_dataset": downstream_dataset,
        "run_timestamp": utc_now().isoformat(),
        "status": status,
        "write_mode": write_mode,
    }
    if run_id is not None:
        record["run_id"] = run_id
    if row_count is not None:
        record["row_count"] = row_count
    if duration_ms is not None:
        record["duration_ms"] = duration_ms
    if metadata:
        record["metadata"] = metadata

    append_jsonl(
        METADATA_ROOT / "lineage" / f"{job_name}.jsonl",
        record,
    )
    upsert_json_record(METADATA_ROOT / "lineage" / "latest_runs.json", downstream_dataset, record)
