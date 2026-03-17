from __future__ import annotations

from spark.utils.common import METADATA_ROOT, append_jsonl, utc_now


def record_lineage(job_name: str, upstream_datasets: list[str], downstream_dataset: str) -> None:
    append_jsonl(
        METADATA_ROOT / "lineage" / f"{job_name}.jsonl",
        {
            "job_name": job_name,
            "upstream_datasets": upstream_datasets,
            "downstream_dataset": downstream_dataset,
            "run_timestamp": utc_now().isoformat(),
        },
    )
