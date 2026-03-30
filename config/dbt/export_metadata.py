#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any


DBT_ROOT = Path(os.getenv("DBT_METADATA_DBT_ROOT", "/app/dbt"))
METADATA_ROOT = Path(os.getenv("DBT_METADATA_ROOT", "/app/metadata"))


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _candidate_target_roots() -> list[Path]:
    candidates: list[Path] = []
    target_path = os.getenv("DBT_TARGET_PATH", "target")
    for root in (
        DBT_ROOT,
        Path.cwd(),
        Path("/app/dbt"),
        Path("/usr/app"),
    ):
        candidate = (root / target_path).resolve()
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _resolve_target_root() -> Path | None:
    for candidate in _candidate_target_roots():
        manifest_path = candidate / "manifest.json"
        run_results_path = candidate / "run_results.json"
        if manifest_path.exists() and run_results_path.exists():
            return candidate
    return None


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, separators=(",", ":"), default=str))
        handle.write("\n")


def _load_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _canonical_relation(node: dict[str, Any]) -> str | None:
    relation = node.get("relation_name")
    if isinstance(relation, str) and relation:
        if relation.startswith("iceberg."):
            return relation
        return f"iceberg.{relation}"

    schema = node.get("schema")
    name = node.get("alias") or node.get("name")
    if isinstance(schema, str) and isinstance(name, str) and schema and name:
        return f"iceberg.{schema}.{name}"
    return None


def _resolve_dataset(unique_id: str, manifest: dict[str, Any]) -> str | None:
    node = manifest.get("nodes", {}).get(unique_id)
    if node is not None:
        return _canonical_relation(node)
    source = manifest.get("sources", {}).get(unique_id)
    if source is not None:
        return _canonical_relation(source)
    return None


def export_metadata(chunk_label: str) -> None:
    target_root = _resolve_target_root()
    if target_root is None:
        print(
            f"No dbt metadata artifacts found for chunk '{chunk_label}'; skipping metadata export.",
            file=sys.stderr,
        )
        return

    manifest = _load_json(target_root / "manifest.json")
    run_results = _load_json(target_root / "run_results.json")
    run_id = run_results.get("metadata", {}).get("invocation_id", f"dbt-{chunk_label}")

    latest_runs_path = METADATA_ROOT / "lineage" / "latest_runs.json"
    latest_results_path = METADATA_ROOT / "table_contracts" / "latest_results.json"
    latest_runs = _load_snapshot(latest_runs_path)
    latest_results = _load_snapshot(latest_results_path)

    for result in run_results.get("results", []):
        unique_id = result.get("unique_id")
        if not isinstance(unique_id, str):
            continue

        node = manifest.get("nodes", {}).get(unique_id) or manifest.get("sources", {}).get(unique_id)
        if not isinstance(node, dict):
            continue

        resource_type = node.get("resource_type")
        depends_on = node.get("depends_on", {}).get("nodes", [])
        upstream = []
        if isinstance(depends_on, list):
            for dependency in depends_on:
                if not isinstance(dependency, str):
                    continue
                resolved = _resolve_dataset(dependency, manifest)
                if resolved:
                    upstream.append(resolved)

        if resource_type == "model":
            downstream = _canonical_relation(node)
            if downstream is None:
                continue
            record = {
                "job_name": "dbt_build",
                "upstream_datasets": upstream,
                "downstream_dataset": downstream,
                "run_timestamp": result.get("timing", [{}])[-1].get("completed_at")
                or run_results.get("metadata", {}).get("generated_at"),
                "run_id": run_id,
                "status": result.get("status", "unknown"),
                "write_mode": "dbt_build",
                "duration_ms": int(float(result.get("execution_time", 0)) * 1000),
                "metadata": {
                    "writer": "dbt",
                    "chunk_label": chunk_label,
                    "unique_id": unique_id,
                },
            }
            _append_jsonl(METADATA_ROOT / "lineage" / "dbt_runs.jsonl", record)
            latest_runs[downstream] = record
            continue

        if resource_type == "test":
            target_dataset = upstream[0] if upstream else None
            if target_dataset is None:
                continue
            status = result.get("status", "unknown")
            failures = result.get("failures")
            failures_value = int(failures) if isinstance(failures, (int, float)) else 0
            record = {
                "rule_name": node.get("name", unique_id),
                "severity": "critical" if status == "fail" else "warning" if status == "warn" else "info",
                "dataset": target_dataset,
                "passed": status == "pass",
                "details": {
                    "dbt_status": status,
                    "failure_count": failures_value,
                    "execution_time_ms": int(float(result.get("execution_time", 0)) * 1000),
                    "chunk_label": chunk_label,
                    "unique_id": unique_id,
                },
                "recorded_at": result.get("timing", [{}])[-1].get("completed_at")
                or run_results.get("metadata", {}).get("generated_at"),
                "run_id": run_id,
            }
            _append_jsonl(METADATA_ROOT / "table_contracts" / "dbt_test_results.jsonl", record)
            latest_results[f"{target_dataset}:{record['rule_name']}"] = record

    _write_json(latest_runs_path, latest_runs)
    _write_json(latest_results_path, latest_results)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: export_metadata.py <chunk_label>", file=sys.stderr)
        return 1
    export_metadata(sys.argv[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
