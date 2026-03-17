from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any

from ml.artifact_store import cache_path, download_file
from ml.train import ARTIFACT_ROOT, LogisticRegressionModel
from ml.trino_utils import run_trino_query, sql_literal


def load_manifest(path: str | Path) -> dict[str, Any]:
    manifest_path = _resolve_artifact_path(path)
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def latest_manifest(feature_group: str, artifact_root: str | Path = ARTIFACT_ROOT) -> Path:
    manifests_dir = Path(artifact_root) / "manifests"
    matches = sorted(manifests_dir.glob(f"{feature_group}_*.json"))
    if matches:
        return matches[-1]

    rows = run_trino_query(
        f"""
        SELECT artifact_manifest_uri
        FROM iceberg.silver.ml_model_registry
        WHERE feature_group = {sql_literal(feature_group)}
        ORDER BY trained_at DESC
        LIMIT 1
        """
    )
    if not rows:
        raise FileNotFoundError(f"No manifests found for feature group {feature_group!r} in {manifests_dir}")
    manifest_uri = rows[0]["artifact_manifest_uri"]
    local_cache = cache_path(manifest_uri)
    if not local_cache.exists():
        download_file(manifest_uri, local_cache)
    return local_cache


def load_model_from_manifest(manifest_path: str | Path) -> tuple[dict[str, Any], LogisticRegressionModel]:
    manifest = load_manifest(manifest_path)
    model_uri = manifest.get("artifact_uris", {}).get("model") or manifest["artifact_paths"]["model"]
    model_path = _resolve_artifact_path(model_uri)
    with model_path.open("rb") as handle:
        model = pickle.load(handle)
    return manifest, model


def _resolve_artifact_path(path_or_uri: str | Path) -> Path:
    path_text = str(path_or_uri)
    if path_text.startswith("s3://"):
        local_cache = cache_path(path_text)
        if not local_cache.exists():
            download_file(path_text, local_cache)
        return local_cache
    return Path(path_or_uri)


def feature_row_for_model(model: LogisticRegressionModel, payload: dict[str, Any]) -> dict[str, float]:
    return {feature_name: float(payload.get(feature_name, 0.0)) for feature_name in model.feature_names}


def predict_one(model: LogisticRegressionModel, payload: dict[str, Any]) -> float:
    row = feature_row_for_model(model, payload)
    return model.predict_proba([row])[0]
