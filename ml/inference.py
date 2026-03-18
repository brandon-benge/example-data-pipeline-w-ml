from __future__ import annotations

import io
import json
import pickle
from pathlib import Path
from typing import Any

from ml.artifact_store import download_bytes
from ml.train import ARTIFACT_ROOT, LogisticRegressionModel
from ml.trino_utils import run_trino_query, sql_literal


class _ModelUnpickler(pickle.Unpickler):
    def find_class(self, module: str, name: str):  # type: ignore[override]
        if module == "__main__" and name == "LogisticRegressionModel":
            return LogisticRegressionModel
        return super().find_class(module, name)


def load_manifest(path: str | Path) -> dict[str, Any]:
    manifest_text = _read_artifact_text(path)
    return json.loads(manifest_text)


def latest_manifest(feature_group: str, artifact_root: str | Path = ARTIFACT_ROOT) -> str:
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
        raise FileNotFoundError(f"No manifests found for feature group {feature_group!r} in model registry")
    return str(rows[0]["artifact_manifest_uri"])


def load_model_from_manifest(manifest_path: str | Path) -> tuple[dict[str, Any], LogisticRegressionModel]:
    manifest = load_manifest(manifest_path)
    model_uri = manifest.get("artifact_uris", {}).get("model") or manifest["artifact_paths"]["model"]
    model = _ModelUnpickler(_read_artifact_stream(model_uri)).load()
    return manifest, model


def _read_artifact_text(path_or_uri: str | Path) -> str:
    path_text = str(path_or_uri)
    if path_text.startswith("s3://"):
        return download_bytes(path_text).decode("utf-8")
    return Path(path_or_uri).read_text(encoding="utf-8")


def _read_artifact_stream(path_or_uri: str | Path) -> io.BytesIO:
    path_text = str(path_or_uri)
    if path_text.startswith("s3://"):
        return io.BytesIO(download_bytes(path_text))
    return io.BytesIO(Path(path_or_uri).read_bytes())


def feature_row_for_model(model: LogisticRegressionModel, payload: dict[str, Any]) -> dict[str, float]:
    return {feature_name: float(payload.get(feature_name, 0.0)) for feature_name in model.feature_names}


def predict_one(model: LogisticRegressionModel, payload: dict[str, Any]) -> float:
    row = feature_row_for_model(model, payload)
    return model.predict_proba([row])[0]
