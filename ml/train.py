from __future__ import annotations

import argparse
import math
import pickle
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ml.artifact_store import upload_file
from ml.evaluate import classifier_metrics, write_metrics
from ml.features import (
    PROJECT_ROOT,
    as_int,
    build_advertiser_feature_rows,
    build_campaign_feature_rows,
    build_customer_training_rows,
    load_records,
    write_json,
    write_jsonl,
)
from ml.trino_utils import run_trino_query, run_trino_statement


ARTIFACT_ROOT = PROJECT_ROOT / "ml" / "artifacts"
MODEL_REGISTRY_TABLE = "iceberg.silver.ml_model_registry"
FEATURE_TABLES = {
    "customer": "iceberg.silver.customer_purchase_features_v1",
    "customer_realtime": "iceberg.silver.customer_purchase_realtime_features_v1",
    "campaign": "iceberg.silver.campaign_success_features_v1",
    "advertiser": "iceberg.silver.advertiser_budget_features_v1",
}


@dataclass
class LogisticRegressionModel:
    feature_names: list[str]
    means: list[float]
    stds: list[float]
    weights: list[float]
    bias: float
    label_name: str
    feature_definition_version: str

    def predict_proba(self, rows: list[dict[str, Any]]) -> list[float]:
        probabilities: list[float] = []
        for row in rows:
            scaled = []
            for index, feature_name in enumerate(self.feature_names):
                value = float(row.get(feature_name, 0.0))
                std = self.stds[index] if self.stds[index] else 1.0
                scaled.append((value - self.means[index]) / std)
            linear = self.bias + sum(weight * value for weight, value in zip(self.weights, scaled))
            probabilities.append(1.0 / (1.0 + math.exp(-linear)))
        return probabilities


LogisticRegressionModel.__module__ = "ml.train"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _chronological_split(rows: list[dict[str, Any]], train_ratio: float = 0.8) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ordered = sorted(rows, key=lambda row: (row["as_of_date"], row.get("entity_id", row.get("customer_token", ""))))
    split_index = max(1, int(len(ordered) * train_ratio))
    split_index = min(split_index, len(ordered) - 1) if len(ordered) > 1 else len(ordered)
    return ordered[:split_index], ordered[split_index:]


def _prepare_matrix(rows: list[dict[str, Any]], feature_names: list[str]) -> list[list[float]]:
    return [[float(row.get(feature_name, 0.0)) for feature_name in feature_names] for row in rows]


def _column_stats(matrix: list[list[float]]) -> tuple[list[float], list[float]]:
    if not matrix:
        return [], []
    width = len(matrix[0])
    means = []
    stds = []
    for index in range(width):
        column = [row[index] for row in matrix]
        mean = sum(column) / len(column)
        variance = sum((value - mean) ** 2 for value in column) / len(column)
        means.append(mean)
        stds.append(math.sqrt(variance) or 1.0)
    return means, stds


def _scale_matrix(matrix: list[list[float]], means: list[float], stds: list[float]) -> list[list[float]]:
    scaled = []
    for row in matrix:
        scaled.append([(value - means[index]) / stds[index] for index, value in enumerate(row)])
    return scaled


def fit_logistic_regression(
    rows: list[dict[str, Any]],
    feature_names: list[str],
    label_name: str,
    feature_definition_version: str,
    learning_rate: float = 0.1,
    epochs: int = 500,
) -> LogisticRegressionModel:
    matrix = _prepare_matrix(rows, feature_names)
    targets = [int(row["label_value"]) for row in rows]
    means, stds = _column_stats(matrix)
    scaled_matrix = _scale_matrix(matrix, means, stds)
    weights = [0.0] * len(feature_names)
    bias = 0.0

    for _ in range(epochs):
        gradient_w = [0.0] * len(feature_names)
        gradient_b = 0.0
        for features, target in zip(scaled_matrix, targets):
            linear = bias + sum(weight * value for weight, value in zip(weights, features))
            prediction = 1.0 / (1.0 + math.exp(-linear))
            error = prediction - target
            for index, value in enumerate(features):
                gradient_w[index] += error * value
            gradient_b += error
        scale = 1.0 / len(scaled_matrix)
        for index in range(len(weights)):
            weights[index] -= learning_rate * gradient_w[index] * scale
        bias -= learning_rate * gradient_b * scale

    return LogisticRegressionModel(
        feature_names=feature_names,
        means=means,
        stds=stds,
        weights=weights,
        bias=bias,
        label_name=label_name,
        feature_definition_version=feature_definition_version,
    )


def _artifact_paths(feature_group: str, label_name: str, artifact_root: Path) -> dict[str, Path]:
    timestamp = _utc_now().strftime("%Y%m%dT%H%M%SZ")
    stem = f"{feature_group}_{label_name}_{timestamp}"
    return {
        "dataset": artifact_root / "datasets" / f"{stem}.jsonl",
        "model": artifact_root / "models" / f"{stem}.pkl",
        "metrics": artifact_root / "metrics" / f"{stem}.json",
        "manifest": artifact_root / "manifests" / f"{stem}.json",
    }


def _ensure_model_registry_table() -> None:
    run_trino_statement(
        f"""
        CREATE TABLE IF NOT EXISTS {MODEL_REGISTRY_TABLE} (
            feature_group VARCHAR,
            label_name VARCHAR,
            feature_definition_version VARCHAR,
            trained_at TIMESTAMP,
            artifact_manifest_uri VARCHAR,
            artifact_model_uri VARCHAR,
            artifact_metrics_uri VARCHAR,
            artifact_dataset_uri VARCHAR,
            local_manifest_path VARCHAR,
            local_model_path VARCHAR,
            train_rows BIGINT,
            test_rows BIGINT,
            accuracy DOUBLE,
            precision DOUBLE,
            recall DOUBLE,
            roc_auc DOUBLE,
            status VARCHAR
        )
        """
    )


def _register_model_version(
    *,
    feature_group: str,
    label_name: str,
    feature_definition_version: str,
    trained_at: str,
    local_paths: dict[str, Path],
    remote_paths: dict[str, str],
    metrics: dict[str, Any],
    status: str,
) -> None:
    _ensure_model_registry_table()
    run_trino_statement(
        f"""
        INSERT INTO {MODEL_REGISTRY_TABLE} (
            feature_group,
            label_name,
            feature_definition_version,
            trained_at,
            artifact_manifest_uri,
            artifact_model_uri,
            artifact_metrics_uri,
            artifact_dataset_uri,
            local_manifest_path,
            local_model_path,
            train_rows,
            test_rows,
            accuracy,
            precision,
            recall,
            roc_auc,
            status
        )
        VALUES (
            '{feature_group}',
            '{label_name}',
            '{feature_definition_version}',
            from_iso8601_timestamp('{trained_at}'),
            '{remote_paths["manifest"]}',
            '{remote_paths["model"]}',
            '{remote_paths["metrics"]}',
            '{remote_paths["dataset"]}',
            '{str(local_paths["manifest"])}',
            '{str(local_paths["model"])}',
            {int(metrics["train_rows"])},
            {int(metrics["test_rows"])},
            {float(metrics["accuracy"])},
            {float(metrics["precision"])},
            {float(metrics["recall"])},
            {float(metrics["roc_auc"] or 0.0)},
            '{status}'
        )
        """
    )


def train_from_rows(
    rows: list[dict[str, Any]],
    feature_group: str,
    artifact_root: str | Path = ARTIFACT_ROOT,
    *,
    publish_artifacts: bool = True,
    register_model: bool = True,
    status: str = "candidate",
) -> dict[str, Any]:
    if not rows:
        raise ValueError("No feature rows available for training")

    train_rows, test_rows = _chronological_split(rows)
    label_name = str(rows[0]["label_name"])
    feature_definition_version = str(rows[0].get("feature_definition_version", feature_group))
    excluded_columns = {
        "as_of_date",
        "entity_id",
        "customer_id",
        "customer_token",
        "feature_group",
        "feature_definition_version",
        "feature_version",
        "online_feature_version",
        "label_name",
        "label_value",
        "advertiser_id",
        "generated_ts",
        "last_event_ts",
        "updated_at",
        "ttl_seconds",
    }
    feature_names = [
        key
        for key in rows[0].keys()
        if key not in excluded_columns
    ]
    model = fit_logistic_regression(train_rows, feature_names, label_name, feature_definition_version)
    test_probabilities = model.predict_proba(test_rows or train_rows)
    test_targets = [int(row["label_value"]) for row in (test_rows or train_rows)]
    metrics = classifier_metrics(test_targets, test_probabilities)
    metrics.update(
        {
            "feature_group": feature_group,
            "label_name": label_name,
            "feature_names": feature_names,
            "feature_definition_version": feature_definition_version,
            "train_rows": len(train_rows),
            "test_rows": len(test_rows or train_rows),
            "trained_at": _utc_now().isoformat(),
        }
    )

    paths = _artifact_paths(feature_group, label_name, Path(artifact_root))
    write_jsonl(paths["dataset"], rows)
    paths["model"].parent.mkdir(parents=True, exist_ok=True)
    with paths["model"].open("wb") as handle:
        pickle.dump(model, handle)
    write_metrics(paths["metrics"], metrics)
    manifest_payload = {
        "feature_group": feature_group,
        "label_name": label_name,
        "feature_definition_version": feature_definition_version,
        "artifact_paths": {name: str(path) for name, path in paths.items()},
        "trained_at": metrics["trained_at"],
    }
    write_json(paths["manifest"], manifest_payload)

    remote_paths = {
        "dataset": str(paths["dataset"]),
        "model": str(paths["model"]),
        "metrics": str(paths["metrics"]),
        "manifest": str(paths["manifest"]),
    }
    if publish_artifacts:
        remote_paths = {
            "dataset": upload_file(paths["dataset"], f"datasets/{paths['dataset'].name}"),
            "model": upload_file(paths["model"], f"models/{paths['model'].name}"),
            "metrics": upload_file(paths["metrics"], f"metrics/{paths['metrics'].name}"),
            "manifest": "",
        }
        manifest_payload["artifact_uris"] = remote_paths
        write_json(paths["manifest"], manifest_payload)
        remote_paths["manifest"] = upload_file(paths["manifest"], f"manifests/{paths['manifest'].name}")
        manifest_payload["artifact_uris"]["manifest"] = remote_paths["manifest"]
        write_json(paths["manifest"], manifest_payload)
        remote_paths["manifest"] = upload_file(paths["manifest"], f"manifests/{paths['manifest'].name}")
    else:
        manifest_payload["artifact_uris"] = remote_paths
        write_json(paths["manifest"], manifest_payload)
    if register_model:
        _register_model_version(
            feature_group=feature_group,
            label_name=label_name,
            feature_definition_version=feature_definition_version,
            trained_at=metrics["trained_at"],
            local_paths=paths,
            remote_paths=remote_paths,
            metrics=metrics,
            status=status,
        )

    return {
        "model": model,
        "metrics": metrics,
        "artifact_paths": {name: str(path) for name, path in paths.items()},
        "artifact_uris": remote_paths,
    }


def _normalize_training_rows(feature_group: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized_row = dict(row)
        if feature_group == "customer":
            normalized_row["customer_token"] = str(row["customer_token"])
            normalized_row["feature_group"] = "customer"
            normalized_row["feature_definition_version"] = str(
                row.get("feature_definition_version", row.get("feature_version", "customer_purchase_features_v1"))
            )
            normalized_row["label_name"] = str(row.get("label_name", "customer_purchase_next_7d"))
            normalized_row["label_value"] = as_int(row.get("label_value", row.get("customer_purchase_next_7d", 0)))
        else:
            normalized_row["entity_id"] = as_int(row.get("entity_id", row.get("customer_id", 0)))
            normalized_row["feature_group"] = str(row.get("feature_group", feature_group))
            normalized_row["feature_definition_version"] = str(row["feature_definition_version"])
            normalized_row["label_name"] = str(row["label_name"])
            normalized_row["label_value"] = as_int(row["label_value"])
        normalized_row["as_of_date"] = str(row["as_of_date"])
        normalized.append(normalized_row)
    return normalized


def build_feature_rows(
    feature_group: str,
    *,
    table_name: str | None = None,
    customer_daily_path: str | None = None,
    order_header_path: str | None = None,
    campaign_daily_path: str | None = None,
    advertiser_daily_path: str | None = None,
    sales_activity_path: str | None = None,
) -> list[dict[str, Any]]:
    # Keep the older file-path-driven integration test contract working while the
    # primary runtime path trains from Iceberg-backed feature tables.
    if customer_daily_path or order_header_path or campaign_daily_path or advertiser_daily_path or sales_activity_path:
        if feature_group == "customer":
            if not customer_daily_path or not order_header_path:
                raise ValueError("customer feature-group requires customer_daily_path and order_header_path")
            return build_customer_training_rows(load_records(customer_daily_path), load_records(order_header_path))
        if feature_group == "campaign":
            if not campaign_daily_path:
                raise ValueError("campaign feature-group requires campaign_daily_path")
            return build_campaign_feature_rows(load_records(campaign_daily_path))
        if feature_group == "advertiser":
            if not advertiser_daily_path or not sales_activity_path:
                raise ValueError("advertiser feature-group requires advertiser_daily_path and sales_activity_path")
            return build_advertiser_feature_rows(load_records(advertiser_daily_path), load_records(sales_activity_path))
        raise ValueError(f"File-path training inputs are not supported for feature group: {feature_group}")

    resolved_table = table_name or FEATURE_TABLES[feature_group]
    rows = run_trino_query(f"SELECT * FROM {resolved_table}")
    if not rows:
        raise ValueError(f"No feature rows found in {resolved_table}")
    return _normalize_training_rows(feature_group, rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train a local baseline model from Iceberg-backed ML feature tables.")
    parser.add_argument("--feature-group", choices=["customer", "customer_realtime", "campaign", "advertiser"], default="customer")
    parser.add_argument("--table-name")
    parser.add_argument("--artifacts-dir", default=str(ARTIFACT_ROOT))
    parser.add_argument("--status", default="candidate")
    parser.add_argument("--no-publish-artifacts", action="store_true")
    parser.add_argument("--skip-empty", action="store_true")
    args = parser.parse_args()

    try:
        rows = build_feature_rows(args.feature_group, table_name=args.table_name)
    except ValueError as exc:
        if args.skip_empty and "No feature rows found" in str(exc):
            print(str(exc))
            return
        raise
    result = train_from_rows(
        rows,
        args.feature_group,
        artifact_root=args.artifacts_dir,
        publish_artifacts=not args.no_publish_artifacts,
        status=args.status,
    )
    print(result["artifact_uris"]["manifest"])


if __name__ == "__main__":
    main()
