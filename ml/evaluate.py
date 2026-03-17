from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any


def accuracy_score(y_true: list[int], y_pred: list[int]) -> float:
    if not y_true:
        return 0.0
    correct = sum(1 for actual, predicted in zip(y_true, y_pred) if actual == predicted)
    return correct / len(y_true)


def precision_score(y_true: list[int], y_pred: list[int]) -> float:
    true_positive = sum(1 for actual, predicted in zip(y_true, y_pred) if actual == 1 and predicted == 1)
    predicted_positive = sum(1 for predicted in y_pred if predicted == 1)
    return true_positive / predicted_positive if predicted_positive else 0.0


def recall_score(y_true: list[int], y_pred: list[int]) -> float:
    true_positive = sum(1 for actual, predicted in zip(y_true, y_pred) if actual == 1 and predicted == 1)
    actual_positive = sum(1 for actual in y_true if actual == 1)
    return true_positive / actual_positive if actual_positive else 0.0


def f1_score(y_true: list[int], y_pred: list[int]) -> float:
    precision = precision_score(y_true, y_pred)
    recall = recall_score(y_true, y_pred)
    return (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0


def log_loss(y_true: list[int], y_prob: list[float]) -> float:
    if not y_true:
        return 0.0
    epsilon = 1e-12
    loss = 0.0
    for actual, probability in zip(y_true, y_prob):
        clipped = min(max(probability, epsilon), 1 - epsilon)
        loss += -(actual * math.log(clipped) + (1 - actual) * math.log(1 - clipped))
    return loss / len(y_true)


def brier_score(y_true: list[int], y_prob: list[float]) -> float:
    if not y_true:
        return 0.0
    return sum((actual - probability) ** 2 for actual, probability in zip(y_true, y_prob)) / len(y_true)


def roc_auc_score(y_true: list[int], y_prob: list[float]) -> float | None:
    positive_scores = [score for actual, score in zip(y_true, y_prob) if actual == 1]
    negative_scores = [score for actual, score in zip(y_true, y_prob) if actual == 0]
    if not positive_scores or not negative_scores:
        return None
    wins = 0.0
    for positive in positive_scores:
        for negative in negative_scores:
            if positive > negative:
                wins += 1.0
            elif positive == negative:
                wins += 0.5
    return wins / (len(positive_scores) * len(negative_scores))


def classifier_metrics(y_true: list[int], y_prob: list[float], threshold: float = 0.5) -> dict[str, Any]:
    y_pred = [1 if probability >= threshold else 0 for probability in y_prob]
    return {
        "threshold": threshold,
        "accuracy": accuracy_score(y_true, y_pred),
        "precision": precision_score(y_true, y_pred),
        "recall": recall_score(y_true, y_pred),
        "f1": f1_score(y_true, y_pred),
        "log_loss": log_loss(y_true, y_prob),
        "brier_score": brier_score(y_true, y_prob),
        "roc_auc": roc_auc_score(y_true, y_prob),
        "support": len(y_true),
        "positive_rate": (sum(y_true) / len(y_true)) if y_true else 0.0,
    }


def write_metrics(path: str | Path, payload: dict[str, Any]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
