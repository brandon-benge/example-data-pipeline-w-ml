from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from ml.train import build_feature_rows, train_from_rows


DATA_DIR = Path(__file__).resolve().parents[1] / "data"


class TrainingPipelineIntegrationTest(unittest.TestCase):
    def test_training_writes_expected_artifacts(self) -> None:
        rows = build_feature_rows(
            "customer",
            customer_daily_path=str(DATA_DIR / "customer_daily_metrics.jsonl"),
            order_header_path=str(DATA_DIR / "order_header.jsonl"),
        )
        with TemporaryDirectory() as temp_dir:
            result = train_from_rows(rows, "customer", artifact_root=temp_dir)
            manifest_path = Path(result["artifact_paths"]["manifest"])
            metrics_path = Path(result["artifact_paths"]["metrics"])
            dataset_path = Path(result["artifact_paths"]["dataset"])
            self.assertTrue(manifest_path.exists())
            self.assertTrue(metrics_path.exists())
            self.assertTrue(dataset_path.exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["feature_group"], "customer")
            self.assertEqual(metrics["label_name"], "customer_purchase_next_7d")


if __name__ == "__main__":
    unittest.main()
