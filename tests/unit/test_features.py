from __future__ import annotations

from pathlib import Path
import unittest

from ml.features import (
    build_advertiser_feature_rows,
    build_campaign_feature_rows,
    build_customer_training_rows,
    load_records,
    tokenize_identifier,
)


DATA_DIR = Path(__file__).resolve().parents[1] / "data"


class FeatureAssemblyTest(unittest.TestCase):
    def test_customer_features_are_point_in_time_correct(self) -> None:
        rows = build_customer_training_rows(
            load_records(DATA_DIR / "customer_daily_metrics.jsonl"),
            load_records(DATA_DIR / "order_header.jsonl"),
        )
        target = next(
            row
            for row in rows
            if row["customer_token"] == tokenize_identifier(101) and row["as_of_date"] == "2026-01-05"
        )
        self.assertEqual(target["purchases_30d"], 1)
        self.assertAlmostEqual(target["avg_order_value_90d"], 120.0)
        self.assertEqual(target["days_since_last_purchase"], 2)
        self.assertEqual(target["label_value"], 1)
        self.assertNotIn("entity_id", target)

    def test_campaign_feature_group_supported(self) -> None:
        rows = build_campaign_feature_rows(load_records(DATA_DIR / "campaign_daily_metrics.jsonl"))
        self.assertEqual(rows[0]["feature_group"], "campaign")
        self.assertIn("ctr_7d", rows[0])
        self.assertIn("label_value", rows[0])

    def test_advertiser_feature_group_supported(self) -> None:
        rows = build_advertiser_feature_rows(
            load_records(DATA_DIR / "advertiser_daily_metrics.jsonl"),
            load_records(DATA_DIR / "campaign_budget_history.jsonl"),
        )
        self.assertEqual(rows[0]["feature_group"], "advertiser")
        self.assertIn("budget_delta_30d", rows[0])


if __name__ == "__main__":
    unittest.main()
