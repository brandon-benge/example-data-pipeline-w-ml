from __future__ import annotations

from pathlib import Path
import unittest

from ml.features import build_customer_training_rows, load_records, tokenize_identifier
from ml.online_store import compare_customer_online_record, expected_customer_online_record


DATA_DIR = Path(__file__).resolve().parents[1] / "data"


class OnlineStoreIntegrationTest(unittest.TestCase):
    def test_customer_online_reconciliation(self) -> None:
        customer_rows = build_customer_training_rows(
            load_records(DATA_DIR / "customer_daily_metrics.jsonl"),
            load_records(DATA_DIR / "order_header.jsonl"),
        )
        customer_row = next(
            row
            for row in customer_rows
            if row["customer_token"] == tokenize_identifier(101) and row["as_of_date"] == "2026-01-05"
        )
        realtime_row = load_records(DATA_DIR / "customer_realtime_parity.jsonl")[0]
        expected = expected_customer_online_record(101, realtime_row, customer_row)
        actual = {
            "customer_id": "101",
            "views_1h": "2",
            "views_24h": "6",
            "ad_clicks_24h": "2",
            "add_to_cart_24h": "1",
            "purchases_30d": "1",
            "avg_order_value_90d": "120.0",
            "days_since_last_purchase": "2",
            "feature_version": "customer_realtime_features_v1",
            "last_event_ts": "2026-01-05T18:00:00+00:00",
            "updated_at": "2026-01-05T18:05:00+00:00",
            "ttl_seconds": "86400",
        }
        self.assertEqual(compare_customer_online_record(expected, actual), {})


if __name__ == "__main__":
    unittest.main()
