from __future__ import annotations

from datetime import date
import unittest

from ml.labels import advertiser_budget_increase_next_30d, campaign_success_flag, customer_purchase_next_7d


class LabelLogicTest(unittest.TestCase):
    def test_customer_purchase_next_7d(self) -> None:
        value = customer_purchase_next_7d(
            date(2026, 1, 5),
            [{"order_ts": "2026-01-08T10:00:00"}],
        )
        self.assertEqual(value, 1)

    def test_campaign_success_flag(self) -> None:
        value = campaign_success_flag(
            date(2026, 1, 1),
            [{"metric_date": "2026-01-03", "impressions": 100, "clicks": 3, "attributed_orders": 1}],
        )
        self.assertEqual(value, 1)

    def test_advertiser_budget_increase_next_30d(self) -> None:
        value = advertiser_budget_increase_next_30d(
            date(2026, 1, 1),
            401,
            [
                {"snapshot_date": "2026-01-01", "advertiser_id": 401, "budget_amount": 500},
                {"snapshot_date": "2026-01-20", "advertiser_id": 401, "budget_amount": 700},
            ],
        )
        self.assertEqual(value, 1)


if __name__ == "__main__":
    unittest.main()
