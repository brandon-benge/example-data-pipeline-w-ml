from __future__ import annotations

from pathlib import Path
import unittest

from generator.config import MIN_SOURCE_TABLE_ROWS, load_settings


PARAMS_PATH = Path(__file__).resolve().parents[2] / "params.yaml"


class GeneratorConfigTest(unittest.TestCase):
    def test_defaults_enforce_source_table_minimums(self) -> None:
        settings = load_settings(
            PARAMS_PATH,
            overrides={
                "customers": 10_000,
                "orders_per_hour": 5_000,
                "events_per_minute": 10_000,
            },
        )

        self.assertTrue(settings.enforce_minimums)
        self.assertEqual(settings.customers, MIN_SOURCE_TABLE_ROWS)
        self.assertEqual(settings.orders_per_hour, MIN_SOURCE_TABLE_ROWS)
        self.assertEqual(settings.sessions, MIN_SOURCE_TABLE_ROWS)

    def test_allow_small_source_tables_disables_floor(self) -> None:
        settings = load_settings(
            PARAMS_PATH,
            overrides={
                "customers": 10_000,
                "orders_per_hour": 5_000,
                "events_per_minute": 10_000,
                "enforce_minimums": False,
            },
        )

        self.assertFalse(settings.enforce_minimums)
        self.assertEqual(settings.customers, 10_000)
        self.assertEqual(settings.orders_per_hour, 5_000)
        self.assertEqual(settings.sales_reps, 5)
        self.assertEqual(settings.advertisers, 40)
        self.assertEqual(settings.products, 83)
        self.assertEqual(settings.campaigns, 80)
        self.assertEqual(settings.sessions, 30_000)
        self.assertEqual(settings.sales_activities, 1_250)


if __name__ == "__main__":
    unittest.main()
