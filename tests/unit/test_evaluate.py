from __future__ import annotations

import unittest

from ml.evaluate import classifier_metrics


class EvaluateTest(unittest.TestCase):
    def test_classifier_metrics(self) -> None:
        metrics = classifier_metrics([0, 1, 1, 0], [0.1, 0.8, 0.6, 0.2])
        self.assertGreater(metrics["accuracy"], 0.9)
        self.assertGreater(metrics["roc_auc"], 0.9)


if __name__ == "__main__":
    unittest.main()
