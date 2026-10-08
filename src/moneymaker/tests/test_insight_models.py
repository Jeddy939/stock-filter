from datetime import date, timedelta
import random
import unittest

from cloud_backend.insights.models import run_validated_models
from cloud_backend.insights.validation import calibration_summary, chronological_partitions


class InsightModelTests(unittest.TestCase):
    def test_partitions_keep_dates_together_and_embargo_training(self):
        start = date(2020, 1, 1)
        dates = []
        for group in range(30):
            dates.extend([start + timedelta(days=group * 14)] * 2)
        partitions = chronological_partitions(dates, horizon_days=28)
        self.assertTrue(partitions["folds"])
        for fold in partitions["folds"]:
            train_dates = {dates[index] for index in fold["train_indices"]}
            validation_dates = {dates[index] for index in fold["validation_indices"]}
            self.assertFalse(train_dates & validation_dates)
            self.assertLessEqual(max(train_dates), min(validation_dates) - timedelta(days=28))

    def test_calibration_is_not_claimed_for_sparse_bins(self):
        result = calibration_summary([0, 1, 0, 1], [0.1, 0.9, 0.2, 0.8])
        self.assertFalse(result["calibrated"])

    def test_regularized_model_uses_holdout_and_finds_seeded_signal(self):
        random.seed(11)
        start = date(2018, 1, 1)
        records = []
        for index in range(160):
            signal = (index % 20) - 10 + random.uniform(-0.5, 0.5)
            records.append({
                "event_at_utc": (start + timedelta(days=index * 14)).isoformat(),
                "benchmark_excess_return_percent": signal * 2 + random.uniform(-1, 1),
                "features": {
                    "seeded_signal": signal,
                    "noise": random.uniform(-10, 10),
                },
            })
        result = run_validated_models(records, horizon_days=28)
        self.assertTrue(result["enabled"])
        self.assertGreaterEqual(len(result["folds"]), 2)
        self.assertGreater(result["holdout"]["count"], 0)
        self.assertEqual(result["features"][0]["feature"], "seeded_signal")
        self.assertGreater(result["walk_forward_auc_mean"], 0.8)
        self.assertIn(result["output_name"], {"score", "estimated_probability"})


if __name__ == "__main__":
    unittest.main()
