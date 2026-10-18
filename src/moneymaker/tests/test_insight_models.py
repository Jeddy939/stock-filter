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

    def test_final_training_set_is_embargoed_before_holdout(self):
        start = date(2020, 1, 1)
        dates = [start + timedelta(days=group * 7) for group in range(60)]
        partitions = chronological_partitions(dates, horizon_days=84)
        development_dates = {dates[index] for index in partitions["development_indices"]}
        self.assertTrue(development_dates)
        self.assertLessEqual(max(development_dates), partitions["holdout_start"] - timedelta(days=84))

    def test_calibration_must_beat_the_base_rate(self):
        labels = [1 if index % 4 == 0 else 0 for index in range(200)]
        base_rate_guess = calibration_summary(labels, [0.25] * 200)
        self.assertFalse(base_rate_guess["calibrated"])
        informative = calibration_summary(labels, [0.9 if label else 0.1 for label in labels])
        self.assertLess(informative["brier_score"], informative["reference_brier_score"])

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
        # Trained on every appraisal, so the base rate is the top-quartile share.
        self.assertAlmostEqual(result["base_rate"], 0.25, delta=0.05)
        self.assertGreater(result["walk_forward_ranking"]["information_coefficient"], 0.5)
        self.assertGreater(result["walk_forward_ranking"]["top_fifth_lift"], 0)


if __name__ == "__main__":
    unittest.main()
