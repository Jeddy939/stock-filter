import random
import unittest

from cloud_backend.insights.analysis import (
    analyze_numeric_features,
    benjamini_hochberg,
    cohort_of,
    market_cohort_cutoffs,
    sample_gate,
)


class InsightAnalysisTests(unittest.TestCase):
    def test_sample_gates_are_hard_boundaries(self):
        self.assertEqual(sample_gate(19).level, "coverage")
        self.assertFalse(sample_gate(19).allow_descriptive)
        self.assertEqual(sample_gate(20).level, "descriptive")
        self.assertFalse(sample_gate(49).allow_regularized_models)
        self.assertTrue(sample_gate(50).allow_regularized_models)
        self.assertFalse(sample_gate(199).allow_boosting)
        self.assertTrue(sample_gate(200).allow_boosting)

    def test_benjamini_hochberg_is_monotonic_and_bounded(self):
        adjusted = benjamini_hochberg({"a": 0.001, "b": 0.02, "c": 0.04, "d": 0.9})
        ordered = [adjusted[key] for key in ("a", "b", "c", "d")]
        self.assertEqual(ordered, sorted(ordered))
        self.assertTrue(all(0 <= value <= 1 for value in ordered))

    def test_small_cohort_does_not_emit_pattern_claims(self):
        records = [
            {"benchmark_excess_return_percent": index, "features": {"signal": index}}
            for index in range(19)
        ]
        result = analyze_numeric_features(records)
        self.assertEqual(result["gate"]["level"], "coverage")
        self.assertEqual(result["findings"], [])

    def test_seeded_signal_ranks_ahead_of_noise(self):
        random.seed(7)
        records = []
        for index in range(80):
            outcome = float(index - 40)
            records.append({
                "benchmark_excess_return_percent": outcome,
                "features": {
                    "signal": outcome + random.uniform(-2, 2),
                    "noise": random.uniform(-100, 100),
                },
            })
        result = analyze_numeric_features(records)
        self.assertEqual(result["gate"]["level"], "regularized")
        self.assertEqual(result["findings"][0]["feature"], "signal")
        self.assertLess(result["findings"][0]["adjusted_p_value"], 0.05)

    def test_cohorts_are_ranked_within_each_market(self):
        # US excess returns all sit far above ASX ones; pooling would put every
        # US pick in the high cohort and every ASX pick in the low cohort.
        records = [
            {"market": "asx", "benchmark_excess_return_percent": float(index), "features": {}}
            for index in range(20)
        ] + [
            {"market": "us", "benchmark_excess_return_percent": 100.0 + index, "features": {}}
            for index in range(20)
        ]
        cutoffs = market_cohort_cutoffs(records)
        self.assertEqual(set(cutoffs), {"asx", "us"})
        for market in ("asx", "us"):
            cohorts = [cohort_of(row, cutoffs) for row in records if row["market"] == market]
            self.assertEqual(cohorts.count("high"), 5)
            self.assertEqual(cohorts.count("low"), 5)
        result = analyze_numeric_features(records)
        self.assertEqual(result["cohorts"]["high"], 10)
        self.assertEqual(result["cohorts"]["low"], 10)
        self.assertNotIn("high_cutoff", result["cohorts"])

    def test_single_market_keeps_flat_cutoffs(self):
        records = [
            {"market": "us", "benchmark_excess_return_percent": float(index), "features": {}}
            for index in range(20)
        ]
        cohorts = analyze_numeric_features(records)["cohorts"]
        self.assertEqual(cohorts["high_cutoff"], cohorts["cutoffs_by_market"]["us"]["high"])
        self.assertEqual(cohorts["low_cutoff"], cohorts["cutoffs_by_market"]["us"]["low"])

    def test_results_are_deterministic(self):
        records = [
            {"benchmark_excess_return_percent": float(index), "features": {"x": float(index % 7)}}
            for index in range(60)
        ]
        self.assertEqual(analyze_numeric_features(records), analyze_numeric_features(records))


if __name__ == "__main__":
    unittest.main()
