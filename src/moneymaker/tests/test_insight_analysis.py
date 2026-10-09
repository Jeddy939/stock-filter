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

    @staticmethod
    def _panel(count, outcome_for, feature_for, tickers=40, weeks=40):
        random.seed(5)
        rows = []
        for index in range(count):
            week = index % weeks
            outcome = outcome_for(index, week)
            rows.append({
                "market": "us",
                "ticker": f"T{index % tickers}",
                "cluster": f"w{week}",
                "event_at_utc": f"2025-{1 + week // 4:02d}-{1 + (week % 4) * 7:02d}",
                "benchmark_excess_return_percent": outcome,
                "features": {"signal": feature_for(index, week, outcome), "noise": random.uniform(-1, 1)},
            })
        return rows

    def test_real_signal_becomes_a_candidate_with_quintiles(self):
        rows = self._panel(
            200,
            lambda index, week: random.gauss(0, 10),
            lambda index, week, outcome: outcome + random.gauss(0, 8),
        )
        findings = {finding["feature"]: finding for finding in analyze_numeric_features(rows)["findings"]}
        signal = findings["signal"]
        self.assertEqual(signal["status"], "candidate", signal["status_reasons"])
        self.assertGreater(signal["rho_ci_low"], 0)
        means = [quintile["mean_excess"] for quintile in signal["quintiles"]]
        self.assertGreater(means[-1], means[0])
        self.assertEqual(findings["noise"]["status"], "exploratory")

    def test_noise_produces_no_candidates(self):
        rows = self._panel(
            300,
            lambda index, week: random.gauss(0, 10),
            lambda index, week, outcome: random.gauss(0, 1),
        )
        findings = analyze_numeric_features(rows)["findings"]
        self.assertFalse([finding for finding in findings if finding["status"] == "candidate"])

    def test_effect_from_one_ticker_is_not_a_candidate(self):
        def feature(index, week, outcome):
            return 50.0 + outcome if index % 40 == 0 else random.gauss(0, 1)

        def outcome(index, week):
            return 200.0 + week if index % 40 == 0 else random.gauss(0, 10)

        rows = self._panel(200, outcome, feature)
        signal = next(finding for finding in analyze_numeric_features(rows)["findings"] if finding["feature"] == "signal")
        self.assertEqual(signal["status"], "exploratory")
        self.assertEqual(signal["largest_contributor"]["ticker"], "T0")

    def test_effect_in_only_one_period_is_not_a_candidate(self):
        # Strongly positive in the final third of the timeline, mildly
        # negative before it: the pooled correlation is positive but does not
        # repeat across periods.
        def feature(index, week, outcome):
            if week >= 27:
                return outcome + random.gauss(0, 2)
            return -outcome * 0.3 + random.gauss(0, 10)

        rows = self._panel(240, lambda index, week: random.gauss(0, 10), feature)
        signal = next(finding for finding in analyze_numeric_features(rows)["findings"] if finding["feature"] == "signal")
        self.assertLess(signal["agreeing_blocks"], 2)
        self.assertEqual(signal["status"], "exploratory")

    def test_results_are_deterministic(self):
        records = [
            {"benchmark_excess_return_percent": float(index), "features": {"x": float(index % 7)}}
            for index in range(60)
        ]
        self.assertEqual(analyze_numeric_features(records), analyze_numeric_features(records))


if __name__ == "__main__":
    unittest.main()
