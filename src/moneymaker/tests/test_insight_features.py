from datetime import date, timedelta
import math
import unittest

from cloud_backend.insights.features import FEATURE_DEFINITIONS, calculate_technical_features


def _rows(count: int, *, start_close: float = 10.0):
    start = date(2020, 1, 1)
    return [
        {
            "date": start + timedelta(days=index),
            "open": start_close + index,
            "high": start_close + index + 1,
            "low": start_close + index - 1,
            "close": start_close + index,
            "volume": 1_000 + index * 10,
        }
        for index in range(count)
    ]


class InsightFeatureTests(unittest.TestCase):
    def test_every_registered_feature_is_returned(self):
        features = calculate_technical_features(_rows(300))
        self.assertEqual(set(features), {definition.name for definition in FEATURE_DEFINITIONS})

    def test_return_uses_only_supplied_point_in_time_rows(self):
        rows = _rows(30)
        features = calculate_technical_features(rows[:21])
        expected = ((rows[20]["close"] / rows[0]["close"]) - 1) * 100
        self.assertTrue(math.isclose(features["return_20d_pct"].value, expected))
        self.assertTrue(features["return_60d_pct"].is_missing)
        self.assertIn("requires 61 sessions", features["return_60d_pct"].missing_reason)

    def test_young_stock_reports_long_moving_averages_as_missing_not_zero(self):
        features = calculate_technical_features(_rows(100))
        for period in (30, 90, 180, 360, 700):
            feature = features[f"distance_ma_{period}w_pct"]
            self.assertIsNone(feature.value)
            self.assertTrue(feature.is_missing)
            self.assertIn("requires", feature.missing_reason)

    def test_empty_history_has_explicit_missing_reason(self):
        features = calculate_technical_features([])
        self.assertTrue(features)
        self.assertTrue(all(feature.is_missing for feature in features.values()))
        self.assertEqual(
            {feature.missing_reason for feature in features.values()},
            {"no price history at appraisal cutoff"},
        )

    def test_rows_are_sorted_before_features_are_calculated(self):
        rows = _rows(22)
        expected = calculate_technical_features(rows)["return_20d_pct"].value
        actual = calculate_technical_features(list(reversed(rows)))["return_20d_pct"].value
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
