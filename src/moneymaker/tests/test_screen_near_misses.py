from datetime import date
import random
import unittest

from cloud_backend.postgres_screener import (
    _result_from_metric,
    near_miss_from_checks,
    screen_rule_checks,
)


SESSION = date(2026, 10, 5)


def metric_row(**overrides):
    row = {
        "ticker": "AAA",
        "week_date": SESSION,
        "available_weeks": 400,
        "history_weeks": 401,
        "close_price": 12.0,
        "previous_close_price": 11.0,
        "price_avg_1": 11.0,
        "avg_volume_52": 1000.0,
        "weekly_volume": 3000.0,
        "volume_ratio_52": 3.0,
        "ma_30": 10.0,
        "ma_90": 9.0,
        "ma_180": 8.0,
        "ma_360": 7.0,
        "ma_700": None,
        "market_cap": 50_000_000.0,
        "latest_daily_close": 12.2,
        "latest_daily_date": SESSION,
        "market_latest_date": SESSION,
    }
    row.update(overrides)
    return row


CONFIG = {
    "volume_multiplier": 2,
    "ma_periods": {"short": 30, "intermediate": 90, "medium": 180, "long": 360},
    "min_market_cap": 0,
    "max_market_cap": 0,
}


class ScreenRuleCheckTests(unittest.TestCase):
    def test_rule_checks_agree_with_hit_logic(self):
        random.seed(3)
        periods = [30, 90, 180, 360, 700]
        for _ in range(5000):
            config = {
                "volume_multiplier": random.choice([1.5, 2, 3]),
                "ma_periods": {f"p{period}": period for period in random.sample(periods, random.randint(0, 4))},
                "min_market_cap": random.choice([0, 0, 20, 100]),
                "max_market_cap": random.choice([0, 0, 500]),
            }
            base = 10.0
            row = metric_row(
                available_weeks=random.choice([40, 60, 120, 400, 800]),
                close_price=base * random.uniform(0.85, 1.15),
                previous_close_price=base * random.uniform(0.9, 1.1),
                price_avg_1=base * random.uniform(0.9, 1.1),
                weekly_volume=1000.0 * random.uniform(0.5, 4),
                latest_daily_close=base * random.uniform(0.85, 1.2),
                latest_daily_date=random.choice([SESSION, SESSION, date(2026, 10, 2)]),
                market_cap=random.choice([None, 10e6, 60e6, 300e6, 900e6]),
                **{f"ma_{period}": random.choice([None, base * random.uniform(0.8, 1.15)]) for period in periods},
            )
            checks = screen_rule_checks(row, config)
            is_hit = _result_from_metric(row, config, require_daily_confirmation=True) is not None
            if checks is None:
                self.assertFalse(is_hit, row)
            else:
                self.assertEqual(all(check["passed"] for check in checks.values()), is_hit, (row, config, checks))

    def test_volume_shortfall_within_margin_is_a_near_miss(self):
        checks = screen_rule_checks(metric_row(weekly_volume=1500.0), CONFIG)
        near_miss = near_miss_from_checks(checks)
        self.assertEqual(near_miss["failed_rule"], "volume")
        self.assertAlmostEqual(near_miss["observed_value"], 1.5)
        self.assertEqual(near_miss["threshold_value"], 2)

    def test_large_volume_shortfall_is_not_a_near_miss(self):
        # 1.0x against a 2x threshold is below the 60% margin.
        self.assertIsNone(near_miss_from_checks(screen_rule_checks(metric_row(weekly_volume=1000.0), CONFIG)))

    def test_two_failed_rules_is_not_a_near_miss(self):
        row = metric_row(weekly_volume=1500.0, close_price=9.8, latest_daily_close=9.9)
        self.assertIsNone(near_miss_from_checks(screen_rule_checks(row, CONFIG)))

    def test_close_just_under_one_moving_average_is_a_near_miss(self):
        row = metric_row(ma_30=12.3, latest_daily_close=12.5)
        near_miss = near_miss_from_checks(screen_rule_checks(row, CONFIG))
        self.assertEqual(near_miss["failed_rule"], "ma_30")
        self.assertLess(near_miss["observed_value"], 0)

    def test_hit_and_ineligible_rows_are_not_near_misses(self):
        self.assertIsNone(near_miss_from_checks(screen_rule_checks(metric_row(), CONFIG)))
        self.assertIsNone(screen_rule_checks(metric_row(available_weeks=40), CONFIG))
        self.assertIsNone(screen_rule_checks(metric_row(latest_daily_date=date(2026, 10, 2)), CONFIG))

    def test_runaway_exclusion_is_its_own_rule(self):
        config = {**CONFIG, "exclude_above_180_ma_2y": True}
        near_miss = near_miss_from_checks(screen_rule_checks(metric_row(runaway_104=True), config))
        self.assertEqual(near_miss["failed_rule"], "runaway")
        self.assertIsNone(near_miss_from_checks(screen_rule_checks(metric_row(runaway_104=False), config)))


if __name__ == "__main__":
    unittest.main()
