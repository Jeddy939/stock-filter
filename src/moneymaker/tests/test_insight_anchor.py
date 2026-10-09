from datetime import date, datetime, timezone
import unittest

from cloud_backend.insights.anchor import appraisal_cutoff_date


def utc(*parts):
    return datetime(*parts, tzinfo=timezone.utc)


class AppraisalAnchorTests(unittest.TestCase):
    def test_asx_rating_during_session_uses_previous_close(self):
        # Thursday 2026-10-08 11:00 in Sydney (AEDT, UTC+11) is Thursday 00:00 UTC.
        self.assertEqual(appraisal_cutoff_date("asx", utc(2026, 10, 8, 0, 0)), date(2026, 10, 7))

    def test_asx_rating_after_close_uses_same_day(self):
        # Thursday 17:00 Sydney is Thursday 06:00 UTC.
        self.assertEqual(appraisal_cutoff_date("asx", utc(2026, 10, 8, 6, 0)), date(2026, 10, 8))

    def test_us_evening_rating_does_not_see_next_utc_day(self):
        # Wednesday 21:00 New York (EDT, UTC-4) is already Thursday 01:00 UTC.
        self.assertEqual(appraisal_cutoff_date("us", utc(2026, 10, 8, 1, 0)), date(2026, 10, 7))

    def test_us_morning_rating_uses_previous_close(self):
        # Thursday 10:00 New York is Thursday 14:00 UTC.
        self.assertEqual(appraisal_cutoff_date("us", utc(2026, 10, 8, 14, 0)), date(2026, 10, 7))

    def test_market_is_case_insensitive_and_naive_times_are_utc(self):
        self.assertEqual(
            appraisal_cutoff_date("ASX", datetime(2026, 10, 8, 6, 0)),
            appraisal_cutoff_date("asx", utc(2026, 10, 8, 6, 0)),
        )


if __name__ == "__main__":
    unittest.main()
