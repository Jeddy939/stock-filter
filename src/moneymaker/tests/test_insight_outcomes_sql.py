"""Postgres integration tests for the appraisal anchor and outcome engine.

Set MONEYMAKER_TEST_DATABASE_URL to a disposable database to run them. Each test
applies every migration into its own temporary schema and drops it afterwards.
"""

from datetime import date, datetime, timezone
import unittest

from cloud_backend.insights.anchor import appraisal_cutoff_date
from moneymaker.tests.postgres_support import PostgresSchemaTestCase


class OutcomeSqlTests(PostgresSchemaTestCase):
    def rating(self, ticker, event_at, *, close_price=None, signal_date=None):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rating_events (
                    event_at_utc, action, market, provider, ticker, label,
                    signal_date, close_price, firebase_uid
                ) VALUES (%s, 'label', 'us', 'yfinance', %s, 'winner', %s, %s, 'uid-1')
                RETURNING id
                """,
                (event_at, ticker, signal_date, close_price),
            )
            event_id = cur.fetchone()[0]
        self.conn.commit()
        return event_id

    def measure(self, horizon=28):
        from cloud_backend.insights.outcomes import measure_rating_outcomes

        with self.conn.cursor() as cur:
            result = measure_rating_outcomes(cur, market="us", limit=1000, horizon=horizon)
        self.conn.commit()
        return result

    def outcome(self, event_id, horizon=28):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT price_at_signal, price_at_horizon, return_percent,
                       benchmark_excess_return_percent, outcome_date,
                       outcome_version, quality_json
                FROM rating_outcomes
                WHERE rating_event_id = %s AND horizon_days = %s
                """,
                (event_id, horizon),
            )
            return cur.fetchone()

    def test_sql_anchor_matches_python(self):
        moments = [
            datetime(2026, 2, 5, 1, 0, tzinfo=timezone.utc),
            datetime(2026, 2, 5, 15, 0, tzinfo=timezone.utc),
            datetime(2026, 10, 8, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 10, 8, 6, 0, tzinfo=timezone.utc),
        ]
        for market in ("asx", "us"):
            for moment in moments:
                row = self.conn.execute("SELECT appraisal_cutoff_date(%s, %s)", (market, moment)).fetchone()
                self.assertEqual(row[0], appraisal_cutoff_date(market, moment), (market, moment))

    def test_outcome_starts_at_anchor_session_and_uses_price_history(self):
        self.bars("SPY", date(2026, 1, 1), date(2026, 6, 30), lambda day: 100.0)
        # Closes 10 through Wed 4 Feb, jump to 12 the next day, 15 from 4 March.
        self.bars(
            "AAA", date(2026, 1, 1), date(2026, 6, 30),
            lambda day: 10.0 if day <= date(2026, 2, 4) else 15.0 if day >= date(2026, 3, 4) else 12.0,
        )
        # Wed 4 Feb 20:00 New York is already Thursday in UTC. The stored close
        # (an unadjusted scan price) must not be used as the entry price.
        event_id = self.rating(
            "AAA", datetime(2026, 2, 5, 1, 0, tzinfo=timezone.utc),
            close_price=20.0, signal_date=date(2026, 2, 2),
        )

        result = self.measure()

        self.assertEqual(result["measured_count"], 1)
        price_at_signal, price_at_horizon, return_percent, excess, outcome_date, version, quality = self.outcome(event_id)
        self.assertEqual(price_at_signal, 10.0)
        self.assertEqual(price_at_horizon, 15.0)
        self.assertAlmostEqual(return_percent, 50.0)
        self.assertAlmostEqual(excess, 50.0)
        self.assertEqual(outcome_date, date(2026, 3, 4))
        self.assertEqual(version, 2)
        self.assertEqual(quality["anchor_date"], "2026-02-04")
        self.assertEqual(quality["horizon_status"], "observed")
        self.assertEqual(quality["price_basis"], "price_history")

    def test_ticker_that_stops_trading_is_measured_to_final_bar(self):
        self.bars("SPY", date(2026, 1, 1), date(2026, 6, 30), lambda day: 100.0)
        self.bars("DEAD", date(2026, 1, 1), date(2026, 2, 13), lambda day: 4.0 if day == date(2026, 2, 13) else 10.0)
        event_id = self.rating("DEAD", datetime(2026, 2, 5, 1, 0, tzinfo=timezone.utc))

        self.measure()

        price_at_signal, price_at_horizon, return_percent, _, outcome_date, _, quality = self.outcome(event_id)
        self.assertEqual(price_at_signal, 10.0)
        self.assertEqual(price_at_horizon, 4.0)
        self.assertAlmostEqual(return_percent, -60.0)
        self.assertEqual(outcome_date, date(2026, 2, 13))
        self.assertEqual(quality["horizon_status"], "terminated_last_price")

    def test_immature_event_is_skipped_and_stale_outcome_removed(self):
        # The whole market's data ends 30 June, so a 20 June rating has not
        # reached its 28-day horizon; it must not be treated as delisted.
        self.bars("SPY", date(2026, 1, 1), date(2026, 6, 30), lambda day: 100.0)
        self.bars("NEW", date(2026, 1, 1), date(2026, 6, 30), lambda day: 10.0)
        event_id = self.rating("NEW", datetime(2026, 6, 20, 1, 0, tzinfo=timezone.utc))
        self.conn.execute(
            """
            INSERT INTO rating_outcomes (rating_event_id, horizon_days, measured_at_utc,
                                         price_at_signal, price_at_horizon, return_percent, outcome_version)
            VALUES (%s, 28, now(), 10, 12, 20, 1)
            """,
            (event_id,),
        )
        self.conn.commit()

        result = self.measure()

        self.assertEqual(result["measured_count"], 0)
        self.assertEqual(result["stale_removed_count"], 1)
        self.assertIsNone(self.outcome(event_id))


if __name__ == "__main__":
    unittest.main()
