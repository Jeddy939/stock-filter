"""Postgres integration tests for near-miss recording and screen observations."""

from datetime import date, datetime, timedelta, timezone
import os
import unittest

from psycopg.types.json import Jsonb

from moneymaker.tests.postgres_support import PostgresSchemaTestCase


SIGNAL = date(2026, 2, 2)  # a Monday: the screen's weekly bars end on Mondays


class ScreenNearMissTests(PostgresSchemaTestCase):
    def metric(self, ticker, week, *, weekly_volume=3000.0, close=12.0, ma_90=10.0):
        self.conn.execute(
            """
            INSERT INTO weekly_metrics (
                market, provider, ticker, week_date, close_price, previous_close_price,
                weekly_volume, avg_volume_52, volume_ratio_52, price_avg_1,
                ma_90, ma_180, ma_360, ma_700, available_weeks, history_weeks
            ) VALUES ('us', 'yfinance', %s, %s, %s, 11, %s, 1000, %s, 11, %s, 9, 8, 7, 800, 801)
            """,
            (ticker, week, close, weekly_volume, weekly_volume / 1000, ma_90),
        )

    def test_screen_records_hits_and_near_misses(self):
        from cloud_backend.postgres_screener import run_postgres_filter

        today = self.conn.execute("SELECT CURRENT_DATE").fetchone()[0]
        session = today - timedelta(days=max(0, today.weekday() - 4))  # latest weekday
        week = today - timedelta(days=today.weekday())  # latest Monday
        tickers = {"HIT": {}, "VOLNEAR": {"weekly_volume": 1500.0}, "VOLFAR": {"weekly_volume": 1000.0},
                   "MANEAR": {"ma_90": 12.3}}
        self.conn.execute(
            "INSERT INTO market_status (market, provider, latest_date) VALUES ('us', 'yfinance', %s)", (session,)
        )
        for ticker, overrides in tickers.items():
            self.conn.execute(
                "INSERT INTO companies (market, ticker, info_json, fetched_at_utc) VALUES ('us', %s, '{}', now())",
                (ticker,),
            )
            self.conn.execute(
                """
                INSERT INTO price_history (market, provider, ticker, price_date, close_price, volume, fetched_at_utc)
                VALUES ('us', 'yfinance', %s, %s, 12.5, 1000, now())
                """,
                (ticker, session),
            )
            self.metric(ticker, week, **overrides)
        self.conn.commit()

        result = run_postgres_filter(self.conn, {
            "market": "us", "provider": "yfinance", "volume_multiplier": 2,
            "ma_short": 90, "ma_intermediate": 180, "ma_medium": 360, "ma_long": 700,
        })

        self.assertEqual([row["ticker"] for row in result["results"]], ["HIT"])
        self.assertEqual(result["near_miss_count"], 2)
        near_misses = dict(self.conn.execute(
            "SELECT ticker, failed_rule FROM scan_near_misses WHERE scan_id = %s", (result["scan_id"],)
        ).fetchall())
        self.assertEqual(near_misses, {"VOLNEAR": "volume", "MANEAR": "ma_90"})


class ScreenObservationTests(PostgresSchemaTestCase):
    def scan(self, created_at):
        return self.conn.execute(
            """
            INSERT INTO scan_runs (
                market, source_id, created_at_utc, provider, cache_file, scanned_count,
                result_count, skipped_no_history, config_json, ticker_universe_json
            ) VALUES ('us', %s, %s, 'yfinance', 'test', 3, 2, 0, '{}', '[]')
            RETURNING id
            """,
            (int(created_at.timestamp()), created_at),
        ).fetchone()[0]

    def hit(self, scan_id, ticker, rank):
        self.conn.execute(
            """
            INSERT INTO scan_results (scan_id, source_id, rank, ticker, signal_date, volume_ratio, result_json)
            VALUES (%s, %s, %s, %s, %s, 3.0, %s)
            """,
            (scan_id, rank, rank, ticker, SIGNAL, Jsonb({"ticker": ticker})),
        )

    def test_hits_and_near_misses_are_snapshotted_and_measured_once(self):
        from cloud_backend.insights.screen_observations import (
            measure_screen_outcomes,
            refresh_screen_observations,
        )

        self.bars("SPY", date(2025, 1, 1), date(2026, 6, 30), lambda day: 100.0)
        self.bars("HIT1", date(2025, 1, 1), date(2026, 6, 30), lambda day: 13.0 if day >= date(2026, 3, 2) else 10.0)
        self.bars("HIT2", date(2025, 1, 1), date(2026, 6, 30), lambda day: 10.0)
        self.bars("MISS1", date(2025, 1, 1), date(2026, 6, 30), lambda day: 9.0 if day > SIGNAL else 10.0)
        first = self.scan(datetime(2026, 2, 3, 7, tzinfo=timezone.utc))
        second = self.scan(datetime(2026, 2, 4, 7, tzinfo=timezone.utc))
        self.hit(first, "HIT1", 1)
        self.hit(first, "HIT2", 2)
        self.hit(second, "HIT1", 1)  # the same stock-week seen by a later scan
        self.conn.execute(
            """
            INSERT INTO scan_near_misses (scan_id, ticker, signal_date, failed_rule, observed_value, threshold_value, volume_ratio)
            VALUES (%s, 'MISS1', %s, 'volume', 1.5, 2, 1.5)
            """,
            (first, SIGNAL),
        )
        self.conn.commit()

        result = refresh_screen_observations(self.conn, market="us", horizons=[28], limit=1000)

        self.assertEqual(result["seeded"], 3)
        self.assertEqual(result["outcomes"][0]["measured_count"], 3)
        self.assertEqual(result["snapshots"]["processed"], 3)
        # US snapshots without SEC filings are kept but marked partial.
        self.assertEqual(result["snapshots"]["partial"], 3)
        rows = {
            row[0]: row[1:]
            for row in self.conn.execute(
                """
                SELECT observation.ticker, observation.feature_as_of_date,
                       observation.technical_json->'return_20d_pct'->>'value',
                       outcome.price_at_signal, outcome.benchmark_excess_return_percent
                FROM screen_observations observation
                JOIN screen_observation_outcomes outcome ON outcome.observation_id = observation.id
                WHERE outcome.horizon_days = 28
                """
            ).fetchall()
        }
        self.assertEqual(set(rows), {"HIT1", "HIT2", "MISS1"})
        as_of, return_20d, entry, excess = rows["HIT1"]
        self.assertEqual(as_of, SIGNAL)
        self.assertIsNotNone(return_20d)
        self.assertEqual(entry, 10.0)
        self.assertAlmostEqual(excess, 30.0)
        self.assertAlmostEqual(rows["MISS1"][3], -10.0)

        # Re-running does not re-seed or re-measure final outcomes.
        again = refresh_screen_observations(self.conn, market="us", horizons=[28], limit=1000)
        self.assertEqual(again["seeded"], 0)
        self.assertEqual(again["outcomes"][0]["measured_count"], 0)
        self.assertEqual(again["snapshots"]["processed"], 0)
        self.assertEqual(measure_screen_outcomes(self.conn, market="us", horizons=[28], limit=1000)[0]["measured_count"], 0)


class DailyOutcomesJobTests(PostgresSchemaTestCase):
    def test_daily_job_builds_rating_snapshots_and_screen_observations(self):
        import firebase.worker as worker
        from moneymaker.tests.postgres_support import DATABASE_URL

        self.bars("^AORD", date(2025, 1, 1), date(2026, 6, 30), lambda day: 100.0, market="asx")
        self.bars("WIN", date(2025, 1, 1), date(2026, 6, 30), lambda day: 12.0 if day > SIGNAL else 10.0, market="asx")
        scan_id = self.conn.execute(
            """
            INSERT INTO scan_runs (market, source_id, created_at_utc, provider, cache_file, scanned_count,
                                   result_count, skipped_no_history, config_json, ticker_universe_json)
            VALUES ('asx', 1, %s, 'yfinance', 'test', 1, 1, 0, '{}', '[]') RETURNING id
            """,
            (datetime(2026, 2, 3, 7, tzinfo=timezone.utc),),
        ).fetchone()[0]
        self.conn.execute(
            """
            INSERT INTO scan_results (scan_id, source_id, rank, ticker, signal_date, volume_ratio, result_json)
            VALUES (%s, 1, 1, 'WIN', %s, 3.0, '{}')
            """,
            (scan_id, SIGNAL),
        )
        self.conn.execute(
            """
            INSERT INTO rating_events (event_at_utc, action, market, provider, ticker, label, signal_date, firebase_uid)
            VALUES (%s, 'label', 'asx', 'yfinance', 'WIN', 'winner', %s, 'uid-1')
            """,
            (datetime(2026, 2, 3, 23, tzinfo=timezone.utc), SIGNAL),
        )
        self.conn.commit()

        separator = "&" if "?" in DATABASE_URL else "?"
        original = (os.environ.get("MONEYMAKER_DATABASE_URL"), worker.update_job)
        os.environ["MONEYMAKER_DATABASE_URL"] = f"{DATABASE_URL}{separator}options=-csearch_path%3D{self.schema}"
        worker.update_job = lambda **values: None
        try:
            result = worker.run_rating_outcomes({"market": "all", "horizons": [28]})
        finally:
            if original[0] is None:
                os.environ.pop("MONEYMAKER_DATABASE_URL", None)
            else:
                os.environ["MONEYMAKER_DATABASE_URL"] = original[0]
            worker.update_job = original[1]

        self.assertEqual(result["horizons"][0]["measured_count"], 1)
        self.assertEqual(result["rating_snapshots"]["created_stubs"], 1)
        self.assertEqual(result["rating_snapshots"]["complete"], 1)
        self.assertEqual(result["screen_observations"]["seeded"], 1)
        self.assertEqual(result["screen_observations"]["outcomes"][0]["measured_count"], 1)
        self.assertEqual(result["screen_observations"]["snapshots"]["complete"], 1)


if __name__ == "__main__":
    unittest.main()
