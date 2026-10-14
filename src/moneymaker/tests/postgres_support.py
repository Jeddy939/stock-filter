"""Shared setup for Postgres integration tests.

Set MONEYMAKER_TEST_DATABASE_URL to a disposable database to run them. Each test
applies every migration into its own temporary schema and drops it afterwards.
"""

from datetime import date, timedelta
import os
import unittest
import uuid


DATABASE_URL = os.environ.get("MONEYMAKER_TEST_DATABASE_URL", "").strip()


def weekdays(start: date, end: date):
    day = start
    while day <= end:
        if day.weekday() < 5:
            yield day
        day += timedelta(days=1)


@unittest.skipUnless(DATABASE_URL, "set MONEYMAKER_TEST_DATABASE_URL to a disposable Postgres database")
class PostgresSchemaTestCase(unittest.TestCase):
    def setUp(self):
        import psycopg
        from firebase.schema import apply_migrations

        self.schema = f"mm_test_{uuid.uuid4().hex[:12]}"
        with psycopg.connect(DATABASE_URL, autocommit=True) as admin:
            admin.execute(f'CREATE SCHEMA "{self.schema}"')
        self.conn = psycopg.connect(DATABASE_URL, options=f"-c search_path={self.schema}")
        apply_migrations(self.conn)

    def tearDown(self):
        import psycopg

        self.conn.close()
        with psycopg.connect(DATABASE_URL, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{self.schema}" CASCADE')

    def bars(self, ticker, start, end, close_for, *, market="us", volume_for=None):
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO price_history (
                    market, provider, ticker, price_date, open_price, high_price,
                    low_price, close_price, volume, fetched_at_utc
                ) VALUES (%s, 'yfinance', %s, %s, %s, %s, %s, %s, %s, now())
                """,
                [
                    (
                        market, ticker, day, close_for(day), close_for(day), close_for(day), close_for(day),
                        volume_for(day) if volume_for else 1000,
                    )
                    for day in weekdays(start, end)
                ],
            )
        self.conn.commit()
