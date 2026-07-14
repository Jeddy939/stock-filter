"""Backfill weekly_metrics from the derived weekly_price_history table."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cloud_backend.weekly_cache import chunks
from cloud_backend.weekly_metrics import sync_weekly_metrics


def ensure_schema(conn: psycopg.Connection) -> None:
    schema = (ROOT / "firebase" / "migrations" / "001_schema.sql").read_text(encoding="utf-8")
    conn.execute(schema)
    conn.commit()


def main() -> None:
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        ensure_schema(conn)
        pairs = [("asx", "yfinance"), ("us", "yfinance")]
        for market, provider in pairs:
            tickers = [
                row[0]
                for row in conn.execute(
                    """
                    SELECT DISTINCT w.ticker
                    FROM weekly_price_history w
                    LEFT JOIN weekly_metrics m
                      ON m.market = w.market
                     AND m.provider = w.provider
                     AND m.ticker = w.ticker
                    WHERE w.market = %s AND w.provider = %s
                      AND m.ticker IS NULL
                    ORDER BY w.ticker
                    """,
                    (market, provider),
                )
            ]
            total = 0
            for index, ticker_batch in enumerate(chunks(tickers), 1):
                total += sync_weekly_metrics(conn, market, provider, ticker_batch)
                print(
                    f"{market.upper()} {provider}: {min(index * 100, len(tickers))}/{len(tickers)} "
                    f"tickers, {total} metrics",
                    flush=True,
                )


if __name__ == "__main__":
    main()
