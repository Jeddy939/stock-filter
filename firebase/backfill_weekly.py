"""Backfill weekly_price_history from the canonical daily price table."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cloud_backend.weekly_cache import chunks, sync_weekly_history
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
                    "SELECT ticker FROM companies WHERE market = %s ORDER BY ticker",
                    (market,),
                )
            ]
            total = 0
            metric_total = 0
            for index, ticker_batch in enumerate(chunks(tickers), 1):
                total += sync_weekly_history(conn, market, provider, ticker_batch)
                metric_total += sync_weekly_metrics(conn, market, provider, ticker_batch)
                print(
                    f"{market.upper()} {provider}: {min(index * 100, len(tickers))}/{len(tickers)} "
                    f"tickers, {total} weeks, {metric_total} metrics",
                    flush=True,
                )


if __name__ == "__main__":
    main()
