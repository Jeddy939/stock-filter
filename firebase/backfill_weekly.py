"""Backfill weekly_price_history from the canonical daily price table."""

from __future__ import annotations

import os

import psycopg

from cloud_backend.weekly_cache import chunks, sync_weekly_history
from firebase.worker import ensure_schema


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
            for index, ticker_batch in enumerate(chunks(tickers), 1):
                total += sync_weekly_history(conn, market, provider, ticker_batch)
                print(f"{market.upper()} {provider}: {min(index * 100, len(tickers))}/{len(tickers)} tickers, {total} weeks", flush=True)


if __name__ == "__main__":
    main()
