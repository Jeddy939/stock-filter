"""Maintain the derived weekly OHLCV table used by cloud screens."""

from __future__ import annotations

from datetime import date
from typing import Iterable

import psycopg


def chunks(values: list[str], size: int = 100) -> Iterable[list[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def sync_weekly_history(
    conn: psycopg.Connection,
    market: str,
    provider: str,
    tickers: Iterable[str],
    start_date: date | str | None = None,
) -> int:
    """Rebuild complete W-MON candles for selected tickers and date range."""

    ticker_list = sorted({str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()})
    if not ticker_list:
        return 0
    affected = 0
    for ticker_batch in chunks(ticker_list):
        with conn.cursor() as cur:
            if start_date is None:
                cur.execute(
                    "DELETE FROM weekly_price_history WHERE market = %s AND provider = %s AND ticker = ANY(%s)",
                    (market, provider, ticker_batch),
                )
            else:
                cur.execute(
                    """
                    DELETE FROM weekly_price_history
                    WHERE market = %s AND provider = %s AND ticker = ANY(%s)
                      AND week_date >= (
                          date_trunc('week', %s::date - INTERVAL '1 day') + INTERVAL '7 days'
                      )::date
                    """,
                    (market, provider, ticker_batch, start_date),
                )
            cur.execute(
                """
                INSERT INTO weekly_price_history (
                    market, provider, ticker, week_date, open_price, high_price,
                    low_price, close_price, volume, refreshed_at_utc
                )
                SELECT
                    market,
                    provider,
                    ticker,
                    (date_trunc('week', price_date - INTERVAL '1 day') + INTERVAL '7 days')::date,
                    (array_agg(open_price ORDER BY price_date))[1],
                    MAX(high_price),
                    MIN(low_price),
                    (array_agg(close_price ORDER BY price_date DESC))[1],
                    SUM(volume),
                    now()
                FROM price_history
                WHERE market = %s AND provider = %s AND ticker = ANY(%s)
                  AND (
                      %s::date IS NULL
                      OR price_date >= (
                          date_trunc('week', %s::date - INTERVAL '1 day') + INTERVAL '1 day'
                      )::date
                  )
                GROUP BY market, provider, ticker,
                    (date_trunc('week', price_date - INTERVAL '1 day') + INTERVAL '7 days')::date
                ON CONFLICT (market, provider, ticker, week_date) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    refreshed_at_utc = EXCLUDED.refreshed_at_utc
                """,
                (market, provider, ticker_batch, start_date, start_date),
            )
            affected += cur.rowcount
        conn.commit()
    return affected
