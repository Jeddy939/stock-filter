"""Maintain precomputed market coverage status."""

from __future__ import annotations

import psycopg


def refresh_market_status(conn: psycopg.Connection, market: str, provider: str = "yfinance") -> None:
    """Refresh one market/provider row for fast UI and SQL Connect status reads."""

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market_status (
                market, provider, ticker_count, history_rows, weekly_rows,
                latest_date, refreshed_at_utc
            )
            SELECT
                %s,
                %s,
                (SELECT COUNT(*) FROM companies WHERE market = %s),
                (SELECT COUNT(*) FROM weekly_metrics WHERE market = %s AND provider = %s),
                (SELECT COUNT(*) FROM weekly_metrics WHERE market = %s AND provider = %s),
                (SELECT MAX(week_date) FROM weekly_metrics WHERE market = %s AND provider = %s),
                now()
            ON CONFLICT (market, provider) DO UPDATE SET
                ticker_count = EXCLUDED.ticker_count,
                history_rows = EXCLUDED.history_rows,
                weekly_rows = EXCLUDED.weekly_rows,
                latest_date = EXCLUDED.latest_date,
                refreshed_at_utc = EXCLUDED.refreshed_at_utc
            """,
            (market, provider, market, market, provider, market, provider, market, provider),
        )
