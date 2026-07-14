"""Maintain derived weekly screening metrics for fast cloud scans."""

from __future__ import annotations

from datetime import date
from typing import Iterable

import psycopg

from cloud_backend.weekly_cache import chunks


def sync_weekly_metrics(
    conn: psycopg.Connection,
    market: str,
    provider: str,
    tickers: Iterable[str],
    start_date: date | str | None = None,
) -> int:
    """Rebuild compact W-MON screening metrics for selected tickers."""

    ticker_list = sorted({str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()})
    if not ticker_list:
        return 0

    affected = 0
    for ticker_batch in chunks(ticker_list):
        with conn.cursor() as cur:
            if start_date is None:
                cur.execute(
                    "DELETE FROM weekly_metrics WHERE market = %s AND provider = %s AND ticker = ANY(%s)",
                    (market, provider, ticker_batch),
                )
            else:
                cur.execute(
                    """
                    DELETE FROM weekly_metrics
                    WHERE market = %s AND provider = %s AND ticker = ANY(%s)
                      AND week_date >= (
                          date_trunc('week', %s::date - INTERVAL '1 day') + INTERVAL '7 days'
                      )::date
                    """,
                    (market, provider, ticker_batch, start_date),
                )
            cur.execute(
                """
                WITH base AS (
                    SELECT
                        w.market,
                        w.provider,
                        w.ticker,
                        w.week_date,
                        w.close_price,
                        w.volume,
                        ROW_NUMBER() OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date
                        ) - 1 AS available_weeks,
                        COUNT(*) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                        ) AS history_weeks,
                        LAG(w.close_price) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date
                        ) AS previous_close_price,
                        AVG(w.volume) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING
                        ) AS raw_avg_volume_52,
                        COUNT(w.volume) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING
                        ) AS volume_count_52,
                        AVG(w.close_price) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
                        ) AS price_avg_1,
                        AVG(w.close_price) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                        ) AS raw_ma_30,
                        COUNT(w.close_price) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                        ) AS count_ma_30,
                        AVG(w.close_price) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING
                        ) AS raw_ma_90,
                        COUNT(w.close_price) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING
                        ) AS count_ma_90,
                        AVG(w.close_price) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 180 PRECEDING AND 1 PRECEDING
                        ) AS raw_ma_180,
                        COUNT(w.close_price) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 180 PRECEDING AND 1 PRECEDING
                        ) AS count_ma_180,
                        AVG(w.close_price) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 360 PRECEDING AND 1 PRECEDING
                        ) AS raw_ma_360,
                        COUNT(w.close_price) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 360 PRECEDING AND 1 PRECEDING
                        ) AS count_ma_360,
                        AVG(w.close_price) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 700 PRECEDING AND 1 PRECEDING
                        ) AS raw_ma_700,
                        COUNT(w.close_price) OVER (
                            PARTITION BY w.market, w.provider, w.ticker
                            ORDER BY w.week_date ROWS BETWEEN 700 PRECEDING AND 1 PRECEDING
                        ) AS count_ma_700
                    FROM weekly_price_history w
                    WHERE w.market = %s AND w.provider = %s AND w.ticker = ANY(%s)
                ),
                enriched AS (
                    SELECT
                        b.*,
                        CASE
                            WHEN c.info_json ? 'marketCap'
                             AND c.info_json->>'marketCap' ~ '^-?[0-9]+(\\.[0-9]+)?$'
                            THEN (c.info_json->>'marketCap')::double precision
                            ELSE NULL
                        END AS market_cap,
                        c.info_json->>'sector' AS sector,
                        c.info_json->>'industry' AS industry
                    FROM base b
                    LEFT JOIN companies c
                      ON c.market = b.market AND c.ticker = b.ticker
                )
                INSERT INTO weekly_metrics (
                    market, provider, ticker, week_date, close_price, previous_close_price,
                    weekly_change, weekly_change_percent, weekly_volume, avg_volume_52,
                    volume_ratio_52, price_avg_1, ma_30, ma_90, ma_180, ma_360, ma_700,
                    available_weeks, history_weeks, market_cap, sector, industry, refreshed_at_utc
                )
                SELECT
                    market,
                    provider,
                    ticker,
                    week_date,
                    close_price,
                    previous_close_price,
                    close_price - previous_close_price,
                    CASE
                        WHEN previous_close_price IS NULL OR previous_close_price = 0
                        THEN NULL
                        ELSE ((close_price - previous_close_price) / previous_close_price) * 100
                    END,
                    volume,
                    CASE WHEN volume_count_52 >= 41 THEN raw_avg_volume_52 ELSE NULL END,
                    CASE
                        WHEN volume_count_52 >= 41 AND raw_avg_volume_52 > 0
                        THEN volume / raw_avg_volume_52
                        ELSE NULL
                    END,
                    price_avg_1,
                    CASE WHEN count_ma_30 >= 24 THEN raw_ma_30 ELSE NULL END,
                    CASE WHEN count_ma_90 >= 72 THEN raw_ma_90 ELSE NULL END,
                    CASE WHEN count_ma_180 >= 144 THEN raw_ma_180 ELSE NULL END,
                    CASE WHEN count_ma_360 >= 288 THEN raw_ma_360 ELSE NULL END,
                    CASE WHEN count_ma_700 >= 560 THEN raw_ma_700 ELSE NULL END,
                    available_weeks,
                    history_weeks,
                    market_cap,
                    sector,
                    industry,
                    now()
                FROM enriched
                WHERE (
                    %s::date IS NULL
                    OR week_date >= (
                        date_trunc('week', %s::date - INTERVAL '1 day') + INTERVAL '7 days'
                    )::date
                )
                ON CONFLICT (market, provider, ticker, week_date) DO UPDATE SET
                    close_price = EXCLUDED.close_price,
                    previous_close_price = EXCLUDED.previous_close_price,
                    weekly_change = EXCLUDED.weekly_change,
                    weekly_change_percent = EXCLUDED.weekly_change_percent,
                    weekly_volume = EXCLUDED.weekly_volume,
                    avg_volume_52 = EXCLUDED.avg_volume_52,
                    volume_ratio_52 = EXCLUDED.volume_ratio_52,
                    price_avg_1 = EXCLUDED.price_avg_1,
                    ma_30 = EXCLUDED.ma_30,
                    ma_90 = EXCLUDED.ma_90,
                    ma_180 = EXCLUDED.ma_180,
                    ma_360 = EXCLUDED.ma_360,
                    ma_700 = EXCLUDED.ma_700,
                    available_weeks = EXCLUDED.available_weeks,
                    history_weeks = EXCLUDED.history_weeks,
                    market_cap = EXCLUDED.market_cap,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    refreshed_at_utc = EXCLUDED.refreshed_at_utc
                """,
                (market, provider, ticker_batch, start_date, start_date),
            )
            affected += cur.rowcount
        conn.commit()
    return affected
