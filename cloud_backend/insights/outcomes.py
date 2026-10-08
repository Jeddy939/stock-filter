"""Fixed-horizon outcome measurement shared by rated picks and screen observations.

Version 2: outcomes are measured from the same anchor session as the
point-in-time feature snapshot, both prices come from price_history so
split/dividend adjustments cannot fake a return, and tickers that stop trading
before the horizon are measured to their final bar instead of being silently
dropped.

Callers supply a ``candidates`` CTE producing
``(id, market, provider, ticker, cutoff_day, benchmark_ticker)`` where
``cutoff_day`` is the last calendar date whose session may be used as the entry.
"""

from __future__ import annotations

from typing import Any

import psycopg

from .anchor import ANCHOR_POLICY


OUTCOME_VERSION = 2
OUTCOME_TARGET_MULTIPLIER = 1.15
OUTCOME_STOP_MULTIPLIER = 0.88
OUTCOME_HORIZON_TOLERANCE_DAYS = 7
# A ticker counts as having stopped trading when the benchmark has bars at
# least this many days after the ticker's final bar.
OUTCOME_TERMINATION_SILENCE_DAYS = 21

RATING_OUTCOME_CANDIDATES_CTE = """
    candidates AS (
        SELECT
            id,
            market,
            COALESCE(NULLIF(provider, ''), 'yfinance') AS provider,
            ticker,
            appraisal_cutoff_date(market, event_at_utc) AS cutoff_day,
            CASE WHEN lower(market) = 'asx' THEN '^AORD' ELSE 'SPY' END AS benchmark_ticker
        FROM rating_events
        WHERE action = 'label'
          AND label IS NOT NULL
          AND market IS NOT NULL
          AND ticker IS NOT NULL
          AND (%(market)s::text IS NULL OR market = %(market)s)
        ORDER BY event_at_utc DESC, id DESC
        LIMIT %(limit)s
    )
"""


def outcomes_upsert_sql(candidates_cte: str, outcome_table: str, key_column: str) -> str:
    """Measure every candidate at ``%(horizon)s`` days and upsert the results."""
    return f"""
    WITH {candidates_cte},
    anchored AS (
        SELECT c.*, anchor.price_date AS anchor_date, anchor.close_price AS anchor_price
        FROM candidates c
        JOIN LATERAL (
            SELECT price_date, close_price
            FROM price_history ph
            WHERE ph.market = c.market
              AND ph.provider = c.provider
              AND ph.ticker = c.ticker
              AND ph.price_date <= c.cutoff_day
              AND ph.close_price > 0
            ORDER BY ph.price_date DESC
            LIMIT 1
        ) anchor ON TRUE
    ),
    resolved AS (
        SELECT
            a.*,
            COALESCE(window_bar.price_date, terminal.price_date) AS outcome_date,
            COALESCE(window_bar.close_price, terminal.close_price) AS price_at_horizon,
            CASE WHEN window_bar.price_date IS NOT NULL THEN 'observed' ELSE 'terminated_last_price' END AS horizon_status
        FROM anchored a
        LEFT JOIN LATERAL (
            SELECT price_date, close_price
            FROM price_history ph
            WHERE ph.market = a.market
              AND ph.provider = a.provider
              AND ph.ticker = a.ticker
              AND ph.price_date >= a.anchor_date + %(horizon)s::int
              AND ph.price_date <= a.anchor_date + %(horizon)s::int + {OUTCOME_HORIZON_TOLERANCE_DAYS}
              AND ph.close_price IS NOT NULL
            ORDER BY ph.price_date ASC
            LIMIT 1
        ) window_bar ON TRUE
        LEFT JOIN LATERAL (
            SELECT last_bar.price_date, last_bar.close_price
            FROM (
                SELECT price_date, close_price
                FROM price_history ph
                WHERE ph.market = a.market
                  AND ph.provider = a.provider
                  AND ph.ticker = a.ticker
                  AND ph.close_price IS NOT NULL
                ORDER BY ph.price_date DESC
                LIMIT 1
            ) last_bar
            WHERE window_bar.price_date IS NULL
              AND last_bar.price_date < a.anchor_date + %(horizon)s::int
              AND EXISTS (
                  SELECT 1
                  FROM price_history bm
                  WHERE bm.market = a.market
                    AND bm.provider = a.provider
                    AND bm.ticker = a.benchmark_ticker
                    AND bm.price_date >= GREATEST(
                        a.anchor_date + %(horizon)s::int + {OUTCOME_HORIZON_TOLERANCE_DAYS},
                        last_bar.price_date + {OUTCOME_TERMINATION_SILENCE_DAYS}
                    )
              )
        ) terminal ON TRUE
        WHERE COALESCE(window_bar.price_date, terminal.price_date) IS NOT NULL
    ),
    measured AS (
        SELECT
            r.id AS outcome_key,
            %(horizon)s::int AS horizon_days,
            NOW() AS measured_at_utc,
            r.anchor_date,
            r.anchor_price AS price_at_signal,
            r.outcome_date,
            r.price_at_horizon,
            r.horizon_status,
            r.provider,
            r.benchmark_ticker,
            benchmark_signal.close_price AS benchmark_price_at_signal,
            benchmark_horizon.close_price AS benchmark_price_at_horizon,
            path.maximum_high,
            path.minimum_low,
            max_gain.price_date AS maximum_gain_at,
            path.target_hit_at,
            path.stop_hit_at
        FROM resolved r
        LEFT JOIN LATERAL (
            SELECT close_price
            FROM price_history ph
            WHERE ph.market = r.market AND ph.provider = r.provider
              AND ph.ticker = r.benchmark_ticker
              AND ph.price_date <= r.anchor_date
              AND ph.close_price IS NOT NULL
            ORDER BY ph.price_date DESC LIMIT 1
        ) benchmark_signal ON TRUE
        LEFT JOIN LATERAL (
            SELECT close_price
            FROM price_history ph
            WHERE ph.market = r.market AND ph.provider = r.provider
              AND ph.ticker = r.benchmark_ticker
              AND ph.price_date <= r.outcome_date
              AND ph.close_price IS NOT NULL
            ORDER BY ph.price_date DESC LIMIT 1
        ) benchmark_horizon ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                MAX(COALESCE(ph.high_price, ph.close_price)) AS maximum_high,
                MIN(COALESCE(ph.low_price, ph.close_price)) AS minimum_low,
                MIN(ph.price_date) FILTER (
                    WHERE COALESCE(ph.high_price, ph.close_price) >= r.anchor_price * {OUTCOME_TARGET_MULTIPLIER}
                ) AS target_hit_at,
                MIN(ph.price_date) FILTER (
                    WHERE COALESCE(ph.low_price, ph.close_price) <= r.anchor_price * {OUTCOME_STOP_MULTIPLIER}
                ) AS stop_hit_at
            FROM price_history ph
            WHERE ph.market = r.market AND ph.provider = r.provider
              AND ph.ticker = r.ticker
              AND ph.price_date > r.anchor_date
              AND ph.price_date <= r.outcome_date
        ) path ON TRUE
        LEFT JOIN LATERAL (
            SELECT ph.price_date
            FROM price_history ph
            WHERE ph.market = r.market AND ph.provider = r.provider
              AND ph.ticker = r.ticker
              AND ph.price_date > r.anchor_date
              AND ph.price_date <= r.outcome_date
              AND COALESCE(ph.high_price, ph.close_price) = path.maximum_high
            ORDER BY ph.price_date ASC LIMIT 1
        ) max_gain ON TRUE
    ),
    upserted AS (
        INSERT INTO {outcome_table} (
            {key_column},
            horizon_days,
            measured_at_utc,
            price_at_signal,
            price_at_horizon,
            return_percent,
            outcome_date,
            benchmark_ticker,
            benchmark_return_percent,
            benchmark_excess_return_percent,
            maximum_gain_percent,
            maximum_drawdown_percent,
            days_to_maximum_gain,
            target_hit,
            stop_hit,
            target_hit_at,
            stop_hit_at,
            outcome_version,
            quality_json
        )
        SELECT
            outcome_key,
            horizon_days,
            measured_at_utc,
            price_at_signal,
            price_at_horizon,
            ((price_at_horizon - price_at_signal) / price_at_signal) * 100,
            outcome_date,
            benchmark_ticker,
            CASE WHEN benchmark_price_at_signal > 0 AND benchmark_price_at_horizon IS NOT NULL
                 THEN ((benchmark_price_at_horizon - benchmark_price_at_signal) / benchmark_price_at_signal) * 100 END,
            CASE WHEN benchmark_price_at_signal > 0 AND benchmark_price_at_horizon IS NOT NULL
                 THEN ((price_at_horizon - price_at_signal) / price_at_signal) * 100
                    - ((benchmark_price_at_horizon - benchmark_price_at_signal) / benchmark_price_at_signal) * 100 END,
            CASE WHEN maximum_high IS NOT NULL THEN ((maximum_high - price_at_signal) / price_at_signal) * 100 END,
            CASE WHEN minimum_low IS NOT NULL THEN ((minimum_low - price_at_signal) / price_at_signal) * 100 END,
            CASE WHEN maximum_gain_at IS NOT NULL THEN maximum_gain_at - anchor_date END,
            target_hit_at IS NOT NULL AND (stop_hit_at IS NULL OR target_hit_at < stop_hit_at),
            stop_hit_at IS NOT NULL AND (target_hit_at IS NULL OR stop_hit_at <= target_hit_at),
            target_hit_at,
            stop_hit_at,
            %(version)s,
            jsonb_build_object(
                'target_percent', 15,
                'stop_percent', -12,
                'horizon_tolerance_days', {OUTCOME_HORIZON_TOLERANCE_DAYS},
                'same_day_target_stop_policy', 'stop_first',
                'anchor_policy', '{ANCHOR_POLICY}',
                'anchor_date', anchor_date,
                'price_basis', 'price_history',
                'provider', provider,
                'horizon_status', horizon_status,
                'benchmark_available', benchmark_price_at_signal IS NOT NULL AND benchmark_price_at_horizon IS NOT NULL
            )
        FROM measured
        ON CONFLICT ({key_column}, horizon_days) DO UPDATE SET
            measured_at_utc = EXCLUDED.measured_at_utc,
            price_at_signal = EXCLUDED.price_at_signal,
            price_at_horizon = EXCLUDED.price_at_horizon,
            return_percent = EXCLUDED.return_percent,
            outcome_date = EXCLUDED.outcome_date,
            benchmark_ticker = EXCLUDED.benchmark_ticker,
            benchmark_return_percent = EXCLUDED.benchmark_return_percent,
            benchmark_excess_return_percent = EXCLUDED.benchmark_excess_return_percent,
            maximum_gain_percent = EXCLUDED.maximum_gain_percent,
            maximum_drawdown_percent = EXCLUDED.maximum_drawdown_percent,
            days_to_maximum_gain = EXCLUDED.days_to_maximum_gain,
            target_hit = EXCLUDED.target_hit,
            stop_hit = EXCLUDED.stop_hit,
            target_hit_at = EXCLUDED.target_hit_at,
            stop_hit_at = EXCLUDED.stop_hit_at,
            outcome_version = EXCLUDED.outcome_version,
            quality_json = EXCLUDED.quality_json
        RETURNING 1
    )
    SELECT COUNT(*)::int FROM upserted
    """


def measure_outcomes(
    cur: psycopg.Cursor,
    *,
    candidates_cte: str,
    outcome_table: str,
    key_column: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Upsert outcomes for one horizon and drop candidates' stale definitions.

    ``params`` must include ``horizon`` and anything the candidates CTE uses.
    """
    params = {**params, "version": OUTCOME_VERSION}
    cur.execute(outcomes_upsert_sql(candidates_cte, outcome_table, key_column), params)
    row = cur.fetchone()
    measured_count = int((next(iter(row.values())) if isinstance(row, dict) else row[0]) or 0)
    # Candidates still on an older outcome definition could not be re-measured
    # under the current one; drop them rather than mix incompatible returns
    # into analysis.
    cur.execute(
        f"""
        WITH {candidates_cte}
        DELETE FROM {outcome_table} outcome
        USING candidates
        WHERE outcome.{key_column} = candidates.id
          AND outcome.horizon_days = %(horizon)s
          AND outcome.outcome_version < %(version)s
        """,
        params,
    )
    return {"horizon_days": params["horizon"], "measured_count": measured_count, "stale_removed_count": cur.rowcount}


def measure_rating_outcomes(cur: psycopg.Cursor, *, market: str | None, limit: int, horizon: int) -> dict[str, Any]:
    return measure_outcomes(
        cur,
        candidates_cte=RATING_OUTCOME_CANDIDATES_CTE,
        outcome_table="rating_outcomes",
        key_column="rating_event_id",
        params={"market": market, "limit": limit, "horizon": horizon},
    )
