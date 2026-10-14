"""Point-in-time snapshots and outcomes for screen hits and near-misses.

Each stock-week the screen flagged (scan_results) or narrowly missed
(scan_near_misses) is one observation, however many scans saw it. Features are
calculated through the signal week's close and outcomes are measured from that
close, so hits, near-misses, and the hits people went on to rate are all
compared from the same starting point.
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import datetime, timezone
import time
from typing import Any, Callable

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .anchor import session_complete_utc
from .features import FEATURE_VERSION, FeatureValue, calculate_technical_features
from .fundamentals import point_in_time_fundamentals
from .outcomes import OUTCOME_VERSION, measure_outcomes


# Enough daily sessions for the longest feature (700-week moving average).
FEATURE_HISTORY_SESSIONS = 3600
SNAPSHOT_QUEUE_BATCH = 2000

ProgressCallback = Callable[[str, int, int | None, str], None]

SCREEN_OUTCOME_CANDIDATES_CTE = """
    candidates AS (
        SELECT observation.id, observation.market, observation.provider, observation.ticker,
               observation.signal_date AS cutoff_day,
               CASE WHEN observation.market = 'asx' THEN '^AORD' ELSE 'SPY' END AS benchmark_ticker
        FROM screen_observations observation
        WHERE observation.feature_version = %(feature_version)s
          AND (%(market)s::text IS NULL OR observation.market = %(market)s)
          AND observation.signal_date <= CURRENT_DATE - %(horizon)s::int
          -- An observed fixed-horizon outcome is final; skip re-measuring it.
          AND NOT EXISTS (
              SELECT 1
              FROM screen_observation_outcomes done
              WHERE done.observation_id = observation.id
                AND done.horizon_days = %(horizon)s
                AND done.outcome_version = %(version)s
                AND done.quality_json->>'horizon_status' = 'observed'
          )
        ORDER BY observation.signal_date DESC, observation.id DESC
        LIMIT %(limit)s
    )
"""


def seed_screen_observations(
    connection: psycopg.Connection,
    *,
    market: str | None = None,
    feature_version: int = FEATURE_VERSION,
) -> int:
    """Queue one observation per screened stock-week; retry-safe."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO screen_observations (market, provider, ticker, signal_date, feature_version)
            SELECT DISTINCT screened.market, screened.provider, screened.ticker, screened.signal_date, %(feature_version)s
            FROM (
                SELECT lower(run.market) AS market, run.provider, hit.ticker, hit.signal_date
                FROM scan_results hit
                JOIN scan_runs run ON run.id = hit.scan_id
                UNION ALL
                SELECT lower(run.market), run.provider, miss.ticker, miss.signal_date
                FROM scan_near_misses miss
                JOIN scan_runs run ON run.id = miss.scan_id
            ) screened
            WHERE screened.market IN ('asx', 'us')
              AND screened.signal_date IS NOT NULL
              AND (%(market)s::text IS NULL OR screened.market = %(market)s)
            ON CONFLICT (market, provider, ticker, signal_date, feature_version) DO NOTHING
            """,
            {"market": market, "feature_version": feature_version},
        )
        created = cursor.rowcount
    connection.commit()
    return created


def _json_values(values: dict[str, FeatureValue]) -> Jsonb:
    return Jsonb({name: value.as_json() for name, value in values.items()})


def _snapshot_observation(
    cursor: psycopg.Cursor[Any],
    observation: dict[str, Any],
    history: list[dict[str, Any]],
    history_dates: list[Any],
) -> str:
    """Calculate and store one observation's features; returns its status."""
    end = bisect_right(history_dates, observation["signal_date"])
    rows = history[max(0, end - FEATURE_HISTORY_SESSIONS):end]
    if not rows:
        cursor.execute(
            """
            UPDATE screen_observations
            SET snapshot_status = 'failed', error = %s, completed_at_utc = now()
            WHERE id = %s
            """,
            ("No price history existed at the signal date", observation["id"]),
        )
        return "failed"

    technical = calculate_technical_features(rows)
    fundamentals, fundamental_quality = point_in_time_fundamentals(
        cursor,
        market=observation["market"],
        ticker=observation["ticker"],
        appraisal_at_utc=session_complete_utc(observation["market"], observation["signal_date"]),
        appraisal_close=float(rows[-1]["close"]),
    )
    missing = [name for name, value in technical.items() if value.is_missing]
    status = "partial" if observation["market"] == "us" and fundamental_quality.get("eligible_fact_rows") == 0 else "complete"
    cursor.execute(
        """
        UPDATE screen_observations
        SET snapshot_status = %s, feature_as_of_date = %s, technical_json = %s,
            fundamental_json = %s, quality_json = %s, error = NULL, completed_at_utc = now()
        WHERE id = %s
        """,
        (
            status,
            rows[-1]["date"],
            _json_values(technical),
            _json_values(fundamentals),
            Jsonb({
                "feature_version": observation["feature_version"],
                "price_basis": "price_history",
                "anchor_policy": "signal_week_close",
                "provider": observation["provider"],
                "history_rows": len(rows),
                "available_feature_count": len(technical) - len(missing),
                "missing_features": missing,
                "fundamentals": fundamental_quality,
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            }),
            observation["id"],
        ),
    )
    return status


def build_observation_snapshots(
    connection: psycopg.Connection,
    *,
    market: str | None = None,
    feature_version: int = FEATURE_VERSION,
    deadline: float | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Build queued snapshots, one price-history read per ticker.

    Stops cleanly at ``deadline`` (a ``time.monotonic()`` value); anything left
    stays queued for the next run.
    """
    counts = {"complete": 0, "partial": 0, "failed": 0}
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS queued
            FROM screen_observations
            WHERE feature_version = %s AND snapshot_status = 'queued'
              AND (%s::text IS NULL OR market = %s)
            """,
            (feature_version, market, market),
        )
        queued_total = int(cursor.fetchone()["queued"])
    processed = 0
    stopped_early = False
    while True:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT id, market, provider, ticker, signal_date, feature_version
                FROM screen_observations
                WHERE feature_version = %s AND snapshot_status = 'queued'
                  AND (%s::text IS NULL OR market = %s)
                ORDER BY market, provider, ticker, signal_date
                LIMIT %s
                """,
                (feature_version, market, market, SNAPSHOT_QUEUE_BATCH),
            )
            queue = list(cursor.fetchall())
        if not queue:
            break
        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for observation in queue:
            groups.setdefault((observation["market"], observation["provider"], observation["ticker"]), []).append(observation)

        for (group_market, provider, ticker), observations in groups.items():
            if deadline is not None and time.monotonic() >= deadline:
                stopped_early = True
                break
            try:
                with connection.cursor(row_factory=dict_row) as cursor:
                    cursor.execute(
                        """
                        SELECT price_date AS date, open_price AS open, high_price AS high,
                               low_price AS low, close_price AS close, volume
                        FROM price_history
                        WHERE market = %s AND provider = %s AND ticker = %s
                          AND price_date <= %s AND close_price IS NOT NULL
                        ORDER BY price_date
                        """,
                        (group_market, provider, ticker, max(item["signal_date"] for item in observations)),
                    )
                    history = list(cursor.fetchall())
                    history_dates = [row["date"] for row in history]
                    for observation in observations:
                        counts[_snapshot_observation(cursor, observation, history, history_dates)] += 1
                connection.commit()
            except Exception as exc:
                # Mark the ticker's observations failed so the queue keeps moving.
                connection.rollback()
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE screen_observations
                        SET snapshot_status = 'failed', error = %s, completed_at_utc = now()
                        WHERE id = ANY(%s)
                        """,
                        (str(exc)[:4000], [item["id"] for item in observations]),
                    )
                connection.commit()
                counts["failed"] += len(observations)
            processed += len(observations)
            if progress:
                progress(
                    "Building screen snapshots", processed, queued_total,
                    f"Snapshotted {processed:,} of {queued_total:,} screen observations ({ticker})",
                )
        if stopped_early:
            break
    connection.commit()  # end the read transaction left by the final queue check
    return {"queued": queued_total, "processed": processed, "stopped_early": stopped_early, **counts}


def measure_screen_outcomes(
    connection: psycopg.Connection,
    *,
    market: str | None,
    horizons: list[int],
    limit: int,
    feature_version: int = FEATURE_VERSION,
) -> list[dict[str, Any]]:
    results = []
    with connection.cursor() as cursor:
        for horizon in horizons:
            results.append(measure_outcomes(
                cursor,
                candidates_cte=SCREEN_OUTCOME_CANDIDATES_CTE,
                outcome_table="screen_observation_outcomes",
                key_column="observation_id",
                params={"market": market, "limit": limit, "horizon": horizon, "feature_version": feature_version},
            ))
    connection.commit()
    return results


def refresh_screen_observations(
    connection: psycopg.Connection,
    *,
    market: str | None,
    horizons: list[int],
    limit: int,
    deadline: float | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Seed, measure, then snapshot; outcomes do not depend on snapshots, so
    they are measured first and a time-limited snapshot backlog never delays them."""
    seeded = seed_screen_observations(connection, market=market)
    outcomes = measure_screen_outcomes(connection, market=market, horizons=horizons, limit=limit)
    snapshots = build_observation_snapshots(connection, market=market, deadline=deadline, progress=progress)
    return {
        "seeded": seeded,
        "outcome_version": OUTCOME_VERSION,
        "outcomes": outcomes,
        "snapshots": snapshots,
    }
