"""Cloud Run Job worker for fetches and screens.

The worker uses a SQLite checkpoint in Cloud Storage for compatibility with the
current fetcher, then imports the changed checkpoint into PostgreSQL. This is
an intentional bridge while the fetcher is being converted to write directly
to the online schema.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from google.cloud import storage
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moneymaker import fetcher
from firebase.migrate_sqlite_to_postgres import import_cache, import_ratings, sqlite_connection
from firebase.schema import apply_migrations
from firebase.snapshot_publisher import publish_weekly_snapshot
from cloud_backend.market_status import refresh_market_status
from cloud_backend.postgres_screener import run_postgres_filter
from cloud_backend.weekly_cache import sync_weekly_history
from cloud_backend.weekly_metrics import sync_weekly_metrics
from cloud_backend.insights.outcomes import OUTCOME_VERSION, measure_rating_outcomes
from cloud_backend.insights.screen_observations import refresh_screen_observations
from cloud_backend.insights.features import FEATURE_VERSION
from cloud_backend.insights.snapshots import (
    ensure_feature_definitions,
    process_snapshot,
    queued_snapshot_ids,
    seed_snapshot_stubs,
)
from cloud_backend.insights.analysis import analyze_numeric_features
from cloud_backend.insights.fundamentals import refresh_sec_fundamentals
from cloud_backend.insights.models import run_validated_models

OUTCOME_HORIZONS = (28, 30, 56, 84, 90, 180, 182, 360)
# The outcomes job has a one-hour Cloud Run timeout. Rating and screen snapshot
# building stop this long after the job starts; the backlog resumes next run.
SCREEN_SNAPSHOT_BUDGET_SECONDS = 45 * 60
MODEL_CANDIDATE_AUC = 0.55



def job_id() -> str:
    value = os.environ.get("MONEYMAKER_JOB_ID", "").strip()
    if not value:
        raise RuntimeError("MONEYMAKER_JOB_ID is required")
    return value


def payload() -> dict[str, Any]:
    return json.loads(os.environ.get("MONEYMAKER_JOB_PAYLOAD", "{}"))


def update_job(**values: Any) -> None:
    update_job_id(job_id(), **values)


def update_job_id(target_job_id: str, **values: Any) -> None:
    if not target_job_id:
        return
    event_metadata = values.pop("event_metadata", {}) or {}
    values.setdefault("log_tail", "")
    assignments = ", ".join(f"{key} = %s" for key in values)
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE job_runs SET {assignments}, updated_at_utc = now() WHERE id = %s",
                (*values.values(), target_job_id),
            )
            stage = str(values.get("stage") or "Working")
            status = str(values.get("status") or "running")
            current = int(values.get("current_count") or 0)
            total = values.get("total_count")
            percent = values.get("percent")
            message = str(values.get("error") or values.get("detail") or status)
            cur.execute(
                """
                INSERT INTO job_events (
                    job_id, stage_code, stage, status, message,
                    current_count, total_count, percent, metadata_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    target_job_id,
                    _stage_code(stage),
                    stage,
                    status,
                    message,
                    current,
                    total,
                    percent,
                    Jsonb(event_metadata),
                ),
            )
        conn.commit()


def _stage_code(value: str) -> str:
    normalized = "_".join(part for part in "".join(
        character.lower() if character.isalnum() else " " for character in value
    ).split() if part)
    return normalized or "working"


def update_refresh_job(refresh_job_id: str, **values: Any) -> None:
    if not refresh_job_id:
        return
    assignments = ", ".join(f"{key} = %s" for key in values)
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE refresh_jobs SET {assignments} WHERE id = %s", (*values.values(), refresh_job_id))
        conn.commit()


def mark_refresh_batches(refresh_job_id: str, status: str, error: str | None = None) -> None:
    if not refresh_job_id:
        return
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            if status == "running":
                cur.execute(
                    """
                    UPDATE refresh_batches
                    SET status = 'running', attempts = attempts + 1, started_at_utc = COALESCE(started_at_utc, now())
                    WHERE refresh_job_id = %s AND status = 'queued'
                    """,
                    (refresh_job_id,),
                )
            elif status == "succeeded":
                cur.execute(
                    """
                    UPDATE refresh_batches
                    SET status = 'succeeded', finished_at_utc = now()
                    WHERE refresh_job_id = %s AND status IN ('queued', 'running')
                    """,
                    (refresh_job_id,),
                )
            elif status == "failed":
                cur.execute(
                    """
                    UPDATE refresh_batches
                    SET status = 'failed', finished_at_utc = now(), error = %s
                    WHERE refresh_job_id = %s AND status IN ('queued', 'running')
                    """,
                    (error, refresh_job_id),
                )
        conn.commit()


def update_parent_fetch_job(
    parent_job_id: str,
    refresh_job_id: str,
    detail: str | None = None,
    *,
    market: str | None = None,
    provider: str | None = None,
    target_price_basis: str | None = None,
) -> None:
    if not parent_job_id or not refresh_job_id:
        return
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)::int AS total_batches,
                    COUNT(*) FILTER (WHERE status = 'succeeded')::int AS succeeded_batches,
                    COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_batches,
                    COUNT(*) FILTER (WHERE status IN ('queued', 'running'))::int AS active_batches
                FROM refresh_batches
                WHERE refresh_job_id = %s
                """,
                (refresh_job_id,),
            )
            total_batches, succeeded_batches, failed_batches, active_batches = cur.fetchone() or (0, 0, 0, 0)
            completed_batches = succeeded_batches + failed_batches
            if total_batches:
                percent = round((completed_batches / total_batches) * 100, 2)
            else:
                percent = 0
            status = "failed" if failed_batches and not active_batches else (
                "succeeded" if total_batches and completed_batches >= total_batches else "running"
            )
            stage = "Complete" if status == "succeeded" else ("Failed" if status == "failed" else "Fetching batches")
            finished = datetime.now(timezone.utc) if status in {"succeeded", "failed"} else None
            cur.execute(
                """
                UPDATE job_runs
                SET status = %s,
                    stage = %s,
                    detail = %s,
                    current_count = %s,
                    total_count = %s,
                    percent = %s,
                    finished_at_utc = COALESCE(%s, finished_at_utc),
                    updated_at_utc = now()
                WHERE id = %s
                """,
                (
                    status,
                    stage,
                    detail or f"{completed_batches} of {total_batches} ticker refresh batches complete",
                    completed_batches,
                    total_batches,
                    percent,
                    finished,
                    parent_job_id,
                ),
            )
            if status == "succeeded" and market and provider and target_price_basis:
                cur.execute(
                    "UPDATE market_status SET price_basis = %s WHERE market = %s AND provider = %s",
                    (target_price_basis, market, provider),
                )
        conn.commit()


def reconcile_refresh_job(cur: psycopg.Cursor, refresh_job_id: str, error: str | None = None) -> tuple[int, int]:
    """Derive refresh progress from batch state so task retries cannot double-count tickers."""
    cur.execute(
        """
        WITH batch_state AS (
            SELECT
                COALESCE(SUM(jsonb_array_length(tickers_json)), 0)::int AS total_tickers,
                COALESCE(SUM(
                    GREATEST(
                        jsonb_array_length(tickers_json)
                        - COALESCE((result_json #>> '{counts,missing_history_count}')::int, 0),
                        0
                    )
                ) FILTER (WHERE status = 'succeeded'), 0)::int AS completed_tickers,
                (
                    COALESCE(SUM(jsonb_array_length(tickers_json))
                        FILTER (WHERE status = 'failed'), 0)
                    + COALESCE(SUM(
                        COALESCE((result_json #>> '{counts,missing_history_count}')::int, 0)
                    ) FILTER (WHERE status = 'succeeded'), 0)
                )::int AS failed_tickers,
                COUNT(*) FILTER (WHERE status IN ('queued', 'running'))::int AS active_batches,
                COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_batches
            FROM refresh_batches
            WHERE refresh_job_id = %s
        )
        UPDATE refresh_jobs refresh
        SET total_tickers = state.total_tickers,
            completed_tickers = state.completed_tickers,
            failed_tickers = state.failed_tickers,
            status = CASE
                WHEN state.active_batches > 0 THEN 'running'
                WHEN state.failed_batches > 0 THEN 'failed'
                ELSE 'succeeded'
            END,
            stage = CASE
                WHEN state.active_batches > 0 THEN 'Fetching batches'
                WHEN state.failed_batches > 0 THEN 'Batch failed'
                WHEN state.failed_tickers > 0 THEN 'Complete with warnings'
                ELSE 'Complete'
            END,
            error = CASE
                WHEN state.active_batches > 0 THEN NULL
                WHEN state.failed_batches > 0 THEN COALESCE(%s, refresh.error, 'One or more refresh batches failed')
                ELSE NULL
            END,
            finished_at_utc = CASE WHEN state.active_batches > 0 THEN NULL ELSE now() END
        FROM batch_state state
        WHERE refresh.id = %s
        RETURNING state.active_batches, state.failed_batches
        """,
        (refresh_job_id, error, refresh_job_id),
    )
    row = cur.fetchone() or (0, 0)
    return int(row[0] or 0), int(row[1] or 0)


def ensure_schema(conn: psycopg.Connection) -> None:
    apply_migrations(conn)


def checkpoint_path(market: str) -> Path:
    return Path("/tmp") / f"stock_cache_{market}.sqlite"


def download_checkpoint(market: str, path: Path) -> bool:
    bucket_name = os.environ.get("MONEYMAKER_CACHE_BUCKET", "").strip()
    if not bucket_name:
        raise RuntimeError("MONEYMAKER_CACHE_BUCKET is required for resumable cloud fetches")
    object_name = f"sqlite/{market}/stock_cache.sqlite"
    try:
        storage.Client().bucket(bucket_name).blob(object_name).download_to_filename(str(path))
    except Exception as exc:
        if "404" in str(exc) or "NotFound" in str(exc):
            return False
        raise
    return path.exists()


def upload_checkpoint(market: str, path: Path) -> None:
    bucket_name = os.environ.get("MONEYMAKER_CACHE_BUCKET", "").strip()
    if not bucket_name:
        raise RuntimeError("MONEYMAKER_CACHE_BUCKET is required for resumable cloud fetches")
    if not path.exists():
        return
    object_name = f"sqlite/{market}/stock_cache.sqlite"
    storage.Client().bucket(bucket_name).blob(object_name).upload_from_filename(str(path))


def storage_bucket_name() -> str:
    bucket_name = (
        os.environ.get("MONEYMAKER_STORAGE_BUCKET", "").strip()
        or os.environ.get("FIREBASE_STORAGE_BUCKET", "").strip()
        or os.environ.get("MONEYMAKER_CACHE_BUCKET", "").strip()
    )
    if not bucket_name:
        raise RuntimeError("MONEYMAKER_STORAGE_BUCKET or FIREBASE_STORAGE_BUCKET is required")
    return bucket_name


def storage_object_path(value: Any, allowed_prefixes: tuple[str, ...]) -> str:
    object_name = str(value or "").strip().replace("\\", "/").lstrip("/")
    if not object_name or ".." in object_name or object_name.endswith("/"):
        raise RuntimeError("A valid Storage object path is required")
    if not object_name.startswith(allowed_prefixes):
        raise RuntimeError(f"Storage object must start with one of: {', '.join(allowed_prefixes)}")
    return object_name


def download_storage_object(object_name: str, local_path: Path) -> None:
    storage.Client().bucket(storage_bucket_name()).blob(object_name).download_to_filename(str(local_path))


def upload_storage_file(local_path: Path, object_name: str) -> None:
    storage.Client().bucket(storage_bucket_name()).blob(object_name).upload_from_filename(str(local_path))


def sqlite_price_tickers(cache: Path) -> set[str]:
    with sqlite_connection(cache) as source:
        return {
            str(row[0]).strip().upper()
            for row in source.execute("SELECT DISTINCT ticker FROM price_history")
            if row[0]
        }


def remove_sqlite_checkpoint(path: Path) -> None:
    for candidate in (path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm")):
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass


def staging_checkpoint_path(market: str) -> Path:
    return Path("/tmp") / f"stock_staging_{market}.sqlite"


def run_fetch(data: dict[str, Any]) -> dict[str, Any]:
    market = str(data.get("market") or "asx").lower()
    provider = fetcher.normalize_provider(data.get("provider") or fetcher.DEFAULT_PROVIDER)
    refresh_job_id = str(data.get("refresh_job_id") or "").strip()
    refresh_batch_id = str(data.get("refresh_batch_id") or "").strip()
    parent_job_id = str(data.get("parent_job_id") or "").strip()
    explicit_tickers = [
        str(ticker).strip().upper()
        for ticker in (data.get("tickers") or [])
        if str(ticker).strip()
    ]
    batch_refresh = bool(refresh_batch_id and explicit_tickers)
    force_full_history = bool(data.get("force_full_history"))
    target_price_basis = str(data.get("target_price_basis") or "").strip()
    incremental_mode = not force_full_history
    cache = staging_checkpoint_path(market) if incremental_mode else checkpoint_path(market)
    if incremental_mode:
        # Incremental parent and task-queue batch refreshes must not pull the
        # multi-GB full checkpoint into memory-backed /tmp. PostgreSQL latest
        # dates drive overlap ranges for this clean, compact staging database.
        remove_sqlite_checkpoint(cache)
    elif force_full_history:
        remove_sqlite_checkpoint(cache)
    else:
        download_checkpoint(market, cache)
    ticker_file = str(data.get("ticker_file") or (
        "us_tickers_nasdaqtrader.txt" if market == "us" else "asx_yfinance_valid_stocks_2026-05-11.txt"
    ))
    ticker_path = ROOT / ticker_file if not Path(ticker_file).is_absolute() else Path(ticker_file)
    requested_tickers = explicit_tickers or fetcher.get_tickers_from_file(str(ticker_path))
    requested_tickers = fetcher.apply_ticker_limit(requested_tickers, data.get("limit"))
    refresh_updates: dict[str, Any] = {
        "status": "running",
        "stage": "Fetching",
    }
    if not refresh_batch_id:
        refresh_updates["total_tickers"] = len(requested_tickers)
    update_refresh_job(refresh_job_id, **refresh_updates)
    if not refresh_batch_id:
        mark_refresh_batches(refresh_job_id, "running")
    history_overlap_days = max(7, int(data.get("history_refresh_days") or 5))
    info_refresh_days = max(0, int(data.get("info_refresh_days") or 30))
    history_end_date = str(data.get("history_end_date") or (date.today() + timedelta(days=1)).isoformat())
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticker, MAX(price_date)
                FROM price_history
                WHERE market = %s AND provider = %s AND ticker = ANY(%s)
                  AND price_date < %s::date
                GROUP BY ticker
                """,
                (market, provider, requested_tickers, history_end_date),
            )
            latest_price_dates = {str(row[0]): row[1] for row in cur.fetchall() if row[1]}
            cur.execute(
                """
                SELECT ticker, info_json, fetched_at_utc
                FROM companies
                WHERE market = %s AND ticker = ANY(%s)
                  AND info_json IS NOT NULL
                  AND fetched_at_utc >= NOW() - (%s * INTERVAL '1 day')
                """,
                (market, requested_tickers, info_refresh_days),
            )
            fresh_company_profiles = list(cur.fetchall())
    existing_tickers = set(latest_price_dates)
    full_tickers = set(requested_tickers) if force_full_history else set(requested_tickers) - existing_tickers
    history_start_overrides = None if force_full_history else {
        ticker: latest_price_dates[ticker] - timedelta(days=history_overlap_days)
        for ticker in requested_tickers
        if ticker in latest_price_dates
    }
    last_update = 0.0

    def progress(stage: str, current: int, total: int | None, message: str) -> None:
        nonlocal last_update
        now = time.monotonic()
        if now - last_update < 1.0 and total is not None and current < total:
            return
        last_update = now
        percent = round((current / total) * 100, 2) if total else 0
        update_job(status="running", stage=stage, current_count=current, total_count=total,
                   percent=percent, detail=message)

    params = dict(data)
    params.pop("market", None)
    params.pop("ticker_file", None)
    params.pop("tickers", None)
    params.pop("parent_job_id", None)
    temp_ticker_file = None
    if explicit_tickers:
        temp_ticker_file = Path("/tmp") / f"tickers_{job_id()}.txt"
        temp_ticker_file.write_text("\n".join(requested_tickers) + "\n", encoding="utf-8")
    seeded_company_count = fetcher.seed_info_cache(str(cache), fresh_company_profiles)
    update_job(
        status="running",
        stage="Preparing incremental fetch" if not force_full_history else "Preparing full-history fetch",
        current_count=0,
        total_count=len(requested_tickers),
        percent=0,
        detail=(
            f"Fetching recent overlap for {len(requested_tickers) - len(full_tickers)} existing tickers and "
            f"full history for {len(full_tickers)} new tickers; reusing {seeded_company_count} fresh company profiles"
            if not force_full_history
            else f"Running required full-history refresh for {len(full_tickers)} tickers"
        ),
    )
    fetch_metadata: dict[str, Any] = {}
    fetch_succeeded = fetcher.fetch_stock_data(
        ticker_file=str(temp_ticker_file or ticker_path),
        output="/tmp/cloud_fetch.json",
        cache_file=str(cache),
        progress_callback=progress,
        export_json=False,
        history_start_overrides=history_start_overrides,
        metadata_output=fetch_metadata,
        **{key: value for key, value in params.items() if key in {
            "years", "workers", "provider", "limit", "info_refresh_days",
            "history_refresh_days", "history_end_date", "prune_missing_tickers", "history_chunk_size",
            "history_pause_seconds", "info_pause_seconds", "rate_limit_pause_seconds",
            "max_rate_limit_retries", "stop_on_rate_limit"
        }},
    )
    # SQLite may still have an open WAL after a large batch. Compact it before
    # uploading the checkpoint so no committed rows are stranded in a sidecar.
    import sqlite3
    with sqlite3.connect(str(cache)) as checkpoint:
        checkpoint.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    overlap_days = max(30, history_overlap_days + 7)
    price_since = (date.today() - timedelta(days=overlap_days)).isoformat()
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        ensure_schema(conn)
        counts = import_cache(
            conn, cache, market, 2_000, False,
            price_since=price_since,
            full_tickers=full_tickers,
        )
        incremental_tickers = set(requested_tickers) - full_tickers
        weekly_rows = sync_weekly_history(conn, market, provider, full_tickers)
        metric_rows = sync_weekly_metrics(conn, market, provider, full_tickers)
        weekly_rows += sync_weekly_history(
            conn,
            market,
            provider,
            incremental_tickers,
            start_date=(date.today() - timedelta(days=overlap_days + 7)),
        )
        metric_rows += sync_weekly_metrics(
            conn,
            market,
            provider,
            incremental_tickers,
            start_date=(date.today() - timedelta(days=overlap_days + 7)),
        )
        counts["weekly_prices"] = weekly_rows
        counts["weekly_metrics"] = metric_rows
        counts["fetch_mode"] = "full_history" if force_full_history else "incremental"
        counts["full_history_tickers"] = len(full_tickers)
        counts["incremental_tickers"] = len(incremental_tickers)
        counts["history_overlap_days"] = history_overlap_days
        counts["reused_company_profiles"] = seeded_company_count
        missing_histories = sorted({str(ticker) for ticker in fetch_metadata.get("missing_histories", [])})
        stopped_on_rate_limit = bool(fetch_metadata.get("stopped_on_rate_limit"))
        counts["missing_history_count"] = len(missing_histories)
        counts["missing_histories"] = missing_histories
        counts["stopped_on_rate_limit"] = stopped_on_rate_limit
        if missing_histories:
            error_type = "rate_limit_deferred" if stopped_on_rate_limit else "no_price_data"
            error_message = (
                "Ticker was deferred after the provider rate limit stopped the batch"
                if stopped_on_rate_limit
                else "No price history was returned after batch and individual retries"
            )
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO fetch_errors (
                        refresh_job_id, refresh_batch_id, market, provider,
                        ticker, error_type, error_message
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (refresh_job_id or None, refresh_batch_id or None, market, provider,
                         ticker, error_type, error_message)
                        for ticker in missing_histories
                    ],
                )
            conn.commit()
        counts["market_status_refreshed"] = False
        if not refresh_batch_id:
            refresh_market_status(conn, market, provider)
            if force_full_history and target_price_basis:
                conn.execute(
                    "UPDATE market_status SET price_basis = %s WHERE market = %s AND provider = %s",
                    (target_price_basis, market, provider),
                )
                conn.commit()
            counts["market_status_refreshed"] = True
    if stopped_on_rate_limit:
        raise RuntimeError(
            f"Provider rate limit interrupted the batch after "
            f"{len(requested_tickers) - len(missing_histories)}/{len(requested_tickers)} tickers; "
            "partial rows were saved and the batch is safe to retry"
        )
    if not fetch_succeeded:
        raise RuntimeError("No price history was returned for any ticker in the batch")
    if not batch_refresh and force_full_history:
        upload_checkpoint(market, cache)
    if refresh_batch_id:
        with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE refresh_batches
                    SET status = 'succeeded', finished_at_utc = now(), error = NULL,
                        result_json = result_json || %s::jsonb
                    WHERE id = %s AND refresh_job_id = %s
                    """,
                    (Jsonb({"counts": counts}), refresh_batch_id, refresh_job_id),
                )
                active_batches, failed_batches = reconcile_refresh_job(cur, refresh_job_id)
                if active_batches == 0:
                    refresh_market_status(conn, market, provider)
                    if force_full_history and target_price_basis and failed_batches == 0:
                        cur.execute(
                            "UPDATE market_status SET price_basis = %s WHERE market = %s AND provider = %s",
                            (target_price_basis, market, provider),
                        )
                    counts["market_status_refreshed"] = True
            conn.commit()
        update_parent_fetch_job(
            parent_job_id,
            refresh_job_id,
            market=market,
            provider=provider,
            target_price_basis=target_price_basis if force_full_history else None,
        )
    else:
        update_refresh_job(
            refresh_job_id,
            status="succeeded",
            stage="Complete",
            completed_tickers=len(requested_tickers),
            finished_at_utc=datetime.now(timezone.utc),
            result_json=Jsonb(counts),
        )
        mark_refresh_batches(refresh_job_id, "succeeded")
    return counts


def run_filter(data: dict[str, Any]) -> dict[str, Any]:
    market = str(data.get("market") or "asx").lower()
    params = dict(data)
    params["market"] = market
    last_update = 0.0

    def progress(stage: str, current: int, total: int | None, message: str) -> None:
        nonlocal last_update
        now = time.monotonic()
        if now - last_update < 1.0 and total is not None and current < total:
            return
        last_update = now
        percent = round((current / total) * 100, 2) if total else 0
        update_job(
            status="running", stage=stage, current_count=current, total_count=total,
            percent=percent, detail=message,
        )

    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        ensure_schema(conn)
        result = run_postgres_filter(conn, params, progress)
    return {
        "filter": {key: value for key, value in result.items() if key != "results"},
        "results": result.get("results", []),
        "source": {"database": "postgresql", "market": market},
    }


def run_import_sqlite(data: dict[str, Any]) -> dict[str, Any]:
    market = str(data.get("market") or "asx").lower()
    if market not in {"asx", "us"}:
        raise RuntimeError("Import market must be asx or us")
    provider = str(data.get("provider") or "yfinance").strip() or "yfinance"
    storage_path = storage_object_path(data.get("storage_path"), ("imports/",))
    ratings_storage_path = data.get("ratings_storage_path")
    chunk_size = max(int(data.get("chunk_size") or 5000), 100)
    price_since = str(data.get("price_since") or "").strip() or None
    rebuild_weekly = bool(data.get("rebuild_weekly", True))

    cache = Path("/tmp") / f"import_{market}.sqlite"
    ratings_db = Path("/tmp") / f"ratings_{market}.sqlite"
    update_job(status="running", stage="Downloading", current_count=0, total_count=4, percent=0,
               detail=f"Downloading {storage_path}")
    download_storage_object(storage_path, cache)

    source_tickers = sqlite_price_tickers(cache)
    full_tickers = source_tickers if bool(data.get("full_tickers", True)) else None
    if ratings_storage_path:
        ratings_object = storage_object_path(ratings_storage_path, ("imports/",))
        download_storage_object(ratings_object, ratings_db)

    update_job(status="running", stage="Importing", current_count=1, total_count=4, percent=25,
               detail=f"Importing {len(source_tickers)} source tickers from SQLite")
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        ensure_schema(conn)
        counts = import_cache(
            conn,
            cache,
            market,
            chunk_size,
            False,
            price_since=price_since,
            full_tickers=full_tickers,
            resume=bool(data.get("resume", True)),
            bulk_prices=bool(data.get("bulk_prices", True)),
        )
        rating_count = 0
        if ratings_storage_path and ratings_db.exists():
            update_job(status="running", stage="Importing ratings", current_count=2, total_count=4, percent=50,
                       detail=f"Importing ratings from {ratings_storage_path}")
            rating_count = import_ratings(conn, ratings_db, chunk_size, False)

        weekly_rows = 0
        weekly_metric_rows = 0
        if rebuild_weekly and source_tickers:
            update_job(status="running", stage="Rebuilding weekly cache", current_count=3, total_count=4, percent=75,
                       detail=f"Rebuilding weekly candles and metrics for {len(source_tickers)} tickers")
            weekly_rows = sync_weekly_history(conn, market, provider, sorted(source_tickers), price_since)
            weekly_metric_rows = sync_weekly_metrics(conn, market, provider, sorted(source_tickers), price_since)
        refresh_market_status(conn, market, provider)

    return {
        "market": market,
        "storage_path": storage_path,
        "source_tickers": len(source_tickers),
        **counts,
        "ratings": rating_count,
        "weekly_rows": weekly_rows,
        "weekly_metric_rows": weekly_metric_rows,
    }


def run_export_ratings(data: dict[str, Any]) -> dict[str, Any]:
    requested_market = str(data.get("market") or "all").strip().lower()
    market = requested_market if requested_market in {"asx", "us"} else None
    limit = min(max(int(data.get("limit") or 25000), 1), 250000)
    output_format = "json" if str(data.get("format") or "csv").strip().lower() == "json" else "csv"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    default_path = f"exports/ratings/ratings_{requested_market or 'all'}_{timestamp}.{output_format}"
    object_name = storage_object_path(data.get("storage_path") or default_path, ("exports/",))
    local_path = Path("/tmp") / Path(object_name).name

    update_job(status="running", stage="Exporting", current_count=0, total_count=2, percent=0,
               detail=f"Exporting latest {limit} rating events")
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, event_at_utc, action, rated_by, user_email, firebase_uid,
                       market, scan_id, ticker, label, note, rank, signal_date,
                       close_price, market_cap, avg_volume, volume_ratio, sector,
                       industry, yahoo_url
                FROM rating_events
                WHERE (%s::text IS NULL OR market = %s)
                ORDER BY event_at_utc DESC
                LIMIT %s
                """,
                (market, market, limit),
            )
            columns = [desc.name for desc in cur.description]
            rows = cur.fetchall()

    if output_format == "json":
        payload_rows = [dict(zip(columns, [str(value) if isinstance(value, (datetime, date)) else value for value in row])) for row in rows]
        local_path.write_text(json.dumps(payload_rows, indent=2), encoding="utf-8")
    else:
        with local_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(columns)
            writer.writerows(rows)

    update_job(status="running", stage="Uploading", current_count=1, total_count=2, percent=50,
               detail=f"Uploading {object_name}")
    upload_storage_file(local_path, object_name)
    return {
        "market": requested_market,
        "format": output_format,
        "row_count": len(rows),
        "storage_bucket": storage_bucket_name(),
        "storage_path": object_name,
    }


def _rating_outcome_horizons(raw_value: Any) -> list[int]:
    raw_items = raw_value if isinstance(raw_value, list) else OUTCOME_HORIZONS
    horizons: list[int] = []
    for item in raw_items:
        try:
            value = int(item)
        except (TypeError, ValueError):
            continue
        if value in OUTCOME_HORIZONS and value not in horizons:
            horizons.append(value)
    return horizons or list(OUTCOME_HORIZONS)


def run_rating_outcomes(data: dict[str, Any]) -> dict[str, Any]:
    started = time.monotonic()
    requested_market = str(data.get("market") or "all").strip().lower()
    market = requested_market if requested_market in {"asx", "us"} else None
    horizons = _rating_outcome_horizons(data.get("horizons"))
    limit = min(max(int(data.get("limit") or 100000), 1), 500000)
    # Final outcomes are skipped unless an admin asks for a full re-measure.
    remeasure = data.get("remeasure") is True
    per_horizon: list[dict[str, Any]] = []

    update_job(
        status="running",
        stage="Preparing outcomes",
        current_count=0,
        total_count=len(horizons),
        percent=0,
        detail=f"Preparing rating outcomes for {requested_market.upper() if market else 'all markets'}",
    )
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::int
                FROM rating_events
                WHERE action = 'label'
                  AND label IS NOT NULL
                  AND market IS NOT NULL
                  AND ticker IS NOT NULL
                  AND (%s::text IS NULL OR market = %s)
                """,
                (market, market),
            )
            candidate_count = int(cur.fetchone()[0] or 0)

            for index, horizon in enumerate(horizons, 1):
                update_job(
                    status="running",
                    stage=f"Measuring {horizon} day outcomes",
                    current_count=index - 1,
                    total_count=len(horizons),
                    percent=round(((index - 1) / len(horizons)) * 100, 2),
                    detail=f"Calculating {horizon} day outcomes from saved ratings",
                )
                per_horizon.append(measure_rating_outcomes(
                    cur, market=market, limit=limit, horizon=horizon, remeasure=remeasure
                ))
            conn.commit()

        # Keep rating snapshots current without a manual backfill, e.g. after a
        # feature-version change. Only new (queued) snapshots are built here.
        seeded_snapshots = seed_snapshot_stubs(conn, market=market, limit=limit)
        rating_snapshots = build_rating_snapshots(
            conn,
            queued_snapshot_ids(conn, market=market, limit=limit, statuses=("queued",)),
            deadline=started + SCREEN_SNAPSHOT_BUDGET_SECONDS,
        )
        rating_snapshots["created_stubs"] = seeded_snapshots

        screen: dict[str, Any] | None = None
        if data.get("screen_observations", True) is not False:
            update_job(
                status="running",
                stage="Measuring screen hits and near-misses",
                current_count=0,
                total_count=None,
                percent=None,
                detail="Queueing screen observations and measuring their outcomes",
            )

            def screen_progress(stage: str, current: int, total: int | None, message: str) -> None:
                update_job(
                    status="running", stage=stage, current_count=current, total_count=total,
                    percent=round((current / total) * 100, 2) if total else None, detail=message,
                )

            screen = refresh_screen_observations(
                conn,
                market=market,
                horizons=horizons,
                limit=limit,
                deadline=started + SCREEN_SNAPSHOT_BUDGET_SECONDS,
                progress=screen_progress,
            )

    total_measured = sum(row["measured_count"] for row in per_horizon)
    return {
        "market": requested_market,
        "candidate_count": candidate_count,
        "limit": limit,
        "horizons": per_horizon,
        "measured_count": total_measured,
        "rating_snapshots": rating_snapshots,
        "screen_observations": screen,
    }


def run_publish_snapshot(data: dict[str, Any]) -> dict[str, Any]:
    bucket_name = storage_bucket_name()

    def progress(stage: str, current: int, total: int, message: str) -> None:
        update_job(
            status="running",
            stage=stage,
            current_count=current,
            total_count=total,
            percent=round((current / total) * 100, 2) if total else None,
            detail=message,
        )

    return publish_weekly_snapshot(
        os.environ["MONEYMAKER_DATABASE_URL"],
        bucket_name,
        progress,
    )


def run_insight_snapshot_backfill(data: dict[str, Any]) -> dict[str, Any]:
    requested_market = str(data.get("market") or "all").strip().lower()
    market = requested_market if requested_market in {"asx", "us"} else None
    owner_uid = str(data.get("owner_uid") or "").strip() or None
    raw_event_ids = data.get("event_ids") if isinstance(data.get("event_ids"), list) else []
    event_ids = [int(value) for value in raw_event_ids if str(value).isdigit()]
    labels = data.get("labels") if isinstance(data.get("labels"), list) else None
    limit = min(max(int(data.get("limit") or 100000), 1), 500000)

    update_job(
        status="running",
        stage="Loading appraisal events",
        current_count=0,
        total_count=None,
        percent=None,
        detail="Finding point-in-time appraisal snapshots to build",
    )
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"], row_factory=dict_row) as conn:
        ensure_schema(conn)
        created = 0
        if not event_ids:
            created = seed_snapshot_stubs(
                conn,
                market=market,
                owner_uid=owner_uid,
                labels=labels,
                limit=limit,
            )
        snapshot_ids = queued_snapshot_ids(
            conn,
            event_ids=event_ids,
            market=market,
            owner_uid=owner_uid,
            limit=limit,
        )
        built = build_rating_snapshots(conn, snapshot_ids)

    return {
        "market": requested_market,
        "owner_uid": owner_uid,
        "created_stubs": created,
        **built,
    }


def build_rating_snapshots(
    conn: psycopg.Connection,
    snapshot_ids: list[int],
    *,
    deadline: float | None = None,
) -> dict[str, Any]:
    """Build rating snapshots one at a time; stops cleanly at ``deadline``."""
    total = len(snapshot_ids)
    complete = failed = skipped = processed = 0
    failures: list[dict[str, Any]] = []
    definition_ids: dict[str, int] | None = None
    if snapshot_ids:
        # Register feature definitions once rather than for every snapshot.
        with conn.cursor(row_factory=dict_row) as cur:
            definition_ids = ensure_feature_definitions(cur)
        conn.commit()
    for index, snapshot_id in enumerate(snapshot_ids, 1):
        if deadline is not None and time.monotonic() >= deadline:
            break
        update_job(
            status="running",
            stage="Building point-in-time snapshots",
            current_count=index - 1,
            total_count=total,
            percent=round(((index - 1) / total) * 100, 2) if total else 100,
            detail=f"Building appraisal snapshot {index} of {total}",
        )
        processed += 1
        try:
            result = process_snapshot(conn, snapshot_id, definition_ids)
            conn.commit()
            if result.get("skipped"):
                skipped += 1
            elif result.get("status") == "complete":
                complete += 1
            else:
                failed += 1
                failures.append(result)
        except Exception as exc:
            conn.rollback()
            failed += 1
            failures.append({"snapshot_id": snapshot_id, "error": str(exc)})
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pick_feature_snapshots
                    SET snapshot_status = 'failed', error = %s, completed_at_utc = now()
                    WHERE id = %s AND snapshot_status <> 'complete'
                    """,
                    (str(exc)[:4000], snapshot_id),
                )
            conn.commit()
    return {
        "snapshot_count": total,
        "processed": processed,
        "complete": complete,
        "skipped": skipped,
        "failed": failed,
        "failures": failures[:100],
    }


def run_insight_analysis(data: dict[str, Any]) -> dict[str, Any]:
    requested_market = str(data.get("market") or "all").strip().lower()
    market = requested_market if requested_market in {"asx", "us"} else None
    scope = str(data.get("scope") or "mine").strip().lower()
    owner_uid = str(data.get("owner_uid") or data.get("requested_by_uid") or "").strip() if scope != "team" else None
    timing = str(data.get("timing") or "decision").strip().lower()
    horizon = int(data.get("horizon_days") or 84)
    feature_version = int(data.get("feature_version") or FEATURE_VERSION)
    requested_by_uid = str(data.get("requested_by_uid") or "").strip()
    requested_by_email = str(data.get("requested_by_email") or "").strip() or None
    config_hash = str(data.get("config_hash") or "").strip()
    run_id = job_id()

    update_job(status="running", stage="Loading point-in-time snapshots", detail="Loading mature winner appraisal snapshots")
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"], row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insight_runs (
                    id, requested_by_uid, requested_by_email, scope, owner_uid,
                    market, horizon_days, feature_version, outcome_version,
                    config_hash, configuration_json, status, stage, started_at_utc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'running', %s, now())
                ON CONFLICT (id) DO UPDATE SET status = 'running', stage = EXCLUDED.stage,
                    started_at_utc = COALESCE(insight_runs.started_at_utc, now()), error = NULL
                """,
                (run_id, requested_by_uid, requested_by_email, scope, owner_uid, requested_market,
                 horizon, feature_version, OUTCOME_VERSION, config_hash, Jsonb(data), "Loading point-in-time snapshots"),
            )
            cur.execute(
                """
                WITH decisions AS (
                    SELECT event.id, event.firebase_uid, event.market, event.ticker,
                           event.event_at_utc, decision.origin_event_id
                    FROM rating_events event
                    JOIN pick_feature_snapshots decision
                      ON decision.rating_event_id = event.id AND decision.feature_version = %s
                    WHERE event.action = 'label' AND event.label = 'winner'
                      AND (%s::text IS NULL OR event.market = %s)
                      AND (%s::text IS NULL OR event.firebase_uid = %s)
                ), selected AS (
                    SELECT decisions.*,
                           snapshot.id AS snapshot_id,
                           snapshot.rating_event_id AS outcome_event_id,
                           snapshot.technical_json, snapshot.fundamental_json
                    FROM decisions
                    JOIN pick_feature_snapshots snapshot
                      ON snapshot.rating_event_id = CASE
                           WHEN %s = 'origin' THEN decisions.origin_event_id ELSE decisions.id END
                     AND snapshot.feature_version = %s
                     AND snapshot.snapshot_status = 'complete'
                ), measured AS (
                    SELECT selected.*, snapshot.feature_as_of_date,
                           outcome.benchmark_excess_return_percent,
                           outcome.maximum_drawdown_percent, outcome.target_hit,
                           -- Re-labels, origin timing, and several team members
                           -- rating the same stock in the same week all produce
                           -- near-identical features and outcomes. Count each
                           -- stock once per anchor week so repeats cannot
                           -- inflate the evidence.
                           ROW_NUMBER() OVER (
                               PARTITION BY selected.market, selected.ticker,
                                            date_trunc('week', snapshot.feature_as_of_date)
                               ORDER BY selected.event_at_utc, selected.id
                           ) AS week_rank
                    FROM selected
                    JOIN pick_feature_snapshots snapshot ON snapshot.id = selected.snapshot_id
                    JOIN rating_outcomes outcome
                      ON outcome.rating_event_id = selected.outcome_event_id
                     AND outcome.horizon_days = %s
                     AND outcome.outcome_version = %s
                     AND outcome.benchmark_excess_return_percent IS NOT NULL
                )
                SELECT *
                FROM measured
                ORDER BY event_at_utc, id
                """,
                (feature_version, market, market, owner_uid, owner_uid, timing, feature_version, horizon, OUTCOME_VERSION),
            )
            measured_rows = list(cur.fetchall())
            rows = [row for row in measured_rows if row["week_rank"] == 1]
            duplicates_removed = len(measured_rows) - len(rows)

            records: list[dict[str, Any]] = []
            for row in rows:
                technical = row.get("technical_json") or {}
                fundamentals = row.get("fundamental_json") or {}
                features = {
                    name: payload.get("value")
                    for name, payload in {**technical, **fundamentals}.items()
                    if isinstance(payload, dict) and isinstance(payload.get("value"), (int, float))
                    and not isinstance(payload.get("value"), bool)
                }
                records.append({
                    "rating_event_id": row["outcome_event_id"],
                    "ticker": row["ticker"],
                    "market": row["market"],
                    "event_at_utc": row["event_at_utc"].isoformat(),
                    # Picks anchored in the same week share market conditions;
                    # the bootstrap resamples whole weeks.
                    "cluster": "{}-W{:02d}".format(*row["feature_as_of_date"].isocalendar()[:2]),
                    "benchmark_excess_return_percent": row["benchmark_excess_return_percent"],
                    "maximum_drawdown_percent": row["maximum_drawdown_percent"],
                    "target_hit": row["target_hit"],
                    "features": features,
                })

            update_job(status="running", stage="Testing individual features", current_count=0,
                       total_count=len(records), percent=None, detail=f"Comparing {len(records)} mature winner appraisals")
            analysis = analyze_numeric_features(records)
            update_job(status="running", stage="Validating patterns", current_count=len(records),
                       total_count=len(records), percent=None, detail="Running chronological walk-forward validation")
            model_result = run_validated_models(records, horizon_days=horizon)
            cur.execute("DELETE FROM insight_findings WHERE run_id = %s", (run_id,))
            for finding in analysis["findings"]:
                direction = "higher" if finding["spearman_rho"] >= 0 else "lower"
                title = f"{finding['feature']}: {direction} values went with better excess returns"
                top = finding["quintiles"][-1]["mean_excess"]
                bottom = finding["quintiles"][0]["mean_excess"]
                explanation = (
                    f"Rank correlation {finding['spearman_rho']:+.2f} "
                    f"(95% CI {finding['rho_ci_low']:+.2f} to {finding['rho_ci_high']:+.2f}) across "
                    f"{finding['available_count']} appraisals in {finding['cluster_count']} signal weeks. "
                    + (f"Highest fifth averaged {top:+.1f}% vs the benchmark, lowest fifth {bottom:+.1f}%. "
                       if top is not None and bottom is not None else "")
                    + ("Passed the candidate checks." if finding["status"] == "candidate"
                       else "Exploratory: " + "; ".join(finding["status_reasons"]) + ".")
                )
                cur.execute(
                    """
                    INSERT INTO insight_findings (
                        run_id, finding_id, finding_type, title, explanation,
                        condition_json, feature_names_json, support_count,
                        average_excess_return, confidence_interval_json, adjusted_p_value,
                        in_sample_metrics_json, status
                    ) VALUES (%s, %s, 'univariate', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (run_id, finding["finding_id"], title, explanation,
                     Jsonb({"direction": direction}), Jsonb([finding["feature"]]),
                     finding["available_count"], finding["top_minus_bottom_quintile_excess"],
                     Jsonb({"spearman_rho": [finding["rho_ci_low"], finding["rho_ci_high"]], "level": 0.95}),
                     finding["adjusted_p_value"], Jsonb(finding), finding["status"]),
                )
            if model_result.get("enabled"):
                holdout_auc = model_result.get("holdout", {}).get("auc")
                for feature in model_result.get("features", [])[:10]:
                    if float(feature.get("direction_stability") or 0) < 1:
                        continue
                    direction = "higher" if float(feature["coefficient"]) > 0 else "lower"
                    finding_identity = json.dumps({"feature": feature["feature"], "direction": direction, "version": feature_version}, sort_keys=True)
                    finding_id = hashlib.sha256(finding_identity.encode()).hexdigest()[:20]
                    # A model relationship is only a candidate when it ranks
                    # unseen appraisals better than chance in both the
                    # walk-forward folds and the untouched holdout.
                    status = (
                        "candidate"
                        if holdout_auc is not None and float(holdout_auc) >= MODEL_CANDIDATE_AUC
                        and float(model_result.get("walk_forward_auc_mean") or 0) >= MODEL_CANDIDATE_AUC
                        else "exploratory"
                    )
                    cur.execute(
                        """
                        INSERT INTO insight_findings (
                            run_id, finding_id, finding_type, title, explanation,
                            condition_json, feature_names_json, support_count,
                            in_sample_metrics_json, out_of_sample_metrics_json, status
                        ) VALUES (%s, %s, 'univariate', %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (run_id, finding_id) DO NOTHING
                        """,
                        (
                            run_id, finding_id,
                            f"{feature['feature']} has a stable {direction} model relationship",
                            "The coefficient direction was stable across chronological folds; inspect counterexamples before using it as a rule.",
                            Jsonb({"direction": direction}), Jsonb([feature["feature"]]),
                            model_result["record_count"], Jsonb(feature),
                            Jsonb({"folds": model_result["folds"], "holdout": model_result["holdout"]}), status,
                        ),
                    )
            cur.execute(
                """
                UPDATE insight_runs
                SET status = 'succeeded', stage = 'Complete', cohort_json = %s,
                    validation_json = %s, performance_json = %s, finished_at_utc = now()
                WHERE id = %s
                """,
                (Jsonb(analysis["cohorts"]), Jsonb(model_result),
                 Jsonb({"gate": analysis["gate"], "mature_count": analysis["mature_count"],
                        "duplicates_removed": duplicates_removed}), run_id),
            )
        conn.commit()
    return {"run_id": run_id, "duplicates_removed": duplicates_removed, **analysis}


def run_fundamentals_refresh(data: dict[str, Any]) -> dict[str, Any]:
    requested_market = str(data.get("market") or "us").strip().lower()
    if requested_market != "us":
        raise RuntimeError("SEC fundamentals refresh currently supports the US market only")
    limit = min(max(int(data.get("limit") or 6000), 1), 10000)
    force = bool(data.get("force", False))
    requested_tickers = data.get("tickers") if isinstance(data.get("tickers"), list) else []
    update_job(status="running", stage="Loading SEC company list", detail="Selecting US companies needing filing refresh")
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"], row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            if requested_tickers:
                tickers = sorted({str(value).strip().upper() for value in requested_tickers if str(value).strip()})[:limit]
            else:
                cur.execute(
                    """
                    SELECT DISTINCT ph.ticker
                    FROM price_history ph
                    WHERE ph.market = 'us' AND ph.provider = 'yfinance'
                      AND ph.ticker !~ '[=^]'
                      AND (
                        %s OR NOT EXISTS (
                          SELECT 1 FROM fundamental_facts facts
                          WHERE facts.market = 'us' AND facts.ticker = ph.ticker
                            AND facts.source_name = 'sec_companyfacts'
                            AND facts.fetched_at_utc >= now() - interval '7 days'
                        )
                      )
                    ORDER BY ph.ticker
                    LIMIT %s
                    """,
                    (force, limit),
                )
                tickers = [str(row["ticker"]) for row in cur.fetchall()]

        def progress(current: int, total: int, ticker: str) -> None:
            update_job(
                status="running",
                stage="Refreshing SEC filings",
                current_count=current,
                total_count=total,
                percent=round((current / total) * 100, 2) if total else 100,
                detail=f"Loading SEC filings for {ticker} ({current + 1} of {total})",
            )

        result = refresh_sec_fundamentals(conn, tickers, progress=progress)
    return {"market": "us", "force": force, **result}


def main() -> int:
    kind = os.environ.get("MONEYMAKER_JOB_TYPE", "").strip().lower()
    data = payload()
    try:
        update_job(status="running", stage="Starting worker", detail=f"Starting {kind} worker")
        if kind == "fetch":
            result = run_fetch(data)
        elif kind == "filter":
            result = run_filter(data)
        elif kind == "import-sqlite":
            result = run_import_sqlite(data)
        elif kind == "export-ratings":
            result = run_export_ratings(data)
        elif kind == "publish-snapshot":
            result = run_publish_snapshot(data)
        elif kind == "rating-outcomes":
            result = run_rating_outcomes(data)
        elif kind == "insight-snapshot-backfill":
            result = run_insight_snapshot_backfill(data)
        elif kind == "insight-run":
            result = run_insight_analysis(data)
        elif kind == "fundamentals-refresh":
            result = run_fundamentals_refresh(data)
        else:
            raise RuntimeError(f"Unknown worker job type: {kind}")
        update_job(status="succeeded", stage="Complete", current_count=1, total_count=1,
                   percent=100, detail="Screen complete" if kind == "filter" else json.dumps(result)[:4000],
                   finished_at_utc=datetime.now(timezone.utc),
                   event_metadata=result.get("performance", {}),
                   parameters_json=Jsonb(result.get("filter", result)),
                   result_json=Jsonb(result.get("results", [])))
        return 0
    except Exception as exc:
        update_job(status="failed", stage="Failed", error=str(exc),
                   detail="".join(traceback.format_exception(exc))[-4000:],
                   finished_at_utc=datetime.now(timezone.utc))
        if kind == "insight-run":
            run_id = str(data.get("run_id") or data.get("insight_run_id") or "").strip()
            if run_id:
                with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE insight_runs
                            SET status = 'failed', stage = 'Failed', error = %s,
                                finished_at_utc = now()
                            WHERE id = %s
                            """,
                            (str(exc), run_id),
                        )
                    conn.commit()
        refresh_job_id = str(data.get("refresh_job_id") or "").strip()
        refresh_batch_id = str(data.get("refresh_batch_id") or "").strip()
        parent_job_id = str(data.get("parent_job_id") or "").strip()
        if refresh_batch_id:
            with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE refresh_batches
                        SET status = 'failed', finished_at_utc = now(), error = %s
                        WHERE id = %s AND refresh_job_id = %s AND status <> 'succeeded'
                        """,
                        (str(exc), refresh_batch_id, refresh_job_id),
                    )
                    reconcile_refresh_job(cur, refresh_job_id, str(exc))
                conn.commit()
            update_parent_fetch_job(parent_job_id, refresh_job_id, detail=f"Batch failed: {str(exc)[:500]}")
        else:
            update_refresh_job(
                refresh_job_id,
                status="failed",
                stage="Failed",
                error=str(exc),
                finished_at_utc=datetime.now(timezone.utc),
            )
            mark_refresh_batches(refresh_job_id, "failed", str(exc))
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
    # yfinance and curl helpers can leave non-daemon threads alive after the
    # final DB write commits. Exit immediately so Cloud Run does not wait for
    # them after a completed/failed job run.
    os._exit(exit_code)
