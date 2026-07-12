"""Cloud Run Job worker for fetches and screens.

The worker uses a SQLite checkpoint in Cloud Storage for compatibility with the
current fetcher, then imports the changed checkpoint into PostgreSQL. This is
an intentional bridge while the fetcher is being converted to write directly
to the online schema.
"""

from __future__ import annotations

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
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moneymaker import fetcher
from firebase.migrate_sqlite_to_postgres import import_cache
from cloud_backend.postgres_screener import run_postgres_filter
from cloud_backend.weekly_cache import sync_weekly_history
from cloud_backend.weekly_metrics import sync_weekly_metrics


def job_id() -> str:
    value = os.environ.get("MONEYMAKER_JOB_ID", "").strip()
    if not value:
        raise RuntimeError("MONEYMAKER_JOB_ID is required")
    return value


def payload() -> dict[str, Any]:
    return json.loads(os.environ.get("MONEYMAKER_JOB_PAYLOAD", "{}"))


def update_job(**values: Any) -> None:
    values.setdefault("log_tail", "")
    assignments = ", ".join(f"{key} = %s" for key in values)
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE job_runs SET {assignments} WHERE id = %s", (*values.values(), job_id()))
        conn.commit()


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


def ensure_schema(conn: psycopg.Connection) -> None:
    schema = (ROOT / "firebase" / "migrations" / "001_schema.sql").read_text(encoding="utf-8")
    conn.execute(schema)
    conn.commit()


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


def run_fetch(data: dict[str, Any]) -> dict[str, Any]:
    market = str(data.get("market") or "asx").lower()
    provider = fetcher.normalize_provider(data.get("provider") or fetcher.DEFAULT_PROVIDER)
    refresh_job_id = str(data.get("refresh_job_id") or "").strip()
    cache = checkpoint_path(market)
    download_checkpoint(market, cache)
    ticker_file = str(data.get("ticker_file") or (
        "us_tickers_nasdaqtrader.txt" if market == "us" else "asx_yfinance_valid_stocks_2026-05-11.txt"
    ))
    ticker_path = ROOT / ticker_file if not Path(ticker_file).is_absolute() else Path(ticker_file)
    requested_tickers = fetcher.get_tickers_from_file(str(ticker_path))
    requested_tickers = fetcher.apply_ticker_limit(requested_tickers, data.get("limit"))
    update_refresh_job(
        refresh_job_id,
        status="running",
        stage="Fetching",
        total_tickers=len(requested_tickers),
    )
    mark_refresh_batches(refresh_job_id, "running")
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT ticker FROM price_history WHERE market = %s", (market,))
            existing_tickers = {row[0] for row in cur.fetchall()}
    full_tickers = set(requested_tickers) - existing_tickers
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
    fetcher.fetch_stock_data(
        ticker_file=str(ticker_path),
        output="/tmp/cloud_fetch.json",
        cache_file=str(cache),
        progress_callback=progress,
        export_json=False,
        **{key: value for key, value in params.items() if key in {
            "years", "workers", "provider", "limit", "info_refresh_days",
            "history_refresh_days", "prune_missing_tickers", "history_chunk_size",
            "history_pause_seconds", "info_pause_seconds", "rate_limit_pause_seconds",
            "max_rate_limit_retries", "stop_on_rate_limit"
        }},
    )
    # SQLite may still have an open WAL after a large batch. Compact it before
    # uploading the checkpoint so no committed rows are stranded in a sidecar.
    import sqlite3
    with sqlite3.connect(str(cache)) as checkpoint:
        checkpoint.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    overlap_days = max(30, int(data.get("history_refresh_days") or 5) + 7)
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
    upload_checkpoint(market, cache)
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
        result = run_postgres_filter(conn, params, progress)
    return {
        "filter": {key: value for key, value in result.items() if key != "results"},
        "results": result.get("results", []),
        "source": {"database": "postgresql", "market": market},
    }


def main() -> None:
    kind = os.environ.get("MONEYMAKER_JOB_TYPE", "").strip().lower()
    data = payload()
    try:
        update_job(status="running", stage="Starting", detail=f"Starting {kind} job")
        result = run_fetch(data) if kind == "fetch" else run_filter(data)
        update_job(status="succeeded", stage="Complete", current_count=1, total_count=1,
                   percent=100, detail=json.dumps(result)[:4000],
                   parameters_json=Jsonb(result.get("filter", result)),
                   result_json=Jsonb(result.get("results", [])))
    except Exception as exc:
        update_job(status="failed", stage="Failed", error=str(exc),
                   detail="".join(traceback.format_exception(exc))[-4000:])
        refresh_job_id = str(data.get("refresh_job_id") or "").strip()
        update_refresh_job(
            refresh_job_id,
            status="failed",
            stage="Failed",
            error=str(exc),
            finished_at_utc=datetime.now(timezone.utc),
        )
        mark_refresh_batches(refresh_job_id, "failed", str(exc))
        raise


if __name__ == "__main__":
    main()
