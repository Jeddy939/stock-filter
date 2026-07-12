"""Cloud Run Job worker for fetches and screens.

The worker uses a SQLite checkpoint in Cloud Storage for compatibility with the
current fetcher, then imports the changed checkpoint into PostgreSQL. This is
an intentional bridge while the fetcher is being converted to write directly
to the online schema.
"""

from __future__ import annotations

import csv
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
from firebase.migrate_sqlite_to_postgres import import_cache, import_ratings, sqlite_connection
from cloud_backend.market_status import refresh_market_status
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
    update_job_id(job_id(), **values)


def update_job_id(target_job_id: str, **values: Any) -> None:
    if not target_job_id:
        return
    values.setdefault("log_tail", "")
    assignments = ", ".join(f"{key} = %s" for key in values)
    with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE job_runs SET {assignments} WHERE id = %s", (*values.values(), target_job_id))
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


def update_parent_fetch_job(parent_job_id: str, refresh_job_id: str, detail: str | None = None) -> None:
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
                    finished_at_utc = COALESCE(%s, finished_at_utc)
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


def run_fetch(data: dict[str, Any]) -> dict[str, Any]:
    market = str(data.get("market") or "asx").lower()
    provider = fetcher.normalize_provider(data.get("provider") or fetcher.DEFAULT_PROVIDER)
    refresh_job_id = str(data.get("refresh_job_id") or "").strip()
    refresh_batch_id = str(data.get("refresh_batch_id") or "").strip()
    parent_job_id = str(data.get("parent_job_id") or "").strip()
    cache = checkpoint_path(market)
    download_checkpoint(market, cache)
    explicit_tickers = [
        str(ticker).strip().upper()
        for ticker in (data.get("tickers") or [])
        if str(ticker).strip()
    ]
    ticker_file = str(data.get("ticker_file") or (
        "us_tickers_nasdaqtrader.txt" if market == "us" else "asx_yfinance_valid_stocks_2026-05-11.txt"
    ))
    ticker_path = ROOT / ticker_file if not Path(ticker_file).is_absolute() else Path(ticker_file)
    requested_tickers = explicit_tickers or fetcher.get_tickers_from_file(str(ticker_path))
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
    params.pop("tickers", None)
    params.pop("parent_job_id", None)
    temp_ticker_file = None
    if explicit_tickers:
        temp_ticker_file = Path("/tmp") / f"tickers_{job_id()}.txt"
        temp_ticker_file.write_text("\n".join(requested_tickers) + "\n", encoding="utf-8")
    fetcher.fetch_stock_data(
        ticker_file=str(temp_ticker_file or ticker_path),
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
        refresh_market_status(conn, market, provider)
        counts["weekly_prices"] = weekly_rows
        counts["weekly_metrics"] = metric_rows
        counts["market_status_refreshed"] = True
    upload_checkpoint(market, cache)
    if refresh_batch_id:
        with psycopg.connect(os.environ["MONEYMAKER_DATABASE_URL"]) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE refresh_batches
                    SET status = 'succeeded', finished_at_utc = now(), result_json = result_json || %s::jsonb
                    WHERE id = %s AND refresh_job_id = %s
                    """,
                    (Jsonb({"counts": counts}), refresh_batch_id, refresh_job_id),
                )
                cur.execute(
                    """
                    UPDATE refresh_jobs
                    SET
                        completed_tickers = completed_tickers + %s,
                        stage = 'Fetching batches',
                        status = CASE
                            WHEN NOT EXISTS (
                                SELECT 1 FROM refresh_batches
                                WHERE refresh_job_id = %s
                                  AND status IN ('queued', 'running')
                            )
                            AND EXISTS (
                                SELECT 1 FROM refresh_batches
                                WHERE refresh_job_id = %s
                                  AND status = 'failed'
                            )
                            THEN 'failed'
                            WHEN NOT EXISTS (
                                SELECT 1 FROM refresh_batches
                                WHERE refresh_job_id = %s
                                  AND status IN ('queued', 'running')
                            )
                            THEN 'succeeded'
                            ELSE 'running'
                        END,
                        finished_at_utc = CASE
                            WHEN NOT EXISTS (
                                SELECT 1 FROM refresh_batches
                                WHERE refresh_job_id = %s
                                  AND status IN ('queued', 'running')
                            )
                            THEN now()
                            ELSE finished_at_utc
                        END
                    WHERE id = %s
                    """,
                    (
                        len(requested_tickers),
                        refresh_job_id,
                        refresh_job_id,
                        refresh_job_id,
                        refresh_job_id,
                        refresh_job_id,
                    ),
                )
            conn.commit()
        update_parent_fetch_job(parent_job_id, refresh_job_id)
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


def main() -> None:
    kind = os.environ.get("MONEYMAKER_JOB_TYPE", "").strip().lower()
    data = payload()
    try:
        update_job(status="running", stage="Starting", detail=f"Starting {kind} job")
        if kind == "fetch":
            result = run_fetch(data)
        elif kind == "filter":
            result = run_filter(data)
        elif kind == "import-sqlite":
            result = run_import_sqlite(data)
        elif kind == "export-ratings":
            result = run_export_ratings(data)
        else:
            raise RuntimeError(f"Unknown worker job type: {kind}")
        update_job(status="succeeded", stage="Complete", current_count=1, total_count=1,
                   percent=100, detail=json.dumps(result)[:4000],
                   parameters_json=Jsonb(result.get("filter", result)),
                   result_json=Jsonb(result.get("results", [])))
    except Exception as exc:
        update_job(status="failed", stage="Failed", error=str(exc),
                   detail="".join(traceback.format_exception(exc))[-4000:])
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
                        WHERE id = %s AND refresh_job_id = %s
                        """,
                        (str(exc), refresh_batch_id, refresh_job_id),
                    )
                    cur.execute(
                        """
                        UPDATE refresh_jobs
                        SET failed_tickers = failed_tickers + %s,
                            stage = 'Batch failed',
                            error = %s
                        WHERE id = %s
                        """,
                        (max(len(data.get("tickers") or []), 1), str(exc), refresh_job_id),
                    )
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
        raise


if __name__ == "__main__":
    main()
