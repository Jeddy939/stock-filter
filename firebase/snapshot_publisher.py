"""Publish an immutable, browser-readable weekly market snapshot.

Cloud SQL remains the build database. The hosted application reads the
published Storage objects while Cloud SQL is stopped between weekly builds.
"""

from __future__ import annotations

import gzip
import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable
from uuid import UUID

from google.cloud import firestore, storage
import psycopg
from psycopg.rows import dict_row


Progress = Callable[[str, int, int, str], None]
SNAPSHOT_SCHEMA_VERSION = 2
SUPPORTED_MARKETS = ("asx", "us")


def _json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    return value


def _json_default(value: Any) -> Any:
    converted = _json_value(value)
    if converted is value:
        raise TypeError(f"Cannot serialize {type(value).__name__}")
    return converted


def _safe_ticker(ticker: str) -> str:
    return re.sub(r"[^A-Z0-9._^=-]+", "_", ticker.upper())


def _two_year_cutoff(value: date) -> date:
    try:
        return value.replace(year=value.year - 2)
    except ValueError:
        return value.replace(year=value.year - 2, day=28)


def _above_180_for_two_years(rows: Iterable[dict[str, Any]]) -> bool:
    """Match weekly_metrics' W-MON candles and prior-week MA180 calculation."""
    weekly: list[tuple[date, float]] = []
    active_week: date | None = None
    close: float | None = None
    for row in rows:
        price_date = row.get("price_date")
        close_price = row.get("close_price")
        if not isinstance(price_date, date) or close_price is None:
            continue
        week_date = price_date + timedelta(days=(-price_date.weekday()) % 7)
        if active_week is not None and week_date != active_week and close is not None:
            weekly.append((active_week, close))
        active_week = week_date
        close = float(close_price)
    if active_week is not None and close is not None:
        weekly.append((active_week, close))
    if not weekly:
        return False

    cutoff = _two_year_cutoff(weekly[-1][0])
    comparisons: list[bool] = []
    rolling_sum = 0.0
    closes: list[float] = []
    for week_date, weekly_close in weekly:
        prior_count = len(closes)
        if week_date >= cutoff and prior_count >= 144:
            window_count = min(180, prior_count)
            moving_average = rolling_sum / window_count
            comparisons.append(weekly_close > moving_average)
        closes.append(weekly_close)
        rolling_sum += weekly_close
        if len(closes) > 180:
            rolling_sum -= closes[-181]
    return len(comparisons) >= 100 and all(comparisons)


def _upload_json(
    bucket: storage.Bucket,
    object_name: str,
    payload: Any,
    *,
    immutable: bool,
    gzip_payload: bool = True,
) -> None:
    raw = json.dumps(payload, separators=(",", ":"), default=_json_default).encode("utf-8")
    blob = bucket.blob(object_name)
    blob.content_type = "application/json; charset=utf-8"
    blob.cache_control = "public, max-age=31536000, immutable" if immutable else "no-cache, max-age=0"
    if gzip_payload:
        raw = gzip.compress(raw, compresslevel=6)
        blob.content_encoding = "gzip"
    blob.upload_from_string(raw, content_type=blob.content_type)
    blob.patch()


def _company_profile(info: Any, ticker: str) -> dict[str, Any]:
    source = info if isinstance(info, dict) else {}

    def text(*keys: str) -> str:
        for key in keys:
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    summary = text("summary", "longBusinessSummary", "description")
    return {
        "name": text("name", "longName", "shortName") or ticker,
        "sector": text("sector"),
        "industry": text("industry"),
        "country": text("country"),
        "website": text("website"),
        "yahoo_url": text("yahoo_url") or f"https://finance.yahoo.com/quote/{ticker}",
        "summary": summary[:4000],
    }


def _latest_scan(cur: psycopg.Cursor[Any], market: str) -> dict[str, Any]:
    cur.execute(
        """
        SELECT id, source_id, created_at_utc, provider, query, scanned_count,
               result_count, skipped_no_history, config_json, config_hash,
               market_snapshot_date
        FROM scan_runs
        WHERE market = %s AND result_count > 0
        ORDER BY created_at_utc DESC
        LIMIT 1
        """,
        (market,),
    )
    scan = cur.fetchone()
    if not scan:
        raise RuntimeError(f"No completed {market.upper()} scan is available to publish")
    cur.execute(
        """
        SELECT id, scan_id, source_id, rank, ticker, signal_date, close_price,
               market_cap, avg_volume, volume_ratio, sector, industry, result_json
        FROM scan_results
        WHERE scan_id = %s
        ORDER BY rank
        """,
        (scan["id"],),
    )
    results: list[dict[str, Any]] = []
    for row in cur.fetchall():
        source = row.get("result_json") if isinstance(row.get("result_json"), dict) else {}
        results.append(
            {
                **source,
                "id": row["id"],
                "scan_id": row["scan_id"],
                "source_id": row["source_id"],
                "rank": row["rank"],
                "ticker": row["ticker"],
                "date": _json_value(source.get("date") or row.get("signal_date")),
                "close_price": _json_value(row.get("close_price")),
                "market_cap": _json_value(row.get("market_cap")),
                "avg_volume": _json_value(row.get("avg_volume")),
                "volume_ratio": _json_value(row.get("volume_ratio")),
                "sector": row.get("sector"),
                "industry": row.get("industry"),
            }
        )
    return {
        "ok": True,
        "scan": {key: _json_value(value) for key, value in scan.items()},
        "results": results,
    }


def _market_status(cur: psycopg.Cursor[Any], market: str) -> dict[str, Any]:
    cur.execute("SELECT * FROM market_status WHERE market = %s AND provider = 'yfinance'", (market,))
    status = cur.fetchone() or {}
    cur.execute(
        """
        SELECT id, status, stage, total_tickers, completed_tickers, failed_tickers,
               started_at_utc, finished_at_utc, error
        FROM refresh_jobs
        WHERE market = %s
        ORDER BY started_at_utc DESC
        LIMIT 1
        """,
        (market,),
    )
    refresh = cur.fetchone()
    return {
        "market": market,
        "provider": "yfinance",
        "latest_bar_date": _json_value(status.get("latest_date")),
        "database_refreshed_at_utc": _json_value(status.get("refreshed_at_utc")),
        "covered_tickers": int(status.get("ticker_count") or 0),
        "history_rows": int(status.get("history_rows") or 0),
        "weekly_metric_rows": int(status.get("weekly_rows") or 0),
        "price_basis": status.get("price_basis"),
        "latest_refresh": {key: _json_value(value) for key, value in refresh.items()} if refresh else None,
    }


def _publish_market_files(
    conn: psycopg.Connection[Any],
    bucket: storage.Bucket,
    version_root: str,
    market: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    with conn.cursor(row_factory=dict_row) as cur:
        scan_payload = _latest_scan(cur, market)
        status_payload = _market_status(cur, market)
        cur.execute(
            """
            WITH recent_coverage AS (
              SELECT week_date, COUNT(*)::int AS ticker_count
              FROM weekly_metrics
              WHERE market = %s AND provider = 'yfinance'
                AND week_date >= (
                  SELECT MAX(week_date) - INTERVAL '8 weeks'
                  FROM weekly_metrics
                  WHERE market = %s AND provider = 'yfinance'
                )
              GROUP BY week_date
            ), covered_week AS (
              SELECT MAX(week_date) AS week_date
              FROM recent_coverage
              WHERE ticker_count >= (SELECT MAX(ticker_count) * 0.5 FROM recent_coverage)
            )
            SELECT metrics.ticker, metrics.week_date, metrics.close_price,
                   metrics.previous_close_price, metrics.weekly_volume,
                   metrics.market_cap, metrics.avg_volume_52, metrics.volume_ratio_52,
                   metrics.price_avg_1, metrics.ma_30, metrics.ma_90, metrics.ma_180,
                   metrics.ma_360, metrics.ma_700, metrics.available_weeks,
                   metrics.history_weeks, metrics.sector, metrics.industry
            FROM weekly_metrics metrics
            WHERE metrics.market = %s AND metrics.provider = 'yfinance'
              AND metrics.week_date = (SELECT week_date FROM covered_week)
            ORDER BY metrics.ticker
            """,
            (market, market, market),
        )
        metrics = [{key: _json_value(value) for key, value in row.items()} for row in cur.fetchall()]
        cur.execute("SELECT ticker, info_json FROM companies WHERE market = %s ORDER BY ticker", (market,))
        profiles = {row["ticker"]: _company_profile(row.get("info_json"), row["ticker"]) for row in cur.fetchall()}

    root = f"{version_root}/markets/{market}"
    _upload_json(bucket, f"{root}/scan.json", scan_payload, immutable=True)
    _upload_json(bucket, f"{root}/metrics.json", {"ok": True, "market": market, "metrics": metrics}, immutable=True)
    _upload_json(bucket, f"{root}/companies.json", {"ok": True, "market": market, "companies": profiles}, immutable=True)
    return scan_payload, status_payload, profiles


def _write_chart(
    bucket: storage.Bucket,
    object_name: str,
    market: str,
    ticker: str,
    profile: dict[str, Any],
    rows: Iterable[dict[str, Any]],
    snapshot_id: str,
) -> None:
    payload = {
        "ok": True,
        "snapshot_id": snapshot_id,
        "market": market,
        "provider": "yfinance",
        "ticker": ticker,
        "company": profile,
        "columns": ["date", "open", "high", "low", "close", "volume"],
        "rows": [
            [
                _json_value(row["price_date"]),
                _json_value(row["open_price"]),
                _json_value(row["high_price"]),
                _json_value(row["low_price"]),
                _json_value(row["close_price"]),
                _json_value(row["volume"]),
            ]
            for row in rows
        ],
    }
    _upload_json(bucket, object_name, payload, immutable=True)


def _publish_charts(
    conn: psycopg.Connection[Any],
    bucket: storage.Bucket,
    version_root: str,
    profiles_by_market: dict[str, dict[str, dict[str, Any]]],
    progress: Progress,
    snapshot_id: str,
) -> tuple[dict[str, int], dict[str, dict[str, dict[str, Any]]]]:
    totals = {market: len(profiles_by_market.get(market, {})) for market in SUPPORTED_MARKETS}
    total = sum(totals.values())
    completed = 0
    counts = {market: 0 for market in SUPPORTED_MARKETS}
    screen_data: dict[str, dict[str, dict[str, Any]]] = {market: {} for market in SUPPORTED_MARKETS}

    for market in SUPPORTED_MARKETS:
        with conn.cursor(row_factory=dict_row) as cur:
            for ticker in sorted(profiles_by_market.get(market, {})):
                cur.execute(
                """
                SELECT ticker, price_date, open_price, high_price, low_price, close_price, volume
                FROM price_history
                WHERE market = %s AND provider = 'yfinance' AND ticker = %s
                ORDER BY price_date
                """,
                    (market, ticker),
                )
                ticker_rows = cur.fetchall()
                if not ticker_rows:
                    continue
                latest = ticker_rows[-1]
                screen_data[market][ticker] = {
                    "latest_daily_date": _json_value(latest.get("price_date")),
                    "latest_daily_close": _json_value(latest.get("close_price")),
                    "above_180_for_2y": _above_180_for_two_years(ticker_rows),
                }
                _write_chart(
                    bucket,
                    f"{version_root}/charts/{market}/{_safe_ticker(ticker)}.json",
                    market,
                    ticker,
                    profiles_by_market[market].get(ticker, _company_profile({}, ticker)),
                    ticker_rows,
                    snapshot_id,
                )
                completed += 1
                counts[market] += 1
                if completed % 25 == 0 or completed == total:
                    progress("Publishing charts", completed, total, f"Published {market.upper()} chart {ticker}")
    return counts, screen_data


def _migrate_collaboration(conn: psycopg.Connection[Any], snapshot_id: str) -> dict[str, int]:
    client = firestore.Client()
    user_count = 0
    appraisal_count = 0
    event_count = 0
    feedback_count = 0
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT profile.firebase_uid, profile.email, profile.display_name,
                   invite.role, invite.status
            FROM user_profiles profile
            JOIN app_user_invites invite
              ON lower(invite.email) = lower(profile.email)
            WHERE profile.firebase_uid IS NOT NULL
              AND invite.status = 'active'
            """
        )
        for row in cur.fetchall():
            source_role = str(row.get("role") or "viewer").lower()
            role = "admin" if source_role in {"admin", "owner"} else "analyst" if source_role in {"analyst", "member"} else "viewer"
            client.collection("app_users").document(str(row["firebase_uid"])).set(
                {
                    "email": row.get("email"),
                    "display_name": row.get("display_name"),
                    "role": role,
                    "status": row.get("status") or "active",
                    "snapshot_id": snapshot_id,
                    "synced_at": firestore.SERVER_TIMESTAMP,
                },
                merge=True,
            )
            user_count += 1

        cur.execute(
            """
            SELECT DISTINCT ON (firebase_uid, market, ticker)
                   id, firebase_uid, user_email, event_at_utc, action, market, ticker,
                   label, note, scan_id, source_id, signal_date, close_price,
                   market_cap, avg_volume, volume_ratio, sector, industry
            FROM rating_events
            WHERE firebase_uid IS NOT NULL
            ORDER BY firebase_uid, market, ticker, event_at_utc DESC, id DESC
            """
        )
        for row in cur.fetchall():
            document_id = f"{row['firebase_uid']}__{row['market']}__{_safe_ticker(row['ticker'])}"
            payload = {key: _json_value(value) for key, value in row.items()}
            payload.update({"owner_uid": row["firebase_uid"], "owner_email": row.get("user_email"), "snapshot_id": snapshot_id})
            client.collection("team_appraisals").document(document_id).set(payload, merge=True)
            appraisal_count += 1

        cur.execute(
            """
            SELECT id, firebase_uid, user_email, event_at_utc, action, market, ticker,
                   label, note, scan_id, source_id, signal_date, close_price,
                   market_cap, avg_volume, volume_ratio, sector, industry
            FROM rating_events
            WHERE firebase_uid IS NOT NULL
            ORDER BY id
            """
        )
        for row in cur.fetchall():
            payload = {key: _json_value(value) for key, value in row.items()}
            payload.update({"owner_uid": row["firebase_uid"], "owner_email": row.get("user_email"), "source": "postgres"})
            client.collection("rating_events").document(f"sql_{row['id']}").set(payload, merge=True)
            event_count += 1

        cur.execute(
            """
            SELECT id, firebase_uid, user_email, category, message, page_path,
                   market, ticker, context_json, status, admin_note,
                   created_at_utc, updated_at_utc
            FROM app_feedback
            ORDER BY id
            """
        )
        for row in cur.fetchall():
            payload = {key: _json_value(value) for key, value in row.items() if key != "id"}
            payload.update(
                {
                    "sql_id": row["id"],
                    "user_uid": row["firebase_uid"],
                    "created_at": row["created_at_utc"],
                    "updated_at": row["updated_at_utc"],
                    "source": "postgres",
                }
            )
            client.collection("feedback").document(f"sql_{row['id']}").set(payload, merge=True)
            feedback_count += 1
    return {
        "users": user_count,
        "appraisals": appraisal_count,
        "rating_events": event_count,
        "feedback": feedback_count,
    }


def _sync_live_events_to_postgres(conn: psycopg.Connection[Any]) -> int:
    """Import Firestore appraisal events created while Cloud SQL was stopped."""
    client = firestore.Client()
    pending = client.collection("rating_events").where("synced_to_postgres", "==", False).stream()
    synced = 0
    for document in pending:
        event = document.to_dict() or {}
        market = str(event.get("market") or "").lower()
        ticker = str(event.get("ticker") or "").upper()
        owner_uid = str(event.get("owner_uid") or "")
        action = str(event.get("action") or "label").lower()
        label = str(event.get("label") or "").lower() or None
        if market not in SUPPORTED_MARKETS or not ticker or not owner_uid or action not in {"label", "clear"}:
            document.reference.update({"sync_error": "Invalid live appraisal payload"})
            continue
        timestamp = event.get("event_at_utc")
        event_at = timestamp if isinstance(timestamp, datetime) else datetime.now(timezone.utc)
        signal_date = event.get("signal_date")
        signal_price = event.get("signal_price")
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT id FROM rating_events WHERE result_json->>'firestore_event_id' = %s LIMIT 1",
                (document.id,),
            )
            existing = cur.fetchone()
            if existing:
                rating_event_id = int(existing["id"])
            else:
                if not signal_date or not signal_price:
                    cur.execute(
                        """
                        SELECT price_date, close_price
                        FROM price_history
                        WHERE market = %s AND provider = 'yfinance' AND ticker = %s
                        ORDER BY price_date DESC LIMIT 1
                        """,
                        (market, ticker),
                    )
                    price = cur.fetchone() or {}
                    signal_date = signal_date or price.get("price_date")
                    signal_price = signal_price or price.get("close_price")
                cur.execute(
                    """
                    INSERT INTO rating_events
                      (event_at_utc, action, rated_by, market, provider, ticker, label,
                       note, signal_date, close_price, market_cap, avg_volume,
                       volume_ratio, sector, industry, result_json, yahoo_url,
                       firebase_uid, user_email)
                    VALUES
                      (%s, %s, %s, %s, 'yfinance', %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        event_at,
                        action,
                        event.get("actor_email") or event.get("owner_email") or "offline user",
                        market,
                        ticker,
                        label if action == "label" else None,
                        event.get("note"),
                        signal_date,
                        signal_price,
                        event.get("market_cap"),
                        event.get("avg_volume"),
                        event.get("volume_ratio"),
                        event.get("sector"),
                        event.get("industry"),
                        json.dumps({"firestore_event_id": document.id, "offline_appraisal": True}),
                        f"https://finance.yahoo.com/quote/{ticker}",
                        owner_uid,
                        event.get("owner_email"),
                    ),
                )
                rating_event_id = int(cur.fetchone()["id"])
            conn.commit()
        document.reference.update(
            {
                "synced_to_postgres": True,
                "postgres_rating_event_id": rating_event_id,
                "synced_at": firestore.SERVER_TIMESTAMP,
            }
        )
        synced += 1
    return synced


def publish_weekly_snapshot(
    database_url: str,
    bucket_name: str,
    progress: Progress | None = None,
) -> dict[str, Any]:
    report = progress or (lambda _stage, _current, _total, _message: None)
    now = datetime.now(timezone.utc)
    snapshot_id = now.strftime("%Y%m%dT%H%M%SZ")
    version_root = f"snapshots/versions/{snapshot_id}"
    bucket = storage.Client().bucket(bucket_name)
    report("Preparing snapshot", 0, 4, f"Creating weekly snapshot {snapshot_id}")

    with psycopg.connect(database_url) as conn:
        synced_live_events = _sync_live_events_to_postgres(conn)
        profiles_by_market: dict[str, dict[str, dict[str, Any]]] = {}
        markets: dict[str, Any] = {}
        scans: dict[str, Any] = {}
        for index, market in enumerate(SUPPORTED_MARKETS, start=1):
            scan, status, profiles = _publish_market_files(conn, bucket, version_root, market)
            scans[market] = {
                "id": scan["scan"]["id"],
                "created_at_utc": scan["scan"]["created_at_utc"],
                "market_snapshot_date": scan["scan"].get("market_snapshot_date"),
                "scanned_count": scan["scan"].get("scanned_count"),
                "result_count": scan["scan"].get("result_count"),
            }
            markets[market] = status
            profiles_by_market[market] = profiles
            report("Publishing market files", index, 4, f"Published {market.upper()} screen and metrics")

        chart_counts, screen_data = _publish_charts(conn, bucket, version_root, profiles_by_market, report, snapshot_id)
        for market, tickers in screen_data.items():
            _upload_json(
                bucket,
                f"{version_root}/markets/{market}/screen-flags.json",
                {"ok": True, "market": market, "tickers": tickers},
                immutable=True,
            )
        collaboration = _migrate_collaboration(conn, snapshot_id)

    manifest = {
        "ok": True,
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "published_at_utc": now.isoformat(),
        "version_root": version_root,
        "markets": markets,
        "scans": scans,
        "chart_counts": chart_counts,
        "collaboration": collaboration,
        "synced_live_events": synced_live_events,
        "paths": {
            "scan": f"{version_root}/markets/{{market}}/scan.json",
            "metrics": f"{version_root}/markets/{{market}}/metrics.json",
            "screen_flags": f"{version_root}/markets/{{market}}/screen-flags.json",
            "companies": f"{version_root}/markets/{{market}}/companies.json",
            "chart": f"{version_root}/charts/{{market}}/{{ticker}}.json",
        },
    }
    _upload_json(bucket, f"{version_root}/manifest.json", manifest, immutable=True)
    _upload_json(bucket, "snapshots/current.json", manifest, immutable=False)
    firestore.Client().collection("system").document("current_snapshot").set(manifest, merge=False)
    report("Publishing snapshot", 4, 4, f"Snapshot {snapshot_id} is live")
    return manifest


if __name__ == "__main__":
    publish_weekly_snapshot(
        os.environ["MONEYMAKER_DATABASE_URL"],
        os.environ.get("MONEYMAKER_STORAGE_BUCKET") or os.environ["MONEYMAKER_CACHE_BUCKET"],
        lambda stage, current, total, message: print(f"[{stage}] {current}/{total} {message}", flush=True),
    )
