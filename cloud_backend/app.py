"""PostgreSQL-backed HTTP API for the deployed MoneyMaker application."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import psycopg
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from cloud_backend.jobs import dispatch
from web_app import INDEX_HTML, MARKET_DEFAULTS, VALID_LABELS


app = FastAPI(title="MoneyMaker API", version="1.0")


def database_url() -> str:
    value = str(os.environ.get("MONEYMAKER_DATABASE_URL", "")).strip()
    if not value:
        raise HTTPException(status_code=503, detail="MONEYMAKER_DATABASE_URL is not configured")
    return value


def db() -> psycopg.Connection:
    return psycopg.connect(database_url(), row_factory=dict_row)


def job_payload(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {"running": False, "success": None, "message": "Idle", "stage": "Idle", "percent": 0}
    status = str(row.get("status") or "queued")
    return {
        **row,
        "running": status in {"queued", "running"},
        "success": True if status == "succeeded" else False if status == "failed" else None,
        "message": row.get("detail") or status.title(),
        "current": row.get("current_count", 0),
        "total": row.get("total_count"),
        "summary": row.get("parameters_json") or {},
        "results": row.get("result_json") or [],
    }


def create_job(job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    job_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc)
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO job_runs
              (id, job_type, market, status, stage, detail, started_at_utc, parameters_json)
            VALUES (%s, %s, %s, 'queued', 'Queued', %s, %s, %s)
            """,
            (job_id, job_type, current_market(payload.get("market")),
             "Queued Cloud Run Job", started, Jsonb(payload)),
        )
        conn.commit()
    try:
        dispatched = dispatch(job_type, payload, job_id=job_id)
    except Exception as exc:
        with db() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE job_runs SET status = 'failed', stage = 'Failed', error = %s, detail = %s WHERE id = %s",
                (str(exc), "Could not dispatch Cloud Run Job", job_id),
            )
            conn.commit()
        raise
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE job_runs SET detail = %s WHERE id = %s",
            (f"Queued Cloud Run Job {dispatched['cloud_run_job']}", job_id),
        )
        conn.commit()
    return {"ok": True, "job": job_payload({
        "id": job_id, "job_type": job_type, "status": "queued",
        "stage": "Queued", "detail": "Cloud Run Job queued", "current_count": 0,
        "total_count": None, "percent": 0, "parameters_json": payload, "result_json": [],
    })}


def current_market(market: str | None, ticker: str | None = None) -> str:
    value = (market or "").strip().lower()
    if value in {"asx", "us"}:
        return value
    return "asx" if (ticker or "").upper().endswith(".AX") else "us"


def auth_required() -> bool:
    return str(os.environ.get("MONEYMAKER_REQUIRE_AUTH", "true")).lower() in {"1", "true", "yes"}


async def require_auth(request: Request) -> dict[str, Any]:
    if not auth_required():
        return {"uid": "local", "email": None, "display_name": "Local user", "role": "owner"}
    authorization = request.headers.get("Authorization", "")
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Sign-in required")
    try:
        import firebase_admin
        from firebase_admin import auth

        if not firebase_admin._apps:
            firebase_admin.initialize_app()
        decoded = auth.verify_id_token(authorization[7:].strip())
        uid = str(decoded.get("uid") or "").strip()
        if not uid:
            raise ValueError("Token has no uid")
        email = str(decoded.get("email") or "").strip().lower() or None
        display_name = str(decoded.get("name") or decoded.get("email") or "").strip() or None
        with db() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT role, status FROM app_user_invites WHERE email = %s",
                (email,),
            )
            invite = cur.fetchone()
            if invite and invite["status"] == "disabled":
                raise HTTPException(status_code=403, detail="This account is disabled")
            role = invite["role"] if invite else "member"
            cur.execute(
                """
                INSERT INTO user_profiles
                  (firebase_uid, email, display_name, role, status, last_seen_at_utc)
                VALUES (%s, %s, %s, %s, 'active', NOW())
                ON CONFLICT (firebase_uid) DO UPDATE SET
                  email = EXCLUDED.email,
                  display_name = EXCLUDED.display_name,
                  role = EXCLUDED.role,
                  status = EXCLUDED.status,
                  last_seen_at_utc = NOW()
                RETURNING role, status
                """,
                (uid, email, display_name, role),
            )
            profile = cur.fetchone() or {"role": role, "status": "active"}
            conn.commit()
        return {"uid": uid, "email": email, "display_name": display_name,
                "role": profile["role"], "status": profile["status"]}
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        raise HTTPException(status_code=401, detail="Invalid Firebase ID token") from exc


def overlay_user_appraisals(conn: psycopg.Connection, user: dict[str, Any], scan_id: int,
                            results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not results:
        return results
    tickers = [str(row.get("ticker") or "").upper() for row in results]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticker, label, note, status, appraised_at_utc
            FROM user_appraisals
            WHERE firebase_uid = %s AND scan_id = %s AND ticker = ANY(%s)
            """,
            (user["uid"], scan_id, tickers),
        )
        appraisals = {row["ticker"]: row for row in cur.fetchall()}
    for row in results:
        appraisal = appraisals.get(str(row.get("ticker") or "").upper())
        row["label"] = appraisal["label"] if appraisal else None
        row["personal_note"] = appraisal["note"] if appraisal else None
        row["personal_status"] = appraisal["status"] if appraisal else None
        row["appraised_at_utc"] = appraisal["appraised_at_utc"] if appraisal else None
    return results


def range_days(range_key: str) -> int:
    return {"3m": 92, "6m": 184, "1y": 366, "2y": 731, "5y": 1827, "10y": 3653}.get(range_key, 366)


def chart_frame(conn: psycopg.Connection, market: str, provider: str, ticker: str, range_key: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT price_date::text AS date, open_price AS open, high_price AS high,
                   low_price AS low, close_price AS close, volume
            FROM price_history
            WHERE market = %s AND provider = %s AND ticker = %s
              AND price_date >= CURRENT_DATE - (%s * INTERVAL '1 day')
            ORDER BY price_date
            """,
            (market, provider, ticker, range_days(range_key)),
        )
        rows = cur.fetchall()
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["date"] = pd.to_datetime(frame["date"])
    return frame.set_index("date").sort_index()


def interval_frame(frame: pd.DataFrame, interval: str) -> pd.DataFrame:
    if frame.empty or interval == "daily":
        return frame
    rule = "W-FRI" if interval == "weekly" else "ME"
    return frame.resample(rule).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna(subset=["close"])


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if pd.isna(value) if not isinstance(value, (str, bytes, dict, list)) else False:
        return None
    return value


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return INDEX_HTML


@app.get("/favicon.ico")
async def favicon() -> dict[str, bool]:
    return {}


@app.get("/api/health")
async def health() -> dict[str, Any]:
    try:
        with db() as conn:
            conn.execute("SELECT 1").fetchone()
        return {"ok": True, "database": "online", "cloud": True}
    except Exception as exc:
        return {"ok": False, "database": "unavailable", "error": str(exc)}


@app.get("/api/config")
async def config(request: Request) -> dict[str, Any]:
    await require_auth(request)
    return {"ok": True, "config": {}, "markets": MARKET_DEFAULTS, "cloud": True}


@app.get("/api/auth-config")
async def auth_config() -> dict[str, Any]:
    """Return public Firebase web configuration; no credentials are included."""
    return {
        "ok": True,
        "enabled": bool(os.environ.get("FIREBASE_API_KEY")),
        "apiKey": os.environ.get("FIREBASE_API_KEY", ""),
        "authDomain": os.environ.get("FIREBASE_AUTH_DOMAIN", "moneymaker-aedf7.firebaseapp.com"),
        "projectId": os.environ.get("GOOGLE_CLOUD_PROJECT", "moneymaker-aedf7"),
        "storageBucket": os.environ.get("FIREBASE_STORAGE_BUCKET", ""),
        "appId": os.environ.get("FIREBASE_APP_ID", ""),
    }


@app.get("/api/profile")
async def profile(request: Request) -> dict[str, Any]:
    user = await require_auth(request)
    return {"ok": True, "user": user}


@app.get("/api/ticker-files")
async def ticker_files(request: Request) -> dict[str, Any]:
    await require_auth(request)
    names = ["asx_yfinance_valid_stocks_2026-05-11.txt", "us_tickers_nasdaqtrader.txt"]
    return {"ok": True, "files": [{"name": name, "size_kb": 0} for name in names]}


@app.get("/api/chart")
async def chart(
    request: Request,
    ticker: str = Query(""),
    market: str | None = Query(None),
    provider: str = Query("yfinance"),
    interval: str = Query("daily"),
    range: str = Query("1y"),
    ma: str = Query(""),
) -> dict[str, Any]:
    await require_auth(request)
    ticker = ticker.strip().upper()
    market = current_market(market, ticker)
    if interval not in {"daily", "weekly", "monthly"}:
        raise HTTPException(status_code=400, detail="Invalid chart interval")
    with db() as conn:
        frame = interval_frame(chart_frame(conn, market, provider, ticker, range), interval)
        company = {}
        with conn.cursor() as cur:
            cur.execute("SELECT info_json FROM companies WHERE market = %s AND ticker = %s", (market, ticker))
            row = cur.fetchone()
            if row:
                company = row["info_json"] or {}
    periods = []
    for raw in ma.split(","):
        try:
            value = int(raw)
        except ValueError:
            continue
        if value > 0:
            periods.append(value)
    candles = []
    moving_averages = {str(period): [] for period in periods}
    if not frame.empty:
        for date, row in frame.iterrows():
            candles.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "open": json_safe(row["open"]),
                    "high": json_safe(row["high"]),
                    "low": json_safe(row["low"]),
                    "close": json_safe(row["close"]),
                    "volume": json_safe(row["volume"]),
                }
            )
        for period in periods:
            moving_averages[str(period)] = [json_safe(value) for value in frame["close"].rolling(period).mean()]
    return {
        "ok": True,
        "ticker": ticker,
        "market": market,
        "provider": provider,
        "interval": interval,
        "range": range,
        "company": company,
        "candles": candles,
        "moving_averages": moving_averages,
        "count": len(candles),
        "start": candles[0]["date"] if candles else None,
        "end": candles[-1]["date"] if candles else None,
    }


@app.get("/api/scans")
async def scans(request: Request, market: str = Query("asx")) -> dict[str, Any]:
    await require_auth(request)
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, source_id, created_at_utc, provider, query, scanned_count,
                   result_count, skipped_no_history, config_json
            FROM scan_runs WHERE market = %s ORDER BY created_at_utc DESC LIMIT 100
            """,
            (current_market(market),),
        )
        return {"ok": True, "scans": cur.fetchall()}


@app.get("/api/labels")
async def labels(request: Request, scan_id: int = Query(...)) -> dict[str, Any]:
    user = await require_auth(request)
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticker, label, note, status, appraised_at_utc AS labeled_at_utc
            FROM user_appraisals
            WHERE firebase_uid = %s AND scan_id = %s ORDER BY ticker
            """,
            (user["uid"], scan_id),
        )
        return {"ok": True, "labels": cur.fetchall()}


@app.get("/api/status")
async def status(
    request: Request,
    market: str = Query("asx"),
    cache_file: str = Query(""),
) -> dict[str, Any]:
    await require_auth(request)
    if "_us" in cache_file.lower():
        market = "us"
    market = current_market(market)
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(DISTINCT ticker) AS ticker_count, COUNT(*) AS history_rows,
                   MAX(price_date)::text AS latest_date
            FROM price_history WHERE market = %s
            """,
            (market,),
        )
        cache = cur.fetchone() or {}
        cache["exists"] = bool(cache.get("ticker_count"))
        cache["size_mb"] = 0
        cur.execute(
            "SELECT DISTINCT ticker FROM price_history WHERE market = %s ORDER BY ticker LIMIT 500",
            (market,),
        )
        cache["tickers"] = [row["ticker"] for row in cur.fetchall()]
        cur.execute(
            "SELECT * FROM job_runs WHERE job_type = 'fetch' ORDER BY started_at_utc DESC LIMIT 1"
        )
        job = cur.fetchone()
        return {"ok": True, "status": dict(cache or {}), "job": job_payload(dict(job) if job else None)}


@app.get("/api/job")
async def job(request: Request) -> dict[str, Any]:
    await require_auth(request)
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM job_runs WHERE job_type = 'fetch' ORDER BY started_at_utc DESC LIMIT 1")
        row = cur.fetchone()
        return {"ok": True, "job": job_payload(dict(row) if row else None)}


@app.get("/api/filter/job")
async def filter_job(request: Request) -> dict[str, Any]:
    await require_auth(request)
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM job_runs WHERE job_type = 'filter' ORDER BY started_at_utc DESC LIMIT 1")
        row = cur.fetchone()
        return {"ok": True, "job": job_payload(dict(row) if row else None)}


@app.post("/api/label")
async def label(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    user = await require_auth(request)
    scan_id = int(payload.get("scan_id") or 0)
    ticker = str(payload.get("ticker") or "").strip().upper()
    label_value = str(payload.get("label") or "").strip().lower().replace(" ", "_")
    if not scan_id or not ticker:
        raise HTTPException(status_code=400, detail="scan_id and ticker are required")
    if label_value in {"", "clear", "none", "unlabelled", "unlabeled"}:
        label_value = None
    elif label_value not in VALID_LABELS:
        raise HTTPException(status_code=400, detail="Invalid rating label")
    event_at = datetime.now(timezone.utc)
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT market FROM scan_runs WHERE id = %s", (scan_id,))
        scan = cur.fetchone()
        if not scan:
            raise HTTPException(status_code=404, detail="Scan not found")
        cur.execute("SELECT * FROM scan_results WHERE scan_id = %s AND ticker = %s", (scan_id, ticker))
        result = cur.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Ticker is not in this scan")
        if label_value is None:
            cur.execute(
                "DELETE FROM user_appraisals WHERE firebase_uid = %s AND scan_id = %s AND source_id = %s AND ticker = %s",
                (user["uid"], scan_id, result["source_id"], ticker),
            )
        else:
            cur.execute(
                """
                INSERT INTO user_appraisals
                  (firebase_uid, scan_id, source_id, market, ticker, label, note, status, appraised_at_utc)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (firebase_uid, scan_id, source_id, ticker) DO UPDATE SET
                    label = EXCLUDED.label, note = EXCLUDED.note,
                    status = EXCLUDED.status, appraised_at_utc = EXCLUDED.appraised_at_utc
                """,
                (user["uid"], scan_id, result["source_id"], scan["market"], ticker,
                 label_value, payload.get("note"), payload.get("status"), event_at),
            )
        cur.execute(
            """
            INSERT INTO rating_events
              (event_at_utc, action, rated_by, market, scan_id, ticker, label,
               note, rank, signal_date, close_price, market_cap, avg_volume,
               volume_ratio, sector, industry, result_json, yahoo_url,
               firebase_uid, user_email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s)
            """,
            (
                event_at,
                "clear" if label_value is None else "label",
                str(user.get("email") or user.get("display_name") or payload.get("rated_by") or "anonymous"),
                scan["market"], scan_id, ticker, label_value, payload.get("note"),
                result["rank"], result["signal_date"], result["close_price"],
                result["market_cap"], result["avg_volume"], result["volume_ratio"],
                result["sector"], result["industry"], result["result_json"],
                f"https://finance.yahoo.com/quote/{ticker}",
                user["uid"], user.get("email"),
            ),
        )
        conn.commit()
    return {"ok": True, "scan_id": scan_id, "ticker": ticker, "label": label_value,
            "online": True, "user": user}


@app.post("/api/fetch")
async def fetch(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    await require_auth(request)
    try:
        return create_job("fetch", payload)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not queue fetch job: {exc}") from exc


@app.post("/api/filter/start")
async def start_filter(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    await require_auth(request)
    try:
        return create_job("filter", payload)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not queue filter job: {exc}") from exc


@app.post("/api/filter")
async def filter_results(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    user = await require_auth(request)
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM job_runs WHERE job_type = 'filter' ORDER BY started_at_utc DESC LIMIT 1")
        row = cur.fetchone()
        result = job_payload(dict(row) if row else None)
        scan_id = int(result.get("summary", {}).get("scan_id") or result.get("scan_id") or 0)
        if scan_id and isinstance(result.get("results"), list):
            result["results"] = overlay_user_appraisals(conn, user, scan_id, result["results"])
        return {"ok": True, **result}
