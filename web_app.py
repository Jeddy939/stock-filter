"""Local browser UI for the Moneymaker stock filter."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import math
import numbers
import os
import shutil
import sqlite3
import sys
import threading
import traceback
import uuid
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

import pandas as pd

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moneymaker import fetcher
from moneymaker.filters import analyze_stock_from_local_data

DEFAULT_CACHE_FILE = fetcher.DEFAULT_CACHE_FILE
DEFAULT_US_CACHE_FILE = fetcher.DEFAULT_US_CACHE_FILE
DEFAULT_TICKER_FILE = "asx_yfinance_valid_stocks_2026-05-11.txt"
DEFAULT_US_TICKER_FILE = fetcher.DEFAULT_US_TICKER_FILE
DEFAULT_OUTPUT_FILE = "stock_data_web.json"
DEFAULT_CENTRAL_RATINGS_FILE = "ratings/central_stock_ratings.sqlite"
LEGACY_CENTRAL_RATINGS_FILE = "central_stock_ratings.sqlite"
DEFAULT_CENTRAL_RATINGS_JSON_FILE = "central_stock_ratings.json"
DEFAULT_CENTRAL_RATINGS_JSONL_FILE = "central_stock_ratings.jsonl"
DEFAULT_SHEETS_PENDING_FILE = "ratings/google_sheets_pending_ratings.jsonl"
FILTER_HISTORY_CHUNK_SIZE = 250
VALID_LABELS = {"winner", "potential_winner", "maybe", "bad"}
MARKET_DEFAULTS = {
    "asx": {
        "label": "ASX",
        "cache_file": DEFAULT_CACHE_FILE,
        "ticker_file": DEFAULT_TICKER_FILE,
        "provider": "yfinance",
        "chart_ticker": "CBA.AX",
        "output_file": DEFAULT_OUTPUT_FILE,
    },
    "us": {
        "label": "US",
        "cache_file": DEFAULT_US_CACHE_FILE,
        "ticker_file": DEFAULT_US_TICKER_FILE,
        "provider": "yfinance",
        "chart_ticker": "AAPL",
        "output_file": "stock_data_us_web.json",
    },
}

JOB_LOCK = threading.Lock()
CURRENT_JOB: Dict[str, Any] = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "success": None,
    "message": "Idle",
    "stage": "Idle",
    "current": 0,
    "total": None,
    "percent": 0,
    "detail": "",
    "log": "",
}
FILTER_LOCK = threading.Lock()
RATING_FILE_LOCK = threading.Lock()
FILTER_JOB: Dict[str, Any] = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "success": None,
    "message": "Idle",
    "stage": "Idle",
    "current": 0,
    "total": None,
    "percent": 0,
    "detail": "",
    "results": [],
    "summary": {},
}


class JobLogWriter:
    """File-like stream that keeps the job log visible while work runs."""

    def __init__(self, buffer: io.StringIO) -> None:
        self.buffer = buffer

    def write(self, text: str) -> int:
        self.buffer.write(text)
        if text:
            with JOB_LOCK:
                CURRENT_JOB["log"] = self.buffer.getvalue()[-30000:]
        return len(text)

    def flush(self) -> None:
        return None


def _json_response(handler: BaseHTTPRequestHandler, payload: Dict[str, Any], status: int = 200) -> None:
    data = json.dumps(payload, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _html_response(handler: BaseHTTPRequestHandler, html: str) -> None:
    data = html.encode("utf-8")
    handler.send_response(HTTPStatus.OK)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _read_json(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0"))
    if length <= 0:
        return {}
    raw = handler.rfile.read(length).decode("utf-8")
    return json.loads(raw) if raw else {}


def _cache_path(cache_file: str = DEFAULT_CACHE_FILE) -> Path:
    return (ROOT / cache_file).resolve() if not Path(cache_file).is_absolute() else Path(cache_file)


def _connect_readonly(cache_file: str = DEFAULT_CACHE_FILE) -> sqlite3.Connection:
    path = _cache_path(cache_file)
    if not path.exists():
        raise FileNotFoundError(f"Cache file not found: {path}")
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _connect_write(cache_file: str = DEFAULT_CACHE_FILE) -> sqlite3.Connection:
    path = _cache_path(cache_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _central_ratings_path() -> Path:
    configured = str(os.environ.get("MONEYMAKER_CENTRAL_RATINGS_DB", "")).strip()
    if configured:
        return _cache_path(configured)
    path = _cache_path(DEFAULT_CENTRAL_RATINGS_FILE)
    legacy_path = _cache_path(LEGACY_CENTRAL_RATINGS_FILE)
    if legacy_path.exists() and not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(legacy_path, path)
    return path


def _central_ratings_json_path() -> Path:
    configured = str(os.environ.get("MONEYMAKER_CENTRAL_RATINGS_JSON", "")).strip()
    return _cache_path(configured or DEFAULT_CENTRAL_RATINGS_JSON_FILE)


def _central_ratings_jsonl_path() -> Path:
    configured = str(os.environ.get("MONEYMAKER_CENTRAL_RATINGS_JSONL", "")).strip()
    return _cache_path(configured or DEFAULT_CENTRAL_RATINGS_JSONL_FILE)


def _sheets_pending_path() -> Path:
    configured = str(os.environ.get("MONEYMAKER_GOOGLE_SHEETS_PENDING_FILE", "")).strip()
    return _cache_path(configured or DEFAULT_SHEETS_PENDING_FILE)


def _ensure_central_ratings_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS rating_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_at_utc TEXT NOT NULL,
            action TEXT NOT NULL,
            rated_by TEXT,
            market TEXT,
            cache_file TEXT NOT NULL,
            scan_id INTEGER NOT NULL,
            scan_created_at_utc TEXT,
            provider TEXT,
            query TEXT,
            ticker TEXT NOT NULL,
            label TEXT,
            note TEXT,
            rank INTEGER,
            signal_date TEXT,
            close_price REAL,
            market_cap REAL,
            avg_volume REAL,
            volume_ratio REAL,
            sector TEXT,
            industry TEXT,
            result_json TEXT,
            yahoo_url TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_rating_events_ticker
            ON rating_events(ticker);
        CREATE INDEX IF NOT EXISTS idx_rating_events_label
            ON rating_events(label);
        CREATE INDEX IF NOT EXISTS idx_rating_events_event_at
            ON rating_events(event_at_utc);
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(rating_events)")}
    if "rated_by" not in columns:
        conn.execute("ALTER TABLE rating_events ADD COLUMN rated_by TEXT")
    conn.commit()


def _central_market_from_cache(cache_file: str) -> str:
    return "US" if "us" in Path(cache_file).stem.lower() else "ASX"


def _central_rater_name() -> Optional[str]:
    configured = str(os.environ.get("MONEYMAKER_RATER_NAME", "")).strip()
    if configured:
        return configured
    return str(os.environ.get("USERNAME") or os.environ.get("USER") or "").strip() or None


def _safe_json_object(raw: Any) -> Dict[str, Any]:
    try:
        value = json.loads(str(raw or "{}"))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _write_central_rating_json_event(event: Dict[str, Any]) -> None:
    """Write a GitHub-friendly central rating log beside the app."""

    json_path = _central_ratings_json_path()
    jsonl_path = _central_ratings_jsonl_path()
    json_path.parent.mkdir(parents=True, exist_ok=True)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    with RATING_FILE_LOCK:
        with jsonl_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")

        events: List[Dict[str, Any]] = []
        if json_path.exists():
            try:
                loaded = json.loads(json_path.read_text(encoding="utf-8"))
                if isinstance(loaded, list):
                    events = [item for item in loaded if isinstance(item, dict)]
            except json.JSONDecodeError:
                backup = json_path.with_suffix(f".bad-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json")
                json_path.replace(backup)
                events = []

        events.append(event)
        temp_path = json_path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(events, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temp_path.replace(json_path)


def _queue_google_sheets_rating_event(event: Dict[str, Any], error: str) -> None:
    pending_path = _sheets_pending_path()
    pending_path.parent.mkdir(parents=True, exist_ok=True)
    queued = dict(event)
    queued["queued_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
    queued["queue_error"] = error[:1000]
    with RATING_FILE_LOCK:
        with pending_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(queued, ensure_ascii=False, sort_keys=True) + "\n")


def _post_google_sheets_rating_event(event: Dict[str, Any]) -> Dict[str, Any]:
    # Cloud deployments write ratings through the authenticated backend.
    # Keeping the old webhook disabled prevents duplicate central records.
    if str(os.environ.get("MONEYMAKER_CLOUD_MODE", "")).strip().lower() in {"1", "true", "yes"}:
        return {"configured": False, "sent": False, "queued": False, "disabled": "cloud_backend"}
    webhook_url = str(
        os.environ.get("MONEYMAKER_GOOGLE_SHEETS_WEBHOOK_URL")
        or os.environ.get("MONEYMAKER_GOOGLE_SHEETS_WEBHOOK")
        or ""
    ).strip()
    if not webhook_url:
        return {"configured": False, "sent": False, "queued": False}

    payload = dict(event)
    secret = str(os.environ.get("MONEYMAKER_GOOGLE_SHEETS_SECRET", "")).strip()
    if secret:
        payload["secret"] = secret

    try:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(webhook_url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        with urlopen(request, timeout=10) as response:
            body = response.read().decode("utf-8", errors="replace")
            if response.status >= 400:
                raise RuntimeError(f"HTTP {response.status}: {body[:500]}")
            try:
                result = json.loads(body) if body else {}
            except json.JSONDecodeError:
                result = {"raw": body}
            if isinstance(result, dict) and result.get("ok") is False:
                raise RuntimeError(str(result.get("error") or result))
        return {"configured": True, "sent": True, "queued": False}
    except Exception as exc:
        _queue_google_sheets_rating_event(event, str(exc))
        return {"configured": True, "sent": False, "queued": True, "error": str(exc)}


def _record_central_rating_event(
    source_conn: sqlite3.Connection,
    cache_file: str,
    scan_id: int,
    ticker: str,
    label: Optional[str],
    note: Optional[str],
    event_at: str,
    action: str,
) -> Optional[Dict[str, Any]]:
    """Append a rating event to the central ratings database."""

    row = source_conn.execute(
        """
        SELECT
            sr.rank,
            sr.signal_date,
            sr.close_price,
            sr.market_cap,
            sr.avg_volume,
            sr.volume_ratio,
            sr.sector,
            sr.industry,
            sr.result_json,
            s.created_at_utc AS scan_created_at_utc,
            s.provider,
            s.query
        FROM scan_results sr
        JOIN scan_runs s ON s.id = sr.scan_id
        WHERE sr.scan_id = ? AND sr.ticker = ?
        """,
        (scan_id, ticker),
    ).fetchone()
    if not row:
        return None

    event = {
        "event_id": str(uuid.uuid4()),
        "event_at_utc": event_at,
        "action": action,
        "rated_by": _central_rater_name(),
        "market": _central_market_from_cache(cache_file),
        "cache_file": str(_cache_path(cache_file)),
        "scan_id": scan_id,
        "scan_created_at_utc": row["scan_created_at_utc"],
        "provider": row["provider"],
        "query": row["query"],
        "ticker": ticker,
        "label": label,
        "note": note,
        "rank": row["rank"],
        "signal_date": row["signal_date"],
        "close_price": _float_or_none(row["close_price"]),
        "market_cap": _float_or_none(row["market_cap"]),
        "avg_volume": _float_or_none(row["avg_volume"]),
        "volume_ratio": _float_or_none(row["volume_ratio"]),
        "sector": row["sector"],
        "industry": row["industry"],
        "result": _safe_json_object(row["result_json"]),
        "yahoo_url": f"https://finance.yahoo.com/quote/{ticker}",
    }
    _write_central_rating_json_event(event)
    sheets_result = _post_google_sheets_rating_event(event)

    central_path = _central_ratings_path()
    central_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(central_path)) as central_conn:
        _ensure_central_ratings_schema(central_conn)
        central_conn.execute(
            """
            INSERT INTO rating_events (
                event_at_utc, action, rated_by, market, cache_file, scan_id,
                scan_created_at_utc, provider, query, ticker, label, note,
                rank, signal_date, close_price, market_cap, avg_volume,
                volume_ratio, sector, industry, result_json, yahoo_url
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["event_at_utc"],
                event["action"],
                event["rated_by"],
                event["market"],
                event["cache_file"],
                event["scan_id"],
                event["scan_created_at_utc"],
                event["provider"],
                event["query"],
                event["ticker"],
                event["label"],
                event["note"],
                event["rank"],
                event["signal_date"],
                event["close_price"],
                event["market_cap"],
                event["avg_volume"],
                event["volume_ratio"],
                event["sector"],
                event["industry"],
                row["result_json"],
                event["yahoo_url"],
            ),
        )
        central_conn.commit()
    event["google_sheets"] = sheets_result
    return event


def _ensure_scan_schema(conn: sqlite3.Connection) -> None:
    conn.execute("DROP VIEW IF EXISTS labelled_scan_results")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS scan_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at_utc TEXT NOT NULL,
            provider TEXT NOT NULL,
            cache_file TEXT NOT NULL,
            years INTEGER,
            limit_count INTEGER,
            query TEXT,
            scanned_count INTEGER NOT NULL,
            result_count INTEGER NOT NULL,
            skipped_no_history INTEGER NOT NULL,
            config_json TEXT NOT NULL,
            ticker_universe_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scan_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            signal_date TEXT,
            close_price REAL,
            market_cap REAL,
            avg_volume REAL,
            volume_ratio REAL,
            sector TEXT,
            industry TEXT,
            result_json TEXT NOT NULL,
            FOREIGN KEY (scan_id) REFERENCES scan_runs(id) ON DELETE CASCADE,
            UNIQUE(scan_id, ticker)
        );

        CREATE TABLE IF NOT EXISTS scan_labels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            label TEXT NOT NULL CHECK(label IN ('winner', 'potential_winner', 'maybe', 'bad')),
            note TEXT,
            labeled_at_utc TEXT NOT NULL,
            FOREIGN KEY (scan_id) REFERENCES scan_runs(id) ON DELETE CASCADE,
            UNIQUE(scan_id, ticker)
        );

        CREATE INDEX IF NOT EXISTS idx_scan_results_scan_rank
            ON scan_results(scan_id, rank);
        CREATE INDEX IF NOT EXISTS idx_scan_results_ticker
            ON scan_results(ticker);
        CREATE INDEX IF NOT EXISTS idx_scan_labels_label
            ON scan_labels(label);
        """
    )
    label_table = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'scan_labels'"
    ).fetchone()
    if label_table and "potential_winner" not in (label_table["sql"] or ""):
        conn.executescript(
            """
            CREATE TABLE scan_labels_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id INTEGER NOT NULL,
                ticker TEXT NOT NULL,
                label TEXT NOT NULL CHECK(label IN ('winner', 'potential_winner', 'maybe', 'bad')),
                note TEXT,
                labeled_at_utc TEXT NOT NULL,
                FOREIGN KEY (scan_id) REFERENCES scan_runs(id) ON DELETE CASCADE,
                UNIQUE(scan_id, ticker)
            );
            INSERT INTO scan_labels_new (id, scan_id, ticker, label, note, labeled_at_utc)
            SELECT id, scan_id, ticker, label, note, labeled_at_utc
            FROM scan_labels
            WHERE label IN ('winner', 'maybe', 'bad');
            DROP TABLE scan_labels;
            ALTER TABLE scan_labels_new RENAME TO scan_labels;
            CREATE INDEX IF NOT EXISTS idx_scan_labels_label
                ON scan_labels(label);
            """
        )
    conn.execute(
        """
        CREATE VIEW labelled_scan_results AS
        SELECT
            sr.scan_id,
            sr.rank,
            sr.ticker,
            sr.signal_date,
            sr.close_price,
            sr.market_cap,
            sr.avg_volume,
            sr.volume_ratio,
            sr.sector,
            sr.industry,
            sl.label,
            sl.note,
            sl.labeled_at_utc,
            sr.result_json,
            s.created_at_utc AS scan_created_at_utc,
            s.provider,
            s.config_json
        FROM scan_results sr
        JOIN scan_runs s ON s.id = sr.scan_id
        LEFT JOIN scan_labels sl
            ON sl.scan_id = sr.scan_id
           AND sl.ticker = sr.ticker
        """
    )
    conn.commit()


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        number = float(value)
        return number if math.isfinite(number) else None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def _json_dumps(payload: Any) -> str:
    return json.dumps(_json_safe(payload), sort_keys=True, separators=(",", ":"))


def _float_or_none(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _cache_status(cache_file: str = DEFAULT_CACHE_FILE) -> Dict[str, Any]:
    path = _cache_path(cache_file)
    status: Dict[str, Any] = {
        "cache_file": str(path),
        "exists": path.exists(),
        "size_mb": round(path.stat().st_size / 1_048_576, 2) if path.exists() else 0,
        "providers": [],
        "ticker_count": 0,
        "history_rows": 0,
        "info_count": 0,
        "earliest_date": None,
        "latest_date": None,
        "tickers": [],
    }
    if not path.exists():
        return status

    with _connect_readonly(cache_file) as conn:
        price = conn.execute(
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT ticker) AS tickers,
                MIN(date) AS earliest,
                MAX(date) AS latest
            FROM price_history
            """
        ).fetchone()
        status["history_rows"] = int(price["rows"] or 0)
        status["ticker_count"] = int(price["tickers"] or 0)
        status["earliest_date"] = price["earliest"]
        status["latest_date"] = price["latest"]
        status["info_count"] = int(conn.execute("SELECT COUNT(*) FROM company_info").fetchone()[0] or 0)
        status["providers"] = [
            row["provider"]
            for row in conn.execute("SELECT DISTINCT provider FROM price_history ORDER BY provider")
        ]
        status["tickers"] = [
            row["ticker"]
            for row in conn.execute("SELECT DISTINCT ticker FROM price_history ORDER BY ticker LIMIT 500")
        ]
    return status


def _ticker_file_options() -> List[Dict[str, Any]]:
    """List local ticker-like text files the web UI can pass to the fetcher."""

    files = []
    for path in sorted(ROOT.glob("*.txt")):
        if path.name.lower() == "requirements.txt":
            continue
        files.append(
            {
                "name": path.name,
                "size_kb": round(path.stat().st_size / 1024, 1),
                "modified": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
            }
        )
    return files


def _load_default_config() -> Dict[str, Any]:
    path = ROOT / "default filter settings.json"
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    return {
        "volume_multiplier": float(data.get("volume_multiplier", 2.0)),
        "price_avg_weeks": int(data.get("price_avg_weeks", 1)),
        "min_market_cap": float(data.get("min_market_cap", 0)),
        "max_market_cap": float(data.get("max_market_cap", 0)),
        "avg_volume_weeks": 52,
        "lookback_weeks": int(data.get("lookback_weeks", 1)),
        "ma_periods": {
            "short": int(data.get("ma_short", 90)),
            "intermediate": int(data.get("ma_intermediate", 180)),
            "medium": int(data.get("ma_medium", 360)),
            "long": int(data.get("ma_long", 700)),
        },
    }


def _filter_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    config = _load_default_config()
    config["volume_multiplier"] = float(payload.get("volume_multiplier", config["volume_multiplier"]))
    config["price_avg_weeks"] = int(payload.get("price_avg_weeks", config["price_avg_weeks"]))
    config["min_market_cap"] = float(payload.get("min_market_cap", config["min_market_cap"]))
    config["max_market_cap"] = float(payload.get("max_market_cap", config["max_market_cap"]))
    config["lookback_weeks"] = int(payload.get("lookback_weeks", config["lookback_weeks"]))
    config["ma_periods"] = {
        "short": int(payload.get("ma_short", config["ma_periods"]["short"])),
        "intermediate": int(payload.get("ma_intermediate", config["ma_periods"]["intermediate"])),
        "medium": int(payload.get("ma_medium", config["ma_periods"]["medium"])),
        "long": int(payload.get("ma_long", config["ma_periods"]["long"])),
    }
    return config


def _split_history(frame: pd.DataFrame) -> Dict[str, Any]:
    return json.loads(frame.to_json(orient="split", date_format="iso"))


def _load_info_map(conn: sqlite3.Connection, tickers: Iterable[str]) -> Dict[str, dict]:
    info: Dict[str, dict] = {}
    for ticker in tickers:
        row = conn.execute("SELECT info_json FROM company_info WHERE ticker = ?", (ticker,)).fetchone()
        if not row:
            continue
        try:
            info[ticker] = json.loads(row["info_json"])
        except json.JSONDecodeError:
            info[ticker] = {}
    return info


def _load_cached_history_frames(
    conn: sqlite3.Connection,
    provider: str,
    tickers: Sequence[str],
    start: datetime,
    end: datetime,
) -> Dict[str, pd.DataFrame]:
    if not tickers:
        return {}
    placeholders = ",".join("?" for _ in tickers)
    rows = conn.execute(
        f"""
        SELECT ticker, date, open, high, low, close, volume
        FROM price_history
        WHERE provider = ?
          AND ticker IN ({placeholders})
          AND date >= ?
          AND date <= ?
        ORDER BY ticker, date
        """,
        (
            provider,
            *tickers,
            fetcher._date_string(start),
            fetcher._date_string(end),
        ),
    ).fetchall()
    if not rows:
        return {}

    frame = pd.DataFrame(
        rows,
        columns=["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"],
    )
    frame["Date"] = pd.to_datetime(frame["Date"])
    histories: Dict[str, pd.DataFrame] = {}
    for ticker, group in frame.groupby("Ticker", sort=False):
        histories[str(ticker)] = group.set_index("Date")[["Open", "High", "Low", "Close", "Volume"]]
    return histories


def _range_start(latest: pd.Timestamp, range_key: str) -> Optional[pd.Timestamp]:
    """Return the earliest timestamp for a chart range."""

    ranges = {
        "3m": pd.DateOffset(months=3),
        "6m": pd.DateOffset(months=6),
        "1y": pd.DateOffset(years=1),
        "2y": pd.DateOffset(years=2),
        "5y": pd.DateOffset(years=5),
        "10y": pd.DateOffset(years=10),
    }
    offset = ranges.get(range_key.lower())
    if not offset:
        return None
    return latest - offset


def _aggregate_candles(frame: pd.DataFrame, interval: str) -> pd.DataFrame:
    """Aggregate daily cached bars to daily, weekly, or monthly candles."""

    interval = interval.lower()
    if interval == "daily":
        candles = frame.copy()
    elif interval == "weekly":
        candles = frame.resample("W-FRI").agg(
            {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
        )
    elif interval == "monthly":
        grouped = frame.groupby(frame.index.to_period("M"))
        candles = grouped.agg(
            {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
        )
        candles.index = pd.DatetimeIndex(grouped.apply(lambda group: group.index.max()).to_list())
    else:
        raise ValueError("interval must be daily, weekly, or monthly")
    return candles.dropna(subset=["Open", "High", "Low", "Close"])


def _chart_payload(params: Dict[str, List[str]]) -> Dict[str, Any]:
    """Return cached candle data for the in-app chart."""

    cache_file = params.get("cache_file", [DEFAULT_CACHE_FILE])[0] or DEFAULT_CACHE_FILE
    provider = fetcher.normalize_provider(params.get("provider", [fetcher.DEFAULT_PROVIDER])[0])
    interval = params.get("interval", ["daily"])[0].lower()
    range_key = params.get("range", ["1y"])[0].lower()
    ticker = params.get("ticker", [""])[0].strip().upper()
    ma_periods = _parse_ma_periods(params.get("ma", [""])[0])

    if not _cache_path(cache_file).exists():
        return {
            "ok": True,
            "ticker": ticker,
            "provider": provider,
            "company": _company_profile_payload("", ticker),
            "interval": interval,
            "range": range_key,
            "candles": [],
            "moving_averages": {},
            "count": 0,
            "start": None,
            "end": None,
            "message": f"Cache file not found: {cache_file}",
        }

    with _connect_readonly(cache_file) as conn:
        if not ticker:
            row = conn.execute(
                "SELECT ticker FROM price_history WHERE provider = ? GROUP BY ticker ORDER BY ticker LIMIT 1",
                (provider,),
            ).fetchone()
            ticker = row["ticker"] if row else ""
        if not ticker:
            return {"ok": True, "ticker": "", "company": {}, "candles": [], "interval": interval, "range": range_key}

        info_row = conn.execute(
            "SELECT info_json FROM company_info WHERE ticker = ?",
            (ticker,),
        ).fetchone()
        company_profile = _company_profile_payload(info_row["info_json"] if info_row else "", ticker)

        rows = conn.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM price_history
            WHERE provider = ? AND ticker = ?
            ORDER BY date
            """,
            (provider, ticker),
        ).fetchall()

    if not rows:
        return {
            "ok": True,
            "ticker": ticker,
            "company": company_profile,
            "candles": [],
            "interval": interval,
            "range": range_key,
            "message": "No cached history for ticker.",
        }

    frame = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume"])
    frame["Date"] = pd.to_datetime(frame["Date"])
    frame = frame.set_index("Date")
    for column in ["Open", "High", "Low", "Close", "Volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["Open", "High", "Low", "Close"])

    full_candles = _aggregate_candles(frame, interval)
    ma_values = _moving_average_payload(full_candles, ma_periods)

    start = _range_start(full_candles.index.max(), range_key)
    if start is not None:
        visible_mask = full_candles.index >= start
        candles = full_candles.loc[visible_mask]
        keep = visible_mask.tolist()
        ma_values = {
            key: [value for value, show in zip(values, keep) if show]
            for key, values in ma_values.items()
        }
    else:
        candles = full_candles
    if len(candles) > 360:
        candles = candles.iloc[-360:]
        ma_values = {key: values[-360:] for key, values in ma_values.items()}

    data = [
        {
            "date": index.strftime("%Y-%m-%d"),
            "open": round(float(row["Open"]), 6),
            "high": round(float(row["High"]), 6),
            "low": round(float(row["Low"]), 6),
            "close": round(float(row["Close"]), 6),
            "volume": 0 if pd.isna(row["Volume"]) else round(float(row["Volume"]), 2),
        }
        for index, row in candles.iterrows()
    ]
    return {
        "ok": True,
        "ticker": ticker,
        "provider": provider,
        "company": company_profile,
        "interval": interval,
        "range": range_key,
        "candles": data,
        "moving_averages": ma_values,
        "count": len(data),
        "start": data[0]["date"] if data else None,
        "end": data[-1]["date"] if data else None,
    }


def _company_profile_payload(raw_info: str, ticker: str = "") -> Dict[str, Any]:
    """Return a compact company profile for the chart header."""

    yahoo_ticker = str(ticker or "").strip().upper()
    base_profile = (
        {"yahoo_url": f"https://finance.yahoo.com/quote/{yahoo_ticker}"}
        if yahoo_ticker
        else {}
    )
    if not raw_info:
        return base_profile
    try:
        info = json.loads(raw_info)
    except (TypeError, json.JSONDecodeError):
        return base_profile
    if not isinstance(info, dict):
        return base_profile

    summary = str(info.get("longBusinessSummary") or "").strip()
    if len(summary) > 620:
        summary = summary[:617].rsplit(" ", 1)[0].rstrip(".,;:") + "..."

    yahoo_ticker = yahoo_ticker or str(info.get("symbol") or "").strip().upper()
    profile = {
        "name": info.get("longName") or info.get("shortName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "country": info.get("country"),
        "website": info.get("website"),
        "yahoo_url": f"https://finance.yahoo.com/quote/{yahoo_ticker}" if yahoo_ticker else None,
        "summary": summary,
    }
    return {key: value for key, value in profile.items() if value}


def _parse_ma_periods(raw_periods: str) -> List[int]:
    """Parse comma-separated moving-average periods from query params."""

    periods = []
    for part in str(raw_periods or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            value = int(part)
        except ValueError:
            continue
        if value > 0 and value not in periods:
            periods.append(value)
    return periods


def _moving_average_payload(candles: pd.DataFrame, periods: Sequence[int]) -> Dict[str, List[Optional[float]]]:
    """Return moving average values aligned to the candle rows."""

    values: Dict[str, List[Optional[float]]] = {}
    closes = pd.to_numeric(candles["Close"], errors="coerce")
    for period in periods:
        series = closes.rolling(window=period, min_periods=period).mean()
        values[str(period)] = [
            None if pd.isna(value) else round(float(value), 6)
            for value in series
        ]
    return values


def _company_features(info: dict) -> Dict[str, Any]:
    """Keep analysis-useful company facts beside each saved scan result."""

    keys = [
        "shortName",
        "longName",
        "sector",
        "industry",
        "marketCap",
        "beta",
        "trailingPE",
        "forwardPE",
        "priceToBook",
        "profitMargins",
        "returnOnEquity",
        "debtToEquity",
        "currentRatio",
        "quickRatio",
        "recommendationKey",
        "targetMeanPrice",
        "fiftyTwoWeekHigh",
        "fiftyTwoWeekLow",
        "averageVolume",
        "averageVolume10days",
        "sharesOutstanding",
    ]
    return {key: info.get(key) for key in keys if info.get(key) is not None}


def _save_scan(
    cache_file: str,
    provider: str,
    years: int,
    limit: int,
    query: str,
    config: Dict[str, Any],
    tickers: List[str],
    results: List[Dict[str, Any]],
    skipped: int,
) -> int:
    with _connect_write(cache_file) as conn:
        _ensure_scan_schema(conn)
        now = datetime.utcnow().isoformat(timespec="seconds")
        cursor = conn.execute(
            """
            INSERT INTO scan_runs (
                created_at_utc, provider, cache_file, years, limit_count, query,
                scanned_count, result_count, skipped_no_history, config_json, ticker_universe_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                provider,
                str(_cache_path(cache_file)),
                years,
                limit if limit > 0 else None,
                query,
                len(tickers),
                len(results),
                skipped,
                _json_dumps(config),
                _json_dumps(tickers),
            ),
        )
        scan_id = int(cursor.lastrowid)
        for rank, row in enumerate(results, 1):
            row["scan_id"] = scan_id
            row["rank"] = rank
            row.setdefault("label", None)
            conn.execute(
                """
                INSERT INTO scan_results (
                    scan_id, rank, ticker, signal_date, close_price, market_cap,
                    avg_volume, volume_ratio, sector, industry, result_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    scan_id,
                    rank,
                    row.get("ticker"),
                    row.get("date"),
                    _float_or_none(row.get("close_price")),
                    _float_or_none(row.get("market_cap")),
                    _float_or_none(row.get("avg_volume")),
                    _float_or_none(row.get("volume_ratio")),
                    row.get("sector"),
                    row.get("industry"),
                    _json_dumps(row),
                ),
            )
        conn.commit()
    return scan_id


def _label_scan_result(payload: Dict[str, Any]) -> Dict[str, Any]:
    cache_file = payload.get("cache_file") or DEFAULT_CACHE_FILE
    scan_id = int(payload.get("scan_id") or 0)
    ticker = str(payload.get("ticker") or "").strip().upper()
    label = str(payload.get("label") or "").strip().lower().replace("-", "_").replace(" ", "_")
    note = str(payload.get("note") or "").strip() or None
    if scan_id <= 0:
        raise ValueError("scan_id is required")
    if not ticker:
        raise ValueError("ticker is required")

    with _connect_write(cache_file) as conn:
        _ensure_scan_schema(conn)
        exists = conn.execute(
            "SELECT 1 FROM scan_results WHERE scan_id = ? AND ticker = ?",
            (scan_id, ticker),
        ).fetchone()
        if not exists:
            raise ValueError(f"{ticker} is not in scan {scan_id}")

        labeled_at = datetime.utcnow().isoformat(timespec="seconds")
        if label in ("", "clear", "none", "unlabelled", "unlabeled"):
            conn.execute("DELETE FROM scan_labels WHERE scan_id = ? AND ticker = ?", (scan_id, ticker))
            event = _record_central_rating_event(
                conn,
                cache_file,
                scan_id,
                ticker,
                None,
                note,
                labeled_at,
                "clear",
            )
            conn.commit()
            return {
                "ok": True,
                "scan_id": scan_id,
                "ticker": ticker,
                "label": None,
                "central_ratings_file": str(_central_ratings_path()),
                "google_sheets": (event or {}).get("google_sheets", {"configured": False}),
            }
        if label not in VALID_LABELS:
            raise ValueError("label must be winner, potential_winner, maybe, bad, or clear")

        conn.execute(
            """
            INSERT INTO scan_labels (scan_id, ticker, label, note, labeled_at_utc)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(scan_id, ticker) DO UPDATE SET
                label = excluded.label,
                note = excluded.note,
                labeled_at_utc = excluded.labeled_at_utc
            """,
            (scan_id, ticker, label, note, labeled_at),
        )
        event = _record_central_rating_event(
            conn,
            cache_file,
            scan_id,
            ticker,
            label,
            note,
            labeled_at,
            "label",
        )
        conn.commit()
    return {
        "ok": True,
        "scan_id": scan_id,
        "ticker": ticker,
        "label": label,
        "note": note,
        "central_ratings_file": str(_central_ratings_path()),
        "google_sheets": (event or {}).get("google_sheets", {"configured": False}),
    }


def _scan_summary(params: Dict[str, List[str]]) -> Dict[str, Any]:
    cache_file = params.get("cache_file", [DEFAULT_CACHE_FILE])[0] or DEFAULT_CACHE_FILE
    limit = int(params.get("limit", ["20"])[0] or 20)
    with _connect_write(cache_file) as conn:
        _ensure_scan_schema(conn)
        scans = [
            dict(row)
            for row in conn.execute(
                """
                SELECT
                    s.id,
                    s.created_at_utc,
                    s.provider,
                    s.scanned_count,
                    s.result_count,
                    s.query,
                    SUM(CASE WHEN sl.label = 'winner' THEN 1 ELSE 0 END) AS winners,
                    SUM(CASE WHEN sl.label = 'potential_winner' THEN 1 ELSE 0 END) AS potential_winners,
                    SUM(CASE WHEN sl.label = 'maybe' THEN 1 ELSE 0 END) AS maybes,
                    SUM(CASE WHEN sl.label = 'bad' THEN 1 ELSE 0 END) AS bads
                FROM scan_runs s
                LEFT JOIN scan_labels sl ON sl.scan_id = s.id
                GROUP BY s.id
                ORDER BY s.id DESC
                LIMIT ?
                """,
                (limit,),
            )
        ]
    return {"ok": True, "scans": scans}


def _scan_labels(params: Dict[str, List[str]]) -> Dict[str, Any]:
    cache_file = params.get("cache_file", [DEFAULT_CACHE_FILE])[0] or DEFAULT_CACHE_FILE
    scan_id = int(params.get("scan_id", ["0"])[0] or 0)
    if scan_id <= 0:
        raise ValueError("scan_id is required")
    with _connect_write(cache_file) as conn:
        _ensure_scan_schema(conn)
        labels = {
            row["ticker"]: row["label"]
            for row in conn.execute(
                "SELECT ticker, label FROM scan_labels WHERE scan_id = ?",
                (scan_id,),
            )
        }
    return {"ok": True, "scan_id": scan_id, "labels": labels}


def _run_filter(payload: Dict[str, Any], progress_callback=None) -> Dict[str, Any]:
    cache_file = payload.get("cache_file") or DEFAULT_CACHE_FILE
    provider = fetcher.normalize_provider(payload.get("provider") or fetcher.DEFAULT_PROVIDER)
    limit = int(payload.get("limit") or 0)
    query = str(payload.get("query") or "").strip().upper()
    config = _filter_config(payload)
    years = int(payload.get("years") or fetcher.DEFAULT_DATA_YEARS)

    with _connect_readonly(cache_file) as conn:
        tickers = [
            row["ticker"]
            for row in conn.execute(
                "SELECT DISTINCT ticker FROM price_history WHERE provider = ? ORDER BY ticker",
                (provider,),
            )
        ]
        if query:
            tickers = [ticker for ticker in tickers if query in ticker]
        if limit > 0:
            tickers = tickers[:limit]

        end = datetime.now()
        start = end - pd.DateOffset(years=years)
        start_dt = start.to_pydatetime() if hasattr(start, "to_pydatetime") else start
        info_map = _load_info_map(conn, tickers)

        results = []
        skipped = 0
        if progress_callback:
            progress_callback("Filtering", 0, len(tickers), f"Scanning {len(tickers)} cached tickers.")
        for chunk_start in range(0, len(tickers), FILTER_HISTORY_CHUNK_SIZE):
            chunk = tickers[chunk_start : chunk_start + FILTER_HISTORY_CHUNK_SIZE]
            histories = _load_cached_history_frames(conn, provider, chunk, start_dt, end)
            for offset, ticker in enumerate(chunk, 1):
                index = chunk_start + offset
                history = histories.get(ticker)
                if history is None or history.empty:
                    skipped += 1
                    if progress_callback:
                        progress_callback("Filtering", index, len(tickers), f"Skipped {ticker}: no cached history.")
                    continue
                result = analyze_stock_from_local_data(
                    ticker,
                    {
                        "info": info_map.get(ticker, {}),
                        "history": _split_history(history),
                    },
                    config,
                )
                if result:
                    result.update(_company_features(info_map.get(ticker, {})))
                    results.append(result)
                if progress_callback:
                    progress_callback("Filtering", index, len(tickers), f"Filtered {index}/{len(tickers)}: {ticker}")
    results.sort(
        key=lambda item: (
            int(item.get("ma_history_sort") or (0 if item.get("ma_data_complete", True) else 99)),
            -float(item.get("volume_ratio") or 0),
        )
    )
    incomplete_ma_count = sum(1 for item in results if not item.get("ma_data_complete", True))
    ma_tier_counts: Dict[str, int] = {}
    for item in results:
        tier = str(item.get("ma_history_label") or ("Full" if item.get("ma_data_complete", True) else "Younger"))
        ma_tier_counts[tier] = ma_tier_counts.get(tier, 0) + 1
    scan_id = _save_scan(cache_file, provider, years, limit, query, config, tickers, results, skipped)

    return {
        "ok": True,
        "scan_id": scan_id,
        "results": results,
        "result_count": len(results),
        "incomplete_ma_count": incomplete_ma_count,
        "ma_tier_counts": ma_tier_counts,
        "scanned_count": len(tickers),
        "skipped_no_history": skipped,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _start_fetch_job(payload: Dict[str, Any]) -> Dict[str, Any]:
    with JOB_LOCK:
        if CURRENT_JOB["running"]:
            return {"ok": False, "error": "A fetch is already running."}
        CURRENT_JOB.update(
            {
                "running": True,
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "finished_at": None,
                "success": None,
                "message": "Fetch running",
                "stage": "Starting",
                "current": 0,
                "total": None,
                "percent": 0,
                "detail": "Starting fetch job.",
                "log": "",
            }
        )

    thread = threading.Thread(target=_fetch_worker, args=(payload,), daemon=True)
    thread.start()
    return {"ok": True, "job": _job_snapshot()}


def _progress_update(stage: str, current: int, total: Optional[int], message: str) -> None:
    percent = 0
    if total and total > 0:
        percent = max(0, min(100, round((current / total) * 100)))
    with JOB_LOCK:
        CURRENT_JOB.update(
            {
                "stage": stage,
                "current": current,
                "total": total,
                "percent": percent,
                "detail": message,
                "message": message,
            }
        )


def _fetch_worker(payload: Dict[str, Any]) -> None:
    log_buffer = io.StringIO()
    log_writer = JobLogWriter(log_buffer)
    success = False
    try:
        ticker_file = str(payload.get("ticker_file") or DEFAULT_TICKER_FILE)
        output = str(payload.get("output") or DEFAULT_OUTPUT_FILE)
        years = int(payload.get("years") or fetcher.DEFAULT_DATA_YEARS)
        workers = int(payload.get("workers") or fetcher.DEFAULT_WORKERS)
        provider = str(payload.get("provider") or fetcher.DEFAULT_PROVIDER)
        limit = int(payload["limit"]) if str(payload.get("limit") or "").strip() else None
        cache_file = str(payload.get("cache_file") or DEFAULT_CACHE_FILE)
        info_refresh_days = int(payload.get("info_refresh_days") or fetcher.DEFAULT_INFO_REFRESH_DAYS)
        history_refresh_days = int(payload.get("history_refresh_days") or fetcher.DEFAULT_HISTORY_REFRESH_DAYS)
        prune_missing_tickers = bool(payload.get("prune_missing_tickers"))
        history_chunk_size = int(payload.get("history_chunk_size") or fetcher.DEFAULT_HISTORY_CHUNK_SIZE)
        history_pause_seconds = float(payload.get("history_pause_seconds") or fetcher.DEFAULT_HISTORY_PAUSE_SECONDS)
        info_pause_seconds = float(payload.get("info_pause_seconds") or fetcher.DEFAULT_INFO_PAUSE_SECONDS)
        rate_limit_pause_seconds = float(payload.get("rate_limit_pause_seconds") or fetcher.DEFAULT_RATE_LIMIT_PAUSE_SECONDS)
        max_rate_limit_retries = int(payload.get("max_rate_limit_retries") or fetcher.DEFAULT_RATE_LIMIT_RETRIES)
        stop_on_rate_limit = bool(payload.get("stop_on_rate_limit"))
        export_json = bool(payload.get("export_json", False))

        with contextlib.redirect_stdout(log_writer), contextlib.redirect_stderr(log_writer):
            success = fetcher.fetch_stock_data(
                ticker_file=ticker_file,
                output=output,
                years=years,
                workers=workers,
                provider=provider,
                limit=limit,
                cache_file=cache_file,
                info_refresh_days=info_refresh_days,
                history_refresh_days=history_refresh_days,
                progress_callback=_progress_update,
                prune_missing_tickers=prune_missing_tickers,
                history_chunk_size=history_chunk_size,
                history_pause_seconds=history_pause_seconds,
                info_pause_seconds=info_pause_seconds,
                rate_limit_pause_seconds=rate_limit_pause_seconds,
                max_rate_limit_retries=max_rate_limit_retries,
                stop_on_rate_limit=stop_on_rate_limit,
                export_json=export_json,
            )
        message = "Fetch complete" if success else "Fetch failed"
    except Exception:
        log_buffer.write(traceback.format_exc())
        message = "Fetch failed"
        success = False

    with JOB_LOCK:
        CURRENT_JOB.update(
            {
                "running": False,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "success": success,
                "message": message,
                "stage": "Complete" if success else "Failed",
                "percent": 100 if success else CURRENT_JOB.get("percent", 0),
                "detail": message,
                "log": log_buffer.getvalue()[-30000:],
            }
        )


def _job_snapshot() -> Dict[str, Any]:
    with JOB_LOCK:
        return dict(CURRENT_JOB)


def _filter_progress_update(stage: str, current: int, total: Optional[int], message: str) -> None:
    percent = 0
    if total and total > 0:
        percent = max(0, min(100, round((current / total) * 100)))
    with FILTER_LOCK:
        FILTER_JOB.update(
            {
                "stage": stage,
                "current": current,
                "total": total,
                "percent": percent,
                "detail": message,
                "message": message,
            }
        )


def _filter_job_snapshot() -> Dict[str, Any]:
    with FILTER_LOCK:
        return dict(FILTER_JOB)


def _start_filter_job(payload: Dict[str, Any]) -> Dict[str, Any]:
    with FILTER_LOCK:
        if FILTER_JOB["running"]:
            return {"ok": False, "error": "A filter scan is already running."}
        FILTER_JOB.update(
            {
                "running": True,
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "finished_at": None,
                "success": None,
                "message": "Filter running",
                "stage": "Starting",
                "current": 0,
                "total": None,
                "percent": 0,
                "detail": "Preparing cached filter scan.",
                "results": [],
                "summary": {},
            }
        )
    threading.Thread(target=_filter_worker, args=(payload,), daemon=True).start()
    return {"ok": True, "job": _filter_job_snapshot()}


def _filter_worker(payload: Dict[str, Any]) -> None:
    try:
        result = _run_filter(payload, _filter_progress_update)
        success = True
        message = "Filter complete"
    except Exception as exc:
        result = {"results": [], "error": str(exc)}
        success = False
        message = "Filter failed"

    with FILTER_LOCK:
        FILTER_JOB.update(
            {
                "running": False,
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "success": success,
                "message": message,
                "stage": "Complete" if success else "Failed",
                "percent": 100 if success else FILTER_JOB.get("percent", 0),
                "detail": message,
                "results": result.get("results", []),
                "summary": {
                    key: value
                    for key, value in result.items()
                    if key not in ("results", "ok")
                },
            }
        )


class AppHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        sys.__stdout__.write("%s - %s\n" % (self.address_string(), format % args))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/":
                _html_response(self, INDEX_HTML)
            elif parsed.path == "/favicon.ico":
                self.send_response(HTTPStatus.NO_CONTENT)
                self.end_headers()
            elif parsed.path == "/api/status":
                params = parse_qs(parsed.query)
                cache_file = params.get("cache_file", [DEFAULT_CACHE_FILE])[0]
                _json_response(self, {"ok": True, "status": _cache_status(cache_file), "job": _job_snapshot()})
            elif parsed.path == "/api/chart":
                _json_response(self, _chart_payload(parse_qs(parsed.query)))
            elif parsed.path == "/api/ticker-files":
                _json_response(self, {"ok": True, "files": _ticker_file_options()})
            elif parsed.path == "/api/scans":
                _json_response(self, _scan_summary(parse_qs(parsed.query)))
            elif parsed.path == "/api/labels":
                _json_response(self, _scan_labels(parse_qs(parsed.query)))
            elif parsed.path == "/api/job":
                _json_response(self, {"ok": True, "job": _job_snapshot()})
            elif parsed.path == "/api/filter/job":
                _json_response(self, {"ok": True, "job": _filter_job_snapshot()})
            elif parsed.path == "/api/config":
                _json_response(self, {"ok": True, "config": _load_default_config(), "markets": MARKET_DEFAULTS})
            else:
                _json_response(self, {"ok": False, "error": "Not found"}, HTTPStatus.NOT_FOUND)
        except Exception as exc:
            _json_response(self, {"ok": False, "error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:
        try:
            payload = _read_json(self)
            if self.path == "/api/fetch":
                _json_response(self, _start_fetch_job(payload))
            elif self.path == "/api/us-tickers":
                output_file = str(payload.get("output_file") or DEFAULT_US_TICKER_FILE)
                _json_response(self, {"ok": True, "result": fetcher.write_us_ticker_file(output_file)})
            elif self.path == "/api/filter/start":
                _json_response(self, _start_filter_job(payload))
            elif self.path == "/api/filter":
                _json_response(self, _run_filter(payload))
            elif self.path == "/api/label":
                _json_response(self, _label_scan_result(payload))
            else:
                _json_response(self, {"ok": False, "error": "Not found"}, HTTPStatus.NOT_FOUND)
        except Exception as exc:
            _json_response(self, {"ok": False, "error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Moneymaker Stock Filter</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #111614;
      --panel: #18211e;
      --panel-2: #202b27;
      --line: #314039;
      --text: #edf4ef;
      --muted: #9fb0a8;
      --accent: #55c47a;
      --accent-2: #6ab8d8;
      --warn: #e0b45b;
      --bad: #df7770;
      --shadow: rgba(0, 0, 0, .25);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font: 14px/1.45 "Segoe UI", system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 14px 20px;
      border-bottom: 1px solid var(--line);
      background: #151c19;
      position: sticky;
      top: 0;
      z-index: 2;
    }
    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 650;
      letter-spacing: 0;
    }
    main {
      display: grid;
      grid-template-columns: minmax(320px, 390px) minmax(0, 1fr);
      min-height: calc(100vh - 58px);
    }
    aside {
      border-right: 1px solid var(--line);
      background: #131a17;
      padding: 16px;
      overflow: auto;
    }
    section.workspace {
      padding: 16px;
      overflow: auto;
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(120px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .metric, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 6px;
      box-shadow: 0 6px 18px var(--shadow);
    }
    .metric { padding: 12px; min-height: 78px; }
    .metric span { color: var(--muted); display: block; font-size: 12px; }
    .metric strong { display: block; font-size: 22px; margin-top: 6px; }
    .panel { padding: 14px; margin-bottom: 14px; }
    .panel h2 {
      margin: 0 0 12px;
      font-size: 14px;
      font-weight: 650;
      color: #dbe8e0;
    }
    label {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin: 9px 0 4px;
    }
    input, select {
      width: 100%;
      background: #0f1513;
      color: var(--text);
      border: 1px solid var(--line);
      border-radius: 5px;
      min-height: 34px;
      padding: 6px 9px;
      font: inherit;
    }
    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
    .grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; }
    .actions { display: flex; gap: 8px; align-items: center; margin-top: 12px; }
    .inline-check {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-top: 10px;
      color: var(--text);
    }
    .inline-check input[type="checkbox"] {
      width: auto;
      min-height: auto;
      margin: 0;
    }
    button {
      border: 1px solid var(--line);
      background: var(--panel-2);
      color: var(--text);
      border-radius: 5px;
      min-height: 36px;
      padding: 7px 12px;
      font: inherit;
      cursor: pointer;
    }
    button.primary { background: #237046; border-color: #35925e; }
    button.secondary { background: #1d3f4d; border-color: #2d6981; }
    button:disabled { opacity: .55; cursor: default; }
    input:disabled {
      opacity: .55;
      cursor: default;
    }
    .status-line {
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      background: #121917;
      border: 1px solid var(--line);
      border-radius: 6px;
      overflow: hidden;
    }
    th, td {
      border-bottom: 1px solid var(--line);
      padding: 8px 9px;
      text-align: right;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    th {
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
      background: #19211e;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    td:first-child, th:first-child { text-align: left; }
    .ticker {
      color: #8fe1a9;
      font-weight: 650;
      text-decoration: none;
    }
    .label-cell {
      display: flex;
      gap: 5px;
      justify-content: flex-end;
      align-items: center;
    }
    .label-btn {
      min-height: 28px;
      padding: 4px 7px;
      font-size: 12px;
      border-radius: 5px;
    }
    .label-btn.active[data-label="winner"] {
      background: #246d42;
      border-color: #49b973;
    }
    .label-btn.active[data-label="potential_winner"] {
      background: #285f6d;
      border-color: #58b8ce;
    }
    .label-btn.active[data-label="maybe"] {
      background: #6d5422;
      border-color: #d1a94e;
    }
    .label-btn.active[data-label="bad"] {
      background: #73342f;
      border-color: #d87268;
    }
    tr.label-winner td:first-child { border-left: 3px solid var(--accent); }
    tr.label-potential_winner td:first-child { border-left: 3px solid var(--accent-2); }
    tr.label-maybe td:first-child { border-left: 3px solid var(--warn); }
    tr.label-bad td:first-child { border-left: 3px solid var(--bad); }
    tr.incomplete-ma td { background: rgba(242, 193, 78, 0.06); }
    tr.incomplete-ma td:nth-child(7) { color: var(--warn); }
    .results-wrap { max-height: calc(100vh - 210px); overflow: auto; border-radius: 6px; }
    .chart-controls {
      display: grid;
      grid-template-columns: minmax(110px, 160px) minmax(110px, 140px) minmax(110px, 140px) auto;
      gap: 8px;
      align-items: end;
      margin-bottom: 10px;
    }
    .chart-box {
      height: 430px;
      min-height: 280px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #0c1110;
      overflow: hidden;
      position: relative;
    }
    #chartPanel.fullscreen {
      position: fixed;
      inset: 12px;
      z-index: 20;
      display: flex;
      flex-direction: column;
      overflow: auto;
      background: var(--panel);
      box-shadow: 0 24px 70px rgba(0, 0, 0, .48);
    }
    body.chart-fullscreen {
      overflow: hidden;
    }
    #chartPanel.fullscreen .chart-box {
      flex: 1 1 auto;
      height: auto;
      min-height: 560px;
    }
    .chart-header-actions {
      display: flex;
      align-items: center;
      gap: 8px;
      justify-content: flex-end;
    }
    .ma-controls {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 0 0 10px;
    }
    .ma-toggle {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      margin: 0;
      padding: 5px 8px;
      color: var(--text);
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #111816;
      font-size: 12px;
    }
    .ma-toggle input {
      width: auto;
      min-height: auto;
      margin: 0;
    }
    .ma-dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      display: inline-block;
    }
    .company-profile {
      margin: 0 0 10px;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #101715;
    }
    .company-profile.empty {
      color: var(--muted);
    }
    .company-profile h3 {
      margin: 0 0 4px;
      font-size: 15px;
      font-weight: 650;
    }
    .company-profile .company-meta {
      margin: 0 0 7px;
      color: var(--muted);
      font-size: 12px;
    }
    .company-profile p {
      margin: 0;
      color: #d6e0da;
      font-size: 13px;
      line-height: 1.45;
    }
    .company-profile a {
      color: var(--accent-2);
      text-decoration: none;
    }
    .company-profile.focused {
      border-color: var(--accent-2);
      box-shadow: 0 0 0 2px rgba(103, 177, 142, .18);
    }
    #priceChart {
      display: block;
      width: 100%;
      height: 100%;
    }
    .log {
      margin: 0;
      max-height: 230px;
      overflow: auto;
      background: #0c1110;
      border: 1px solid var(--line);
      border-radius: 5px;
      padding: 10px;
      color: #b7c6bd;
      font: 12px/1.35 Consolas, "Cascadia Mono", monospace;
      white-space: pre-wrap;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      color: var(--muted);
      border: 1px solid var(--line);
      background: #111816;
      border-radius: 999px;
      padding: 5px 9px;
      font-size: 12px;
    }
    .modal {
      position: fixed;
      inset: 0;
      z-index: 10;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 18px;
      background: rgba(3, 7, 6, .72);
    }
    .modal.hidden { display: none; }
    .progress-window {
      width: min(760px, 100%);
      max-height: min(720px, calc(100vh - 32px));
      display: flex;
      flex-direction: column;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 7px;
      box-shadow: 0 18px 55px rgba(0, 0, 0, .45);
      overflow: hidden;
    }
    .progress-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      background: #151d1a;
    }
    .progress-head h2 {
      margin: 0;
      font-size: 15px;
    }
    .progress-body {
      padding: 16px;
      overflow: auto;
    }
    .progress-bar {
      height: 14px;
      overflow: hidden;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #0d1411;
      margin: 10px 0 8px;
    }
    .progress-fill {
      width: 0%;
      height: 100%;
      background: linear-gradient(90deg, #48b36c, #69c9dd);
      transition: width .2s ease;
    }
    .progress-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 8px;
      margin: 12px 0;
    }
    .progress-tile {
      border: 1px solid var(--line);
      border-radius: 5px;
      padding: 9px;
      background: #111816;
      min-height: 58px;
    }
    .progress-tile span {
      display: block;
      color: var(--muted);
      font-size: 12px;
    }
    .progress-tile strong {
      display: block;
      margin-top: 4px;
      font-size: 17px;
    }
    .ok { color: var(--accent); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }
    @media (max-width: 940px) {
      main { grid-template-columns: 1fr; }
      aside { border-right: 0; border-bottom: 1px solid var(--line); }
      .metrics { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
      .chart-controls { grid-template-columns: 1fr 1fr; }
    }
    @media (max-width: 520px) {
      header {
        align-items: flex-start;
        flex-direction: column;
        gap: 4px;
      }
      header .status-line {
        white-space: normal;
      }
      .progress-grid {
        grid-template-columns: 1fr;
      }
      .chart-controls { grid-template-columns: 1fr; }
      .chart-box { height: 330px; }
      main {
        min-height: calc(100vh - 82px);
      }
    }
  </style>
</head>
<body>
  <header>
    <h1>Moneymaker Stock Filter</h1>
    <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;justify-content:flex-end">
      <span class="status-line" id="topStatus">Loading cache status...</span>
      <input id="authEmail" type="email" placeholder="Email" autocomplete="username" style="width:180px">
      <input id="authPassword" type="password" placeholder="Password" autocomplete="current-password" style="width:140px">
      <button id="authSignIn" type="button">Sign in</button>
      <button id="authCreate" type="button">Create account</button>
      <button id="authSignOut" type="button" hidden>Sign out</button>
    </div>
  </header>
  <main>
    <aside>
      <div class="panel">
        <h2>Cache</h2>
        <label for="marketSelect">Market</label>
        <select id="marketSelect">
          <option value="asx">ASX</option>
          <option value="us">US</option>
        </select>
        <label for="cacheFile">SQLite file</label>
        <input id="cacheFile" value="stock_cache.sqlite">
        <div class="actions">
          <button id="refreshStatus">Refresh</button>
          <span class="pill" id="cachePill">No cache</span>
        </div>
      </div>

      <div class="panel">
        <h2>Fetch</h2>
        <div class="actions" style="margin-top:0">
          <button id="downloadUsTickers" type="button">Download US Tickers</button>
          <span class="status-line" id="tickerSourceStatus">Nasdaq Trader source</span>
        </div>
        <label for="tickerFileSelect">Available ticker files</label>
        <select id="tickerFileSelect"></select>
        <label for="tickerFile">Ticker file</label>
        <input id="tickerFile" value="asx_yfinance_valid_stocks_2026-05-11.txt">
        <div class="grid-2">
          <div>
            <label for="provider">Provider</label>
            <select id="provider"><option>yfinance</option><option>stooq</option></select>
          </div>
          <div>
            <label for="fetchLimit">Limit</label>
            <input id="fetchLimit" type="number" min="1" value="100" disabled>
          </div>
        </div>
        <label class="inline-check">
          <input id="useFetchLimit" type="checkbox">
          Limit fetch to first N tickers
        </label>
        <div class="grid-2">
          <div>
            <label for="years">Years</label>
            <input id="years" type="number" min="1" value="15">
          </div>
          <div>
            <label for="workers">Workers</label>
            <input id="workers" type="number" min="1" value="1">
          </div>
        </div>
        <div class="grid-2">
          <div>
            <label for="infoRefresh">Info refresh days</label>
            <input id="infoRefresh" type="number" min="0" value="7">
          </div>
          <div>
            <label for="historyRefresh">History overlap days</label>
            <input id="historyRefresh" type="number" min="0" value="5">
          </div>
        </div>
        <div class="grid-2">
          <div>
            <label for="historyChunkSize">History batch size</label>
            <input id="historyChunkSize" type="number" min="1" value="50">
          </div>
          <div>
            <label for="historyPause">Batch pause sec</label>
            <input id="historyPause" type="number" min="0" step="1" value="5">
          </div>
        </div>
        <div class="grid-2">
          <div>
            <label for="infoPause">Info pause sec</label>
            <input id="infoPause" type="number" min="0" step="1" value="1">
          </div>
          <div>
            <label for="ratePause">Rate-limit pause sec</label>
            <input id="ratePause" type="number" min="0" step="10" value="900">
          </div>
        </div>
        <div class="grid-2">
          <div>
            <label for="rateRetries">Rate-limit retries</label>
            <input id="rateRetries" type="number" min="0" value="3">
          </div>
          <div>
            <label>&nbsp;</label>
            <span class="status-line">Use workers=1 for safest full ASX runs.</span>
          </div>
        </div>
        <label class="inline-check">
          <input id="stopOnRateLimit" type="checkbox" checked>
          Stop and resume later if rate-limited
        </label>
        <label class="inline-check">
          <input id="pruneMissing" type="checkbox">
          Create cleaned ticker file when symbols fail
        </label>
        <div class="actions">
          <button class="primary" id="startFetch">Update Cache</button>
          <span class="status-line" id="fetchStatus">Idle</span>
        </div>
      </div>

      <div class="panel">
        <h2>Filter</h2>
        <div class="grid-2">
          <div>
            <label for="volumeMultiplier">Volume multiplier</label>
            <input id="volumeMultiplier" type="number" step="0.1" value="2.0">
          </div>
          <div>
            <label for="lookbackWeeks">Lookback weeks</label>
            <input id="lookbackWeeks" type="number" min="1" value="1">
          </div>
        </div>
        <div class="grid-2">
          <div>
            <label for="minCap">Min cap M</label>
            <input id="minCap" type="number" step="1" value="0">
          </div>
          <div>
            <label for="maxCap">Max cap M</label>
            <input id="maxCap" type="number" step="1" value="0">
          </div>
        </div>
        <label>Moving averages weeks (0 off)</label>
        <div class="grid-4">
          <input id="maShort" type="number" value="90">
          <input id="maIntermediate" type="number" value="180">
          <input id="maMedium" type="number" value="360">
          <input id="maLong" type="number" value="700" title="Set to 0 to turn off the 700-week moving average">
        </div>
        <div class="grid-2">
          <div>
            <label for="query">Ticker search</label>
            <input id="query" placeholder="CBA">
          </div>
          <div>
            <label for="scanLimit">Scan limit</label>
            <input id="scanLimit" type="number" min="1" value="250" disabled>
          </div>
        </div>
        <label class="inline-check">
          <input id="useScanLimit" type="checkbox">
          Limit scan to first N cached tickers
        </label>
        <div class="actions">
          <button class="secondary" id="runFilter">Run Filter</button>
          <span class="status-line" id="filterStatus">Ready</span>
        </div>
      </div>

      <div class="panel">
        <h2>Fetch Log</h2>
        <pre class="log" id="fetchLog"></pre>
      </div>
    </aside>

    <section class="workspace">
      <div class="metrics">
        <div class="metric"><span>Cached tickers</span><strong id="metricTickers">0</strong></div>
        <div class="metric"><span>History rows</span><strong id="metricRows">0</strong></div>
        <div class="metric"><span>Latest bar</span><strong id="metricLatest">-</strong></div>
        <div class="metric"><span>Matches</span><strong id="metricMatches">0</strong></div>
      </div>
      <div class="panel" id="chartPanel">
        <div class="actions" style="justify-content:space-between;margin-top:0;margin-bottom:10px">
          <h2 style="margin:0">Chart</h2>
          <div class="chart-header-actions">
            <span class="status-line" id="chartStatus">Select a cached ticker</span>
            <button id="toggleChartFullscreen" type="button" aria-pressed="false">Fullscreen</button>
          </div>
        </div>
        <div class="chart-controls">
          <div>
            <label for="chartTicker">Ticker</label>
            <input id="chartTicker" value="CBA.AX">
          </div>
          <div>
            <label for="chartInterval">Candles</label>
            <select id="chartInterval">
              <option value="daily">Daily</option>
              <option value="weekly">Weekly</option>
              <option value="monthly">Monthly</option>
            </select>
          </div>
          <div>
            <label for="chartRange">Timeframe</label>
            <select id="chartRange">
              <option value="3m">3M</option>
              <option value="6m">6M</option>
              <option value="1y" selected>1Y</option>
              <option value="2y">2Y</option>
              <option value="5y">5Y</option>
              <option value="10y">10Y</option>
              <option value="all">All</option>
            </select>
          </div>
          <button id="loadChart">Load Chart</button>
        </div>
        <div class="ma-controls" aria-label="Moving average overlays">
          <label class="ma-toggle"><input class="ma-check" type="checkbox" value="30" checked><span class="ma-dot" style="background:#f2c14e"></span>MA 30</label>
          <label class="ma-toggle"><input class="ma-check" type="checkbox" value="90" checked><span class="ma-dot" style="background:#8dd7ff"></span>MA 90</label>
          <label class="ma-toggle"><input class="ma-check" type="checkbox" value="180" checked><span class="ma-dot" style="background:#b58cff"></span>MA 180</label>
          <label class="ma-toggle"><input class="ma-check" type="checkbox" value="360" checked><span class="ma-dot" style="background:#ff9f7a"></span>MA 360</label>
          <label class="ma-toggle"><input class="ma-check" type="checkbox" value="700" checked><span class="ma-dot" style="background:#d9e672"></span>MA 700</label>
        </div>
        <div class="company-profile empty" id="companyProfile">
          <p>Load a cached ticker to show the company description.</p>
        </div>
        <div class="chart-box">
          <canvas id="priceChart"></canvas>
        </div>
      </div>
      <div class="panel">
        <div class="actions" style="justify-content:space-between;margin-top:0;margin-bottom:10px">
          <h2 style="margin:0">Results</h2>
          <span class="status-line" id="resultMeta">No scan yet</span>
        </div>
        <div class="results-wrap">
          <table>
            <thead>
              <tr>
                <th style="width:90px">Ticker</th>
                <th style="width:90px">Date</th>
                <th style="width:80px">Close</th>
                <th style="width:105px">Market Cap</th>
                <th style="width:110px">Avg Volume</th>
                <th style="width:95px">Volume Ratio</th>
                <th style="width:150px">MA Data</th>
                <th style="width:260px">Label</th>
              </tr>
            </thead>
            <tbody id="resultsBody">
              <tr><td colspan="8" style="text-align:left;color:var(--muted)">No results loaded.</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>
  </main>

  <div class="modal hidden" id="progressModal" role="dialog" aria-modal="true" aria-labelledby="progressTitle">
    <div class="progress-window">
      <div class="progress-head">
        <h2 id="progressTitle">Process Progress</h2>
        <button id="closeProgress">Close</button>
      </div>
      <div class="progress-body">
        <div class="status-line" id="progressDetail">Idle</div>
        <div class="progress-bar" aria-label="Progress">
          <div class="progress-fill" id="progressFill"></div>
        </div>
        <div class="progress-grid">
          <div class="progress-tile"><span>Stage</span><strong id="progressStage">Idle</strong></div>
          <div class="progress-tile"><span>Count</span><strong id="progressCount">0</strong></div>
          <div class="progress-tile"><span>Complete</span><strong id="progressPercent">0%</strong></div>
        </div>
        <pre class="log" id="modalFetchLog"></pre>
      </div>
    </div>
  </div>

  <script>
    const $ = (id) => document.getElementById(id);
    const fmt = new Intl.NumberFormat();
    let authReady = Promise.resolve(null);
    let firebaseAuth = null;
    let firebaseAuthMethods = null;
    let currentUserProfile = null;
    async function initializeFirebaseAuth() {
      try {
        const response = await fetch("/api/auth-config");
        if (!response.ok) return null;
        const config = await response.json();
        if (!config.enabled || !config.apiKey || !config.appId) return null;
        const [{ initializeApp }, authModule] = await Promise.all([
          import("https://www.gstatic.com/firebasejs/10.12.5/firebase-app.js"),
          import("https://www.gstatic.com/firebasejs/10.12.5/firebase-auth.js")
        ]);
        const app = initializeApp(config);
        const auth = authModule.getAuth(app);
        firebaseAuth = auth;
        firebaseAuthMethods = authModule;
        authModule.onAuthStateChanged(auth, async (user) => {
          if (user) {
            $("authEmail").value = user.email || "";
            $("authSignOut").hidden = false;
            const anonymous = Boolean(user.isAnonymous);
            $("authSignIn").hidden = !anonymous;
            $("authCreate").hidden = !anonymous;
            $("authPassword").hidden = !anonymous;
            if (currentUserProfile?.user) $("topStatus").textContent = `Signed in as ${currentUserProfile.user.email || "anonymous"}`;
            else $("topStatus").textContent = anonymous ? "Anonymous session" : `Signed in as ${user.email || "user"}`;
          }
        });
        await auth.authStateReady();
        const result = auth.currentUser ? { user: auth.currentUser } : await authModule.signInAnonymously(auth);
        return () => authModule.getIdToken(result.user, true);
      } catch (error) {
        console.warn("Firebase authentication is unavailable", error);
        return null;
      }
    }
    authReady = initializeFirebaseAuth();
    let lastCandles = [];
    let lastChartTicker = "";
    let lastMovingAverages = {};
    let currentScanId = null;
    const maColors = {
      "30": "#f2c14e",
      "90": "#8dd7ff",
      "180": "#b58cff",
      "360": "#ff9f7a",
      "700": "#d9e672"
    };
    const marketDefaults = {
      asx: {
        cacheFile: "stock_cache.sqlite",
        tickerFile: "asx_yfinance_valid_stocks_2026-05-11.txt",
        provider: "yfinance",
        chartTicker: "CBA.AX",
        output: "stock_data_web.json"
      },
      us: {
        cacheFile: "stock_cache_us.sqlite",
        tickerFile: "us_tickers_nasdaqtrader.txt",
        provider: "yfinance",
        chartTicker: "AAPL",
        output: "stock_data_us_web.json"
      }
    };

    function numberValue(id) {
      const value = $(id).value.trim();
      return value === "" ? null : Number(value);
    }

    function marketCap(value) {
      if (value === null || value === undefined) return "N/A";
      if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)}B`;
      if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
      return fmt.format(Math.round(value));
    }

    function renderCompanyProfile(company, ticker) {
      const box = $("companyProfile");
      box.replaceChildren();
      if (!company || (!company.summary && !company.name && !company.sector && !company.industry)) {
        box.className = "company-profile empty";
        const p = document.createElement("p");
        p.textContent = `${ticker || "Ticker"}: no cached company description yet. Run a fetch with company info enabled to fill this in.`;
        box.appendChild(p);
        if (company && company.yahoo_url) {
          const meta = document.createElement("div");
          meta.className = "company-meta";
          const yahoo = document.createElement("a");
          yahoo.href = company.yahoo_url;
          yahoo.target = "_blank";
          yahoo.rel = "noreferrer";
          yahoo.textContent = "Yahoo Finance";
          meta.appendChild(yahoo);
          box.appendChild(meta);
        }
        return;
      }

      box.className = "company-profile";
      const heading = document.createElement("h3");
      heading.textContent = company.name || ticker;
      box.appendChild(heading);

      const metaParts = [company.sector, company.industry, company.country].filter(Boolean);
      if (metaParts.length || company.website || company.yahoo_url) {
        const meta = document.createElement("div");
        meta.className = "company-meta";
        meta.appendChild(document.createTextNode(metaParts.join(" / ")));
        if (company.website) {
          if (metaParts.length) meta.appendChild(document.createTextNode(" / "));
          const link = document.createElement("a");
          link.href = company.website;
          link.target = "_blank";
          link.rel = "noreferrer";
          link.textContent = "Website";
          meta.appendChild(link);
        }
        if (company.yahoo_url) {
          if (metaParts.length || company.website) meta.appendChild(document.createTextNode(" / "));
          const yahoo = document.createElement("a");
          yahoo.href = company.yahoo_url;
          yahoo.target = "_blank";
          yahoo.rel = "noreferrer";
          yahoo.textContent = "Yahoo Finance";
          meta.appendChild(yahoo);
        }
        box.appendChild(meta);
      }

      const summary = document.createElement("p");
      summary.textContent = company.summary || "No business summary was included in the cached company info.";
      box.appendChild(summary);
    }

    function priceLabel(value) {
      if (!Number.isFinite(value)) return "";
      if (value >= 100) return value.toFixed(2);
      if (value >= 1) return value.toFixed(3);
      return value.toFixed(4);
    }

    function selectedMaPeriods() {
      return Array.from(document.querySelectorAll(".ma-check:checked"))
        .map((input) => input.value)
        .join(",");
    }

    function activeLimit(checkboxId, inputId) {
      return $(checkboxId).checked ? $(inputId).value.trim() : "";
    }

    function bindLimitToggle(checkboxId, inputId) {
      const sync = () => {
        $(inputId).disabled = !$(checkboxId).checked;
      };
      $(checkboxId).addEventListener("change", sync);
      sync();
    }

    async function api(path, options = {}) {
      const getToken = await authReady;
      const headers = new Headers(options.headers || {});
      headers.set("Content-Type", "application/json");
      if (getToken) {
        const token = await getToken();
        if (token) headers.set("Authorization", `Bearer ${token}`);
      }
      const response = await fetch(path, { ...options, headers });
      const payload = await response.json();
      if (!payload.ok) throw new Error(payload.error || "Request failed");
      return payload;
    }

    function currentMarketDefaults() {
      return marketDefaults[$("marketSelect").value] || marketDefaults.asx;
    }

    function applyMarketDefaults() {
      const defaults = currentMarketDefaults();
      $("cacheFile").value = defaults.cacheFile;
      $("tickerFile").value = defaults.tickerFile;
      $("provider").value = defaults.provider;
      $("chartTicker").value = defaults.chartTicker;
      $("tickerSourceStatus").textContent = $("marketSelect").value === "us"
        ? "US uses Nasdaq Trader tickers and a separate SQLite cache"
        : "ASX uses the existing local ticker files and cache";
      loadTickerFiles();
      refreshStatus().catch((error) => {
        $("topStatus").textContent = error.message;
        $("topStatus").className = "status-line bad";
      }).finally(() => loadChart());
    }

    async function refreshStatus() {
      const cache = encodeURIComponent($("cacheFile").value.trim() || "stock_cache.sqlite");
      const market = encodeURIComponent($("marketSelect").value);
      const payload = await api(`/api/status?market=${market}&cache_file=${cache}`);
      const s = payload.status;
      $("metricTickers").textContent = fmt.format(s.ticker_count || 0);
      $("metricRows").textContent = fmt.format(s.history_rows || 0);
      $("metricLatest").textContent = s.latest_date || "-";
      $("cachePill").textContent = s.exists ? `${s.size_mb} MB` : "No cache";
      $("cachePill").className = `pill ${s.exists ? "ok" : "warn"}`;
      $("topStatus").textContent = s.exists
        ? `${s.ticker_count} tickers, ${s.history_rows} rows, latest ${s.latest_date || "-"}`
        : "Cache not found";
      if (s.tickers && s.tickers.length && !$("chartTicker").value.trim()) {
        $("chartTicker").value = s.tickers[0];
      }
      updateJob(payload.job);
    }

    async function loadTickerFiles() {
      try {
        const payload = await api("/api/ticker-files");
        const select = $("tickerFileSelect");
        select.innerHTML = (payload.files || []).map((file) =>
          `<option value="${file.name}">${file.name} (${file.size_kb} KB)</option>`
        ).join("");
        const current = $("tickerFile").value.trim();
        if (current) select.value = current;
        if (!current && payload.files && payload.files.length) {
          $("tickerFile").value = payload.files[0].name;
        }
      } catch (error) {
        $("fetchStatus").textContent = error.message;
        $("fetchStatus").className = "status-line bad";
      }
    }

    async function downloadUsTickers() {
      $("tickerSourceStatus").textContent = "Downloading US ticker list...";
      $("tickerSourceStatus").className = "status-line warn";
      try {
        const response = await api("/api/us-tickers", {
          method: "POST",
          body: JSON.stringify({ output_file: marketDefaults.us.tickerFile })
        });
        const result = response.result;
        $("marketSelect").value = "us";
        await loadTickerFiles();
        applyMarketDefaults();
        $("tickerSourceStatus").textContent = `Wrote ${fmt.format(result.ticker_count || 0)} symbols to ${result.output_file}`;
        $("tickerSourceStatus").className = "status-line ok";
      } catch (error) {
        $("tickerSourceStatus").textContent = error.message;
        $("tickerSourceStatus").className = "status-line bad";
      }
    }

    function focusChartPanel() {
      $("chartPanel").scrollIntoView({ behavior: "smooth", block: "start" });
      $("companyProfile").classList.add("focused");
      window.setTimeout(() => $("companyProfile").classList.remove("focused"), 1600);
    }

    function redrawChartSoon() {
      window.setTimeout(() => {
        if (lastCandles.length) drawCandles(lastCandles, lastChartTicker, lastMovingAverages);
      }, 80);
    }

    function toggleChartFullscreen(force = null) {
      const panel = $("chartPanel");
      const button = $("toggleChartFullscreen");
      const enabled = force === null ? !panel.classList.contains("fullscreen") : !!force;
      panel.classList.toggle("fullscreen", enabled);
      document.body.classList.toggle("chart-fullscreen", enabled);
      button.textContent = enabled ? "Exit Fullscreen" : "Fullscreen";
      button.setAttribute("aria-pressed", String(enabled));
      redrawChartSoon();
    }

    async function loadChart(ticker = null, focusChart = false) {
      if (ticker) $("chartTicker").value = ticker;
      const selectedTicker = $("chartTicker").value.trim().toUpperCase();
      if (!selectedTicker) {
        $("chartStatus").textContent = "Enter a ticker";
        if (focusChart) focusChartPanel();
        return;
      }
      $("chartTicker").value = selectedTicker;
      $("chartStatus").textContent = "Loading candles...";
      $("chartStatus").className = "status-line";
      const params = new URLSearchParams({
        market: $("marketSelect").value,
        cache_file: $("cacheFile").value.trim() || "stock_cache.sqlite",
        provider: $("provider").value,
        ticker: selectedTicker,
        interval: $("chartInterval").value,
        range: $("chartRange").value,
        ma: selectedMaPeriods()
      });
      try {
        const payload = await api(`/api/chart?${params.toString()}`);
        renderCompanyProfile(payload.company || {}, payload.ticker || selectedTicker);
        drawCandles(payload.candles || [], payload.ticker, payload.moving_averages || {});
        $("chartStatus").textContent = payload.count
          ? `${payload.ticker} ${payload.interval} candles, ${payload.start} to ${payload.end}`
          : `${payload.ticker || selectedTicker}: no cached candles`;
      } catch (error) {
        renderCompanyProfile({}, selectedTicker);
        $("chartStatus").textContent = error.message;
        $("chartStatus").className = "status-line bad";
      } finally {
        if (focusChart) focusChartPanel();
      }
    }

    function drawCandles(candles, ticker, movingAverages = {}) {
      lastCandles = candles;
      lastChartTicker = ticker || lastChartTicker;
      lastMovingAverages = movingAverages || {};
      const canvas = $("priceChart");
      const box = canvas.parentElement;
      const dpr = window.devicePixelRatio || 1;
      const width = Math.max(320, box.clientWidth);
      const height = Math.max(260, box.clientHeight);
      canvas.width = Math.floor(width * dpr);
      canvas.height = Math.floor(height * dpr);
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;

      const ctx = canvas.getContext("2d");
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);
      ctx.fillStyle = "#0c1110";
      ctx.fillRect(0, 0, width, height);

      if (!candles.length) {
        ctx.fillStyle = "#9fb0a8";
        ctx.font = "13px Segoe UI, sans-serif";
        ctx.fillText("No cached candle data for this selection.", 18, 32);
        return;
      }

      const pad = { left: 58, right: 12, top: 44, bottom: 28 };
      const volumeBandHeight = Math.max(70, Math.min(150, height * 0.22));
      const volumeGap = 16;
      const priceTop = pad.top;
      const volumeBottom = height - pad.bottom;
      const volumeTop = volumeBottom - volumeBandHeight;
      const priceBottom = Math.max(priceTop + 80, volumeTop - volumeGap);
      const chartWidth = width - pad.left - pad.right;
      const priceHeight = priceBottom - priceTop;

      const maValues = Object.values(movingAverages || {})
        .flat()
        .filter((value) => Number.isFinite(value));
      const high = Math.max(...candles.map(c => c.high), ...maValues);
      const low = Math.min(...candles.map(c => c.low), ...maValues);
      const volumeMax = Math.max(...candles.map(c => c.volume || 0), 1);
      const priceRange = Math.max(high - low, 0.000001);
      const y = (price) => priceTop + ((high - price) / priceRange) * priceHeight;
      const candleStep = chartWidth / candles.length;
      const bodyWidth = Math.max(2, Math.min(11, candleStep * 0.62));

      ctx.strokeStyle = "#24332d";
      ctx.lineWidth = 1;
      ctx.fillStyle = "#9fb0a8";
      ctx.font = "11px Segoe UI, sans-serif";
      ctx.textAlign = "right";
      ctx.textBaseline = "middle";

      for (let i = 0; i <= 4; i++) {
        const py = priceTop + (priceHeight / 4) * i;
        const price = high - (priceRange / 4) * i;
        ctx.beginPath();
        ctx.moveTo(pad.left, py);
        ctx.lineTo(width - pad.right, py);
        ctx.stroke();
        ctx.fillText(priceLabel(price), pad.left - 8, py);
      }

      ctx.strokeStyle = "#1f2d27";
      ctx.beginPath();
      ctx.moveTo(pad.left, volumeTop);
      ctx.lineTo(width - pad.right, volumeTop);
      ctx.stroke();
      ctx.fillStyle = "#9fb0a8";
      ctx.font = "11px Segoe UI, sans-serif";
      ctx.textAlign = "right";
      ctx.fillText("Volume", pad.left - 8, volumeTop + 12);

      candles.forEach((candle, index) => {
        const x = pad.left + index * candleStep + candleStep / 2;
        const up = candle.close >= candle.open;
        const color = up ? "#59c981" : "#e47b72";
        const wickTop = y(candle.high);
        const wickBottom = y(candle.low);
        const openY = y(candle.open);
        const closeY = y(candle.close);
        const bodyTop = Math.min(openY, closeY);
        const bodyHeight = Math.max(1, Math.abs(closeY - openY));
        const volumeHeight = ((candle.volume || 0) / volumeMax) * (volumeBottom - volumeTop);

        ctx.strokeStyle = color;
        ctx.beginPath();
        ctx.moveTo(x, wickTop);
        ctx.lineTo(x, wickBottom);
        ctx.stroke();

        ctx.fillStyle = color;
        ctx.fillRect(x - bodyWidth / 2, bodyTop, bodyWidth, bodyHeight);
        ctx.globalAlpha = 0.35;
        ctx.fillRect(x - bodyWidth / 2, volumeBottom - volumeHeight, bodyWidth, volumeHeight);
        ctx.globalAlpha = 1;
      });

      Object.entries(movingAverages || {}).forEach(([period, values]) => {
        if (!values.some((value) => Number.isFinite(value))) return;
        ctx.strokeStyle = maColors[period] || "#f1f4f0";
        ctx.lineWidth = 1.6;
        ctx.beginPath();
        let drawing = false;
        values.forEach((value, index) => {
          if (!Number.isFinite(value)) {
            drawing = false;
            return;
          }
          const x = pad.left + index * candleStep + candleStep / 2;
          const py = y(value);
          if (!drawing) {
            ctx.moveTo(x, py);
            drawing = true;
          } else {
            ctx.lineTo(x, py);
          }
        });
        ctx.stroke();
      });

      ctx.fillStyle = "#edf4ef";
      ctx.font = "13px Segoe UI, sans-serif";
      ctx.textAlign = "left";
      ctx.textBaseline = "top";
      ctx.fillText(`${ticker} ${$("chartInterval").value.toUpperCase()} ${$("chartRange").value.toUpperCase()}`, pad.left, 8);

      let legendX = pad.left;
      Object.entries(movingAverages || {}).forEach(([period, values]) => {
        if (!values.some((value) => Number.isFinite(value))) return;
        ctx.fillStyle = maColors[period] || "#f1f4f0";
        ctx.fillRect(legendX, 28, 14, 2);
        ctx.fillStyle = "#cbd8d1";
        ctx.font = "11px Segoe UI, sans-serif";
        ctx.fillText(`MA ${period}`, legendX + 18, 22);
        legendX += 62;
      });

      ctx.fillStyle = "#9fb0a8";
      ctx.font = "11px Segoe UI, sans-serif";
      ctx.textAlign = "center";
      const dateIndexes = [0, Math.floor(candles.length / 2), candles.length - 1];
      [...new Set(dateIndexes)].forEach((index) => {
        const x = pad.left + index * candleStep + candleStep / 2;
        ctx.fillText(candles[index].date, x, height - 16);
      });
    }

    function updateJob(job) {
      $("fetchStatus").textContent = job.message || "Idle";
      $("startFetch").disabled = !!job.running;
      $("fetchLog").textContent = job.log || "";
      $("modalFetchLog").textContent = job.log || "";
      $("progressStage").textContent = job.stage || "Idle";
      $("progressDetail").textContent = job.detail || job.message || "Idle";
      const total = job.total || 0;
      $("progressCount").textContent = total ? `${job.current || 0} / ${total}` : `${job.current || 0}`;
      $("progressPercent").textContent = `${job.percent || 0}%`;
      $("progressFill").style.width = `${job.percent || 0}%`;
      $("closeProgress").disabled = !!job.running;
      if (job.running || (!$("progressModal").classList.contains("hidden") && job.success !== null)) {
        $("progressModal").classList.remove("hidden");
      }
      if (job.running) {
        $("fetchStatus").className = "status-line warn";
      } else if (job.success === false) {
        $("fetchStatus").className = "status-line bad";
      } else {
        $("fetchStatus").className = "status-line";
      }
    }

    async function pollJob() {
      try {
        const payload = await api("/api/job");
        updateJob(payload.job);
        if (payload.job.running) {
          setTimeout(pollJob, 1200);
        } else {
          await refreshStatus();
        }
      } catch (error) {
        $("fetchStatus").textContent = error.message;
        $("fetchStatus").className = "status-line bad";
      }
    }

    function updateFilterJob(job) {
      $("filterStatus").textContent = job.message || "Idle";
      $("runFilter").disabled = !!job.running;
      $("progressStage").textContent = job.stage || "Filtering";
      $("progressDetail").textContent = job.detail || job.message || "Filtering cached data...";
      const total = job.total || 0;
      $("progressCount").textContent = total ? `${job.current || 0} / ${total}` : `${job.current || 0}`;
      $("progressPercent").textContent = `${job.percent || 0}%`;
      $("progressFill").style.width = `${job.percent || 0}%`;
      $("modalFetchLog").textContent = job.summary && job.summary.error ? job.summary.error : "";
      $("closeProgress").disabled = !!job.running;
      if (job.running || (!$("progressModal").classList.contains("hidden") && job.success !== null)) {
        $("progressModal").classList.remove("hidden");
      }
      if (job.running) {
        $("filterStatus").className = "status-line warn";
      } else if (job.success === false) {
        $("filterStatus").className = "status-line bad";
      } else {
        $("filterStatus").className = "status-line";
      }
    }

    async function pollFilterJob() {
      try {
        const payload = await api("/api/filter/job");
        updateFilterJob(payload.job);
        if (payload.job.running) {
          setTimeout(pollFilterJob, 500);
          return;
        }
        if (payload.job.success) {
          const summary = payload.job.summary || {};
          currentScanId = summary.scan_id || null;
          renderResults(payload.job.results || [], currentScanId);
          $("metricMatches").textContent = fmt.format((payload.job.results || []).length);
          const scanText = currentScanId ? `scan ${currentScanId}, ` : "";
          const youngerCount = summary.incomplete_ma_count || 0;
          const tierCounts = summary.ma_tier_counts || {};
          const youngerParts = Object.entries(tierCounts)
            .filter(([label]) => label !== "Full")
            .map(([label, count]) => `${count} ${label.toLowerCase()}`);
          const youngerText = youngerCount ? `, younger groups: ${youngerParts.join(", ")}` : "";
          $("resultMeta").textContent = `${scanText}${(payload.job.results || []).length} matches${youngerText} from ${summary.scanned_count || 0} scanned at ${summary.generated_at || ""}`;
        }
      } catch (error) {
        $("filterStatus").textContent = error.message;
        $("filterStatus").className = "status-line bad";
      }
    }

    async function startFetch() {
      $("fetchStatus").textContent = "Starting...";
      $("progressModal").classList.remove("hidden");
      $("progressStage").textContent = "Starting";
      $("progressDetail").textContent = "Preparing fetch job...";
      $("progressFill").style.width = "0%";
      $("progressPercent").textContent = "0%";
      $("progressCount").textContent = "0";
      $("modalFetchLog").textContent = "";
      const payload = {
        market: $("marketSelect").value,
        ticker_file: $("tickerFile").value.trim(),
        provider: $("provider").value,
        limit: activeLimit("useFetchLimit", "fetchLimit"),
        years: numberValue("years"),
        workers: numberValue("workers"),
        cache_file: $("cacheFile").value.trim(),
        output: currentMarketDefaults().output,
        info_refresh_days: numberValue("infoRefresh"),
        history_refresh_days: numberValue("historyRefresh"),
        history_chunk_size: numberValue("historyChunkSize"),
        history_pause_seconds: numberValue("historyPause"),
        info_pause_seconds: numberValue("infoPause"),
        rate_limit_pause_seconds: numberValue("ratePause"),
        max_rate_limit_retries: numberValue("rateRetries"),
        stop_on_rate_limit: $("stopOnRateLimit").checked,
        prune_missing_tickers: $("pruneMissing").checked,
        export_json: false
      };
      try {
        const response = await api("/api/fetch", { method: "POST", body: JSON.stringify(payload) });
        updateJob(response.job);
        pollJob();
      } catch (error) {
        $("fetchStatus").textContent = error.message;
        $("fetchStatus").className = "status-line bad";
      }
    }

    async function runFilter() {
      $("filterStatus").textContent = "Scanning...";
      $("runFilter").disabled = true;
      $("progressModal").classList.remove("hidden");
      $("progressStage").textContent = "Filtering";
      $("progressDetail").textContent = "Preparing cached filter scan...";
      $("progressFill").style.width = "0%";
      $("progressPercent").textContent = "0%";
      $("progressCount").textContent = "0";
      $("modalFetchLog").textContent = "";
      const payload = {
        market: $("marketSelect").value,
        cache_file: $("cacheFile").value.trim(),
        provider: $("provider").value,
        years: numberValue("years"),
        limit: $("useScanLimit").checked ? numberValue("scanLimit") : null,
        query: $("query").value.trim(),
        volume_multiplier: numberValue("volumeMultiplier"),
        min_market_cap: numberValue("minCap"),
        max_market_cap: numberValue("maxCap"),
        lookback_weeks: numberValue("lookbackWeeks"),
        ma_short: numberValue("maShort"),
        ma_intermediate: numberValue("maIntermediate"),
        ma_medium: numberValue("maMedium"),
        ma_long: numberValue("maLong"),
        price_avg_weeks: 1
      };
      try {
        const response = await api("/api/filter/start", { method: "POST", body: JSON.stringify(payload) });
        updateFilterJob(response.job);
        pollFilterJob();
      } catch (error) {
        $("filterStatus").textContent = error.message;
        $("filterStatus").className = "status-line bad";
        $("runFilter").disabled = false;
      }
    }

    function labelText(label) {
      if (label === "winner") return "Winner";
      if (label === "potential_winner") return "Potential Winner";
      if (label === "maybe") return "Maybe";
      if (label === "bad") return "Bad";
      return "";
    }

    function labelButtons(row) {
      const labels = [
        ["winner", "Winner"],
        ["potential_winner", "Potential"],
        ["maybe", "Maybe"],
        ["bad", "Bad"]
      ];
      return `<div class="label-cell">${labels.map(([value, text]) =>
        `<button class="label-btn ${row.label === value ? "active" : ""}" data-label="${value}" data-ticker="${row.ticker}" title="Mark ${text}">${text}</button>`
      ).join("")}</div>`;
    }

    function maDataText(row) {
      if (row.ma_data_complete !== false) return "Full";
      if (row.ma_history_label) {
        const weeks = Number(row.available_ma_weeks);
        return Number.isFinite(weeks) ? `${row.ma_history_label} (${weeks}w)` : row.ma_history_label;
      }
      const missing = Array.isArray(row.missing_ma_periods) ? row.missing_ma_periods : [];
      if (!missing.length) return "Younger";
      return `Younger: missing ${missing.map((item) => `${item.period}w`).join(", ")}`;
    }

    function applyRowLabel(ticker, label) {
      const row = document.querySelector(`tr[data-ticker="${ticker}"]`);
      if (!row) return;
      row.classList.remove("label-winner", "label-potential_winner", "label-maybe", "label-bad");
      if (label) row.classList.add(`label-${label}`);
      row.querySelectorAll(".label-btn").forEach((button) => {
        button.classList.toggle("active", button.dataset.label === label);
      });
    }

    async function labelResult(ticker, label) {
      if (!currentScanId) {
        $("resultMeta").textContent = "Run a saved scan before labelling results.";
        return;
      }
      const row = document.querySelector(`tr[data-ticker="${ticker}"]`);
      const existing = row && row.querySelector(`.label-btn.active`);
      const nextLabel = existing && existing.dataset.label === label ? "clear" : label;
      try {
        const response = await api("/api/label", {
          method: "POST",
          body: JSON.stringify({
            market: $("marketSelect").value,
            cache_file: $("cacheFile").value.trim() || "stock_cache.sqlite",
            scan_id: currentScanId,
            ticker,
            label: nextLabel,
            note: row?.dataset.note || null
          })
        });
        applyRowLabel(ticker, response.label);
        let centralMessage = response.central_ratings_file ? " - saved to central ratings DB" : "";
        if (response.google_sheets?.sent) {
          centralMessage += " and Google Sheets";
        } else if (response.google_sheets?.queued) {
          centralMessage += " - Google Sheets queued for retry";
        } else if (response.google_sheets?.configured === false) {
          centralMessage += " - Google Sheets not configured";
        }
        $("resultMeta").textContent = response.label
          ? `${ticker} marked ${labelText(response.label)} in scan ${currentScanId}${centralMessage}`
          : `${ticker} label cleared in scan ${currentScanId}${centralMessage}`;
      } catch (error) {
        $("resultMeta").textContent = error.message;
        $("resultMeta").className = "status-line bad";
      }
    }

    function renderResults(results, scanId = null) {
      const body = $("resultsBody");
      if (!results.length) {
        body.innerHTML = `<tr><td colspan="8" style="text-align:left;color:var(--muted)">No matches.</td></tr>`;
        return;
      }
      body.innerHTML = results.map((row) => {
        const rowScanId = row.scan_id || scanId || "";
        const rowClasses = [row.label ? `label-${row.label}` : "", row.ma_data_complete === false ? "incomplete-ma" : ""].filter(Boolean).join(" ");
        return `<tr data-ticker="${row.ticker}" data-scan-id="${rowScanId}" data-note="${String(row.personal_note || "").replaceAll('"', '&quot;') }" class="${rowClasses}">
          <td><a class="ticker chart-link" href="#" data-ticker="${row.ticker}">${row.ticker}</a></td>
          <td>${row.date}</td>
          <td>${Number(row.close_price).toFixed(2)}</td>
          <td>${marketCap(row.market_cap)}</td>
          <td>${fmt.format(Math.round(row.avg_volume || 0))}</td>
          <td>${Number(row.volume_ratio).toFixed(2)}x</td>
          <td>${maDataText(row)}</td>
          <td>${labelButtons(row)} <button class="note-btn" data-ticker="${row.ticker}" type="button" title="Edit private note">Note</button></td>
        </tr>`;
      }).join("");
      body.querySelectorAll(".chart-link").forEach((link) => {
        link.addEventListener("click", (event) => {
          event.preventDefault();
          loadChart(link.dataset.ticker, true);
        });
      });
      body.querySelectorAll(".label-btn").forEach((button) => {
        button.addEventListener("click", () => labelResult(button.dataset.ticker, button.dataset.label));
      });
      body.querySelectorAll(".note-btn").forEach((button) => {
        button.addEventListener("click", () => noteResult(button.dataset.ticker));
      });
    }

    async function noteResult(ticker) {
      const row = document.querySelector(`tr[data-ticker="${ticker}"]`);
      if (!row || !currentScanId) return;
      const note = window.prompt(`Private note for ${ticker}`, row.dataset.note || "");
      if (note === null) return;
      const active = row.querySelector(".label-btn.active");
      try {
        const response = await api("/api/label", {
          method: "POST",
          body: JSON.stringify({
            market: $("marketSelect").value,
            cache_file: $("cacheFile").value.trim() || "stock_cache.sqlite",
            scan_id: currentScanId,
            ticker,
            label: active ? active.dataset.label : "maybe",
            note
          })
        });
        row.dataset.note = note;
        $("resultMeta").textContent = `${ticker} private note saved for ${response.user?.email || "this account"}`;
      } catch (error) {
        $("resultMeta").textContent = error.message;
        $("resultMeta").className = "status-line bad";
      }
    }

    $("authSignIn").addEventListener("click", async () => {
      try {
        await authReady;
        await firebaseAuthMethods.signInWithEmailAndPassword(firebaseAuth, $("authEmail").value.trim(), $("authPassword").value);
        window.location.reload();
      } catch (error) { $("topStatus").textContent = error.message; }
    });
    $("authCreate").addEventListener("click", async () => {
      try {
        await authReady;
        await firebaseAuthMethods.createUserWithEmailAndPassword(firebaseAuth, $("authEmail").value.trim(), $("authPassword").value);
        window.location.reload();
      } catch (error) { $("topStatus").textContent = error.message; }
    });
    $("authSignOut").addEventListener("click", async () => {
      await firebaseAuthMethods.signOut(firebaseAuth);
      window.location.reload();
    });

    $("marketSelect").addEventListener("change", applyMarketDefaults);
    $("refreshStatus").addEventListener("click", refreshStatus);
    bindLimitToggle("useFetchLimit", "fetchLimit");
    bindLimitToggle("useScanLimit", "scanLimit");
    $("tickerFileSelect").addEventListener("change", () => {
      if ($("tickerFileSelect").value) $("tickerFile").value = $("tickerFileSelect").value;
    });
    $("downloadUsTickers").addEventListener("click", downloadUsTickers);
    $("startFetch").addEventListener("click", startFetch);
    $("runFilter").addEventListener("click", runFilter);
    $("loadChart").addEventListener("click", () => loadChart());
    $("toggleChartFullscreen").addEventListener("click", () => toggleChartFullscreen());
    $("chartInterval").addEventListener("change", () => loadChart());
    $("chartRange").addEventListener("change", () => loadChart());
    document.querySelectorAll(".ma-check").forEach((input) => {
      input.addEventListener("change", () => loadChart());
    });
    $("chartTicker").addEventListener("keydown", (event) => {
      if (event.key === "Enter") loadChart();
    });
    window.addEventListener("resize", () => {
      if (lastCandles.length) drawCandles(lastCandles, lastChartTicker, lastMovingAverages);
    });
    window.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && $("chartPanel").classList.contains("fullscreen")) {
        toggleChartFullscreen(false);
      }
    });
    $("closeProgress").addEventListener("click", () => $("progressModal").classList.add("hidden"));
    applyMarketDefaults();
  </script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local Moneymaker web UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), AppHandler)
    print(f"Moneymaker web UI running at http://{args.host}:{args.port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
