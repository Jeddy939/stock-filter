"""Send existing MoneyMaker ratings from local files to Google Sheets.

This is intended for older machines where ratings were made before Google
Sheets sync was configured. It scans the old scan label tables, central SQLite
rating events, and JSON/JSONL backup logs, then POSTs normalized events to the
same Google Apps Script webhook used by the app.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.request import Request, urlopen


DEFAULT_CACHE_FILES = ["stock_cache.sqlite", "stock_cache_us.sqlite"]
DEFAULT_CENTRAL_SQLITE_FILES = ["ratings/central_stock_ratings.sqlite", "central_stock_ratings.sqlite"]
DEFAULT_JSON_FILES = ["central_stock_ratings.json"]
DEFAULT_JSONL_FILES = ["central_stock_ratings.jsonl"]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
    )


def _safe_json(raw: Any) -> Dict[str, Any]:
    try:
        value = json.loads(str(raw or "{}"))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _event_id(*parts: Any) -> str:
    text = "|".join("" if part is None else str(part) for part in parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"moneymaker-rating:{text}"))


def _market_from_file(path: Path) -> str:
    return "US" if "us" in path.stem.lower() else "ASX"


def _rows_from_scan_label_cache(cache_file: Path, rater_name: str) -> List[Dict[str, Any]]:
    if not cache_file.exists():
        return []

    conn = sqlite3.connect(str(cache_file))
    conn.row_factory = sqlite3.Row
    try:
        required = ("scan_labels", "scan_results", "scan_runs")
        if not all(_table_exists(conn, table) for table in required):
            return []
        rows = conn.execute(
            """
            SELECT
                sl.label,
                sl.labeled_at_utc,
                sl.note,
                sl.scan_id,
                sr.rank,
                sr.ticker,
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
            FROM scan_labels sl
            JOIN scan_results sr
              ON sr.scan_id = sl.scan_id
             AND sr.ticker = sl.ticker
            JOIN scan_runs s
              ON s.id = sl.scan_id
            ORDER BY sl.labeled_at_utc, sl.scan_id, sr.rank, sr.ticker
            """
        ).fetchall()
    finally:
        conn.close()

    events = []
    for row in rows:
        event_id = _event_id(
            "scan_label",
            cache_file.resolve(),
            row["scan_id"],
            row["ticker"],
            row["label"],
            row["labeled_at_utc"],
        )
        events.append(
            {
                "event_id": event_id,
                "event_at_utc": row["labeled_at_utc"] or row["scan_created_at_utc"] or datetime.utcnow().isoformat(timespec="seconds"),
                "action": "label",
                "rated_by": rater_name,
                "market": _market_from_file(cache_file),
                "ticker": row["ticker"],
                "label": row["label"],
                "note": row["note"] or "",
                "scan_id": row["scan_id"],
                "scan_created_at_utc": row["scan_created_at_utc"],
                "rank": row["rank"],
                "signal_date": row["signal_date"],
                "close_price": row["close_price"],
                "market_cap": row["market_cap"],
                "avg_volume": row["avg_volume"],
                "volume_ratio": row["volume_ratio"],
                "sector": row["sector"],
                "industry": row["industry"],
                "provider": row["provider"],
                "query": row["query"],
                "cache_file": str(cache_file),
                "yahoo_url": f"https://finance.yahoo.com/quote/{row['ticker']}",
                "result": _safe_json(row["result_json"]),
                "import_source": "scan_labels",
            }
        )
    return events


def _rows_from_central_sqlite(path: Path, fallback_rater_name: str) -> List[Dict[str, Any]]:
    if not path.exists():
        return []

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        if not _table_exists(conn, "rating_events"):
            return []
        rows = conn.execute("SELECT * FROM rating_events ORDER BY event_at_utc, id").fetchall()
    finally:
        conn.close()

    events = []
    for row in rows:
        event = dict(row)
        event["event_id"] = event.get("event_id") or _event_id("central_sqlite", path.resolve(), event.get("id"))
        event["rated_by"] = event.get("rated_by") or fallback_rater_name
        event["result"] = _safe_json(event.pop("result_json", "{}"))
        event["import_source"] = "central_sqlite"
        events.append(event)
    return events


def _rows_from_json_file(path: Path, fallback_rater_name: str) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        loaded = json.loads(path.read_text(encoding="utf-8") or "[]")
    except json.JSONDecodeError:
        return []
    if isinstance(loaded, dict):
        loaded = [loaded]
    if not isinstance(loaded, list):
        return []
    return [_normalize_existing_event(item, path, fallback_rater_name) for item in loaded if isinstance(item, dict)]


def _rows_from_jsonl_file(path: Path, fallback_rater_name: str) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    events = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            events.append(_normalize_existing_event(item, path, fallback_rater_name, line_number))
    return events


def _normalize_existing_event(
    item: Dict[str, Any],
    path: Path,
    fallback_rater_name: str,
    line_number: Optional[int] = None,
) -> Dict[str, Any]:
    event = dict(item)
    event["event_id"] = event.get("event_id") or _event_id(
        "json_event",
        path.resolve(),
        line_number or "",
        event.get("ticker"),
        event.get("label"),
        event.get("event_at_utc") or event.get("labeled_at_utc"),
    )
    event["event_at_utc"] = event.get("event_at_utc") or event.get("labeled_at_utc") or datetime.utcnow().isoformat(timespec="seconds")
    event["action"] = event.get("action") or "label"
    event["rated_by"] = event.get("rated_by") or fallback_rater_name
    event["yahoo_url"] = event.get("yahoo_url") or f"https://finance.yahoo.com/quote/{event.get('ticker', '')}"
    event["import_source"] = event.get("import_source") or path.name
    return event


def collect_events(args: argparse.Namespace) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for cache_file in args.cache_file:
        events.extend(_rows_from_scan_label_cache(Path(cache_file), args.rater_name))
    for sqlite_file in args.central_sqlite:
        events.extend(_rows_from_central_sqlite(Path(sqlite_file), args.rater_name))
    for json_file in args.json_file:
        events.extend(_rows_from_json_file(Path(json_file), args.rater_name))
    for jsonl_file in args.jsonl_file:
        events.extend(_rows_from_jsonl_file(Path(jsonl_file), args.rater_name))

    unique: Dict[str, Dict[str, Any]] = {}
    for event in events:
        event_id = str(event.get("event_id") or "")
        ticker = str(event.get("ticker") or "").strip()
        label = str(event.get("label") or "").strip()
        if not event_id or not ticker or not label:
            continue
        unique[event_id] = event
    return list(unique.values())


def post_event(webhook_url: str, event: Dict[str, Any], secret: str = "") -> Dict[str, Any]:
    payload = dict(event)
    if secret:
        payload["secret"] = secret
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(webhook_url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=20) as response:
        body = response.read().decode("utf-8", errors="replace")
        if response.status >= 400:
            raise RuntimeError(f"HTTP {response.status}: {body[:500]}")
        try:
            result = json.loads(body) if body else {}
        except json.JSONDecodeError:
            result = {"ok": True, "raw": body}
        if isinstance(result, dict) and result.get("ok") is False:
            raise RuntimeError(str(result.get("error") or result))
        return result if isinstance(result, dict) else {"ok": True}


def write_audit(events: List[Dict[str, Any]], sent: int, duplicates: int, failed: List[Dict[str, Any]]) -> Path:
    out = Path("exports")
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    path = out / f"sent_existing_ratings_{stamp}.json"
    path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "events_found": len(events),
                "sent": sent,
                "duplicates": duplicates,
                "failed_count": len(failed),
                "failed": failed,
                "events": events,
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Send existing MoneyMaker ratings to Google Sheets.")
    parser.add_argument("--webhook-url", default=os.environ.get("MONEYMAKER_GOOGLE_SHEETS_WEBHOOK_URL", ""))
    parser.add_argument("--secret", default=os.environ.get("MONEYMAKER_GOOGLE_SHEETS_SECRET", ""))
    parser.add_argument("--rater-name", default=os.environ.get("MONEYMAKER_RATER_NAME") or os.environ.get("USERNAME") or "imported")
    parser.add_argument("--cache-file", action="append", default=list(DEFAULT_CACHE_FILES))
    parser.add_argument("--central-sqlite", action="append", default=list(DEFAULT_CENTRAL_SQLITE_FILES))
    parser.add_argument("--json-file", action="append", default=list(DEFAULT_JSON_FILES))
    parser.add_argument("--jsonl-file", action="append", default=list(DEFAULT_JSONL_FILES))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.webhook_url and not args.dry_run:
        raise SystemExit("Google Sheets webhook URL is required.")

    events = collect_events(args)
    print(f"Found {len(events)} existing rating events.")
    if not events:
        audit = write_audit(events, 0, 0, [])
        print(f"Audit file: {audit}")
        return

    sent = 0
    duplicates = 0
    failed: List[Dict[str, Any]] = []
    if args.dry_run:
        for event in events:
            print(f"DRY RUN: {event.get('ticker')} {event.get('label')} {event.get('event_at_utc')}")
    else:
        for index, event in enumerate(events, 1):
            try:
                result = post_event(args.webhook_url, event, args.secret)
                if result.get("duplicate"):
                    duplicates += 1
                else:
                    sent += 1
                print(f"[{index}/{len(events)}] sent {event.get('ticker')} {event.get('label')}")
            except Exception as exc:
                print(f"[{index}/{len(events)}] FAILED {event.get('ticker')} {event.get('label')}: {exc}")
                failed.append(
                    {
                        "event_id": event.get("event_id"),
                        "ticker": event.get("ticker"),
                        "label": event.get("label"),
                        "error": str(exc),
                    }
                )

    audit = write_audit(events, sent, duplicates, failed)
    print()
    print(f"Sent: {sent}")
    print(f"Duplicates already in sheet: {duplicates}")
    print(f"Failed: {len(failed)}")
    print(f"Audit file: {audit}")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
