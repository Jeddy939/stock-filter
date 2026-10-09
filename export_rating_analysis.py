"""Export central MoneyMaker rating events with latest-price tracking."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_CENTRAL_DB = "ratings/central_stock_ratings.sqlite"
LEGACY_CENTRAL_DB = "central_stock_ratings.sqlite"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
    )


def _latest_price(cache_file: str, ticker: str) -> Tuple[Optional[str], Optional[float]]:
    path = Path(cache_file)
    if not path.exists():
        # Handle databases copied between machines where only the filename still matches.
        local = Path(path.name)
        path = local if local.exists() else path
    if not path.exists():
        return None, None

    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute(
            """
            SELECT date, close
            FROM price_history
            WHERE ticker = ?
            ORDER BY date DESC
            LIMIT 1
            """,
            (ticker,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None, None
    return row[0], float(row[1]) if row[1] is not None else None


def _return_pct(start_price: Any, latest_price: Optional[float]) -> Optional[float]:
    try:
        start = float(start_price)
    except (TypeError, ValueError):
        return None
    if not start or latest_price is None:
        return None
    return round(((latest_price - start) / start) * 100, 4)


def _days_between(start_date: Any, end_date: Optional[str]) -> Optional[int]:
    if not start_date or not end_date:
        return None
    try:
        start = datetime.fromisoformat(str(start_date)[:10])
        end = datetime.fromisoformat(str(end_date)[:10])
    except ValueError:
        return None
    return (end - start).days


def load_events(central_db: str) -> List[Dict[str, Any]]:
    path = Path(central_db)
    if not path.exists() and central_db == DEFAULT_CENTRAL_DB and Path(LEGACY_CENTRAL_DB).exists():
        path = Path(LEGACY_CENTRAL_DB)
    if not path.exists():
        return []
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        if not _table_exists(conn, "rating_events"):
            return []
        rows = [dict(row) for row in conn.execute("SELECT * FROM rating_events ORDER BY event_at_utc, id")]
    finally:
        conn.close()

    latest_by_scan_ticker = {}
    for row in rows:
        key = (row.get("cache_file"), row.get("scan_id"), row.get("ticker"))
        latest_by_scan_ticker[key] = row

    enriched = []
    for row in rows:
        latest_date, latest_close = _latest_price(str(row.get("cache_file") or ""), str(row.get("ticker") or ""))
        key = (row.get("cache_file"), row.get("scan_id"), row.get("ticker"))
        latest_event = latest_by_scan_ticker.get(key) or {}
        current_label = latest_event.get("label") if latest_event.get("action") == "label" else None
        row["latest_price_date"] = latest_date
        row["latest_close_price"] = latest_close
        row["return_pct_since_signal"] = _return_pct(row.get("close_price"), latest_close)
        row["days_since_signal"] = _days_between(row.get("signal_date"), latest_date)
        row["current_label_for_scan"] = current_label
        row["still_active_label"] = row.get("id") == latest_event.get("id") and row.get("action") == "label"
        enriched.append(row)
    return enriched


def write_outputs(rows: List[Dict[str, Any]], output_dir: str) -> Dict[str, Path]:
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    csv_file = out / f"central_rating_analysis_{stamp}.csv"
    txt_file = out / f"central_rating_analysis_{stamp}.txt"

    fieldnames = [
        "id",
        "event_at_utc",
        "action",
        "rated_by",
        "market",
        "ticker",
        "label",
        "current_label_for_scan",
        "still_active_label",
        "scan_id",
        "rank",
        "signal_date",
        "close_price",
        "latest_price_date",
        "latest_close_price",
        "return_pct_since_signal",
        "days_since_signal",
        "volume_ratio",
        "market_cap",
        "avg_volume",
        "sector",
        "industry",
        "provider",
        "query",
        "note",
        "yahoo_url",
        "cache_file",
    ]
    with csv_file.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    with txt_file.open("w", encoding="utf-8") as handle:
        handle.write("MoneyMaker Central Rating Analysis\n")
        handle.write(f"Generated: {datetime.now().isoformat(timespec='seconds')}\n")
        handle.write(f"Total rating events: {len(rows)}\n\n")
        for label in ("winner", "potential_winner", "needs_confirmation", "maybe", "bad", None):
            group = [row for row in rows if row.get("label") == label]
            if not group:
                continue
            title = "Cleared/Unlabelled" if label is None else str(label).replace("_", " ").title()
            handle.write(f"{title} ({len(group)})\n")
            handle.write("-" * 80 + "\n")
            for row in group:
                handle.write(
                    f"{row.get('ticker')} | {row.get('market')} | {row.get('event_at_utc')} | "
                    f"rated by {row.get('rated_by') or 'unknown'} | active {row.get('still_active_label')} | "
                    f"signal {row.get('signal_date')} @ {row.get('close_price')} | "
                    f"latest {row.get('latest_price_date')} @ {row.get('latest_close_price')} | "
                    f"return {row.get('return_pct_since_signal')}%\n"
                )
                handle.write(f"  scan {row.get('scan_id')} rank {row.get('rank')} | {row.get('yahoo_url')}\n")
            handle.write("\n")

    return {"csv_file": csv_file, "txt_file": txt_file}


def main() -> None:
    parser = argparse.ArgumentParser(description="Export central rating events with latest-price tracking.")
    parser.add_argument("--central-db", default=DEFAULT_CENTRAL_DB)
    parser.add_argument("--output-dir", default="exports")
    args = parser.parse_args()

    rows = load_events(args.central_db)
    outputs = write_outputs(rows, args.output_dir)
    print(f"Exported {len(rows)} central rating events.")
    print(f"TXT: {outputs['txt_file']}")
    print(f"CSV: {outputs['csv_file']}")


if __name__ == "__main__":
    main()
