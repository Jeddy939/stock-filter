"""Export saved MoneyMaker scan labels from SQLite caches.

The web app stores labels in the SQLite cache files, not in the old JSON export.
This script writes a readable TXT report and a CSV file that can be opened in
Excel or handed to another analysis tool.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


DEFAULT_CACHES = ["stock_cache.sqlite", "stock_cache_us.sqlite"]
LABEL_ORDER = {"winner": 0, "potential_winner": 1, "maybe": 2, "bad": 3}


def _json_loads(raw: str) -> Dict[str, Any]:
    try:
        value = json.loads(raw or "{}")
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return bool(row)


def _rows_from_cache(cache_file: Path) -> List[Dict[str, Any]]:
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
                s.query,
                s.scanned_count,
                s.result_count
            FROM scan_labels sl
            JOIN scan_results sr
              ON sr.scan_id = sl.scan_id
             AND sr.ticker = sl.ticker
            JOIN scan_runs s
              ON s.id = sl.scan_id
            ORDER BY sl.scan_id DESC, sl.label, sr.rank, sr.ticker
            """
        ).fetchall()
    finally:
        conn.close()

    exported = []
    for row in rows:
        result = _json_loads(row["result_json"])
        exported.append(
            {
                "cache_file": cache_file.name,
                "market": "US" if "us" in cache_file.stem.lower() else "ASX",
                "scan_id": row["scan_id"],
                "scan_created_at_utc": row["scan_created_at_utc"],
                "label": row["label"],
                "labeled_at_utc": row["labeled_at_utc"],
                "ticker": row["ticker"],
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
                "scanned_count": row["scanned_count"],
                "result_count": row["result_count"],
                "note": row["note"] or "",
                "yahoo_url": f"https://finance.yahoo.com/quote/{row['ticker']}",
                "result_json": json.dumps(result, sort_keys=True, separators=(",", ":")),
            }
        )
    return exported


def _write_csv(rows: List[Dict[str, Any]], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "market",
        "cache_file",
        "scan_id",
        "scan_created_at_utc",
        "label",
        "labeled_at_utc",
        "ticker",
        "rank",
        "signal_date",
        "close_price",
        "market_cap",
        "avg_volume",
        "volume_ratio",
        "sector",
        "industry",
        "provider",
        "query",
        "scanned_count",
        "result_count",
        "note",
        "yahoo_url",
        "result_json",
    ]
    with output_file.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_txt(rows: List[Dict[str, Any]], output_file: Path, caches: Iterable[Path]) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as handle:
        handle.write("MoneyMaker Scan Label Export\n")
        handle.write(f"Generated: {datetime.now().isoformat(timespec='seconds')}\n")
        handle.write("Caches checked:\n")
        for cache in caches:
            handle.write(f"- {cache}\n")
        handle.write(f"\nTotal labels: {len(rows)}\n")

        if not rows:
            handle.write("\nNo saved labels were found.\n")
            return

        for label in ("winner", "potential_winner", "maybe", "bad"):
            label_rows = [row for row in rows if row["label"] == label]
            handle.write(f"\n{label.replace('_', ' ').title()} ({len(label_rows)})\n")
            handle.write("-" * 72 + "\n")
            for row in label_rows:
                handle.write(
                    f"{row['ticker']} | scan {row['scan_id']} | rank {row['rank']} | "
                    f"date {row['signal_date']} | close {row['close_price']} | "
                    f"volume ratio {row['volume_ratio']}\n"
                )
                if row.get("sector") or row.get("industry"):
                    handle.write(f"  {row.get('sector') or '-'} / {row.get('industry') or '-'}\n")
                handle.write(f"  Yahoo: {row['yahoo_url']}\n")
                if row.get("note"):
                    handle.write(f"  Note: {row['note']}\n")


def export_labels(cache_files: Iterable[str], output_dir: str) -> Dict[str, Any]:
    caches = [Path(cache_file) for cache_file in cache_files]
    rows: List[Dict[str, Any]] = []
    for cache in caches:
        rows.extend(_rows_from_cache(cache))

    rows.sort(
        key=lambda row: (
            str(row["market"]),
            int(row["scan_id"]),
            LABEL_ORDER.get(str(row["label"]), 99),
            int(row["rank"] or 0),
            str(row["ticker"]),
        )
    )

    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_path = Path(output_dir)
    txt_file = output_path / f"scan_labels_{stamp}.txt"
    csv_file = output_path / f"scan_labels_{stamp}.csv"

    _write_txt(rows, txt_file, caches)
    _write_csv(rows, csv_file)
    return {"label_count": len(rows), "txt_file": txt_file, "csv_file": csv_file}


def main() -> None:
    parser = argparse.ArgumentParser(description="Export saved MoneyMaker scan labels.")
    parser.add_argument(
        "--cache-file",
        action="append",
        dest="cache_files",
        help="SQLite cache to read. Can be supplied multiple times. Defaults to ASX and US caches.",
    )
    parser.add_argument("--output-dir", default="exports", help="Folder for exported TXT/CSV files.")
    args = parser.parse_args()

    result = export_labels(args.cache_files or DEFAULT_CACHES, args.output_dir)
    print(f"Exported {result['label_count']} labels.")
    print(f"TXT: {result['txt_file']}")
    print(f"CSV: {result['csv_file']}")


if __name__ == "__main__":
    main()
