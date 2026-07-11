"""Import MoneyMaker SQLite caches and ratings into Firebase SQL Connect.

The importer is resumable: all writes use stable market/source keys and
upserts. Source SQLite files are opened read-only and are never modified.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

try:
    import psycopg
    from psycopg.types.json import Jsonb
except ModuleNotFoundError:  # Allows --dry-run on a local-only installation.
    psycopg = None  # type: ignore[assignment]

    class Jsonb:  # type: ignore[no-redef]
        def __init__(self, value: Any) -> None:
            self.value = value


ROOT = Path(__file__).resolve().parents[1]
CHUNK_SIZE = 2_000


def chunks(rows: Iterable[tuple], size: int) -> Iterator[list[tuple]]:
    batch: list[tuple] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def sqlite_connection(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(path)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def json_value(value: Any) -> Jsonb:
    if value is None:
        return Jsonb({})
    if isinstance(value, (dict, list)):
        return Jsonb(value)
    try:
        return Jsonb(json.loads(str(value)))
    except (TypeError, ValueError, json.JSONDecodeError):
        return Jsonb({"raw": str(value)})


def nullable_json(value: Any) -> Jsonb | None:
    if value is None or value == "":
        return None
    return json_value(value)


def parse_market(ticker: str, fallback: str) -> str:
    return "asx" if ticker.upper().endswith(".AX") else fallback


def import_cache(
    db: psycopg.Connection,
    cache_path: Path,
    market: str,
    chunk_size: int,
    dry_run: bool,
    price_since: str | None = None,
    full_tickers: set[str] | None = None,
) -> dict[str, int]:
    counts = {"companies": 0, "prices": 0, "scans": 0, "results": 0, "labels": 0}
    source = sqlite_connection(cache_path)
    try:
        company_rows = (
            (market, row["ticker"], json_value(row["info_json"]), row["fetched_at_utc"])
            for row in source.execute("SELECT ticker, info_json, fetched_at_utc FROM company_info")
        )
        for batch in chunks(company_rows, chunk_size):
            if not dry_run:
                with db.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO companies (market, ticker, info_json, fetched_at_utc)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (market, ticker) DO UPDATE SET
                            info_json = EXCLUDED.info_json,
                            fetched_at_utc = EXCLUDED.fetched_at_utc
                        """,
                        batch,
                    )
                db.commit()
            counts["companies"] += len(batch)

        price_query = """
            SELECT provider, ticker, date, open, high, low, close, volume,
                   fetched_at_utc
            FROM price_history
        """
        price_params: list[str] = []
        if price_since:
            price_query += " WHERE date >= ?"
            price_params.append(price_since)
            if full_tickers:
                placeholders = ", ".join("?" for _ in full_tickers)
                price_query += f" OR ticker IN ({placeholders})"
                price_params.extend(sorted(full_tickers))
        price_query += " ORDER BY ticker, date"
        price_rows = (
            (
                market,
                row["provider"],
                row["ticker"],
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["fetched_at_utc"],
            )
            for row in source.execute(price_query, price_params)
        )
        for batch in chunks(price_rows, chunk_size):
            if not dry_run:
                with db.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO price_history
                          (market, provider, ticker, price_date, open_price,
                           high_price, low_price, close_price, volume, fetched_at_utc)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (market, provider, ticker, price_date) DO UPDATE SET
                          open_price = EXCLUDED.open_price,
                          high_price = EXCLUDED.high_price,
                          low_price = EXCLUDED.low_price,
                          close_price = EXCLUDED.close_price,
                          volume = EXCLUDED.volume,
                          fetched_at_utc = EXCLUDED.fetched_at_utc
                        """,
                        batch,
                    )
                db.commit()
            counts["prices"] += len(batch)

        scan_map: dict[int, int] = {}
        for row in source.execute("SELECT * FROM scan_runs ORDER BY id"):
            if dry_run:
                scan_map[row["id"]] = row["id"]
                counts["scans"] += 1
                continue
            with db.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scan_runs
                      (market, source_id, created_at_utc, provider, cache_file,
                       years, limit_count, query, scanned_count, result_count,
                       skipped_no_history, config_json, ticker_universe_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (market, source_id) DO UPDATE SET
                      created_at_utc = EXCLUDED.created_at_utc,
                      config_json = EXCLUDED.config_json,
                      ticker_universe_json = EXCLUDED.ticker_universe_json
                    RETURNING id
                    """,
                    (
                        market,
                        row["id"],
                        row["created_at_utc"],
                        row["provider"],
                        row["cache_file"],
                        row["years"],
                        row["limit_count"],
                        row["query"],
                        row["scanned_count"],
                        row["result_count"],
                        row["skipped_no_history"],
                        json_value(row["config_json"]),
                        json_value(row["ticker_universe_json"]),
                    ),
                )
                scan_map[row["id"]] = cur.fetchone()[0]
            db.commit()
            counts["scans"] += 1

        result_rows = source.execute("SELECT * FROM scan_results ORDER BY id")
        for batch_rows in chunks(result_rows, chunk_size):
            values = []
            for row in batch_rows:
                target_scan = scan_map.get(row["scan_id"])
                if target_scan is None:
                    continue
                values.append(
                    (
                        target_scan,
                        row["id"],
                        row["rank"],
                        row["ticker"],
                        row["signal_date"],
                        row["close_price"],
                        row["market_cap"],
                        row["avg_volume"],
                        row["volume_ratio"],
                        row["sector"],
                        row["industry"],
                        json_value(row["result_json"]),
                    )
                )
            if values and not dry_run:
                with db.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO scan_results
                          (scan_id, source_id, rank, ticker, signal_date,
                           close_price, market_cap, avg_volume, volume_ratio,
                           sector, industry, result_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (scan_id, source_id) DO UPDATE SET
                          rank = EXCLUDED.rank,
                          result_json = EXCLUDED.result_json
                        """,
                        values,
                    )
                db.commit()
            counts["results"] += len(values)

        label_rows = source.execute("SELECT * FROM scan_labels ORDER BY id")
        for batch_rows in chunks(label_rows, chunk_size):
            values = []
            for row in batch_rows:
                target_scan = scan_map.get(row["scan_id"])
                if target_scan is not None:
                    values.append(
                        (target_scan, row["id"], row["ticker"], row["label"], row["note"], row["labeled_at_utc"])
                    )
            if values and not dry_run:
                with db.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO scan_labels
                          (scan_id, source_id, ticker, label, note, labeled_at_utc)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (scan_id, source_id, ticker) DO UPDATE SET
                          label = EXCLUDED.label,
                          note = EXCLUDED.note,
                          labeled_at_utc = EXCLUDED.labeled_at_utc
                        """,
                        values,
                    )
                db.commit()
            counts["labels"] += len(values)
    finally:
        source.close()
    return counts


def import_ratings(
    db: psycopg.Connection,
    ratings_path: Path,
    chunk_size: int,
    dry_run: bool,
) -> int:
    source = sqlite_connection(ratings_path)
    count = 0
    try:
        rows = source.execute("SELECT * FROM rating_events ORDER BY id")
        for batch_rows in chunks(rows, chunk_size):
            values = []
            for row in batch_rows:
                values.append(
                    (
                        row["id"], row["event_at_utc"], row["action"], row["rated_by"],
                        row["market"], row["cache_file"], row["scan_id"],
                        row["scan_created_at_utc"], row["provider"], row["query"],
                        row["ticker"], row["label"], row["note"], row["rank"],
                        row["signal_date"], row["close_price"], row["market_cap"],
                        row["avg_volume"], row["volume_ratio"], row["sector"],
                        row["industry"], nullable_json(row["result_json"]), row["yahoo_url"],
                    )
                )
            if values and not dry_run:
                with db.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO rating_events
                          (source_id, event_at_utc, action, rated_by, market,
                           cache_file, scan_id, scan_created_at_utc, provider,
                           query, ticker, label, note, rank, signal_date,
                           close_price, market_cap, avg_volume, volume_ratio,
                           sector, industry, result_json, yahoo_url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_id, event_at_utc, ticker, action) DO NOTHING
                        """,
                        values,
                    )
                db.commit()
            count += len(values)
    finally:
        source.close()
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("MONEYMAKER_DATABASE_URL"))
    parser.add_argument("--market", choices=("asx", "us"), required=True)
    parser.add_argument("--cache", required=True, type=Path)
    parser.add_argument("--ratings-db", type=Path)
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not args.database_url and not args.dry_run:
        parser.error("--database-url or MONEYMAKER_DATABASE_URL is required unless --dry-run is used")

    if not args.dry_run and psycopg is None:
        parser.error("psycopg is required for a real migration; install requirements-cloud.txt first")

    if args.dry_run:
        print(f"Would import {args.market} cache: {args.cache}")
        if args.ratings_db:
            print(f"Would import ratings: {args.ratings_db}")
        return

    with psycopg.connect(args.database_url) as db:
        cache_counts = import_cache(db, args.cache, args.market, args.chunk_size, False)
        rating_count = 0
        if args.ratings_db and args.ratings_db.exists():
            rating_count = import_ratings(db, args.ratings_db, args.chunk_size, False)
    print(json.dumps({"market": args.market, **cache_counts, "ratings": rating_count}, indent=2))


if __name__ == "__main__":
    main()
