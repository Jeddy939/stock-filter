"""Run the existing MoneyMaker screen directly against PostgreSQL."""

from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Any, Callable

import pandas as pd
import psycopg
from psycopg.types.json import Jsonb

from moneymaker import fetcher
from moneymaker.filters import analyze_stock_from_local_data
from web_app import _company_features, _filter_config, _split_history


ProgressCallback = Callable[[str, int, int | None, str], None]
HISTORY_CHUNK_SIZE = 100


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if hasattr(value, "isoformat"):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _histories(rows: list[tuple[Any, ...]]) -> dict[str, pd.DataFrame]:
    if not rows:
        return {}
    frame = pd.DataFrame(
        rows,
        columns=["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"],
    )
    frame["Date"] = pd.to_datetime(frame["Date"])
    return {
        str(ticker): group.set_index("Date")[["Open", "High", "Low", "Close", "Volume"]]
        for ticker, group in frame.groupby("Ticker", sort=False)
    }


def run_postgres_filter(
    conn: psycopg.Connection,
    payload: dict[str, Any],
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    market = str(payload.get("market") or "asx").strip().lower()
    if market not in {"asx", "us"}:
        raise ValueError("market must be asx or us")
    provider = fetcher.normalize_provider(payload.get("provider") or fetcher.DEFAULT_PROVIDER)
    limit = int(payload.get("limit") or 0)
    query = str(payload.get("query") or "").strip().upper()
    years = int(payload.get("years") or fetcher.DEFAULT_DATA_YEARS)
    config = _filter_config(payload)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker, info_json FROM companies WHERE market = %s ORDER BY ticker",
            (market,),
        )
        company_rows = cur.fetchall()
    info_map = {str(ticker): (info or {}) for ticker, info in company_rows}
    tickers = list(info_map)
    if query:
        tickers = [ticker for ticker in tickers if query in ticker]
    if limit > 0:
        tickers = tickers[:limit]

    end = date.today()
    start = (pd.Timestamp(end) - pd.DateOffset(years=years)).date()
    results: list[dict[str, Any]] = []
    skipped = 0
    total = len(tickers)
    if progress:
        progress("Filtering", 0, total, f"Scanning {total:,} {market.upper()} stocks online.")

    for chunk_start in range(0, total, HISTORY_CHUNK_SIZE):
        chunk = tickers[chunk_start : chunk_start + HISTORY_CHUNK_SIZE]
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticker, week_date, open_price, high_price, low_price, close_price, volume
                FROM weekly_price_history
                WHERE market = %s AND provider = %s AND ticker = ANY(%s)
                  AND week_date >= %s AND week_date <= %s + 7
                ORDER BY ticker, week_date
                """,
                (market, provider, chunk, start, end),
            )
            histories = _histories(cur.fetchall())
        for offset, ticker in enumerate(chunk, 1):
            current = chunk_start + offset
            history = histories.get(ticker)
            if history is None or history.empty:
                skipped += 1
            else:
                result = analyze_stock_from_local_data(
                    ticker,
                    {"info": info_map.get(ticker, {}), "history": _split_history(history)},
                    config,
                )
                if result:
                    result.update(_company_features(info_map.get(ticker, {})))
                    results.append(result)
            if progress and (current == total or current % 25 == 0):
                progress("Filtering", current, total, f"Screened {current:,}/{total:,}: {ticker}")

    results.sort(
        key=lambda item: (
            int(item.get("ma_history_sort") or (0 if item.get("ma_data_complete", True) else 99)),
            -float(item.get("volume_ratio") or 0),
        )
    )
    incomplete = sum(1 for item in results if not item.get("ma_data_complete", True))
    tier_counts: dict[str, int] = {}
    for item in results:
        tier = str(item.get("ma_history_label") or ("Full" if item.get("ma_data_complete", True) else "Younger"))
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    created_at = datetime.now(timezone.utc)
    source_id = int(created_at.timestamp() * 1_000_000)
    if progress:
        progress("Saving scan", total, total, f"Saving {len(results):,} matches online.")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scan_runs (
                market, source_id, created_at_utc, provider, cache_file, years, limit_count,
                query, scanned_count, result_count, skipped_no_history, config_json,
                ticker_universe_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                market, source_id, created_at, provider, "postgresql://price_history", years,
                limit if limit > 0 else None, query, total, len(results), skipped,
                Jsonb(_json_safe(config)), Jsonb(tickers),
            ),
        )
        scan_id = int(cur.fetchone()[0])
        for rank, row in enumerate(results, 1):
            row["scan_id"] = scan_id
            row["source_id"] = rank
            row["rank"] = rank
            row.setdefault("label", None)
            safe_row = _json_safe(row)
            cur.execute(
                """
                INSERT INTO scan_results (
                    scan_id, source_id, rank, ticker, signal_date, close_price, market_cap,
                    avg_volume, volume_ratio, sector, industry, result_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    scan_id, rank, rank, row.get("ticker"), row.get("date"),
                    _number(row.get("close_price")), _number(row.get("market_cap")),
                    _number(row.get("avg_volume")), _number(row.get("volume_ratio")),
                    row.get("sector"), row.get("industry"), Jsonb(safe_row),
                ),
            )
    conn.commit()
    return {
        "ok": True,
        "scan_id": scan_id,
        "results": [_json_safe(row) for row in results],
        "result_count": len(results),
        "incomplete_ma_count": incomplete,
        "ma_tier_counts": tier_counts,
        "scanned_count": total,
        "skipped_no_history": skipped,
        "generated_at": created_at.isoformat(timespec="seconds"),
        "source": "online_database",
    }
