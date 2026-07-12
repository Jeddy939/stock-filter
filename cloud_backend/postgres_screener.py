"""Run the existing MoneyMaker screen directly against PostgreSQL."""

from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Any, Callable

import pandas as pd
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from moneymaker import fetcher
from moneymaker.filters import _ma_history_tier, analyze_stock_from_local_data
from web_app import _company_features, _filter_config, _split_history


ProgressCallback = Callable[[str, int, int | None, str], None]
HISTORY_CHUNK_SIZE = 100
SUPPORTED_METRIC_MAS = {30, 90, 180, 360, 700}


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


def _metric_config_supported(config: dict[str, Any]) -> bool:
    try:
        avg_volume_weeks = int(config.get("avg_volume_weeks", 52))
        price_avg_weeks = int(config.get("price_avg_weeks", 1))
        ma_periods = {
            int(period)
            for period in (config.get("ma_periods") or {}).values()
            if period and int(period) > 0
        }
    except (TypeError, ValueError):
        return False
    return (
        avg_volume_weeks == 52
        and price_avg_weeks == 1
        and ma_periods.issubset(SUPPORTED_METRIC_MAS)
    )


def _metric_column(period: int) -> str:
    if period not in SUPPORTED_METRIC_MAS:
        raise ValueError(f"unsupported MA period {period}")
    return f"ma_{period}"


def _result_from_metric(row: dict[str, Any], config: dict[str, Any]) -> dict[str, Any] | None:
    ma_periods = {
        name: int(period)
        for name, period in (config.get("ma_periods") or {}).items()
        if period and int(period) > 0
    }
    available_weeks = int(row.get("available_weeks") or 0)
    shortest_ma_period = min(ma_periods.values()) if ma_periods else 0
    if shortest_ma_period and available_weeks < shortest_ma_period:
        return None
    if available_weeks < 53:
        return None

    close_price = _number(row.get("close_price"))
    previous_close_price = _number(row.get("previous_close_price"))
    avg_volume = _number(row.get("avg_volume_52"))
    weekly_volume = _number(row.get("weekly_volume"))
    price_avg_1 = _number(row.get("price_avg_1"))
    if (
        close_price is None
        or previous_close_price is None
        or avg_volume is None
        or avg_volume == 0
        or weekly_volume is None
        or price_avg_1 is None
    ):
        return None
    if weekly_volume < float(config["volume_multiplier"]) * avg_volume:
        return None
    if close_price <= previous_close_price or close_price <= price_avg_1:
        return None

    missing_ma_periods = [
        {"name": name, "period": period, "available_weeks": available_weeks}
        for name, period in ma_periods.items()
        if available_weeks < period
    ]
    for _name, period in ma_periods.items():
        if available_weeks < period:
            continue
        ma_value = _number(row.get(_metric_column(period)))
        if ma_value is None or close_price <= ma_value:
            return None

    market_cap = _number(row.get("market_cap"))
    min_cap_m = float(config.get("min_market_cap", 0) or 0)
    max_cap_m = float(config.get("max_market_cap", 0) or 0)
    if market_cap is None:
        if min_cap_m > 0:
            return None
    else:
        market_cap_m = market_cap / 1_000_000
        if min_cap_m > 0 and market_cap_m < min_cap_m:
            return None
        if max_cap_m > 0 and market_cap_m > max_cap_m:
            return None

    ma_history = _ma_history_tier(ma_periods, missing_ma_periods)
    volume_ratio = _number(row.get("volume_ratio_52")) or (weekly_volume / avg_volume)
    return {
        "ticker": row.get("ticker"),
        "date": row.get("week_date").isoformat() if hasattr(row.get("week_date"), "isoformat") else str(row.get("week_date")),
        "close_price": close_price,
        "market_cap": market_cap,
        "avg_volume": avg_volume,
        "volume_ratio": volume_ratio,
        **ma_history,
        "missing_ma_periods": missing_ma_periods,
        "available_ma_weeks": available_weeks,
        "history_weeks": int(row.get("history_weeks") or 0),
        "sector": row.get("sector"),
        "industry": row.get("industry"),
    }


def _run_metric_filter(
    conn: psycopg.Connection,
    market: str,
    provider: str,
    tickers: list[str],
    config: dict[str, Any],
    progress: ProgressCallback | None,
) -> tuple[list[dict[str, Any]], int]:
    if not tickers:
        return [], 0
    lookback_weeks = max(1, int(config.get("lookback_weeks", 1) or 1))
    total = len(tickers)
    results: list[dict[str, Any]] = []
    skipped = 0
    if progress:
        progress("Filtering", 0, total, f"Scanning {total:,} {market.upper()} metric rows.")

    for chunk_start in range(0, total, HISTORY_CHUNK_SIZE * 5):
        chunk = tickers[chunk_start : chunk_start + (HISTORY_CHUNK_SIZE * 5)]
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                WITH latest AS (
                    SELECT ticker, MAX(week_date) AS latest_week
                    FROM weekly_metrics
                    WHERE market = %s AND provider = %s AND ticker = ANY(%s)
                      AND week_date <= CURRENT_DATE
                    GROUP BY ticker
                    HAVING CURRENT_DATE - MAX(week_date) <= 7
                ),
                ranked AS (
                    SELECT
                        wm.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY wm.ticker ORDER BY wm.week_date DESC
                        ) AS lookback_rank
                    FROM weekly_metrics wm
                    JOIN latest l
                      ON l.ticker = wm.ticker
                    WHERE wm.market = %s AND wm.provider = %s AND wm.ticker = ANY(%s)
                      AND wm.week_date <= l.latest_week
                )
                SELECT *
                FROM ranked
                WHERE lookback_rank <= %s
                ORDER BY ticker, week_date DESC
                """,
                (market, provider, chunk, market, provider, chunk, lookback_weeks),
            )
            rows = cur.fetchall()
        rows_by_ticker: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            rows_by_ticker.setdefault(str(row["ticker"]), []).append(dict(row))
        for offset, ticker in enumerate(chunk, 1):
            current = chunk_start + offset
            match = None
            for row in rows_by_ticker.get(ticker, []):
                match = _result_from_metric(row, config)
                if match:
                    break
            if match:
                results.append(match)
            else:
                skipped += 1
            if progress and (current == total or current % 100 == 0):
                progress("Filtering", current, total, f"Screened {current:,}/{total:,}: {ticker}")
    return results, skipped


def _has_weekly_metrics(
    conn: psycopg.Connection,
    market: str,
    provider: str,
    tickers: list[str],
) -> bool:
    if not tickers:
        return True
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH weekly_tickers AS (
                SELECT DISTINCT ticker
                FROM weekly_price_history
                WHERE market = %s AND provider = %s AND ticker = ANY(%s)
            ),
            metric_tickers AS (
                SELECT DISTINCT ticker
                FROM weekly_metrics
                WHERE market = %s AND provider = %s AND ticker = ANY(%s)
            )
            SELECT
                (SELECT COUNT(*) FROM weekly_tickers),
                (SELECT COUNT(*) FROM metric_tickers)
            """,
            (market, provider, tickers, market, provider, tickers),
        )
        weekly_count, metric_count = cur.fetchone()
        return int(metric_count or 0) >= int(weekly_count or 0)


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

    results: list[dict[str, Any]] = []
    skipped = 0
    total = len(tickers)
    scan_source = "postgresql://weekly_price_history"
    if _metric_config_supported(config) and _has_weekly_metrics(conn, market, provider, tickers):
        scan_source = "postgresql://weekly_metrics"
        results, skipped = _run_metric_filter(conn, market, provider, tickers, config, progress)
    else:
        end = date.today()
        start = (pd.Timestamp(end) - pd.DateOffset(years=years)).date()
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
                market, source_id, created_at, provider, scan_source, years,
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
