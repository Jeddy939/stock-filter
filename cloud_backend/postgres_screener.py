"""Run the existing MoneyMaker screen directly against PostgreSQL."""

from __future__ import annotations

import math
import hashlib
import json
import time
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
STAGE_LOADING_SNAPSHOT = "Loading market snapshot"
STAGE_APPLYING_CRITERIA = "Applying screen criteria"
STAGE_RANKING_MATCHES = "Ranking matches"
STAGE_SAVING_SCREEN = "Saving screen"


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


def _canonical_number(value: Any) -> Any:
    number = _number(value)
    if number is None:
        return 0
    if number < 0:
        return 0
    return int(number) if number.is_integer() else number


def normalize_screen_config(payload: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Match the Functions config hash for dedupe/default scan lookup."""
    current = config or _filter_config(payload)
    raw_periods = payload.get("ma_periods")
    if not isinstance(raw_periods, dict):
        raw_periods = {
            "short": current.get("ma_periods", {}).get("short", 90),
            "intermediate": current.get("ma_periods", {}).get("intermediate", 180),
            "medium": current.get("ma_periods", {}).get("medium", 360),
            "long": current.get("ma_periods", {}).get("long", 700),
        }
    ma_periods = {
        str(name): max(0, int(_number(period) or 0))
        for name, period in raw_periods.items()
        if max(0, int(_number(period) or 0)) > 0
    }
    market = "us" if str(payload.get("market") or "asx").strip().lower() == "us" else "asx"
    return {
        "avg_volume_weeks": int(current.get("avg_volume_weeks", 52) or 52),
        "limit": max(0, int(_number(payload.get("limit")) or 0)),
        "lookback_weeks": int(current.get("lookback_weeks", 1) or 1),
        "market": market,
        "max_market_cap": _canonical_number(current.get("max_market_cap", 0)),
        "min_market_cap": _canonical_number(current.get("min_market_cap", 0)),
        "price_avg_weeks": int(current.get("price_avg_weeks", 1) or 1),
        "provider": fetcher.normalize_provider(payload.get("provider") or fetcher.DEFAULT_PROVIDER),
        "query": str(payload.get("query") or "").strip().upper(),
        "volume_multiplier": _canonical_number(current.get("volume_multiplier", 2)),
        "ma_periods": {key: ma_periods[key] for key in sorted(ma_periods)},
    }


def screen_config_hash(payload: dict[str, Any], config: dict[str, Any] | None = None) -> str:
    encoded = json.dumps(
        normalize_screen_config(payload, config),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _compatible_filter_config(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload.get("ma_periods"), dict):
        return _filter_config(payload)
    periods = payload["ma_periods"]
    bridged = dict(payload)
    period_names = {
        "short": "ma_short",
        "intermediate": "ma_intermediate",
        "medium": "ma_medium",
        "long": "ma_long",
    }
    for name, legacy_key in period_names.items():
        if legacy_key not in bridged and name in periods:
            bridged[legacy_key] = periods[name]
    return _filter_config(bridged)


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


def _run_metric_filter_chunked(
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
        progress(STAGE_APPLYING_CRITERIA, 0, total, f"Scanning {total:,} {market.upper()} metric rows.")

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
            metric_rows = rows_by_ticker.get(ticker, [])
            for row in metric_rows:
                match = _result_from_metric(row, config)
                if match:
                    break
            if match:
                results.append(match)
            elif not metric_rows:
                skipped += 1
            if progress and (current == total or current % 100 == 0):
                progress(STAGE_APPLYING_CRITERIA, current, total, f"Screened {current:,}/{total:,}: {ticker}")
    return results, skipped


def _run_metric_filter_set_based(
    conn: psycopg.Connection,
    market: str,
    provider: str,
    tickers: list[str],
    config: dict[str, Any],
    progress: ProgressCallback | None,
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    if not tickers:
        return [], 0, {"query_ms": 0}

    lookback_weeks = max(1, int(config.get("lookback_weeks", 1) or 1))
    ma_periods = sorted({
        int(period)
        for period in (config.get("ma_periods") or {}).values()
        if period and int(period) > 0
    })
    for period in ma_periods:
        _metric_column(period)

    min_cap_m = float(config.get("min_market_cap", 0) or 0)
    max_cap_m = float(config.get("max_market_cap", 0) or 0)
    total = len(tickers)
    min_weeks = min(ma_periods) if ma_periods else 53
    required_weeks = max(53, min_weeks)
    ma_conditions = [
        f"(ranked.available_weeks < {period} OR (ranked.{_metric_column(period)} IS NOT NULL AND ranked.close_price > ranked.{_metric_column(period)}))"
        for period in ma_periods
    ]
    cap_conditions: list[str] = []
    params: list[Any] = [market, provider, market, provider, lookback_weeks, required_weeks, float(config["volume_multiplier"])]
    if min_cap_m > 0:
        cap_conditions.append("(ranked.market_cap IS NOT NULL AND ranked.market_cap >= %s)")
        params.append(min_cap_m * 1_000_000)
    if max_cap_m > 0:
        cap_conditions.append("(ranked.market_cap IS NULL OR ranked.market_cap <= %s)")
        params.append(max_cap_m * 1_000_000)

    where_sql = "\n                  AND ".join([
        "ranked.lookback_rank <= %s",
        "ranked.available_weeks >= %s",
        "ranked.close_price IS NOT NULL",
        "ranked.previous_close_price IS NOT NULL",
        "ranked.avg_volume_52 IS NOT NULL",
        "ranked.avg_volume_52 <> 0",
        "ranked.weekly_volume IS NOT NULL",
        "ranked.price_avg_1 IS NOT NULL",
        "ranked.weekly_volume >= %s * ranked.avg_volume_52",
        "ranked.close_price > ranked.previous_close_price",
        "ranked.close_price > ranked.price_avg_1",
        *ma_conditions,
        *cap_conditions,
    ])

    if progress:
        progress(STAGE_APPLYING_CRITERIA, 0, total, f"Querying {total:,} {market.upper()} weekly metric rows.")
    started = time.perf_counter()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            WITH input_tickers AS (
                SELECT unnest(%s::text[]) AS ticker
            ),
            latest AS (
                SELECT wm.ticker, MAX(wm.week_date) AS latest_week
                FROM weekly_metrics wm
                JOIN input_tickers it ON it.ticker = wm.ticker
                WHERE wm.market = %s
                  AND wm.provider = %s
                  AND wm.week_date <= CURRENT_DATE
                GROUP BY wm.ticker
                HAVING CURRENT_DATE - MAX(wm.week_date) <= 7
            ),
            ranked AS (
                SELECT
                    wm.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY wm.ticker ORDER BY wm.week_date DESC
                    ) AS lookback_rank
                FROM weekly_metrics wm
                JOIN latest l ON l.ticker = wm.ticker
                WHERE wm.market = %s
                  AND wm.provider = %s
                  AND wm.week_date <= l.latest_week
            ),
            matched AS (
                SELECT DISTINCT ON (ranked.ticker) ranked.*
                FROM ranked
                WHERE {where_sql}
                ORDER BY ranked.ticker, ranked.week_date DESC
            )
            SELECT *
            FROM matched
            ORDER BY ticker
            """,
            (tickers, *params),
        )
        rows = [dict(row) for row in cur.fetchall()]
        cur.execute(
            """
            WITH input_tickers AS (
                SELECT unnest(%s::text[]) AS ticker
            ),
            latest AS (
                SELECT wm.ticker, MAX(wm.week_date) AS latest_week
                FROM weekly_metrics wm
                JOIN input_tickers it ON it.ticker = wm.ticker
                WHERE wm.market = %s
                  AND wm.provider = %s
                  AND wm.week_date <= CURRENT_DATE
                GROUP BY wm.ticker
                HAVING CURRENT_DATE - MAX(wm.week_date) <= 7
            )
            SELECT COUNT(*)::int
            FROM latest
            """,
            (tickers, market, provider),
        )
        recent_ticker_count = int(cur.fetchone()["count"])

    query_ms = round((time.perf_counter() - started) * 1000, 2)
    results = [
        result
        for row in rows
        if (result := _result_from_metric(row, config)) is not None
    ]
    skipped = max(0, total - recent_ticker_count)
    if progress:
        progress(STAGE_APPLYING_CRITERIA, total, total, f"Matched {len(results):,} stocks in {query_ms:,.0f} ms.")
    return results, skipped, {
        "path": "weekly_metrics_set_based",
        "query_ms": query_ms,
        "recent_ticker_count": recent_ticker_count,
    }


def _run_metric_filter(
    conn: psycopg.Connection,
    market: str,
    provider: str,
    tickers: list[str],
    config: dict[str, Any],
    progress: ProgressCallback | None,
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    return _run_metric_filter_set_based(conn, market, provider, tickers, config, progress)


def _has_recent_weekly_metrics(
    conn: psycopg.Connection,
    market: str,
    provider: str,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM weekly_metrics
                WHERE market = %s
                  AND provider = %s
                  AND week_date >= CURRENT_DATE - 14
                LIMIT 1
            )
            """,
            (market, provider),
        )
        return bool(cur.fetchone()[0])


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
    config = _compatible_filter_config(payload)
    config_hash = str(payload.get("config_hash") or screen_config_hash(payload, config))
    performance: dict[str, Any] = {"stages": {}}

    stage_started = time.perf_counter()
    if progress:
        progress(STAGE_LOADING_SNAPSHOT, 0, None, f"Loading {market.upper()} stock universe.")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker, info_json FROM companies WHERE market = %s ORDER BY ticker",
            (market,),
        )
        company_rows = cur.fetchall()
        cur.execute(
            """
            SELECT latest_date
            FROM market_status
            WHERE market = %s AND provider = %s
            """,
            (market, provider),
        )
        status_row = cur.fetchone()
        market_snapshot_date = status_row[0] if status_row else None
        if market_snapshot_date is None:
            cur.execute(
                """
                SELECT MAX(week_date)
                FROM weekly_metrics
                WHERE market = %s AND provider = %s
                """,
                (market, provider),
            )
            market_snapshot_date = cur.fetchone()[0]
    performance["stages"]["loading_market_snapshot_ms"] = round((time.perf_counter() - stage_started) * 1000, 2)
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
    if _metric_config_supported(config) and _has_recent_weekly_metrics(conn, market, provider):
        scan_source = "postgresql://weekly_metrics"
        results, skipped, metric_performance = _run_metric_filter(conn, market, provider, tickers, config, progress)
        performance.update(metric_performance)
    else:
        end = date.today()
        start = (pd.Timestamp(end) - pd.DateOffset(years=years)).date()
        if progress:
            progress(STAGE_APPLYING_CRITERIA, 0, total, f"Scanning {total:,} {market.upper()} stocks online.")

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
                    progress(STAGE_APPLYING_CRITERIA, current, total, f"Screened {current:,}/{total:,}: {ticker}")

    rank_started = time.perf_counter()
    if progress:
        progress(STAGE_RANKING_MATCHES, 0, len(results), f"Ranking {len(results):,} matches.")
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
    performance["stages"]["ranking_matches_ms"] = round((time.perf_counter() - rank_started) * 1000, 2)

    created_at = datetime.now(timezone.utc)
    source_id = int(created_at.timestamp() * 1_000_000)
    if progress:
        progress(STAGE_SAVING_SCREEN, total, total, f"Saving {len(results):,} matches online.")
    save_started = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scan_runs (
                market, source_id, created_at_utc, provider, cache_file, years, limit_count,
                query, scanned_count, result_count, skipped_no_history, config_json,
                ticker_universe_json, config_hash, market_snapshot_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                market, source_id, created_at, provider, scan_source, years,
                limit if limit > 0 else None, query, total, len(results), skipped,
                Jsonb(_json_safe(config)), Jsonb(tickers), config_hash, market_snapshot_date,
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
    performance["stages"]["saving_screen_ms"] = round((time.perf_counter() - save_started) * 1000, 2)
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
        "config_hash": config_hash,
        "market_snapshot_date": market_snapshot_date.isoformat() if hasattr(market_snapshot_date, "isoformat") else market_snapshot_date,
        "performance": performance,
        "source": "online_database",
    }
