"""Filtering routines used by Moneymaker applications."""


from datetime import datetime
from typing import Dict, Optional

import pandas as pd

_WEEKLY_AGGREGATIONS = {
    "Open": "first",
    "High": "max",
    "Low": "min",
    "Close": "last",
    "Volume": "sum",
}


def _ma_history_tier(ma_periods: Dict[str, int], missing_ma_periods: list) -> Dict:
    if not missing_ma_periods:
        return {
            "ma_data_complete": True,
            "ma_history_tier": "full",
            "ma_history_sort": 0,
            "ma_history_label": "Full",
        }

    missing_periods = sorted({int(item["period"]) for item in missing_ma_periods})
    bucket_period = missing_periods[0]
    active_periods_desc = sorted({int(period) for period in ma_periods.values()}, reverse=True)
    sort_index = active_periods_desc.index(bucket_period) + 1 if bucket_period in active_periods_desc else 99
    return {
        "ma_data_complete": False,
        "ma_history_tier": f"missing_{bucket_period}",
        "ma_history_sort": sort_index,
        "ma_history_label": f"Too young for {bucket_period}w",
    }


def _history_index_to_datetime(index_values) -> pd.DatetimeIndex:
    """Parse split-orient JSON indexes without the overhead of read_json."""

    values = list(index_values or [])
    if not values:
        return pd.DatetimeIndex([])

    first_value = next((value for value in values if value is not None), None)
    if isinstance(first_value, (int, float)):
        return pd.to_datetime(values, unit="ms")
    return pd.to_datetime(values)


def _daily_frame_from_history(history_json: Dict) -> pd.DataFrame:
    """Convert the saved split-orient history payload back into a DataFrame."""

    if not isinstance(history_json, dict):
        return pd.DataFrame()

    columns = history_json.get("columns")
    rows = history_json.get("data")
    index_values = history_json.get("index")
    if columns is None or rows is None or index_values is None:
        return pd.DataFrame()

    frame = pd.DataFrame(rows, columns=columns)
    frame.index = _history_index_to_datetime(index_values)
    if frame.empty:
        return frame

    frame = frame.sort_index()
    for column in _WEEKLY_AGGREGATIONS:
        if column not in frame.columns:
            return pd.DataFrame()
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _weekly_data_from_history(history_json: Dict) -> pd.DataFrame:
    """Build completed weekly OHLCV bars from saved daily history."""

    hist_daily = _daily_frame_from_history(history_json)
    if hist_daily.empty:
        return pd.DataFrame()

    weekly_data = (
        hist_daily.resample("W-MON")
        .agg(_WEEKLY_AGGREGATIONS)
        .dropna(subset=["Close", "Volume"])
    )
    weekly_data = weekly_data[weekly_data["Volume"] > 0]
    if weekly_data.empty:
        return weekly_data

    if datetime.now().date() < weekly_data.index[-1].date():
        weekly_data = weekly_data.iloc[:-1]
    return weekly_data


def analyze_stock_from_local_data(
    ticker: str,
    data: Dict,
    config: Dict,
    progress_queue: Optional[object] = None,
    log_queue: Optional[object] = None,
) -> Optional[Dict]:
    """Evaluate a stock's data against filter configuration.

    Parameters mirror the earlier standalone implementations so that legacy
    scripts can reuse this routine without duplication.
    """
    try:
        info = data.get("info", {})
        history_json = data.get("history")
        if not history_json:
            if log_queue:
                log_queue.put(f"  -> SKIPPED: {ticker} - Missing essential history data.")
            return None

        market_cap = info.get("marketCap") if info else None
        min_cap_m = config.get("min_market_cap", 0)
        max_cap_m = config.get("max_market_cap", 0)

        weekly_data = _weekly_data_from_history(history_json)
        if weekly_data.empty:
            if log_queue:
                log_queue.put(f"  -> SKIPPED: {ticker} - No valid weekly data.")
            return None

        latest_week = weekly_data.index[-1]
        if (datetime.now().date() - latest_week.date()).days > 7:
            if progress_queue:
                progress_queue.put(f"Status: {ticker} has no recent data. Skipping.")
            if log_queue:
                log_queue.put(f"  -> SKIPPED: {ticker} - No recent data.")
            return None

        lookback_weeks = config.get("lookback_weeks", 1)
        ma_periods = {
            name: int(period)
            for name, period in (config.get("ma_periods") or {}).items()
            if period and int(period) > 0
        }
        avg_volume_weeks = max(1, int(config["avg_volume_weeks"]))
        price_avg_weeks = max(1, int(config.get("price_avg_weeks", 1)))
        avg_weekly_volume_series = weekly_data["Volume"].shift(1).rolling(
            window=avg_volume_weeks,
            min_periods=int(avg_volume_weeks * 0.8),
        ).mean()
        price_avg_series = weekly_data["Close"].shift(1).rolling(
            window=price_avg_weeks,
            min_periods=price_avg_weeks,
        ).mean()
        ma_series_by_name = {
            name: weekly_data["Close"].shift(1).rolling(
                window=period, min_periods=int(period * 0.8)
            ).mean()
            for name, period in ma_periods.items()
        }

        for i in range(1, lookback_weeks + 1):
            if len(weekly_data) < i:
                break
            target_week_index = -i
            pre_target_slice = weekly_data.iloc[:target_week_index]
            available_weeks = len(pre_target_slice)

            missing_ma_periods = [
                {"name": name, "period": period, "available_weeks": available_weeks}
                for name, period in ma_periods.items()
                if available_weeks < period
            ]
            shortest_ma_period = min(ma_periods.values()) if ma_periods else 0
            if shortest_ma_period and available_weeks < shortest_ma_period:
                if log_queue and i == 1:
                    log_queue.put(
                        f"  -> SKIPPED: {ticker} - Not enough data for shortest moving average ({shortest_ma_period} weeks)."
                    )
                continue

            if available_weeks < avg_volume_weeks + 1:
                if log_queue and i == 1:
                    log_queue.put(
                        f"  -> SKIPPED: {ticker} - Not enough data for volume average ({avg_volume_weeks} weeks)."
                    )
                continue

            current_week_volume = weekly_data["Volume"].iloc[target_week_index]
            target_week_start_date = weekly_data.index[target_week_index]
            preceding_avg_volume = avg_weekly_volume_series.get(target_week_start_date, float("nan"))
            if pd.isna(preceding_avg_volume) or preceding_avg_volume == 0:
                continue
            if not current_week_volume >= config["volume_multiplier"] * preceding_avg_volume:
                continue

            if available_weeks < price_avg_weeks:
                continue
            current_week_close_price = weekly_data["Close"].iloc[target_week_index]
            if len(weekly_data) < i + 1:
                continue
            previous_week_close_price = weekly_data["Close"].iloc[target_week_index - 1]
            if current_week_close_price <= previous_week_close_price:
                continue

            price_avg_value = price_avg_series.get(target_week_start_date, float("nan"))
            if pd.isna(price_avg_value) or current_week_close_price <= price_avg_value:
                continue

            price_conditions_met = True
            for ma_name, period in ma_periods.items():
                if available_weeks < period:
                    continue
                ma_value = ma_series_by_name[ma_name].get(target_week_start_date, float("nan"))
                if pd.isna(ma_value) or current_week_close_price <= ma_value:
                    price_conditions_met = False
                    break

            if price_conditions_met:
                if market_cap is None:
                    if min_cap_m > 0:
                        if log_queue:
                            log_queue.put(
                                f"  -> WEEK SKIPPED ({target_week_start_date.date()}): No market cap data, but min cap filter is active."
                            )
                        continue
                else:
                    market_cap_in_millions = market_cap / 1_000_000
                    if min_cap_m > 0 and market_cap_in_millions < min_cap_m:
                        if log_queue:
                            log_queue.put(
                                f"  -> WEEK SKIPPED ({target_week_start_date.date()}): Mkt Cap ({market_cap_in_millions:.2f}M) is below min ({min_cap_m}M)."
                            )
                        continue
                    if max_cap_m > 0 and market_cap_in_millions > max_cap_m:
                        if log_queue:
                            log_queue.put(
                                f"  -> WEEK SKIPPED ({target_week_start_date.date()}): Mkt Cap ({market_cap_in_millions:.2f}M) is above max ({max_cap_m}M)."
                            )
                        continue

                ma_history = _ma_history_tier(ma_periods, missing_ma_periods)
                return {
                    "ticker": ticker,
                    "date": target_week_start_date.strftime("%Y-%m-%d"),
                    "close_price": current_week_close_price,
                    "market_cap": market_cap,
                    "avg_volume": preceding_avg_volume,
                    "volume_ratio": current_week_volume / preceding_avg_volume
                    if preceding_avg_volume > 0
                    else float("inf"),
                    **ma_history,
                    "missing_ma_periods": missing_ma_periods,
                    "available_ma_weeks": available_weeks,
                    "history_weeks": len(weekly_data),
                }

        if log_queue:
            log_queue.put(
                f"  -> SKIPPED: {ticker} - No week in the lookback period met all criteria."
            )
        return None
    except Exception as e:  # pragma: no cover - debug path
        if progress_queue:
            progress_queue.put(f"Error processing {ticker}: {str(e)[:100]}")
        if log_queue:
            log_queue.put(f"  -> ERROR: {ticker} - {str(e)[:100]}")
    return None


def validate_latest_week(
    ticker: str,
    data: Dict,
    config: Dict,
    progress_queue: Optional[object] = None,
    log_queue: Optional[object] = None,
) -> Optional[Dict]:
    """Validate only the most recent completed week for a given ``ticker``.

    This is a convenience wrapper around :func:`analyze_stock_from_local_data`
    that forces ``lookback_weeks`` to ``1`` and then performs additional
    validations:

    * ensures the returned week corresponds to the latest *completed* week
    * verifies the week had a positive price change (close > open)

    If all checks pass, the result from
    :func:`analyze_stock_from_local_data` is returned.  Otherwise ``None`` is
    returned.
    """

    cfg = dict(config)
    cfg["lookback_weeks"] = 1
    result = analyze_stock_from_local_data(
        ticker, data, cfg, progress_queue, log_queue
    )
    if not result:
        return None

    history_json = data.get("history")
    if not history_json:
        return None

    weekly_data = _weekly_data_from_history(history_json)
    if weekly_data.empty:
        return None

    latest_week_start = weekly_data.index[-1]

    # Reject if the analysis result is not for the most recent completed week
    if result["date"] != latest_week_start.strftime("%Y-%m-%d"):
        if log_queue:
            log_queue.put(
                f"  -> REJECTED: {ticker} - Result is not from the latest completed week."
            )
        return None

    latest_week_row = weekly_data.loc[latest_week_start]
    if latest_week_row["Close"] <= latest_week_row["Open"]:
        if log_queue:
            log_queue.put(
                f"  -> REJECTED: {ticker} - Weekly close price not higher than open."
            )
        return None

    if len(weekly_data) < 2:
        if log_queue:
            log_queue.put(
                f"  -> REJECTED: {ticker} - No preceding week to compare close price."
            )
        return None
    previous_week_close = weekly_data["Close"].iloc[-2]
    if latest_week_row["Close"] <= previous_week_close:
        if log_queue:
            log_queue.put(
                f"  -> REJECTED: {ticker} - Weekly close price not higher than prior week's close."
            )
        return None

    return result
