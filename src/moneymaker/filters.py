"""Filtering routines used by Moneymaker applications."""


from datetime import datetime
from io import StringIO
from typing import Dict, Optional

import json
import pandas as pd


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

        hist_daily = pd.read_json(StringIO(json.dumps(history_json)), orient="split")
        if hist_daily.empty:
            if log_queue:
                log_queue.put(f"  -> SKIPPED: {ticker} - No historical data after processing.")
            return None

        agg_functions = {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }
        weekly_data = hist_daily.resample("W-MON").agg(agg_functions).dropna(subset=["Close", "Volume"])
        weekly_data = weekly_data[weekly_data["Volume"] > 0]
        if weekly_data.empty:
            if log_queue:
                log_queue.put(f"  -> SKIPPED: {ticker} - No valid weekly data.")
            return None

        if datetime.now().date() < weekly_data.index[-1].date():
            weekly_data = weekly_data.iloc[:-1]
        if weekly_data.empty:
            if log_queue:
                log_queue.put(f"  -> SKIPPED: {ticker} - No weekly data after removing incomplete week.")
            return None

        latest_week = weekly_data.index[-1]
        if (datetime.now().date() - latest_week.date()).days > 7:
            if progress_queue:
                progress_queue.put(f"Status: {ticker} has no recent data. Skipping.")
            if log_queue:
                log_queue.put(f"  -> SKIPPED: {ticker} - No recent data.")
            return None

        lookback_weeks = config.get("lookback_weeks", 1)
        for i in range(1, lookback_weeks + 1):
            if len(weekly_data) < i:
                break
            target_week_index = -i
            if len(weekly_data.iloc[:target_week_index]) < config["ma_periods"]["short"] + 1:
                if log_queue and i == 1:
                    log_queue.put(
                        f"  -> SKIPPED: {ticker} - Too young for shortest MA ({config['ma_periods']['short']} weeks)."
                    )
                continue
            if len(weekly_data.iloc[:target_week_index]) < config["avg_volume_weeks"] + 1:
                if log_queue and i == 1:
                    log_queue.put(
                        f"  -> SKIPPED: {ticker} - Not enough data for volume average ({config['avg_volume_weeks']} weeks)."
                    )
                continue

            avg_weekly_volume_series = weekly_data["Volume"].shift(1).rolling(
                window=config["avg_volume_weeks"],
                min_periods=int(config["avg_volume_weeks"] * 0.8),
            ).mean()
            current_week_volume = weekly_data["Volume"].iloc[target_week_index]
            target_week_start_date = weekly_data.index[target_week_index]
            preceding_avg_volume = avg_weekly_volume_series.get(target_week_start_date, float("nan"))
            if pd.isna(preceding_avg_volume) or preceding_avg_volume == 0:
                continue
            if not current_week_volume >= config["volume_multiplier"] * preceding_avg_volume:
                continue

            if len(weekly_data.iloc[:target_week_index]) < config.get("price_avg_weeks", 1):
                continue
            current_week_close_price = weekly_data["Close"].iloc[target_week_index]
            if len(weekly_data) < i + 1:
                continue
            previous_week_close_price = weekly_data["Close"].iloc[target_week_index - 1]
            if current_week_close_price <= previous_week_close_price:
                continue

            price_avg_start_index = target_week_index - config.get("price_avg_weeks", 1)
            if not weekly_data["Close"].iloc[price_avg_start_index:target_week_index].empty:
                if current_week_close_price <= weekly_data["Close"].iloc[price_avg_start_index:target_week_index].mean():
                    continue
            else:
                continue

            price_conditions_met = True
            for ma_name, period in config["ma_periods"].items():
                if len(weekly_data.iloc[:target_week_index]) >= period:
                    ma_series = weekly_data["Close"].shift(1).rolling(
                        window=period, min_periods=int(period * 0.8)
                    ).mean()
                    ma_value = ma_series.get(target_week_start_date, float("nan"))
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

                return {
                    "ticker": ticker,
                    "date": target_week_start_date.strftime("%Y-%m-%d"),
                    "close_price": current_week_close_price,
                    "market_cap": market_cap,
                    "avg_volume": preceding_avg_volume,
                    "volume_ratio": current_week_volume / preceding_avg_volume
                    if preceding_avg_volume > 0
                    else float("inf"),
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

    hist_daily = pd.read_json(StringIO(json.dumps(history_json)), orient="split")
    if hist_daily.empty:
        return None

    agg_functions = {
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum",
    }
    weekly_data = (
        hist_daily.resample("W-MON").agg(agg_functions).dropna(subset=["Close", "Volume"])
    )
    weekly_data = weekly_data[weekly_data["Volume"] > 0]
    if weekly_data.empty:
        return None

    if datetime.now().date() < weekly_data.index[-1].date():
        weekly_data = weekly_data.iloc[:-1]
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
