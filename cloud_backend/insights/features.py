"""Deterministic, point-in-time technical features.

Callers must pass rows already restricted to the appraisal cutoff. Functions in
this module never fetch current data and deliberately report missing history.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
import math
from statistics import fmean, pstdev
from typing import Any, Iterable, Sequence


# Version 3: price history is cut off at the last session completed in the
# exchange's local time (see anchor.py) instead of the UTC calendar date.
FEATURE_VERSION = 3


@dataclass(frozen=True)
class FeatureDefinition:
    name: str
    category: str
    value_type: str
    unit: str | None
    description: str
    formula: str
    required_history_days: int | None
    source_name: str = "price_history"


@dataclass(frozen=True)
class FeatureValue:
    value: float | bool | str | None
    missing_reason: str | None = None
    source_as_of_utc: str | None = None

    @property
    def is_missing(self) -> bool:
        return self.value is None

    def as_json(self) -> dict[str, Any]:
        return asdict(self)


def _definition(
    name: str,
    category: str,
    value_type: str,
    unit: str | None,
    description: str,
    formula: str,
    required_history_days: int | None,
) -> FeatureDefinition:
    return FeatureDefinition(name, category, value_type, unit, description, formula, required_history_days)


FEATURE_DEFINITIONS: tuple[FeatureDefinition, ...] = tuple(
    [
        _definition(f"return_{period}d_pct", "trend", "numeric", "percent", f"Close return over {period} trading sessions", f"(close_t / close_t-{period} - 1) * 100", period,)
        for period in (1, 5, 20, 60, 120, 252)
    ]
    + [
        _definition(f"distance_ma_{period}w_pct", "trend", "numeric", "percent", f"Distance from the {period}-week moving average", f"(latest weekly close / mean(last {period} weekly closes) - 1) * 100", period * 7,)
        for period in (30, 90, 180, 360, 700)
    ]
    + [
        _definition("return_acceleration_20d_pct", "trend", "numeric", "percentage_points", "Recent 20-session return minus the preceding 20-session return", "return(close[-1], close[-21]) - return(close[-21], close[-41])", 41),
        _definition("distance_52w_high_pct", "trend", "numeric", "percent", "Distance from the highest close in the preceding 252 sessions", "(latest close / max(last 252 closes) - 1) * 100", 252),
        _definition("trend_slope_60d_pct", "trend", "numeric", "percent_per_session", "Least-squares slope of log close over 60 sessions", "OLS slope(log(close), session) * 100", 60),
        _definition("trend_r2_60d", "trend", "numeric", "ratio", "R-squared of the 60-session log-price trend", "squared correlation(session, log(close))", 60),
        _definition("efficiency_ratio_20d", "trend", "numeric", "ratio", "Directional movement divided by total movement over 20 sessions", "abs(close_t-close_t-20) / sum(abs(delta close))", 21),
        _definition("relative_volume_1d", "volume", "numeric", "ratio", "Latest volume divided by prior 20-session mean volume", "volume_t / mean(volume_t-20:t-1)", 21),
        _definition("relative_volume_5d", "volume", "numeric", "ratio", "Latest five-session mean volume divided by prior 20-session mean", "mean(volume_t-4:t) / mean(volume_t-24:t-5)", 25),
        _definition("dollar_volume_20d", "liquidity", "numeric", "currency", "Mean close multiplied by volume over 20 sessions", "mean(close * volume, 20 sessions)", 20),
        _definition("up_day_volume_share_20d", "volume", "numeric", "ratio", "Share of 20-session volume occurring on non-negative sessions", "up-day volume / total volume", 21),
        _definition("realized_volatility_20d_pct", "risk", "numeric", "annualized_percent", "Annualized standard deviation of daily log returns over 20 sessions", "std(log returns, 20) * sqrt(252) * 100", 21),
        _definition("realized_volatility_60d_pct", "risk", "numeric", "annualized_percent", "Annualized standard deviation of daily log returns over 60 sessions", "std(log returns, 60) * sqrt(252) * 100", 61),
        _definition("atr_14d_pct", "risk", "numeric", "percent", "Fourteen-session average true range as a percentage of close", "mean(true range, 14) / close * 100", 15),
        _definition("maximum_drawdown_120d_pct", "risk", "numeric", "percent", "Largest peak-to-trough close decline over 120 sessions", "min(close / running_max(close) - 1) * 100", 120),
        _definition("history_sessions", "quality", "numeric", "sessions", "Number of daily sessions available at appraisal", "count(price_history rows through cutoff)", 1),
        _definition("history_weeks", "quality", "numeric", "weeks", "Number of point-in-time weekly closes available", "count(weekly aggregates through cutoff)", 1),
    ]
)


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _row_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _missing(reason: str, source: str | None) -> FeatureValue:
    return FeatureValue(None, reason, source)


def _present(value: float | bool | str, source: str | None) -> FeatureValue:
    return FeatureValue(value, None, source)


def _return(closes: Sequence[float], sessions: int, source: str | None) -> FeatureValue:
    if len(closes) <= sessions:
        return _missing(f"requires {sessions + 1} sessions; {len(closes)} available", source)
    base = closes[-sessions - 1]
    if base <= 0:
        return _missing("base close is not positive", source)
    return _present((closes[-1] / base - 1.0) * 100.0, source)


def _weekly_closes(rows: Sequence[dict[str, Any]]) -> list[float]:
    weeks: dict[date, tuple[date, float]] = {}
    for row in rows:
        close = _number(row.get("close"))
        if close is None:
            continue
        day = _row_date(row.get("date"))
        week_end = day + timedelta(days=(4 - day.weekday()) % 7)
        prior = weeks.get(week_end)
        if prior is None or day >= prior[0]:
            weeks[week_end] = (day, close)
    return [weeks[key][1] for key in sorted(weeks)]


def _ols_log_trend(values: Sequence[float], source: str | None) -> tuple[FeatureValue, FeatureValue]:
    if len(values) < 60:
        reason = f"requires 60 sessions; {len(values)} available"
        return _missing(reason, source), _missing(reason, source)
    ys = [math.log(value) for value in values[-60:] if value > 0]
    if len(ys) != 60:
        return _missing("non-positive close in trend window", source), _missing("non-positive close in trend window", source)
    xs = list(range(60))
    mean_x, mean_y = fmean(xs), fmean(ys)
    variance_x = sum((x - mean_x) ** 2 for x in xs)
    covariance = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    slope = covariance / variance_x
    variance_y = sum((y - mean_y) ** 2 for y in ys)
    r2 = (covariance * covariance / (variance_x * variance_y)) if variance_y else 1.0
    return _present(slope * 100.0, source), _present(r2, source)


def calculate_technical_features(rows: Iterable[dict[str, Any]]) -> dict[str, FeatureValue]:
    """Calculate v1 features from chronologically ordered point-in-time rows."""
    valid_rows = sorted(
        (dict(row) for row in rows if _number(row.get("close")) is not None),
        key=lambda row: _row_date(row.get("date")),
    )
    if not valid_rows:
        return {definition.name: _missing("no price history at appraisal cutoff", None) for definition in FEATURE_DEFINITIONS}

    source = f"{_row_date(valid_rows[-1]['date']).isoformat()}T23:59:59Z"
    closes = [_number(row.get("close")) for row in valid_rows]
    close_values = [value for value in closes if value is not None]
    weekly = _weekly_closes(valid_rows)
    output: dict[str, FeatureValue] = {}

    for period in (1, 5, 20, 60, 120, 252):
        output[f"return_{period}d_pct"] = _return(close_values, period, source)

    for period in (30, 90, 180, 360, 700):
        name = f"distance_ma_{period}w_pct"
        if len(weekly) < period:
            output[name] = _missing(f"requires {period} weeks; {len(weekly)} available", source)
        else:
            average = fmean(weekly[-period:])
            output[name] = _present((weekly[-1] / average - 1.0) * 100.0, source) if average > 0 else _missing("moving average is not positive", source)

    if len(close_values) >= 41 and close_values[-41] > 0 and close_values[-21] > 0:
        recent = (close_values[-1] / close_values[-21] - 1.0) * 100.0
        preceding = (close_values[-21] / close_values[-41] - 1.0) * 100.0
        output["return_acceleration_20d_pct"] = _present(recent - preceding, source)
    else:
        output["return_acceleration_20d_pct"] = _missing(f"requires 41 sessions; {len(close_values)} available", source)

    if len(close_values) >= 252:
        high = max(close_values[-252:])
        output["distance_52w_high_pct"] = _present((close_values[-1] / high - 1.0) * 100.0, source)
    else:
        output["distance_52w_high_pct"] = _missing(f"requires 252 sessions; {len(close_values)} available", source)

    slope, r2 = _ols_log_trend(close_values, source)
    output["trend_slope_60d_pct"] = slope
    output["trend_r2_60d"] = r2

    if len(close_values) >= 21:
        movement = sum(abs(close_values[index] - close_values[index - 1]) for index in range(len(close_values) - 20, len(close_values)))
        output["efficiency_ratio_20d"] = _present(abs(close_values[-1] - close_values[-21]) / movement if movement else 0.0, source)
    else:
        output["efficiency_ratio_20d"] = _missing(f"requires 21 sessions; {len(close_values)} available", source)

    volumes = [_number(row.get("volume")) for row in valid_rows]
    if len(volumes) >= 21 and all(value is not None for value in volumes[-21:]):
        prior = fmean(value for value in volumes[-21:-1] if value is not None)
        output["relative_volume_1d"] = _present(volumes[-1] / prior, source) if prior > 0 and volumes[-1] is not None else _missing("prior volume mean is zero", source)
    else:
        output["relative_volume_1d"] = _missing("requires 21 non-missing volume sessions", source)

    if len(volumes) >= 25 and all(value is not None for value in volumes[-25:]):
        latest_five = fmean(value for value in volumes[-5:] if value is not None)
        prior_twenty = fmean(value for value in volumes[-25:-5] if value is not None)
        output["relative_volume_5d"] = _present(latest_five / prior_twenty, source) if prior_twenty > 0 else _missing("prior volume mean is zero", source)
    else:
        output["relative_volume_5d"] = _missing("requires 25 non-missing volume sessions", source)

    if len(valid_rows) >= 20 and all(_number(row.get("volume")) is not None for row in valid_rows[-20:]):
        dollar_values = [_number(row.get("close")) * _number(row.get("volume")) for row in valid_rows[-20:]]
        output["dollar_volume_20d"] = _present(fmean(dollar_values), source)
    else:
        output["dollar_volume_20d"] = _missing("requires 20 non-missing close and volume sessions", source)

    if len(valid_rows) >= 21 and all(_number(row.get("volume")) is not None for row in valid_rows[-20:]):
        selected = valid_rows[-20:]
        total = sum(_number(row.get("volume")) or 0 for row in selected)
        start_index = len(valid_rows) - 20
        up_volume = sum(
            _number(row.get("volume")) or 0
            for index, row in enumerate(selected, start=start_index)
            if (_number(row.get("close")) or 0) >= (_number(valid_rows[index - 1].get("close")) or 0)
        )
        output["up_day_volume_share_20d"] = _present(up_volume / total, source) if total > 0 else _missing("total volume is zero", source)
    else:
        output["up_day_volume_share_20d"] = _missing("requires 21 close sessions and 20 volume sessions", source)

    log_returns = [math.log(close_values[index] / close_values[index - 1]) for index in range(1, len(close_values)) if close_values[index - 1] > 0]
    for period in (20, 60):
        name = f"realized_volatility_{period}d_pct"
        output[name] = _present(pstdev(log_returns[-period:]) * math.sqrt(252) * 100.0, source) if len(log_returns) >= period else _missing(f"requires {period + 1} sessions; {len(close_values)} available", source)

    if len(valid_rows) >= 15:
        true_ranges: list[float] = []
        for index in range(len(valid_rows) - 14, len(valid_rows)):
            high = _number(valid_rows[index].get("high"))
            low = _number(valid_rows[index].get("low"))
            prior_close = _number(valid_rows[index - 1].get("close"))
            if high is None or low is None or prior_close is None:
                true_ranges = []
                break
            true_ranges.append(max(high - low, abs(high - prior_close), abs(low - prior_close)))
        output["atr_14d_pct"] = _present(fmean(true_ranges) / close_values[-1] * 100.0, source) if true_ranges and close_values[-1] > 0 else _missing("requires complete OHLC history", source)
    else:
        output["atr_14d_pct"] = _missing(f"requires 15 sessions; {len(valid_rows)} available", source)

    if len(close_values) >= 120:
        peak = close_values[-120]
        drawdown = 0.0
        for value in close_values[-120:]:
            peak = max(peak, value)
            drawdown = min(drawdown, value / peak - 1.0)
        output["maximum_drawdown_120d_pct"] = _present(drawdown * 100.0, source)
    else:
        output["maximum_drawdown_120d_pct"] = _missing(f"requires 120 sessions; {len(close_values)} available", source)

    output["history_sessions"] = _present(float(len(valid_rows)), source)
    output["history_weeks"] = _present(float(len(weekly)), source)
    return output
