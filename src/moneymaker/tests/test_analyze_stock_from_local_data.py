"""Tests for :func:`moneymaker.filters.analyze_stock_from_local_data`."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Iterable, Tuple

import pandas as pd

from moneymaker.filters import analyze_stock_from_local_data


def _build_history(closes: Iterable[float], volumes: Iterable[float]) -> Tuple[dict, pd.DatetimeIndex]:
    """Create historical price data aligned to completed Mondays."""

    closes = list(closes)
    volumes = list(volumes)
    if len(closes) != len(volumes):
        raise ValueError("closes and volumes must be the same length")

    today = datetime.now().date()
    latest_monday = today - timedelta(days=today.weekday())
    mondays = pd.date_range(end=pd.Timestamp(latest_monday), periods=len(closes), freq="W-MON")

    frame = pd.DataFrame(
        {
            "Open": closes,
            "High": closes,
            "Low": closes,
            "Close": closes,
            "Volume": volumes,
        },
        index=mondays,
    )

    return json.loads(frame.to_json(orient="split")), mondays


def _base_config(**overrides):
    config = {
        "lookback_weeks": 1,
        "avg_volume_weeks": 2,
        "volume_multiplier": 1.0,
        "price_avg_weeks": 1,
        "ma_periods": {"short": 2, "intermediate": 3},
        "min_market_cap": 0,
        "max_market_cap": 0,
    }
    config.update(overrides)
    return config


def _build_payload(closes: Iterable[float], volumes: Iterable[float]):
    history, mondays = _build_history(closes, volumes)
    payload = {
        "info": {"marketCap": 5_000_000},
        "history": history,
    }
    return payload, mondays


def test_accepts_when_price_exceeds_all_configured_mas():
    payload, mondays = _build_payload(
        closes=[10, 11, 12, 13, 20],
        volumes=[100, 110, 120, 130, 140],
    )

    result = analyze_stock_from_local_data("TEST", payload, _base_config())

    assert result is not None
    assert result["date"] == mondays[-1].strftime("%Y-%m-%d")
    assert result["close_price"] == 20


def test_rejects_when_missing_history_for_any_configured_ma():
    payload, _ = _build_payload(
        closes=[10, 11, 12, 13, 20],
        volumes=[100, 110, 120, 130, 140],
    )

    result = analyze_stock_from_local_data(
        "TEST",
        payload,
        _base_config(ma_periods={"short": 2, "long": 6}),
    )

    assert result is None


def test_rejects_when_latest_price_not_above_all_mas():
    payload, _ = _build_payload(
        closes=[200, 190, 180, 170, 175],
        volumes=[100, 110, 120, 130, 140],
    )

    result = analyze_stock_from_local_data(
        "TEST",
        payload,
        _base_config(ma_periods={"short": 2, "intermediate": 4}),
    )

    assert result is None

