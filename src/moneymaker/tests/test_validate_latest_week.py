import json
from datetime import datetime, timedelta

import pandas as pd

from ..filters import validate_latest_week

CONFIG = {
    "avg_volume_weeks": 2,
    "volume_multiplier": 2,
    "price_avg_weeks": 1,
    "ma_periods": {"short": 2},
    "min_market_cap": 0,
    "max_market_cap": 0,
}


def build_history(week_specs):
    today = datetime.now().date()
    last_monday = today - timedelta(days=today.weekday())
    start_date = last_monday - timedelta(weeks=len(week_specs)) + timedelta(days=1)
    dates = []
    rows = []
    for i, spec in enumerate(week_specs):
        week_start = start_date + timedelta(weeks=i)
        for d in range(5):
            date = week_start + timedelta(days=d)
            open_price = spec["open"]
            close_price = spec["close"] if d == 4 else spec["open"]
            high = max(open_price, close_price) + 1
            low = min(open_price, close_price) - 1
            vol = spec["volume"] / 5
            rows.append({
                "Open": open_price,
                "High": high,
                "Low": low,
                "Close": close_price,
                "Volume": vol,
            })
            dates.append(pd.Timestamp(date))
    df = pd.DataFrame(rows, index=pd.to_datetime(dates))
    return json.loads(df.to_json(date_format="iso", orient="split"))


def test_validate_latest_week_requires_volume_spike():
    history = build_history([
        {"open": 10, "close": 10, "volume": 100},
        {"open": 10, "close": 11, "volume": 100},
        {"open": 11, "close": 12, "volume": 100},
        {"open": 12, "close": 15, "volume": 150},
    ])
    result = validate_latest_week("TEST", {"history": history}, CONFIG)
    assert result is None


def test_validate_latest_week_rejects_negative_weekly_change():
    history = build_history([
        {"open": 10, "close": 10, "volume": 100},
        {"open": 10, "close": 11, "volume": 100},
        {"open": 11, "close": 12, "volume": 100},
        {"open": 20, "close": 15, "volume": 300},
    ])
    result = validate_latest_week("TEST", {"history": history}, CONFIG)
    assert result is None


def test_validate_latest_week_rejects_price_below_ma():
    history = build_history([
        {"open": 10, "close": 10, "volume": 100},
        {"open": 10, "close": 20, "volume": 100},
        {"open": 20, "close": 10, "volume": 100},
        {"open": 11, "close": 12, "volume": 300},
    ])
    result = validate_latest_week("TEST", {"history": history}, CONFIG)
    assert result is None


def test_validate_latest_week_accepts_valid_data():
    history = build_history([
        {"open": 10, "close": 10, "volume": 100},
        {"open": 10, "close": 11, "volume": 100},
        {"open": 11, "close": 12, "volume": 100},
        {"open": 12, "close": 15, "volume": 300},
    ])
    result = validate_latest_week("TEST", {"history": history}, CONFIG)
    assert result is not None
    last_monday = datetime.now().date() - timedelta(days=datetime.now().weekday())
    assert result["date"] == last_monday.strftime("%Y-%m-%d")
