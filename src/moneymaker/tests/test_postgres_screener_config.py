from datetime import date

from cloud_backend.postgres_screener import (
    _result_from_metric,
    normalize_screen_config,
    screen_config_hash,
)


def test_screen_config_hash_accepts_legacy_and_normalized_ma_payloads():
    legacy_payload = {
        "market": "US",
        "provider": "YFinance",
        "limit": "0",
        "query": "  ibrx ",
        "volume_multiplier": "2",
        "avg_volume_weeks": "52",
        "price_avg_weeks": "1",
        "lookback_weeks": "1",
        "ma_short": "90",
        "ma_intermediate": "180",
        "ma_medium": "360",
        "ma_long": "700",
        "scheduled": True,
    }
    normalized_payload = {
        "market": "us",
        "provider": "yfinance",
        "limit": 0,
        "query": "IBRX",
        "volume_multiplier": 2,
        "avg_volume_weeks": 52,
        "price_avg_weeks": 1,
        "lookback_weeks": 1,
        "ma_periods": {"short": 90, "intermediate": 180, "medium": 360, "long": 700},
        "min_market_cap": 0,
        "max_market_cap": 0,
    }

    assert normalize_screen_config(legacy_payload) == normalize_screen_config(normalized_payload)
    assert screen_config_hash(legacy_payload) == screen_config_hash(normalized_payload)


def test_screen_config_hash_ignores_scheduled_flag():
    payload = {"market": "asx", "scheduled": True}
    comparable = {"market": "asx"}

    assert screen_config_hash(payload) == screen_config_hash(comparable)


def _metric_row(**overrides):
    row = {
        "ticker": "ARI",
        "week_date": date(2026, 7, 13),
        "close_price": 10.31,
        "previous_close_price": 10.29,
        "weekly_volume": 17_492_300,
        "avg_volume_52": 6_467_548,
        "volume_ratio_52": 2.70,
        "price_avg_1": 10.29,
        "ma_90": 9.15,
        "ma_180": 8.62,
        "ma_360": 7.92,
        "ma_700": 6.83,
        "available_weeks": 788,
        "history_weeks": 790,
        "market_latest_date": date(2026, 7, 16),
        "latest_daily_date": date(2026, 7, 16),
        "latest_daily_close": 10.50,
        "market_cap": 919_249_664,
        "sector": "Real Estate",
        "industry": "REIT - Mortgage",
    }
    row.update(overrides)
    return row


def _default_config():
    return {
        "volume_multiplier": 2,
        "min_market_cap": 0,
        "max_market_cap": 0,
        "ma_periods": {"short": 90, "intermediate": 180, "medium": 360, "long": 700},
    }


def test_daily_confirmation_keeps_a_current_signal_that_still_meets_the_rules():
    result = _result_from_metric(_metric_row(), _default_config(), require_daily_confirmation=True)

    assert result is not None
    assert result["confirmation_date"] == "2026-07-16"
    assert result["confirmation_close_price"] == 10.50


def test_daily_confirmation_rejects_ari_style_post_signal_price_gap():
    result = _result_from_metric(
        _metric_row(latest_daily_close=7.02),
        _default_config(),
        require_daily_confirmation=True,
    )

    assert result is None


def test_daily_confirmation_rejects_a_ticker_missing_the_latest_market_session():
    result = _result_from_metric(
        _metric_row(latest_daily_date=date(2026, 7, 15)),
        _default_config(),
        require_daily_confirmation=True,
    )

    assert result is None
