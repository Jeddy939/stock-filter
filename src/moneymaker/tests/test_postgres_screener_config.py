from cloud_backend.postgres_screener import normalize_screen_config, screen_config_hash


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
