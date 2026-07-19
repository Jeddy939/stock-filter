import json
from datetime import datetime

import pandas as pd
import pytest

from moneymaker import fetcher
from moneymaker.fetcher import (
    apply_ticker_limit,
    get_tickers_from_file,
    normalize_provider,
)


def test_all_ords_file_gets_asx_suffix_and_deduplicates(tmp_path):
    ticker_file = tmp_path / "all_ords_tickers_Current_2026-05-03.txt"
    ticker_file.write_text(
        "Symbol\n"
        "BHP\n"
        "ASX:CBA\n"
        "CBA [1]\n"
        "# comment\n",
        encoding="utf-8",
    )

    assert get_tickers_from_file(str(ticker_file)) == ["BHP.AX", "CBA.AX"]


def test_nasdaq_pipe_file_does_not_get_asx_suffix(tmp_path):
    ticker_file = tmp_path / "your_nasdaq_screener_list.txt"
    ticker_file.write_text(
        "Symbol|Security Name\n"
        "AAPL|Apple Inc.\n"
        "MSFT|Microsoft Corporation\n",
        encoding="utf-8",
    )

    assert get_tickers_from_file(str(ticker_file)) == ["AAPL", "MSFT"]


def test_market_index_symbols_keep_their_yahoo_caret(tmp_path):
    ticker_file = tmp_path / "asx_benchmark_tickers.txt"
    ticker_file.write_text("^AORD\n^GSPC\n", encoding="utf-8")

    assert get_tickers_from_file(str(ticker_file)) == ["^AORD", "^GSPC"]


def test_apply_ticker_limit_preserves_order():
    assert apply_ticker_limit(["AAPL", "MSFT", "NVDA"], 2) == ["AAPL", "MSFT"]
    assert apply_ticker_limit(["AAPL", "MSFT"], None) == ["AAPL", "MSFT"]


def test_apply_ticker_limit_rejects_non_positive_values():
    with pytest.raises(ValueError, match="positive integer"):
        apply_ticker_limit(["AAPL"], 0)


def test_normalize_provider_accepts_supported_values():
    assert normalize_provider("YFINANCE") == "yfinance"
    assert normalize_provider(" stooq ") == "stooq"


def test_normalize_provider_rejects_unknown_values():
    with pytest.raises(ValueError, match="Unsupported provider"):
        normalize_provider("example")


def test_stooq_symbol_maps_common_yahoo_symbols():
    assert fetcher._stooq_symbol("AAPL") == "aapl.us"
    assert fetcher._stooq_symbol("BHP.AX") == "bhp.au"
    assert fetcher._stooq_symbol("BRK/B") == "brk-b.us"
    assert fetcher._stooq_symbol("vod.uk") == "vod.uk"


def test_parse_nasdaq_trader_symbols_excludes_test_issues():
    content = (
        "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares\n"
        "AAPL|Apple Inc.|Q|N|N|100|N|N\n"
        "ZZZZ|Example Test Issue|Q|Y|N|100|N|N\n"
        "BRK/B|Berkshire Hathaway Inc.|Q|N|N|100|N|N\n"
        "File Creation Time: 0513202612:00\n"
    )

    assert fetcher._parse_nasdaq_trader_symbols(content, "Symbol") == ["AAPL", "BRK-B"]


def test_write_us_ticker_file_uses_nasdaq_trader_sources(tmp_path, monkeypatch):
    responses = {
        fetcher.NASDAQ_LISTED_URL: (
            "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares\n"
            "AAPL|Apple Inc.|Q|N|N|100|N|N\n"
        ),
        fetcher.NASDAQ_OTHER_LISTED_URL: (
            "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol\n"
            "IBM|International Business Machines|N|IBM|N|100|N|IBM\n"
        ),
    }
    monkeypatch.setattr(fetcher, "_read_url_text", lambda url, timeout=30: responses[url])
    output = tmp_path / "us_tickers.txt"

    result = fetcher.write_us_ticker_file(str(output))

    assert result["ticker_count"] == 2
    assert output.read_text(encoding="utf-8").splitlines()[-2:] == ["AAPL", "IBM"]


def test_fetch_stock_data_uses_provider_limit_and_metadata(tmp_path, monkeypatch):
    ticker_file = tmp_path / "tickers.txt"
    ticker_file.write_text("AAA\nBBB\nCCC\n", encoding="utf-8")
    output_file = tmp_path / "stock_data.json"
    provider_calls = []

    def fake_download(tickers, start, end, workers, provider, *args, **kwargs):
        provider_calls.append((list(tickers), workers, provider))
        index = pd.to_datetime(["2026-01-02", "2026-01-03"])
        frame = pd.DataFrame(
            {
                "Open": [1.0, 2.0],
                "High": [2.0, 3.0],
                "Low": [0.5, 1.5],
                "Close": [1.5, 2.5],
                "Volume": [100, 200],
            },
            index=index,
        )
        return {"AAA": frame}, {"BBB"}, False

    def fake_info(tickers, workers, *args, **kwargs):
        return {"AAA": {"regularMarketPrice": 1.5, "marketCap": 1000}}

    monkeypatch.setattr(fetcher, "download_historical_data", fake_download)
    monkeypatch.setattr(fetcher, "fetch_info_individual", fake_info)

    success = fetcher.fetch_stock_data(
        str(ticker_file),
        str(output_file),
        years=1,
        workers=3,
        provider="stooq",
        limit=2,
        cache_file=None,
    )

    assert success is True
    assert provider_calls == [(["AAA", "BBB"], 3, "stooq")]
    payload = json.loads(output_file.read_text(encoding="utf-8"))
    assert list(payload["stocks"].keys()) == ["AAA"]
    metadata = payload["metadata"]
    assert metadata["success"] is True
    assert metadata["error"] is None
    assert metadata["provider"] == "stooq"
    assert metadata["requested_tickers"] == ["AAA", "BBB"]
    assert metadata["requested_ticker_count"] == 2
    assert metadata["source_ticker_count"] == 3
    assert metadata["limit"] == 2
    assert metadata["successful_histories"] == ["AAA"]
    assert metadata["missing_histories"] == ["BBB"]
    assert metadata["missing_info_count"] == 1
    assert metadata["missing_market_cap_count"] == 0
    assert any("historical OHLCV only" in item for item in metadata["provider_limitations"])


def test_fetch_stock_data_returns_false_when_every_history_is_missing(tmp_path, monkeypatch):
    ticker_file = tmp_path / "tickers.txt"
    ticker_file.write_text("AAA\nBBB\n", encoding="utf-8")
    output_file = tmp_path / "stock_data.json"

    monkeypatch.setattr(fetcher, "download_historical_data", lambda *args: ({}, {"AAA", "BBB"}, False))
    monkeypatch.setattr(fetcher, "fetch_info_individual", lambda *args: {})

    success = fetcher.fetch_stock_data(
        str(ticker_file),
        str(output_file),
        years=1,
        workers=1,
        provider="stooq",
        limit=None,
        cache_file=None,
    )

    payload = json.loads(output_file.read_text(encoding="utf-8"))
    assert success is False
    assert payload["metadata"]["success"] is False
    assert "No historical data" in payload["metadata"]["error"]
    assert payload["metadata"]["missing_history_count"] == 2


def test_fetch_stock_data_cache_mode_counts_cached_tickers_without_loading_frames(tmp_path, monkeypatch):
    ticker_file = tmp_path / "tickers.txt"
    ticker_file.write_text("AAA\nBBB\n", encoding="utf-8")
    cache_file = tmp_path / "stock_cache.sqlite"
    output_file = tmp_path / "stock_data.json"
    index = pd.to_datetime(["2026-01-02", "2026-01-03"])
    frame = pd.DataFrame(
        {
            "Open": [1.0, 2.0],
            "High": [2.0, 3.0],
            "Low": [0.5, 1.5],
            "Close": [1.5, 2.5],
            "Volume": [100, 200],
        },
        index=index,
    )

    def fake_download(*args, **kwargs):
        return {"AAA": frame}, {"BBB"}, False

    def fail_if_full_history_load(*args, **kwargs):
        raise AssertionError("web cache update should not reload full history frames")

    monkeypatch.setattr(fetcher, "download_historical_data", fake_download)
    monkeypatch.setattr(fetcher, "fetch_info_individual", lambda *args, **kwargs: {})
    monkeypatch.setattr(fetcher, "_load_cached_histories", fail_if_full_history_load)

    success = fetcher.fetch_stock_data(
        str(ticker_file),
        str(output_file),
        years=1,
        workers=1,
        cache_file=str(cache_file),
        export_json=False,
    )

    assert success is True
    assert not output_file.exists()


def test_yfinance_history_download_sets_auto_adjust_false(monkeypatch):
    calls = []
    index = pd.to_datetime(["2026-01-02"])
    frame = pd.DataFrame(
        {
            "Open": [1.0],
            "High": [2.0],
            "Low": [0.5],
            "Close": [1.5],
            "Volume": [100],
        },
        index=index,
    )

    def fake_download(*args, **kwargs):
        calls.append(kwargs)
        return frame

    monkeypatch.setattr(fetcher.yf, "download", fake_download)

    histories, missing, stopped = fetcher._download_historical_data(
        ["AAA"],
        datetime(2026, 1, 1),
        datetime(2026, 1, 31),
        workers=1,
    )

    assert list(histories) == ["AAA"]
    assert missing == set()
    assert stopped is False
    assert calls[0]["auto_adjust"] is False


def test_extract_histories_accepts_price_first_yfinance_multiindex():
    index = pd.to_datetime(["2026-01-02"])
    columns = pd.MultiIndex.from_product([["Close", "Volume"], ["AAA"]], names=["Price", "Ticker"])
    data = pd.DataFrame([[1.5, 100]], index=index, columns=columns)

    histories = fetcher._extract_histories_from_frame(["AAA"], data)

    assert list(histories) == ["AAA"]
    assert histories["AAA"].loc[index[0], "Close"] == 1.5
    assert histories["AAA"].loc[index[0], "Volume"] == 100


def test_extract_histories_accepts_ticker_first_yfinance_multiindex():
    index = pd.to_datetime(["2026-01-02"])
    columns = pd.MultiIndex.from_product([["AAA"], ["Close", "Volume"]], names=["Ticker", "Price"])
    data = pd.DataFrame([[1.5, 100]], index=index, columns=columns)

    histories = fetcher._extract_histories_from_frame(["AAA"], data)

    assert list(histories) == ["AAA"]
    assert histories["AAA"].loc[index[0], "Close"] == 1.5


def test_history_download_end_is_exclusive_and_stable():
    assert fetcher._history_download_end("2026-07-17") == datetime(2026, 7, 17)
    assert fetcher._history_download_end(now=datetime(2026, 7, 17, 23, 59)) == datetime(2026, 7, 18)


def test_read_stooq_history_rejects_api_key_instruction_response(monkeypatch):
    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def read(self):
            return b"Get your apikey:\nhttps://stooq.com/db/h/"

    monkeypatch.setattr(fetcher, "urlopen", lambda *args, **kwargs: FakeResponse())

    with pytest.raises(ValueError, match="API key"):
        fetcher._read_stooq_history("CBA.AX", datetime(2025, 1, 1), datetime(2025, 1, 31))


def test_sqlite_cache_round_trips_history(tmp_path):
    cache_file = tmp_path / "stock_cache.sqlite"
    frame = pd.DataFrame(
        {
            "Open": [1.0],
            "High": [2.0],
            "Low": [0.5],
            "Close": [1.5],
            "Volume": [100],
        },
        index=pd.to_datetime(["2026-01-02"]),
    )

    with fetcher._cache_connect(str(cache_file)) as conn:
        assert fetcher._store_history_cache(conn, "yfinance", {"AAA": frame}) == 1
        cached = fetcher._load_cached_histories(
            conn,
            "yfinance",
            ["AAA"],
            datetime(2026, 1, 1),
            datetime(2026, 1, 31),
        )

    assert list(cached) == ["AAA"]
    assert cached["AAA"].iloc[0]["Close"] == 1.5


def test_history_fetch_groups_skip_recent_complete_cache(tmp_path):
    cache_file = tmp_path / "stock_cache.sqlite"
    frame = pd.DataFrame(
        {
            "Open": [1.0, 1.2],
            "High": [2.0, 2.2],
            "Low": [0.5, 0.7],
            "Close": [1.5, 1.7],
            "Volume": [100, 120],
        },
        index=pd.to_datetime(["2026-01-01", "2026-01-20"]),
    )

    with fetcher._cache_connect(str(cache_file)) as conn:
        fetcher._store_history_cache(conn, "yfinance", {"AAA": frame})
        groups = fetcher._history_fetch_groups(
            conn,
            "yfinance",
            ["AAA", "BBB"],
            datetime(2026, 1, 1),
            datetime(2026, 1, 23),
            refresh_days=5,
        )

    assert groups == {"2026-01-01": ["BBB"]}
