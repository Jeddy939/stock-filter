from datetime import date, timedelta

from firebase.snapshot_publisher import _above_180_for_two_years, _company_profile, _safe_ticker


def test_snapshot_ticker_path_is_stable_and_safe():
    assert _safe_ticker("cba.ax") == "CBA.AX"
    assert _safe_ticker("^aord") == "^AORD"
    assert _safe_ticker("bad/path") == "BAD_PATH"


def test_company_profile_uses_cached_yahoo_fields():
    profile = _company_profile(
        {
            "longName": "Example Limited",
            "longBusinessSummary": "Builds examples.",
            "sector": "Industrials",
        },
        "EXM.AX",
    )
    assert profile["name"] == "Example Limited"
    assert profile["summary"] == "Builds examples."
    assert profile["sector"] == "Industrials"
    assert profile["yahoo_url"].endswith("/EXM.AX")


def test_two_year_180_flag_matches_prior_week_moving_average_rule():
    start = date(2020, 1, 6)
    rows = [
        {"price_date": start + timedelta(weeks=index), "close_price": 100 + index}
        for index in range(300)
    ]
    assert _above_180_for_two_years(rows) is True

    rows[-20]["close_price"] = 1
    assert _above_180_for_two_years(rows) is False


def test_two_year_180_flag_requires_nearly_two_years_of_comparisons():
    start = date(2020, 1, 6)
    rows = [
        {"price_date": start + timedelta(weeks=index), "close_price": 100 + index}
        for index in range(220)
    ]
    assert _above_180_for_two_years(rows) is False
