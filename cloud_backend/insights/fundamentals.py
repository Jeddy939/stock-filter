"""SEC filing ingestion and point-in-time fundamental features.

Only filing facts published on or before the appraisal timestamp are eligible.
Current profile values and fiscal period end dates are never treated as the
information-availability timestamp.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
import os
import time
from typing import Any, Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .features import FeatureDefinition, FeatureValue


SEC_BASE = "https://data.sec.gov"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

CONCEPTS: dict[str, tuple[str, ...]] = {
    "revenue": ("RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet"),
    "net_income": ("NetIncomeLoss", "ProfitLoss"),
    "operating_income": ("OperatingIncomeLoss",),
    "assets": ("Assets",),
    "liabilities": ("Liabilities",),
    "equity": ("StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
    "shares": ("EntityCommonStockSharesOutstanding", "CommonStockSharesOutstanding"),
    "eps_diluted": ("EarningsPerShareDiluted",),
    "operating_cash_flow": ("NetCashProvidedByUsedInOperatingActivities",),
    "capital_expenditure": ("PaymentsToAcquirePropertyPlantAndEquipment",),
}

FUNDAMENTAL_DEFINITIONS: tuple[FeatureDefinition, ...] = (
    FeatureDefinition("earnings_negative", "fundamental", "boolean", None, "Latest filed annual diluted earnings were non-positive", "latest annual diluted EPS <= 0", 0, "sec_companyfacts"),
    FeatureDefinition("pe_trailing_filed", "valuation", "numeric", "multiple", "Price divided by the latest annual diluted EPS filed before appraisal", "appraisal close / latest eligible annual diluted EPS", 0, "sec_companyfacts"),
    FeatureDefinition("earnings_yield_pct", "valuation", "numeric", "percent", "Inverse filed trailing P/E for positive earnings", "annual diluted EPS / appraisal close * 100", 0, "sec_companyfacts"),
    FeatureDefinition("price_to_sales_filed", "valuation", "numeric", "multiple", "Point-in-time market capitalization divided by latest filed annual revenue", "price * filed shares / filed annual revenue", 0, "sec_companyfacts"),
    FeatureDefinition("price_to_book_filed", "valuation", "numeric", "multiple", "Point-in-time market capitalization divided by latest filed equity", "price * filed shares / filed equity", 0, "sec_companyfacts"),
    FeatureDefinition("revenue_growth_yoy_pct", "growth", "numeric", "percent", "Growth between the latest two eligible annual revenue filings", "(latest annual revenue / previous annual revenue - 1) * 100", 0, "sec_companyfacts"),
    FeatureDefinition("net_income_growth_yoy_pct", "growth", "numeric", "percent", "Growth between the latest two eligible annual net-income filings", "(latest annual net income / previous annual net income - 1) * 100", 0, "sec_companyfacts"),
    FeatureDefinition("operating_margin_pct", "quality", "numeric", "percent", "Latest filed annual operating income divided by revenue", "annual operating income / annual revenue * 100", 0, "sec_companyfacts"),
    FeatureDefinition("net_margin_pct", "quality", "numeric", "percent", "Latest filed annual net income divided by revenue", "annual net income / annual revenue * 100", 0, "sec_companyfacts"),
    FeatureDefinition("return_on_equity_pct", "quality", "numeric", "percent", "Latest filed annual net income divided by filed equity", "annual net income / equity * 100", 0, "sec_companyfacts"),
    FeatureDefinition("liabilities_to_equity", "balance_sheet", "numeric", "ratio", "Latest filed liabilities divided by equity", "liabilities / equity", 0, "sec_companyfacts"),
    FeatureDefinition("free_cash_flow_yield_pct", "cash_flow", "numeric", "percent", "Filed operating cash flow less capital expenditure divided by point-in-time market capitalization", "(operating cash flow - capex) / (price * shares) * 100", 0, "sec_companyfacts"),
    FeatureDefinition("shares_growth_yoy_pct", "dilution", "numeric", "percent", "Change between the latest two eligible filed share counts", "(latest shares / previous shares - 1) * 100", 0, "sec_companyfacts"),
)


def _request_json(url: str, user_agent: str) -> Any:
    request = Request(url, headers={"User-Agent": user_agent, "Accept": "application/json"})
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError) as exc:
        raise RuntimeError(f"SEC request failed for {url}: {exc}") from exc


def sec_ticker_map(user_agent: str) -> dict[str, int]:
    payload = _request_json(SEC_TICKERS_URL, user_agent)
    return {
        str(row.get("ticker") or "").upper(): int(row["cik_str"])
        for row in payload.values()
        if row.get("ticker") and row.get("cik_str") is not None
    }


def _source_key(accession: str, concept: str, unit: str, fact: dict[str, Any]) -> str:
    identity = {
        "accession": accession,
        "concept": concept,
        "unit": unit,
        "start": fact.get("start"),
        "end": fact.get("end"),
        "filed": fact.get("filed"),
        "form": fact.get("form"),
        "fy": fact.get("fy"),
        "fp": fact.get("fp"),
        "frame": fact.get("frame"),
    }
    return hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def ingest_companyfacts(
    connection: psycopg.Connection[Any],
    ticker: str,
    cik: int,
    user_agent: str,
) -> int:
    payload = _request_json(f"{SEC_BASE}/api/xbrl/companyfacts/CIK{cik:010d}.json", user_agent)
    inserted = 0
    with connection.cursor() as cursor:
        for concept, concept_payload in (payload.get("facts", {}).get("us-gaap", {}) or {}).items():
            for unit, facts in (concept_payload.get("units", {}) or {}).items():
                for fact in facts:
                    if fact.get("val") is None or not fact.get("end") or not fact.get("filed") or not fact.get("accn"):
                        continue
                    try:
                        numeric_value = float(fact["val"])
                        period_end = date.fromisoformat(str(fact["end"])[:10])
                        period_start = date.fromisoformat(str(fact["start"])[:10]) if fact.get("start") else None
                        filed_at = datetime.fromisoformat(str(fact["filed"])[:10]).replace(tzinfo=timezone.utc)
                    except (TypeError, ValueError):
                        continue
                    cursor.execute(
                        """
                        INSERT INTO fundamental_facts (
                            market, ticker, source_name, accession_id, fact_name, unit,
                            period_start, period_end, filed_at_utc, fiscal_year,
                            fiscal_period, form_type, numeric_value, source_fact_key, raw_json
                        ) VALUES ('us', %s, 'sec_companyfacts', %s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (market, ticker, source_name, source_fact_key) DO NOTHING
                        """,
                        (
                            ticker, fact["accn"], concept, unit, period_start, period_end,
                            filed_at, fact.get("fy"), fact.get("fp"), fact.get("form"),
                            numeric_value, _source_key(fact["accn"], concept, unit, fact), Jsonb(fact),
                        ),
                    )
                    inserted += cursor.rowcount
    connection.commit()
    return inserted


def refresh_sec_fundamentals(
    connection: psycopg.Connection[Any],
    tickers: Iterable[str],
    *,
    user_agent: str | None = None,
    progress: Callable[[int, int, str], None] | None = None,
) -> dict[str, Any]:
    agent = (user_agent or os.environ.get("MONEYMAKER_SEC_USER_AGENT") or "").strip()
    if not agent or "@" not in agent:
        raise RuntimeError("MONEYMAKER_SEC_USER_AGENT must identify the application and include a contact email")
    normalized = sorted({str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()})
    mapping = sec_ticker_map(agent)
    inserted = missing = failed = 0
    errors: list[dict[str, str]] = []
    for index, ticker in enumerate(normalized, 1):
        if progress:
            progress(index - 1, len(normalized), ticker)
        cik = mapping.get(ticker)
        if cik is None:
            missing += 1
            continue
        try:
            inserted += ingest_companyfacts(connection, ticker, cik, agent)
        except Exception as exc:
            connection.rollback()
            failed += 1
            errors.append({"ticker": ticker, "error": str(exc)[:1000]})
        time.sleep(0.12)
    return {"tickers": len(normalized), "facts_inserted": inserted, "missing_cik": missing, "failed": failed, "errors": errors[:100]}


def _missing(reason: str, source: str | None = None) -> FeatureValue:
    return FeatureValue(None, reason, source)


def _value(value: float | bool, filed_at: datetime | None) -> FeatureValue:
    return FeatureValue(value, None, filed_at.isoformat() if filed_at else None)


def point_in_time_fundamentals(
    cursor: psycopg.Cursor[Any],
    *,
    market: str,
    ticker: str,
    appraisal_at_utc: datetime,
    appraisal_close: float,
) -> tuple[dict[str, FeatureValue], dict[str, Any]]:
    if market != "us":
        reason = "historical ASX filing fundamentals are not configured"
        return ({definition.name: _missing(reason) for definition in FUNDAMENTAL_DEFINITIONS}, {"source": None, "coverage": 0, "reason": reason})

    aliases = [concept for choices in CONCEPTS.values() for concept in choices]
    cursor.execute(
        """
        SELECT fact_name, numeric_value, period_start, period_end, filed_at_utc,
               fiscal_year, fiscal_period, form_type, unit
        FROM fundamental_facts
        WHERE market = 'us' AND ticker = %s AND source_name = 'sec_companyfacts'
          AND fact_name = ANY(%s::text[]) AND filed_at_utc <= %s
          AND form_type IN ('10-K', '10-K/A')
        ORDER BY filed_at_utc DESC, period_end DESC
        """,
        (ticker, aliases, appraisal_at_utc),
    )
    rows = list(cursor.fetchall())
    by_concept: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_concept.setdefault(str(row["fact_name"]), []).append(row)

    def facts(name: str) -> list[dict[str, Any]]:
        combined: list[dict[str, Any]] = []
        for alias in CONCEPTS[name]:
            combined.extend(by_concept.get(alias, []))
        unique: dict[tuple[Any, Any], dict[str, Any]] = {}
        for row in sorted(combined, key=lambda item: (item["period_end"], item["filed_at_utc"]), reverse=True):
            unique.setdefault((row["period_start"], row["period_end"]), row)
        return list(unique.values())

    def latest(name: str) -> dict[str, Any] | None:
        available = facts(name)
        return available[0] if available else None

    def growth(name: str) -> FeatureValue:
        available = facts(name)
        if len(available) < 2:
            return _missing(f"requires two eligible annual {name} filings")
        newest, previous = available[0], available[1]
        old = float(previous["numeric_value"])
        if old == 0:
            return _missing(f"previous annual {name} is zero", newest["filed_at_utc"].isoformat())
        return _value((float(newest["numeric_value"]) / old - 1) * 100, newest["filed_at_utc"])

    latest_rows = {name: latest(name) for name in CONCEPTS}
    source_dates = [row["filed_at_utc"] for row in latest_rows.values() if row]
    source = max(source_dates) if source_dates else None
    values = {definition.name: _missing("eligible SEC filing fact unavailable") for definition in FUNDAMENTAL_DEFINITIONS}
    eps = latest_rows["eps_diluted"]
    if eps:
        eps_value = float(eps["numeric_value"])
        values["earnings_negative"] = _value(eps_value <= 0, eps["filed_at_utc"])
        if eps_value > 0 and appraisal_close > 0:
            values["pe_trailing_filed"] = _value(appraisal_close / eps_value, eps["filed_at_utc"])
            values["earnings_yield_pct"] = _value(eps_value / appraisal_close * 100, eps["filed_at_utc"])

    shares = latest_rows["shares"]
    market_cap = appraisal_close * float(shares["numeric_value"]) if shares and appraisal_close > 0 else None
    revenue, net_income, operating_income = latest_rows["revenue"], latest_rows["net_income"], latest_rows["operating_income"]
    equity, liabilities = latest_rows["equity"], latest_rows["liabilities"]
    if market_cap and revenue and float(revenue["numeric_value"]) > 0:
        values["price_to_sales_filed"] = _value(market_cap / float(revenue["numeric_value"]), max(shares["filed_at_utc"], revenue["filed_at_utc"]))
    if market_cap and equity and float(equity["numeric_value"]) > 0:
        values["price_to_book_filed"] = _value(market_cap / float(equity["numeric_value"]), max(shares["filed_at_utc"], equity["filed_at_utc"]))
    values["revenue_growth_yoy_pct"] = growth("revenue")
    values["net_income_growth_yoy_pct"] = growth("net_income")
    if revenue and float(revenue["numeric_value"]) != 0:
        if operating_income:
            values["operating_margin_pct"] = _value(float(operating_income["numeric_value"]) / float(revenue["numeric_value"]) * 100, max(operating_income["filed_at_utc"], revenue["filed_at_utc"]))
        if net_income:
            values["net_margin_pct"] = _value(float(net_income["numeric_value"]) / float(revenue["numeric_value"]) * 100, max(net_income["filed_at_utc"], revenue["filed_at_utc"]))
    if net_income and equity and float(equity["numeric_value"]) != 0:
        values["return_on_equity_pct"] = _value(float(net_income["numeric_value"]) / float(equity["numeric_value"]) * 100, max(net_income["filed_at_utc"], equity["filed_at_utc"]))
    if liabilities and equity and float(equity["numeric_value"]) != 0:
        values["liabilities_to_equity"] = _value(float(liabilities["numeric_value"]) / float(equity["numeric_value"]), max(liabilities["filed_at_utc"], equity["filed_at_utc"]))
    cash_flow, capex = latest_rows["operating_cash_flow"], latest_rows["capital_expenditure"]
    if market_cap and cash_flow and capex:
        values["free_cash_flow_yield_pct"] = _value((float(cash_flow["numeric_value"]) - abs(float(capex["numeric_value"]))) / market_cap * 100, max(cash_flow["filed_at_utc"], capex["filed_at_utc"], shares["filed_at_utc"]))
    values["shares_growth_yoy_pct"] = growth("shares")
    available_count = sum(not value.is_missing for value in values.values())
    return values, {
        "source": "sec_companyfacts",
        "source_as_of_utc": source.isoformat() if source else None,
        "coverage": available_count,
        "feature_count": len(values),
        "eligible_fact_rows": len(rows),
    }
