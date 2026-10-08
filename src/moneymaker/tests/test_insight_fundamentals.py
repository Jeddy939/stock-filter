from datetime import date, datetime, timezone
import unittest

from cloud_backend.insights.fundamentals import _source_key, point_in_time_fundamentals


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.params = None

    def execute(self, _sql, params):
        self.params = params

    def fetchall(self):
        return self.rows


def fact(name, value, filed="2024-03-01", end="2023-12-31", start="2023-01-01"):
    return {
        "fact_name": name,
        "numeric_value": value,
        "period_start": date.fromisoformat(start) if start else None,
        "period_end": date.fromisoformat(end),
        "filed_at_utc": datetime.fromisoformat(filed).replace(tzinfo=timezone.utc),
        "fiscal_year": int(end[:4]),
        "fiscal_period": "FY",
        "form_type": "10-K",
        "unit": "USD",
    }


class PointInTimeFundamentalTests(unittest.TestCase):
    def test_asx_values_are_explicitly_missing(self):
        values, quality = point_in_time_fundamentals(
            FakeCursor([]), market="asx", ticker="CBA.AX",
            appraisal_at_utc=datetime(2025, 1, 1, tzinfo=timezone.utc), appraisal_close=100,
        )
        self.assertTrue(values)
        self.assertTrue(all(value.is_missing for value in values.values()))
        self.assertIn("ASX", quality["reason"])

    def test_filing_cutoff_is_passed_to_database_query(self):
        cutoff = datetime(2024, 6, 1, tzinfo=timezone.utc)
        cursor = FakeCursor([])
        point_in_time_fundamentals(cursor, market="us", ticker="TEST", appraisal_at_utc=cutoff, appraisal_close=10)
        self.assertEqual(cursor.params[2], cutoff)

    def test_negative_earnings_never_become_zero_pe(self):
        cursor = FakeCursor([fact("EarningsPerShareDiluted", -2.0)])
        values, _quality = point_in_time_fundamentals(
            cursor, market="us", ticker="LOSS", appraisal_at_utc=datetime(2024, 6, 1, tzinfo=timezone.utc), appraisal_close=10,
        )
        self.assertTrue(values["earnings_negative"].value)
        self.assertTrue(values["pe_trailing_filed"].is_missing)
        self.assertTrue(values["earnings_yield_pct"].is_missing)

    def test_point_in_time_ratios_and_growth(self):
        rows = [
            fact("RevenueFromContractWithCustomerExcludingAssessedTax", 200),
            fact("RevenueFromContractWithCustomerExcludingAssessedTax", 100, filed="2023-03-01", end="2022-12-31", start="2022-01-01"),
            fact("NetIncomeLoss", 20),
            fact("OperatingIncomeLoss", 30),
            fact("StockholdersEquity", 100, start=None),
            fact("Liabilities", 50, start=None),
            fact("EntityCommonStockSharesOutstanding", 10, start=None),
            fact("EarningsPerShareDiluted", 2),
            fact("NetCashProvidedByUsedInOperatingActivities", 40),
            fact("PaymentsToAcquirePropertyPlantAndEquipment", 10),
        ]
        values, quality = point_in_time_fundamentals(
            FakeCursor(rows), market="us", ticker="TEST",
            appraisal_at_utc=datetime(2024, 6, 1, tzinfo=timezone.utc), appraisal_close=10,
        )
        self.assertEqual(values["pe_trailing_filed"].value, 5)
        self.assertEqual(values["price_to_sales_filed"].value, 0.5)
        self.assertEqual(values["revenue_growth_yoy_pct"].value, 100)
        self.assertEqual(values["operating_margin_pct"].value, 15)
        self.assertEqual(values["free_cash_flow_yield_pct"].value, 30)
        self.assertGreater(quality["coverage"], 5)

    def test_source_key_changes_when_filing_identity_changes(self):
        base = {"start": "2023-01-01", "end": "2023-12-31", "filed": "2024-03-01", "form": "10-K", "fy": 2023, "fp": "FY", "frame": None}
        first = _source_key("0001", "Revenue", "USD", base)
        second = _source_key("0001", "Revenue", "USD", {**base, "filed": "2024-03-02"})
        self.assertNotEqual(first, second)


if __name__ == "__main__":
    unittest.main()
