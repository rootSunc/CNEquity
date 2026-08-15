"""Multi-report fundamentals: item breadth and announce-date resolution."""

from datetime import date

from cn_market_lake.adapters.eastmoney.fundamentals import (
    _ANNOUNCE_SOURCE,
    _REPORTS,
    _announce_date_map,
    _parse_rows,
    fetch_financial_statement_items,
)

_BALANCE = next(r for r in _REPORTS if r.name == "RPT_DMSK_FN_BALANCE")


class FakeDatacenterClient:
    """Serves canned rows keyed by the report name appearing in the URL."""

    def __init__(self, batches: dict[str, list[dict]]):
        self.batches = batches
        self.urls: list[str] = []

    def get(self, url, **kwargs):
        self.urls.append(url)

        class Resp:
            def __init__(self, data):
                self._data = data

            def raise_for_status(self):
                return None

            def json(self):
                return {"success": True, "result": {"data": self._data}}

        for key, rows in self.batches.items():
            if key in url:
                return Resp(rows)
        return Resp([])

    def close(self):
        return None


def _lico_row(**over):
    return {
        "SECURITY_CODE": "000002",
        "SECUCODE": "000002.SZ",
        "REPORTDATE": "2016-12-31",
        "NOTICE_DATE": "2017-03-27",
        "TOTAL_OPERATE_INCOME": 240477236923.34,
        "BPS": 14.6,
        **over,
    }


def _balance_row(**over):
    return {
        "SECURITY_CODE": "000002",
        "SECUCODE": "000002.SZ",
        "REPORT_DATE": "2016-12-31",
        # EastMoney serves the *republication* date on the statement reports.
        "NOTICE_DATE": "2018-03-27",
        "TOTAL_ASSETS": 830674213924.14,
        "TOTAL_EQUITY": 161676571281.0,
        **over,
    }


# --- report specs -----------------------------------------------------------


def test_each_report_requests_its_own_report_date_field():
    """LICO says REPORTDATE, DMSK says REPORT_DATE; mixing them fetches garbage."""
    assert "REPORTDATE," in _ANNOUNCE_SOURCE.columns
    assert "REPORT_DATE," in _BALANCE.columns
    for report in _REPORTS:
        assert "SECUCODE" in report.columns
        assert "NOTICE_DATE" in report.columns


def test_item_set_covers_the_factor_building_blocks():
    items = {code for report in _REPORTS for _stmt, code in report.items}
    # Book equity and assets (B/P, asset growth), gross profit inputs
    # (profitability), and operating cash flow (accruals).
    assert {"total_equity", "total_assets", "bps"} <= items
    assert {"revenue", "operating_cost", "net_profit"} <= items
    assert {"net_cash_operate", "capex"} <= items


def test_exactly_one_report_owns_the_announcement_date():
    owners = [r for r in _REPORTS if r.authoritative_announce_date]
    assert owners == [_ANNOUNCE_SOURCE]


# --- announce-date resolution ----------------------------------------------


def test_announce_date_map_keys_on_symbol_and_period():
    assert _announce_date_map([_lico_row()]) == {("000002.SZ", "2016Q4"): date(2017, 3, 27)}


def test_lico_date_overrides_the_late_republication_date():
    """The whole point: FY2016 must be dated 2017-03, not 2018-03."""
    amap = _announce_date_map([_lico_row()])

    rows, fallbacks = _parse_rows(
        [_balance_row()], _BALANCE, default_notice="2026-07-21", announce_dates=amap
    )

    assert fallbacks == 0
    assert {r["announce_date"] for r in rows} == {date(2017, 3, 27)}


def test_falls_back_to_report_notice_date_when_lico_has_no_row():
    """Late is acceptable; early would be look-ahead."""
    rows, fallbacks = _parse_rows(
        [_balance_row()], _BALANCE, default_notice="2026-07-21", announce_dates={}
    )

    assert fallbacks == 1
    assert {r["announce_date"] for r in rows} == {date(2018, 3, 27)}


def test_null_items_are_skipped_not_zero_filled():
    rows, _ = _parse_rows([_balance_row(TOTAL_EQUITY=None)], _BALANCE, default_notice="2026-07-21")

    codes = {r["item_code"] for r in rows}
    assert "total_assets" in codes
    assert "total_equity" not in codes


# --- daily path -------------------------------------------------------------


def test_daily_fetch_queries_every_report():
    client = FakeDatacenterClient(
        {
            "RPT_LICO_FN_CPD": [_lico_row(REPORTDATE="2024-03-31", NOTICE_DATE="2024-04-28")],
            "RPT_DMSK_FN_BALANCE": [
                _balance_row(REPORT_DATE="2024-03-31", NOTICE_DATE="2024-04-28")
            ],
        }
    )

    df = fetch_financial_statement_items(date(2024, 4, 28), client=client)  # type: ignore[arg-type]

    assert {r.name for r in _REPORTS} <= {name for name in _REPORTS_in(client.urls)}
    assert set(df["statement_type"].to_list()) == {"income", "indicator", "balance"}
    assert {"total_assets", "total_equity", "bps", "revenue"} <= set(df["item_code"].to_list())
    assert set(df["announce_date"].to_list()) == {date(2024, 4, 28)}


def _REPORTS_in(urls: list[str]) -> set[str]:
    return {report.name for report in _REPORTS for url in urls if report.name in url}
