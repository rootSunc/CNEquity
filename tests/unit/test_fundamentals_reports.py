"""Multi-report fundamentals: item breadth and announce-date resolution."""

from datetime import date

import pytest

from cnequity.adapters.eastmoney import fundamentals as fundamentals_module
from cnequity.adapters.eastmoney.fundamentals import (
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
    # Stored LICO summary items are the date repair's evidence. Only the
    # income statement may repeat revenue and net_profit: its NOTICE_DATE is a
    # republication, never earlier than the first disclosure, and the evidence
    # takes the minimum date, so a restated vintage cannot move a period early.
    for report in _REPORTS:
        if report is _ANNOUNCE_SOURCE:
            continue
        assert not any(statement == "indicator" for statement, _code in report.items)
        if report.name != "RPT_DMSK_FN_INCOME":
            assert not any(code in {"revenue", "net_profit"} for _s, code in report.items)


def test_backfill_keeps_the_first_announced_figure_on_a_shared_key(monkeypatch):
    # A backfill dates the income statement's rows from LICO, so a restated
    # TOTAL_OPERATE_INCOME lands on LICO's key and must not replace its figure.
    import polars as pl

    monkeypatch.setattr(fundamentals_module, "_report_period_dates", lambda *a, **k: ["2016-12-31"])
    income = {
        "SECURITY_CODE": "000002",
        "SECUCODE": "000002.SZ",
        "REPORT_DATE": "2016-12-31",
        "NOTICE_DATE": "2018-03-27",
        "TOTAL_OPERATE_INCOME": 250000000000.0,
    }
    monkeypatch.setattr(
        fundamentals_module,
        "_fetch_report",
        lambda client, report, expr, **kwargs: (
            [_lico_row()]
            if report is _ANNOUNCE_SOURCE
            else ([income] if report.name == "RPT_DMSK_FN_INCOME" else [])
        ),
    )
    frame = fetch_financial_statement_items(
        date(2026, 10, 9), backfill=True, client=FakeDatacenterClient({})
    )
    revenue = frame.filter(pl.col("item_code") == "revenue")
    assert revenue.select("announce_date", "item_value").rows() == [
        (date(2017, 3, 27), 240477236923.34)
    ]


# --- announce-date resolution ----------------------------------------------


def test_announce_date_map_keys_on_symbol_and_period():
    assert _announce_date_map([_lico_row()]) == {("000002.SZ", "2016Q4"): date(2017, 3, 27)}


def test_announce_date_map_uses_earliest_duplicate_notice_date():
    rows = [
        _lico_row(NOTICE_DATE="2017-03-28"),
        _lico_row(NOTICE_DATE="2017-03-27"),
    ]

    assert _announce_date_map(rows) == {("000002.SZ", "2016Q4"): date(2017, 3, 27)}


def test_announce_date_map_ignores_source_sentinel_date():
    assert _announce_date_map([_lico_row(NOTICE_DATE="1900-01-01")]) == {}


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


def test_nonfinite_items_are_skipped():
    rows, _ = _parse_rows(
        [_balance_row(TOTAL_EQUITY="nan", TOTAL_ASSETS="inf")],
        _BALANCE,
        default_notice="2026-07-21",
    )
    assert rows == []


def test_invalid_announce_date_skips_only_the_bad_row():
    rows, fallbacks = _parse_rows(
        [_balance_row(NOTICE_DATE="not-a-date"), _balance_row()],
        _BALANCE,
        default_notice="2026-07-21",
        announce_dates={},
    )
    assert fallbacks == 1
    assert rows
    assert {r["announce_date"] for r in rows} == {date(2018, 3, 27)}


def test_source_sentinel_announce_date_is_not_a_pit_vintage():
    rows, fallbacks = _parse_rows(
        [_balance_row(NOTICE_DATE="1900-01-01")],
        _BALANCE,
        default_notice="2026-07-21",
        announce_dates={},
    )

    assert rows == []
    assert fallbacks == 0


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


def test_daily_fetch_rejects_rows_from_another_notice_date():
    client = FakeDatacenterClient(
        {"RPT_LICO_FN_CPD": [_lico_row(REPORTDATE="2024-03-31", NOTICE_DATE="2024-04-27")]}
    )

    with pytest.raises(RuntimeError, match="NOTICE_DATE row for 2024-04-28"):
        fetch_financial_statement_items(date(2024, 4, 28), client=client)  # type: ignore[arg-type]


def test_daily_comparative_keeps_notice_vintage_and_adds_marked_reconstruction(monkeypatch):
    calls = []

    def fetch(_client, report, expr, **kwargs):
        calls.append(expr)
        if report is _ANNOUNCE_SOURCE:
            return [] if "NOTICE_DATE" in expr else [_lico_row()]
        return [_balance_row()] if report is _BALANCE else []

    monkeypatch.setattr(fundamentals_module, "_fetch_report", fetch)
    frame = fetch_financial_statement_items(date(2018, 3, 27), client=FakeDatacenterClient({}))
    assets = frame.filter(frame["item_code"] == "total_assets").sort("announce_date")
    assert assets.select("announce_date", "source").rows() == [
        (date(2017, 3, 27), "eastmoney_backfill"),
        (date(2018, 3, 27), "eastmoney"),
    ]
    assert any("REPORTDATE='2016-12-31'" in call and '"000002.SZ"' in call for call in calls)


def test_daily_reconstruction_publishes_with_exact_wire_archive(tmp_path, monkeypatch):
    from urllib.parse import parse_qs, urlparse

    import httpx
    import polars as pl

    from cnequity.config import Config
    from cnequity.domain.pit import classify_pit_rows
    from cnequity.steps.fundamentals import step_financial_statement_items
    from cnequity.storage.raw_archive import captured_records

    class WireClient:
        def get(self, url, **kwargs):
            params = parse_qs(urlparse(url).query)
            report = params["reportName"][0]
            expr = params["filter"][0]
            rows = []
            if report == _BALANCE.name:
                rows = [_balance_row()]
            elif report == _ANNOUNCE_SOURCE.name and "NOTICE_DATE" not in expr:
                rows = [_lico_row()]
            return httpx.Response(
                200,
                json={"success": True, "result": {"data": rows, "pages": 1, "count": len(rows)}},
                request=httpx.Request("GET", url),
            )

        def close(self):
            pass

    monkeypatch.setattr(fundamentals_module, "EastMoneyClient", lambda **kw: WireClient())
    cfg = Config(data_root=tmp_path / "lake")
    result = step_financial_statement_items(cfg, date(2018, 3, 27), "run-1", {})
    assert result["rows_written"] == 4
    files = list((cfg.staging_root / "financial_statement_items").rglob("*.parquet"))
    frame = pl.read_parquet(files)
    assert set(frame["source"]) == {"eastmoney", "eastmoney_backfill"}
    reconstructed = frame.filter(pl.col("source") == "eastmoney_backfill")
    assert reconstructed["available_at"].null_count() == reconstructed.height
    assert reconstructed["source_published_at"].null_count() == reconstructed.height
    strict = classify_pit_rows(
        frame, "financial_statement_items", as_of=date(2100, 1, 1), pit_mode="strict"
    )
    assert set(strict["source"]) == {"eastmoney"}
    records = captured_records(
        cfg,
        "financial_statement_items",
        "run-1",
        source="eastmoney",
        request_scope="daily:2018-03-27",
    )
    # One daily request per report, plus the LICO date lookup.
    assert len(records) == len(_REPORTS) + 1
    assert all(record.http_metadata["wire_exact"] for record in records)


def test_daily_reconstruction_does_not_replace_a_committed_original(tmp_path, monkeypatch):
    import polars as pl

    from cnequity.config import Config
    from cnequity.domain.schemas import with_provenance

    cfg = Config(data_root=tmp_path / "lake", raw_archive_enabled=False)
    lico, _ = _parse_rows([_lico_row()], _ANNOUNCE_SOURCE, default_notice="2017-03-27")
    balance, _ = _parse_rows(
        [_balance_row(NOTICE_DATE="2017-03-27")], _BALANCE, default_notice="2017-03-27"
    )
    path = (
        cfg.curated_root
        / "financial_statement_items"
        / "report_period=2016Q4"
        / "part-merged.parquet"
    )
    path.parent.mkdir(parents=True)
    with_provenance(
        pl.DataFrame(lico + balance), source="eastmoney", data_version="v1"
    ).write_parquet(path)
    monkeypatch.setattr(
        fundamentals_module,
        "_fetch_report",
        lambda client, report, expr, **kwargs: [_balance_row()] if report is _BALANCE else [],
    )
    frame = fetch_financial_statement_items(
        date(2018, 3, 27), config=cfg, client=FakeDatacenterClient({})
    )
    # The 2018 filing repeats the committed 2016Q4 figures as comparatives: no
    # new vintage, and the committed original stays the only one.
    assert frame.is_empty()

    restated = _balance_row(TOTAL_ASSETS=830674213925.0 * 1.01)
    monkeypatch.setattr(
        fundamentals_module,
        "_fetch_report",
        lambda client, report, expr, **kwargs: [restated] if report is _BALANCE else [],
    )
    frame = fetch_financial_statement_items(
        date(2018, 3, 27), config=cfg, client=FakeDatacenterClient({})
    )
    # A changed figure is a restatement, kept on the date it was republished.
    assert frame.select("item_code", "source", "announce_date").rows() == [
        ("total_assets", "eastmoney", date(2018, 3, 27))
    ]


def test_backfill_retains_an_earlier_committed_lico_date(tmp_path, monkeypatch):
    import polars as pl

    from cnequity.config import Config
    from cnequity.domain.schemas import with_provenance

    cfg = Config(data_root=tmp_path / "lake", raw_archive_enabled=False)
    headline, _ = _parse_rows([_lico_row(BPS=None)], _ANNOUNCE_SOURCE, default_notice="2017-03-27")
    path = (
        cfg.curated_root
        / "financial_statement_items"
        / "report_period=2016Q4"
        / "part-merged.parquet"
    )
    path.parent.mkdir(parents=True)
    with_provenance(pl.DataFrame(headline), source="eastmoney", data_version="v1").write_parquet(
        path
    )
    monkeypatch.setattr(fundamentals_module, "_report_period_dates", lambda *a, **k: ["2016-12-31"])
    monkeypatch.setattr(
        fundamentals_module,
        "_fetch_report",
        lambda client, report, expr, **kwargs: (
            [_lico_row(NOTICE_DATE="2019-03-27")]
            if report is _ANNOUNCE_SOURCE
            else ([_balance_row()] if report is _BALANCE else [])
        ),
    )
    frame = fetch_financial_statement_items(
        date(2026, 10, 9), backfill=True, config=cfg, client=FakeDatacenterClient({})
    )
    assert frame.filter(pl.col("item_code") == "total_assets")["announce_date"].to_list() == [
        date(2017, 3, 27)
    ]


def test_backfill_rejects_rows_from_another_report_period(monkeypatch):
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.fundamentals._report_period_dates",
        lambda *args, **kwargs: ["2024-03-31"],
    )
    client = FakeDatacenterClient(
        {
            "RPT_LICO_FN_CPD": [_lico_row(REPORTDATE="2024-03-31")],
            "RPT_DMSK_FN_BALANCE": [_balance_row(REPORT_DATE="2023-12-31")],
        }
    )

    with pytest.raises(RuntimeError, match="REPORT_DATE row for 2024-03-31"):
        fetch_financial_statement_items(
            date(2024, 6, 28),
            backfill=True,
            client=client,  # type: ignore[arg-type]
        )


def test_backfill_keeps_other_report_units_when_one_request_fails(monkeypatch):
    monkeypatch.setattr(fundamentals_module, "_report_period_dates", lambda *a, **k: ["2024-03-31"])
    queried: list[str] = []

    def fake_report(_client, report, _expr, **_kwargs):
        queried.append(report.name)
        if report.name == "RPT_DMSK_FN_BALANCE":
            raise RuntimeError("gateway reset")
        if report is _ANNOUNCE_SOURCE:
            return [_lico_row(REPORTDATE="2024-03-31")]
        return []

    monkeypatch.setattr(fundamentals_module, "_fetch_report", fake_report)
    staged: list[str] = []
    failures: list[tuple[str, str]] = []
    frame = fetch_financial_statement_items(
        date(2024, 6, 28),
        backfill=True,
        client=FakeDatacenterClient({}),  # type: ignore[arg-type]
        on_unit=lambda unit, scope, rows: staged.append(unit),
        failures=failures,
    )
    assert frame.height > 0
    assert "2024-03-31|RPT_LICO_FN_CPD" in staged
    assert failures == [("2024-03-31|RPT_DMSK_FN_BALANCE", "gateway reset")]
    assert "RPT_DMSK_FN_CASHFLOW" in queried


def _REPORTS_in(urls: list[str]) -> set[str]:
    return {report.name for report in _REPORTS for url in urls if report.name in url}


def test_closing_cash_comes_from_every_f10_cashflow_family():
    """DMSK's END_CCE is empty; the F10 reports carry it for all four families."""
    owners = {r.name for r in _REPORTS if ("cashflow", "end_cash") in r.items}
    assert owners == {f"RPT_F10_FINANCE_{family}CASHFLOW" for family in "GBIS"}
