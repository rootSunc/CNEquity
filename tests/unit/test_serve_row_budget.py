"""The dashboard's row viewer refuses a page whose read would not be bounded.

``load()`` holds the whole selection before the page is cut, so a request
with no period, or one ``trade_ticks`` day, could hold gigabytes for 200
rows. The viewer uses the budget MCP ``query_dataset`` uses.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest
from fastapi.testclient import TestClient

import cnequity.query.read_budget as read_budget
from cnequity.serve.app import create_app

DAYS = [date(2024, 6, 24), date(2024, 6, 25), date(2024, 6, 26)]


def _fund_flow(config) -> list[int]:
    sizes = []
    for day in DAYS:
        part = config.curated_root / "fund_flow" / f"trade_date={day}"
        part.mkdir(parents=True)
        path = part / "part-0.parquet"
        pl.DataFrame(
            {
                "symbol": ["600519.SH", "000001.SZ"],
                "trade_date": [day, day],
                **{
                    column: [1.0, 2.0]
                    for column in (
                        "main_net_inflow",
                        "super_large_net_inflow",
                        "large_net_inflow",
                        "medium_net_inflow",
                        "small_net_inflow",
                    )
                },
                "source": ["eastmoney"] * 2,
                "data_version": ["v1"] * 2,
                "fetched_at": [datetime(2024, 6, 27, tzinfo=timezone.utc)] * 2,
            }
        ).write_parquet(path)
        sizes.append(pl.read_parquet(path).height)
    return sizes


def _rows(config, dataset, **params):
    client = TestClient(create_app(config), base_url="http://127.0.0.1")
    return client.get(f"/api/datasets/{dataset}/rows", params=params)


def test_a_page_without_a_period_is_refused_before_reading(config, monkeypatch):
    sizes = _fund_flow(config)
    monkeypatch.setattr(read_budget, "SCAN_BUDGET_ROWS", sizes[-1] + 1)
    import cnequity.query.reader as reader

    def _must_not_load(*_args, **_kwargs):
        raise AssertionError("an oversized page must be refused before load()")

    monkeypatch.setattr(reader, "load", _must_not_load)

    response = _rows(config, "fund_flow")

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "budget" in detail and detail.endswith("Pick a period or enter a symbol.")


def test_one_period_or_one_symbol_fits(config, monkeypatch):
    sizes = _fund_flow(config)
    monkeypatch.setattr(read_budget, "SCAN_BUDGET_ROWS", sizes[-1] + 1)

    by_period = _rows(config, "fund_flow", period="2024-06-26")
    by_symbol = _rows(config, "fund_flow", symbol="600519.SH")

    assert by_period.status_code == 200 and by_period.json()["total"] == 2
    assert by_symbol.status_code == 200 and by_symbol.json()["total"] == 3


def _two_quarters_of_statements(config, monkeypatch) -> list[dict]:
    root = config.curated_root / "financial_statement_items"
    for quarter in ("2024Q3", "2024Q4"):
        part = root / f"report_period={quarter}"
        part.mkdir(parents=True)
        pl.DataFrame({"n": range(1000)}).write_parquet(part / "part-0.parquet")
    monkeypatch.setattr(read_budget, "SCAN_BUDGET_ROWS", 1500)
    import cnequity.query.reader as reader

    calls: list[dict] = []

    def _load(dataset, **kwargs):
        calls.append(kwargs)
        return pl.DataFrame({"report_period": ["2024Q3", "2024Q4"]})

    monkeypatch.setattr(reader, "load", _load)
    return calls


def test_a_point_in_time_page_is_sized_by_its_period(config, monkeypatch):
    # load() scans only the report period's partition, so one quarter costs
    # one quarter, not the whole dataset.
    calls = _two_quarters_of_statements(config, monkeypatch)

    response = _rows(config, "financial_statement_items", period="2024Q4", as_of="2025-01-01")

    assert response.status_code == 200 and response.json()["total"] == 1
    assert (calls[0]["start"], calls[0]["end"]) == (date(2024, 10, 1), date(2024, 12, 31))


def test_a_point_in_time_page_without_a_period_asks_for_one(config, monkeypatch):
    calls = _two_quarters_of_statements(config, monkeypatch)

    response = _rows(config, "financial_statement_items", as_of="2025-01-01")

    assert response.status_code == 422 and not calls
    assert response.json()["detail"].endswith("Pick a period or enter a symbol.")


def test_an_announcement_page_is_sized_up_to_its_as_of(config, monkeypatch):
    # A correction announced after the day but by as_of replaces the row, so
    # those days are scanned too and count toward the budget.
    root = config.curated_root / "announcement_index"
    for day in ("2024-06-10", "2024-06-11", "2024-06-12"):
        part = root / f"announce_date={day}"
        part.mkdir(parents=True)
        pl.DataFrame({"n": range(1000)}).write_parquet(part / "part-0.parquet")
    monkeypatch.setattr(read_budget, "SCAN_BUDGET_ROWS", 1500)
    import cnequity.query.reader as reader

    monkeypatch.setattr(reader, "load", lambda *_a, **_k: pl.DataFrame({"announcement_id": ["A"]}))

    fits = _rows(config, "announcement_index", period="2024-06-10", as_of="2024-06-10")
    over = _rows(config, "announcement_index", period="2024-06-10", as_of="2024-06-12")

    assert fits.status_code == 200
    assert over.status_code == 422 and over.json()["detail"].endswith("Enter a symbol.")


@pytest.mark.parametrize("language", ["zh_CN"])
def test_the_refusal_is_translated(config, monkeypatch, language):
    from cnequity.i18n import use_language

    sizes = _fund_flow(config)
    monkeypatch.setattr(read_budget, "SCAN_BUDGET_ROWS", sizes[-1] + 1)
    from cnequity.serve.lake import LakeView

    with use_language(language), pytest.raises(ValueError, match="请选择一个时段或输入标的"):
        LakeView(config).rows("fund_flow")
