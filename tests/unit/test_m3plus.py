from datetime import date

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.adapters.eastmoney.fundamentals import fetch_financial_statement_items
from cnequity.adapters.eastmoney.index_constituents import fetch_index_constituents
from cnequity.adapters.eastmoney.industry import fetch_industry_members
from cnequity.adapters.eastmoney.sectors import fetch_sector_members
from cnequity.config import Config
from cnequity.domain.schemas import validate_dataframe
from cnequity.orchestrator.registry import get_step
from cnequity.query import load


class FakeDatacenterClient:
    def __init__(self, batches: dict[str, list[dict]]):
        self.batches = batches

    def get(self, url, **kwargs):
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


def test_m3plus_steps_registered():
    for name in ("financial_statement_items", "index_constituents", "industry_members"):
        assert get_step(name).fn is not None


def test_financial_statement_items_parses_notice_date():
    client = FakeDatacenterClient(
        {
            "RPT_LICO_FN_CPD": [
                {
                    "SECURITY_CODE": "600519",
                    "SECUCODE": "600519.SH",
                    "REPORTDATE": "2024-03-31",
                    "NOTICE_DATE": "2024-04-28",
                    "TOTAL_OPERATE_INCOME": 100.0,
                    "WEIGHTAVG_ROE": 0.25,
                }
            ]
        }
    )
    df = fetch_financial_statement_items(date(2024, 4, 28), client=client)  # type: ignore[arg-type]
    assert df.height >= 2
    assert set(df["item_code"].to_list()) >= {"revenue", "roe"}
    assert df["announce_date"][0] == date(2024, 4, 28)
    assert df["report_period"][0] == "2024Q1"


def test_financial_statement_items_drops_non_a_share():
    """NEEQ (.NQ) rows dominate same-day announcements; they must be filtered out."""
    client = FakeDatacenterClient(
        {
            "RPT_LICO_FN_CPD": [
                {
                    "SECURITY_CODE": "834948",
                    "SECUCODE": "834948.NQ",
                    "REPORTDATE": "2024-03-31",
                    "NOTICE_DATE": "2024-04-28",
                    "TOTAL_OPERATE_INCOME": 100.0,
                },
                {
                    "SECURITY_CODE": "000001",
                    "SECUCODE": "000001.SZ",
                    "REPORTDATE": "2024-03-31",
                    "NOTICE_DATE": "2024-04-28",
                    "TOTAL_OPERATE_INCOME": 200.0,
                },
            ]
        }
    )
    df = fetch_financial_statement_items(date(2024, 4, 28), client=client)  # type: ignore[arg-type]
    assert set(df["symbol"].to_list()) == {"000001.SZ"}


def test_financial_statement_items_backfill_walks_report_periods():
    from cnequity.adapters.eastmoney.fundamentals import _report_period_dates

    periods = _report_period_dates(date(2026, 7, 7))
    assert "2001-03-31" in periods
    assert "2016-03-31" in periods
    assert "2026-03-31" in periods
    assert "2026-09-30" not in periods  # future quarter excluded
    assert periods == sorted(periods, reverse=True)


def test_financial_statement_items_backfill_walk_honors_start_end():
    from cnequity.adapters.eastmoney.fundamentals import _report_period_dates

    periods = _report_period_dates(
        date(2026, 7, 7),
        start=date(2010, 1, 1),
        end=date(2010, 12, 31),
    )
    assert periods[0] == "2010-12-31"
    assert periods[-1] == "2010-03-31"
    assert all(p.startswith("2010-") for p in periods)
    assert (
        _report_period_dates(date(2026, 7, 7), start=date(2027, 1, 1), end=date(2027, 6, 30)) == []
    )


def test_index_constituents_schema():
    raw = pl.DataFrame(
        {
            "index_symbol": ["000300.SH"],
            "symbol": ["600519.SH"],
            "as_of_date": [date(2024, 6, 28)],
            "weight": [0.05],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    )
    out = validate_dataframe(raw, "index_constituents")
    assert out.height == 1


def test_index_constituents_fetch():
    client = FakeDatacenterClient(
        {
            "RPT_INDEX_CONSTITUENT": [
                {
                    "INDEX_CODE": "000300",
                    "SECURITY_CODE": "600519",
                    "TRADE_DATE": "2024-06-28",
                }
            ]
        }
    )
    df = fetch_index_constituents(date(2024, 6, 28), indices=["000300.SH"], client=client)  # type: ignore[arg-type]
    assert df.height == 1
    assert df["index_symbol"][0] == "000300.SH"
    assert df["symbol"][0] == "600519.SH"


@pytest.mark.parametrize(
    ("module_name", "fetcher"),
    [
        ("cnequity.adapters.eastmoney.sectors", fetch_sector_members),
        ("cnequity.adapters.eastmoney.industry", fetch_industry_members),
    ],
)
def test_membership_adapters_close_owned_client_when_parsing_fails(
    monkeypatch, module_name, fetcher
):
    created = []

    class _Client:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    def _factory(**kwargs):
        client = _Client()
        created.append(client)
        return client

    monkeypatch.setattr(f"{module_name}.EastMoneyClient", _factory)
    monkeypatch.setattr(
        f"{module_name}.fetch_datacenter",
        lambda *args, **kwargs: [object()],
    )
    with pytest.raises(AttributeError, match="get"):
        fetcher(date(2024, 6, 28))
    assert created[0].closed is True


def test_index_constituents_accepts_latest_change_date_snapshot():
    client = FakeDatacenterClient(
        {
            "RPT_INDEX_CONSTITUENT": [
                {
                    "INDEX_CODE": "000300",
                    "SECURITY_CODE": "600519",
                    "TRADE_DATE": "2024-06-27",
                }
            ]
        }
    )

    df = fetch_index_constituents(
        date(2024, 6, 28),
        indices=["000300.SH"],
        client=client,  # type: ignore[arg-type]
    )
    assert df.height == 1
    assert df["symbol"][0] == "600519.SH"
    assert df["as_of_date"][0] == date(2024, 6, 28)


def test_index_constituents_rejects_an_empty_requested_index():
    class _Client:
        def get(self, url, **kwargs):
            class _Response:
                def raise_for_status(self):
                    return None

                def json(self):
                    return {"success": True, "result": {"data": []}}

            return _Response()

        def close(self):
            return None

    with pytest.raises(RuntimeError, match="000300.SH"):
        fetch_index_constituents(date(2024, 6, 28), indices=["000300.SH"], client=_Client())


def test_index_constituents_explicit_empty_selection_is_a_noop():
    class _Client:
        def get(self, url, **kwargs):
            raise AssertionError("empty index selection must not fetch the default universe")

        def close(self):
            return None

    out = fetch_index_constituents(date(2024, 6, 28), indices=[], client=_Client())
    assert out.is_empty()


def test_industry_members_fetch():
    client = FakeDatacenterClient(
        {
            "RPT_BOARD_CONSTITUENT": [
                {
                    "SECURITY_CODE": "600519",
                    "BOARD_CODE": "3405",
                    "BOARD_NAME": "白酒",
                    "BOARD_TYPE_NEW": "2",
                }
            ]
        }
    )
    df = fetch_industry_members(date(2024, 6, 28), client=client)  # type: ignore[arg-type]
    assert df.height == 1
    assert df["industry_name"][0] == "白酒"


def test_sector_members_dedupes_and_skips_missing_board_keys():
    client = FakeDatacenterClient(
        {
            "RPT_BOARD_CONSTITUENT": [
                {
                    "SECURITY_CODE": "600519",
                    "BOARD_CODE": "BK0001",
                    "BOARD_NAME": "白酒",
                    "BOARD_TYPE_NEW": "3",
                },
                {
                    "SECURITY_CODE": "600519",
                    "BOARD_CODE": "BK0001",
                    "BOARD_NAME": "白酒",
                    "BOARD_TYPE_NEW": "3",
                },
                {
                    "SECURITY_CODE": "600519",
                    "BOARD_CODE": "",
                    "BOARD_NAME": "缺少代码",
                    "BOARD_TYPE_NEW": "3",
                },
            ]
        }
    )
    df = fetch_sector_members(date(2024, 6, 28), client=client)  # type: ignore[arg-type]
    assert df.height == 1
    assert df["sector_code"].to_list() == ["BK0001"]


@pytest.fixture
def lake(tmp_path):
    root = tmp_path / "data"
    curated = root / "curated"
    fsi = curated / "financial_statement_items" / "report_period=2024Q1"
    fsi.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "report_period": ["2024Q1", "2024Q1"],
            "statement_type": ["indicator", "indicator"],
            "item_code": ["roe", "revenue"],
            "item_value": [0.25, 100.0],
            "announce_date": [date(2024, 4, 28), date(2024, 5, 15)],
            "source": ["eastmoney", "eastmoney"],
            "data_version": ["v1", "v1"],
            "fetched_at": ["2024-04-28T00:00:00+00:00", "2024-05-15T00:00:00+00:00"],
        }
    ).write_parquet(fsi / "part-0.parquet")
    return Config(data_root=root)


def test_load_financial_statement_items_by_as_of(lake):
    df = load("financial_statement_items", as_of="2024-04-30", items=["roe"], config=lake)
    assert df.height == 1
    assert df["item_code"][0] == "roe"
