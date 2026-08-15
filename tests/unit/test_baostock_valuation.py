"""Offline tests for the baostock historical valuation backfill path."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cn_market_lake.adapters.baostock.valuation import (
    fetch_valuation_history,
    to_baostock_symbol,
)
from cn_market_lake.domain.datasets import get_dataset
from cn_market_lake.domain.schemas import VALUATION_METRICS_SCHEMA


def test_to_baostock_symbol():
    assert to_baostock_symbol("600519.SH") == "sh.600519"
    assert to_baostock_symbol("000001.SZ") == "sz.000001"
    assert to_baostock_symbol("920819.BJ") == "bj.920819"


class _FakeResultSet:
    """Mimics baostock's cursor-style result set."""

    def __init__(
        self, rows: list[list[str]], error_code: str = "0", fields: list[str] | None = None
    ):
        self.error_code = error_code
        self.error_msg = "" if error_code == "0" else "boom"
        self._rows = rows
        self._i = -1
        self.fields = fields or []

    def next(self) -> bool:
        self._i += 1
        return self._i < len(self._rows)

    def get_row_data(self) -> list[str]:
        return self._rows[self._i]


class _FakeBaostock:
    def __init__(
        self,
        per_symbol: dict[str, list[list[str]]],
        login_ok: bool = True,
        error_codes: dict[str, str] | None = None,
        profit_q4: dict[tuple[str, int], list[list[str]]] | None = None,
    ):
        self._per_symbol = per_symbol
        self._login_ok = login_ok
        self._error_codes = error_codes or {}
        self._profit_q4 = profit_q4 or {}
        self.logged_out = False
        self.logins = 0

    def login(self):
        self.logins += 1
        return _FakeResultSet([], error_code="0" if self._login_ok else "10001")

    def query_history_k_data_plus(self, code, fields, **kwargs):
        err = self._error_codes.get(code, "0")
        return _FakeResultSet(self._per_symbol.get(code, []), error_code=err)

    def query_profit_data(self, code, year, quarter):
        rows = self._profit_q4.get((code, year), []) if quarter == 4 else []
        fields = [
            "code",
            "pubDate",
            "statDate",
            "roeAvg",
            "npMargin",
            "gpMargin",
            "netProfit",
            "epsTTM",
            "MBRevenue",
            "totalShare",
            "liqaShare",
        ]
        return _FakeResultSet(rows, fields=fields)

    def logout(self):
        self.logged_out = True


def test_fetch_valuation_history_maps_market_cap():
    # fields: date,code,close,amount,turn,peTTM,pbMRQ,psTTM
    # float_mv = amount / (turn/100); total_mv = close * totalShare (Q4 asof)
    bs = _FakeBaostock(
        {
            "sh.600519": [
                ["2016-01-04", "sh.600519", "200.0", "1000000.0", "1.0", "12.5", "3.1", "8.0"],
                [
                    "2016-01-05",
                    "sh.600519",
                    "210.0",
                    "",
                    "",
                    "12.6",
                    "",
                    "8.1",
                ],  # suspend → null mv
            ],
            "sz.000001": [
                ["2016-01-04", "sz.000001", "10.0", "500000.0", "2.0", "7.0", "0.9", "1.5"],
            ],
        },
        profit_q4={
            ("sh.600519", 2015): [
                [
                    "sh.600519",
                    "2016-03-01",
                    "2015-12-31",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "1000000000",
                    "800000000",
                ]
            ],
            ("sz.000001", 2015): [
                [
                    "sz.000001",
                    "2016-03-01",
                    "2015-12-31",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "500000000",
                    "500000000",
                ]
            ],
        },
    )
    df, failed = fetch_valuation_history(
        ["600519.SH", "000001.SZ"], date(2016, 1, 1), date(2016, 1, 5), bs=bs, sleep=lambda _: None
    )

    assert bs.logged_out is True
    assert failed == []
    assert df.height == 3
    assert set(df.columns) == set(VALUATION_METRICS_SCHEMA) - {
        "source",
        "data_version",
        "fetched_at",
    }

    moutai = df.filter(
        (pl.col("symbol") == "600519.SH") & (pl.col("trade_date") == date(2016, 1, 4))
    )
    assert moutai["float_mv"].item() == pytest.approx(100_000_000.0)  # 1e6 / 0.01
    assert moutai["total_mv"].item() == pytest.approx(200.0 * 1_000_000_000)
    assert moutai["pe_ttm"].item() == 12.5

    suspended = df.filter(
        (pl.col("symbol") == "600519.SH") & (pl.col("trade_date") == date(2016, 1, 5))
    )
    assert suspended["float_mv"].item() is None
    assert suspended["pb"].item() is None
    # total_mv still from close × shares (close present)
    assert suspended["total_mv"].item() == pytest.approx(210.0 * 1_000_000_000)


def test_fetch_valuation_history_skips_uncovered_symbol():
    bs = _FakeBaostock(
        {
            "sh.600519": [
                ["2016-01-04", "sh.600519", "200.0", "1000000.0", "1.0", "12.5", "3.1", "8.0"]
            ]
        }
    )
    df, failed = fetch_valuation_history(
        ["600519.SH", "999999.SH"], date(2016, 1, 1), date(2016, 1, 5), bs=bs, sleep=lambda _: None
    )
    assert df.height == 1
    assert df["symbol"].unique().to_list() == ["600519.SH"]
    assert failed == []


def test_fetch_valuation_history_reports_failed_symbols_fail_loud():
    bs = _FakeBaostock(
        {
            "sh.600519": [
                ["2016-01-04", "sh.600519", "200.0", "1000000.0", "1.0", "12.5", "3.1", "8.0"]
            ]
        },
        error_codes={"sz.000001": "10002"},
    )
    df, failed = fetch_valuation_history(
        ["600519.SH", "000001.SZ"], date(2016, 1, 1), date(2016, 1, 5), bs=bs, sleep=lambda _s: None
    )
    assert df.height == 1
    assert failed == ["000001.SZ"]
    assert bs.logins > 1


def test_fetch_valuation_history_fails_loud_on_login_error():
    bs = _FakeBaostock({}, login_ok=False)
    with pytest.raises(RuntimeError, match="login failed"):
        fetch_valuation_history(
            ["600519.SH"],
            date(2016, 1, 1),
            date(2016, 1, 5),
            bs=bs,
            sleep=lambda _s: None,
        )


def test_valuation_metrics_declares_backfill_source():
    spec = get_dataset("valuation_metrics")
    assert spec.fetch_semantics == "snapshot"
    assert spec.backfill_source == "baostock"


def test_symbols_needing_backfill_includes_null_mv(tmp_path):
    from cn_market_lake.config import Config
    from cn_market_lake.steps.fundamentals import _symbols_needing_backfill

    root = tmp_path / "data"
    part = root / "curated" / "valuation_metrics" / "trade_date=2016-01-04"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "trade_date": [date(2016, 1, 4), date(2016, 1, 4)],
            "pe_ttm": [12.0, 7.0],
            "pb": [3.0, 1.0],
            "ps_ttm": [8.0, 1.5],
            "total_mv": [None, 1.0e10],
            "float_mv": [None, 1.0e10],
            "source": ["baostock", "baostock"],
            "data_version": ["v1", "v1"],
            "fetched_at": ["2016-01-04T00:00:00+00:00"] * 2,
        }
    ).write_parquet(part / "part-0.parquet")

    cfg = Config(data_root=root)
    todo = _symbols_needing_backfill(cfg, ["600519.SH", "000001.SZ", "000002.SZ"])
    assert "600519.SH" in todo  # null float_mv → refill
    assert "000001.SZ" not in todo  # already has float_mv (100% fill)
    assert "000002.SZ" in todo  # never backfilled


def test_symbols_needing_backfill_sparse_fill(tmp_path):
    """A single non-null day must not mark a sparse history as done (<80%)."""
    from cn_market_lake.config import Config
    from cn_market_lake.steps.fundamentals import _symbols_needing_backfill

    root = tmp_path / "data"
    part = root / "curated" / "valuation_metrics" / "trade_date=2016-01-04"
    part.mkdir(parents=True)
    # 1/5 days filled → 20% < 80% done threshold
    pl.DataFrame(
        {
            "symbol": ["600519.SH"] * 5,
            "trade_date": [date(2016, 1, d) for d in (4, 5, 6, 7, 8)],
            "pe_ttm": [12.0] * 5,
            "pb": [3.0] * 5,
            "ps_ttm": [8.0] * 5,
            "total_mv": [1.0e10, None, None, None, None],
            "float_mv": [1.0e10, None, None, None, None],
            "source": ["baostock"] * 5,
            "data_version": ["v1"] * 5,
            "fetched_at": ["2016-01-04T00:00:00+00:00"] * 5,
        }
    ).write_parquet(part / "part-0.parquet")

    cfg = Config(data_root=root)
    todo = _symbols_needing_backfill(cfg, ["600519.SH"])
    assert "600519.SH" in todo
