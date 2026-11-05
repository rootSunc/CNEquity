"""Offline tests for the optional Tushare-only datasets."""

from __future__ import annotations

from datetime import date

import httpx
import polars as pl
import pytest

from cnequity.adapters.tushare import convertible_bonds as cb
from cnequity.adapters.tushare import holder_events as he
from cnequity.adapters.tushare import market_stats as ms
from cnequity.config import Config
from cnequity.domain.schemas import validate_dataframe, with_provenance


def _valid(frame: pl.DataFrame, dataset: str) -> None:
    validate_dataframe(with_provenance(frame, source="tushare", data_version="v1"), dataset)


def test_bonds_join_their_offering_and_map_types():
    basic = pl.DataFrame(
        {
            "ts_code": ["113001.SH", "132001.SH"],
            "bond_full_name": ["A", "B"],
            "bond_short_name": ["A转债", "B EB"],
            "cb_type": ["CB", "EB"],
            "stk_code": ["600001.SH", "600002.SH"],
            "exchange": ["SH", "SH"],
            "par": [100.0, 100.0],
            "issue_price": [100.0, 100.0],
            "issue_size": [1e9, 2e9],
            "remain_size": [5e8, 0.0],
            "maturity": [6.0, 5.0],
            "coupon_rate": [0.3, 1.0],
            "rate_clause": [None, None],
            "value_date": ["20200101", "20190101"],
            "maturity_date": ["20260101", "20240101"],
            "list_date": ["20200120", None],
            "delist_date": [None, "20240101"],
            "conv_start_date": ["20200701", "20190701"],
            "conv_end_date": ["20251231", "20231231"],
            "conv_stop_date": [None, None],
            "first_conv_price": [10.0, 20.0],
            "conv_price": [9.5, 20.0],
        }
    )
    issue = pl.DataFrame(
        {
            "ts_code": ["113001.SH"],
            "ann_date": ["20191225"],
            "issue_type": ["优先配售和上网定价"],
            "onl_date": ["2019-12-30"],
            "shd_ration_record_date": ["2019-12-27"],
        }
    )
    bonds = cb.bonds_from_source(basic, issue).sort("symbol")
    assert bonds["bond_type"].to_list() == ["convertible", "exchangeable"]
    assert bonds["subscription_date"].to_list() == [date(2019, 12, 30), None]
    _valid(bonds, "convertible_bonds")


def test_bond_bars_store_bonds_and_yuan():
    names = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "vol", "amount"]
    bars = cb.normalize_bars(names, [["110077.SH", "20261104", 1, 2, 1, 2, 1, 56529.0, 11262.7602]])
    assert bars["volume"][0] == 565290
    assert bars["amount"][0] == pytest.approx(112_627_602.0)
    _valid(bars, "convertible_bond_bars")


def test_holder_trades_drop_exact_repeats_and_keep_distinct_trades():
    names = ["ts_code", "ann_date", "holder_name", "holder_type", "in_de", "change_vol",
             "change_ratio", "after_share", "after_ratio", "avg_price"]  # fmt: skip
    row = ["430017.BJ", "20240603", "张三", "G", "DE", 1000.0, 0.01, None, None, None]
    other = [*row[:5], 2000.0, *row[6:]]
    trades = he.normalize_holder_trades(names, [row, row, other])
    assert trades.height == 2
    assert trades["event_id"].n_unique() == 2
    assert set(trades["symbol"]) == {"920017.BJ"}
    assert set(trades["holder_type"]) == {"executive"}
    _valid(trades, "holder_trades")


def test_repurchase_stages_and_pledge_units():
    names = ["ts_code", "ann_date", "end_date", "proc", "exp_date", "vol", "amount",
             "high_limit", "low_limit"]  # fmt: skip
    buys = he.normalize_share_repurchases(
        names, [["601126.SH", "20261102", "20261031", "实施", None, 1.0, 2.0, 3.0, 1.0]]
    )
    assert buys["stage"].to_list() == ["in_progress"]
    _valid(buys, "share_repurchases")
    names = ["ts_code", "end_date", "pledge_count", "unrest_pledge", "rest_pledge",
             "total_share", "pledge_ratio"]  # fmt: skip
    pledges = he.normalize_pledge_stats(
        names, [["000001.SZ", "20261030", 9, 2438.48, 0.0, 1940591.82, 0.13]] * 2
    )
    assert pledges.height == 1
    assert pledges["total_shares"][0] == pytest.approx(19_405_918_200.0)
    _valid(pledges, "pledge_stats")


def test_chip_distribution_and_top10_columns():
    names = ["ts_code", "trade_date", "his_low", "his_high", "cost_5pct", "cost_15pct",
             "cost_50pct", "cost_85pct", "cost_95pct", "weight_avg", "winner_rate"]  # fmt: skip
    chips = ms.normalize_chip_distribution(
        names, [["600039.SH", "20261104", 0.7, 10.1, 6.8, 7.6, 8.7, 9.6, 9.9, 8.56, 57.55]]
    )
    assert chips["cost_mean"][0] == 8.56
    _valid(chips, "chip_distribution")
    names = ["trade_date", "ts_code", "name", "close", "change", "rank", "market_type",
             "amount", "net_amount", "buy", "sell"]  # fmt: skip
    top = ms.normalize_northbound_top10(
        names,
        [
            ["20240816", "600519.SH", "x", 1.0, 0.5, 1, 1, 9.0, 1.0, 5.0, 4.0],
            ["20240816", "000001.SZ", "y", 1.0, 0.5, 1, 3, 9.0, None, None, None],
            ["20240816", "00700.HK", "z", 1.0, 0.5, 1, 2, 9.0, None, None, None],
        ],
    )
    assert top.sort("channel")["channel"].to_list() == ["sh_connect", "sz_connect"]
    _valid(top, "northbound_top10")


@pytest.mark.parametrize(
    "step",
    [
        "convertible_bonds",
        "convertible_bond_bars",
        "holder_trades",
        "share_repurchases",
        "pledge_stats",
        "chip_distribution",
        "northbound_top10",
    ],
)
def test_steps_send_nothing_without_a_token(step, tmp_path, monkeypatch):
    import cnequity.steps  # noqa: F401
    from cnequity.orchestrator.registry import get_step

    def boom(*_a, **_k):  # pragma: no cover - must not be called
        raise AssertionError("no request without a token")

    monkeypatch.setattr(httpx.Client, "post", boom)
    result = get_step(step).fn(Config(data_root=tmp_path), date(2026, 11, 4), "run-1", {})
    assert result["status"] == "skipped"
