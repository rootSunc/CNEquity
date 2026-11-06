"""Offline tests for the optional Tushare-only datasets."""

from __future__ import annotations

from datetime import date

import httpx
import polars as pl
import pytest

from cnequity.adapters.tushare import company_people as cp
from cnequity.adapters.tushare import convertible_bond_events as cbe
from cnequity.adapters.tushare import convertible_bonds as cb
from cnequity.adapters.tushare import disclosures as dc
from cnequity.adapters.tushare import futures_positions as fp
from cnequity.adapters.tushare import holder_events as he
from cnequity.adapters.tushare import hot_money as hm
from cnequity.adapters.tushare import market_stats as ms
from cnequity.adapters.tushare import sentiment_lists as sl
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
        "limit_events",
        "futures_member_positions",
        "earnings_forecasts",
        "business_segments",
        "investor_visits",
        "hot_money_traders",
        "hot_money_trades",
        "convertible_bond_calls",
        "convertible_bond_conversions",
        "convertible_bond_coupons",
        "popularity_ranks",
        "limit_ladder",
        "strongest_sectors",
        "kpl_limit_ups",
        "fund_flow_ths_tushare",
        "investor_qa",
        "executives",
        "executive_compensation",
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


def test_forecasts_prefer_the_flagged_copy_and_store_yuan():
    names = ["ts_code", "ann_date", "end_date", "type", "p_change_min", "p_change_max",
             "net_profit_min", "net_profit_max", "last_parent_net", "first_ann_date",
             "summary", "change_reason", "update_flag"]  # fmt: skip
    base = ["300389.SZ", "20261009", "20260930", "略增", 32.3, 43.1, 24500.0, 26500.0,
            18517.75, "20261009", "预计盈利", None]  # fmt: skip
    rows = [[*base, "1"], [*base[:10], "旧格式", None, "0"]]
    out = dc.normalize_earnings_forecasts(names, rows)
    assert out.height == 1
    assert out["summary"][0] == "预计盈利"
    assert out["forecast_type"][0] == "slight_increase"
    assert out["net_profit_min"][0] == pytest.approx(245_000_000.0)
    _valid(out, "earnings_forecasts")


def test_segments_keep_the_current_beijing_code():
    names = ["ts_code", "end_date", "bz_item", "bz_code", "bz_sales", "bz_profit",
             "bz_cost", "curr_type"]  # fmt: skip
    current = ["920017.BJ", "20251231", "设备 ", "P", 2.0, 1.0, 1.0, "CNY"]
    legacy = ["430017.BJ", "20251231", "设备", "P", 9.0, 1.0, 8.0, "CNY"]
    for rows in ([current, legacy], [legacy, current]):
        out = dc.normalize_business_segments(names, rows)
        assert out.height == 1
        assert out["revenue"][0] == 2.0
        assert out["segment_type"][0] == "product"
    _valid(out, "business_segments")


def test_visits_null_placeholders_and_drop_exact_repeats():
    names = ["ts_code", "name", "surv_date", "fund_visitors", "rece_place", "rece_mode",
             "rece_org", "org_type", "comp_rece"]  # fmt: skip
    row = ["300857.SZ", "x", "20261029", "--", None, "特定对象调研", "兴业证券", "--", "董秘"]
    other = [*row[:6], "华福证券", *row[7:]]
    out = dc.normalize_investor_visits(names, [row, row, other])
    assert out.height == 2
    assert out["visitors"].null_count() == 2
    assert out["org_type"].null_count() == 2
    _valid(out, "investor_visits")


def test_repeated_page_finds_a_relay_repeat():
    rows = [[i] for i in range(10)]
    assert dc.repeated_page(rows, 4) is None
    assert dc.repeated_page(rows[:4] + rows[:4] + rows[8:], 4) == 1


def test_limit_events_parse_times_and_streaks():
    names = ["trade_date", "ts_code", "industry", "name", "close", "pct_chg", "amount",
             "limit_amount", "float_mv", "total_mv", "turnover_ratio", "fd_amount",
             "first_time", "last_time", "open_times", "up_stat", "limit_times", "limit"]  # fmt: skip
    rows = [
        ["20261104", "605288.SH", "i", "n", 72.7, 10.0, 1.4e8, None, 7.1e9, 7.2e9, 2.02,
         3.1e7, "93142", "101821", 0.0, "3/6", 1.0, "U"],
        ["20261104", "002774.SZ", "i", "n", 14.7, 9.4, 2e8, None, 4e9, 5e9, 4.9,
         None, "0", None, 6.0, "0/0", None, "Z"],
    ]  # fmt: skip
    out = ms.normalize_limit_events(names, rows).sort("symbol")
    assert out["limit_type"].to_list() == ["up_broken", "up"]
    assert out["first_limit_time"].to_list() == [None, "09:31:42"]
    assert out["recent_limit_ups"].to_list() == [0, 3]
    assert out["recent_window_days"].to_list() == [0, 6]
    _valid(out, "limit_events")


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("ag2606", ("SHF", "AG", "AG2606.SHF", "contract")),
        ("TA705", ("CZC", "TA", "TA2705.CZC", "contract")),
        ("AGACTV", ("SHF", "AG", "AG.SHF", "product")),
        ("AG                  ", ("SHF", "AG", "AG.SHF", "product")),
        ("PTA", ("CZC", "TA", "TA.CZC", "product")),
        ("棉花CF", ("CZC", "CF", "CF.CZC", "product")),
        ("IO2611", ("CFE", "IO", "IO2611.CFE", "contract")),
        ("0", None),
    ],
)
def test_futures_codes_map_to_lake_symbols(code, expected):
    assert fp.contract_symbol(code, date(2026, 6, 29)) == expected


def test_futures_positions_keep_the_named_czce_copy():
    names = ["trade_date", "symbol", "broker", "vol", "vol_chg", "long_hld", "long_chg",
             "short_hld", "short_chg"]  # fmt: skip
    rows = [
        ["20260617", "苹果AP", "一德期货（代客）", 0.0, 0.0, 1.0, 1.0, 4730.0, 49.0],
        ["20260617", "AP", "一德期货(代客)", None, None, 1.0, 1.0, 4730.0, 49.0],
        ["20260617", "0", "x", 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
    ]
    out = fp.normalize_futures_member_positions(names, rows, date(2026, 6, 17))
    assert out.height == 1
    assert out["member"][0] == "一德期货(代客)"
    assert out["volume"][0] == 0
    _valid(out, "futures_member_positions")


def test_quarter_ends():
    from cnequity.steps.tushare_extras import quarter_ends

    assert quarter_ends(date(2025, 5, 1), date(2026, 3, 31)) == [
        date(2025, 6, 30),
        date(2025, 9, 30),
        date(2025, 12, 31),
        date(2026, 3, 31),
    ]


def test_popularity_ranks_keep_the_latest_snapshot_and_drop_futures():
    names = ["trade_date", "data_type", "ts_code", "ts_name", "rank", "pct_change",
             "current_price", "hot", "concept", "rank_time", "rank_reason"]  # fmt: skip
    rows = [
        [
            "20250808",
            "热股",
            "600519.SH",
            "a",
            5.0,
            1.0,
            1.0,
            9.0,
            None,
            "2025-08-08 15:30:00",
            None,
        ],
        [
            "20250808",
            "热股",
            "600519.SH",
            "a",
            2.0,
            1.0,
            1.0,
            9.0,
            None,
            "2025-08-08 22:30:00",
            None,
        ],
        ["20250808", "期货", "002511.SZ", "碳酸锂2511", 4.0, None, 0.0, None, None, "x", None],
    ]
    out = sl.normalize_popularity_ranks("ths", names, rows)
    assert out.height == 1
    assert out["rank"][0] == 2
    assert out["list"][0] == "stock"
    _valid(out, "popularity_ranks")


def test_ths_fund_flow_stores_yuan():
    names = ["trade_date", "ts_code", "name", "pct_change", "latest", "net_amount",
             "net_d5_amount", "buy_lg_amount", "buy_lg_amount_rate", "buy_md_amount",
             "buy_md_amount_rate", "buy_sm_amount", "buy_sm_amount_rate"]  # fmt: skip
    row = ["20260825", "603061.SH", "x", -2.23, 339.57, -2451.69, 10951.81, 2905.81, 2.23,
           -7882.78, -6.04, 2525.28, 1.93]  # fmt: skip
    out = sl.normalize_fund_flow_ths(names, [row])
    assert out["net_amount"][0] == pytest.approx(-24_516_900.0)
    total = out.select(
        pl.col("large_net_amount") + pl.col("medium_net_amount") + pl.col("small_net_amount")
    ).item()
    assert total == pytest.approx(out["net_amount"][0])
    _valid(out, "fund_flow_ths_tushare")


def test_limit_lists_and_kpl():
    ladder = sl.normalize_limit_ladder(
        ["ts_code", "name", "trade_date", "nums"], [["002086.SZ", "x", "20241210", "2"]]
    )
    assert ladder["consecutive_limits"].to_list() == [2]
    _valid(ladder, "limit_ladder")
    names = ["ts_code", "name", "trade_date", "days", "up_stat", "cons_nums", "up_nums",
             "pct_chg", "rank"]  # fmt: skip
    sectors = sl.normalize_strongest_sectors(
        names, [["885521.TI", "粤港澳大湾区", "20260318", "1", "7天4板", "3", "11", "1.679", "10"]]
    )
    assert sectors["sector_code"].to_list() == ["885521"]
    _valid(sectors, "strongest_sectors")
    names = ["ts_code", "name", "trade_date", "lu_time", "ld_time", "open_time", "last_time",
             "lu_desc", "tag", "theme", "net_change", "bid_amount", "status", "bid_change",
             "bid_turnover", "lu_bid_vol", "pct_chg", "bid_pct_chg", "rt_pct_chg",
             "limit_order", "amount", "turnover_rate", "free_float", "lu_limit_order"]  # fmt: skip
    row = ["600732.SH", "x", "20220531", "11:15:59", None, None, "11:15:59", "光伏", "涨停",
           "BC电池", 85223630.0, None, "首板", None, None, None, None, None, None,
           105303848.0, 847137822.0, 5.57, 16062709738.0, 303998912.0]  # fmt: skip
    kpl = sl.normalize_kpl_limit_ups(names, [row])
    assert kpl["status"][0] == "首板"
    assert kpl["limit_order_amount"][0] == 105303848.0
    _valid(kpl, "kpl_limit_ups")


def test_hot_money_rows_per_seat_group():
    names = ["trade_date", "ts_code", "ts_name", "buy_amount", "sell_amount", "net_amount",
             "hm_name", "hm_orgs"]  # fmt: skip
    rows = [
        ["20240119", "603083.SH", "x", 1.0, 0.0, 1.0, "赵老哥", "A营业部"],
        ["20240119", "603083.SH", "x", 2.0, 0.0, 2.0, "赵老哥", "B营业部"],
    ]
    out = hm.normalize_hot_money_trades(names, rows)
    assert out.height == 2
    _valid(out, "hot_money_trades")
    roster = hm.normalize_hot_money_traders(["name", "desc", "orgs"], [["苏南帮", "", '["x"]']])
    assert roster["description"].to_list() == [None]
    _valid(roster, "hot_money_traders")


def test_bond_calls_conversions_and_coupons():
    names = ["ts_code", "call_type", "is_call", "ann_date", "call_date", "call_price",
             "call_price_tax", "call_vol", "call_amount", "payment_date", "call_reg_date"]  # fmt: skip
    calls = cbe.normalize_calls(
        names,
        [
            [
                "127029.SZ",
                "强赎",
                "公告实施强赎",
                "20231010",
                "20231031",
                100.37,
                None,
                37460.0,
                374.6,
                "20231107",
                "20231030",
            ]
        ],  # fmt: skip
    )
    assert calls.row(0, named=True)["stage"] == "calling"
    assert calls["call_amount"][0] == pytest.approx(3_746_000.0)
    _valid(calls, "convertible_bond_calls")
    names = ["ts_code", "bond_short_name", "publish_date", "end_date", "issue_size",
             "convert_price_initial", "convert_price", "convert_val", "convert_vol",
             "convert_ratio", "acc_convert_val", "acc_convert_vol", "acc_convert_ratio",
             "remain_size", "total_shares"]  # fmt: skip
    conv = cbe.normalize_conversions(
        names,
        [
            [
                "110033.SH",
                "x",
                "20170420",
                "20170418",
                2.8e9,
                8.93,
                8.93,
                239993.75,
                26875.0,
                0.008571,
                684966.54,
                76659.0,
                0.0245,
                2799315033.46,
                1664546681.0,
            ]
        ],  # fmt: skip
    )
    _valid(conv, "convertible_bond_conversions")
    coupons = cbe.normalize_coupons(
        ["ts_code", "rate_freq", "rate_start_date", "rate_end_date", "coupon_rate"],
        [["113667.SH", "1", "20240317", "20250316", "0.5"]],
    )
    assert coupons["payments_per_year"].to_list() == [1]
    _valid(coupons, "convertible_bond_coupons")


def test_people_and_qa():
    names = ["ts_code", "ann_date", "end_date", "name", "title", "reward", "hold_vol"]
    pay = cp.normalize_executive_compensation(
        names,
        [
            ["300626.SZ", "20250430", "20241231", "张永田", None, 287500.0, 0.0],
            ["300626.SZ", "20250430", "20241231", "张永田", "监事会主席", 287500.0, 0.0],
        ],
    )
    assert pay["record_id"].n_unique() == 2
    _valid(pay, "executive_compensation")
    names = ["ts_code", "ann_date", "name", "gender", "lev", "title", "edu", "national",
             "birthday", "begin_date", "end_date"]  # fmt: skip
    people = cp.normalize_executives(
        names,
        [
            [
                "601728.SH",
                "20230524",
                "罗振东",
                "M",
                "监事",
                "监事",
                "硕士",
                "中国",
                "1978",
                "20230523",
                None,
            ]
        ]
        * 2,  # fmt: skip
    )
    assert people.height == 1
    _valid(people, "executives")
    names = ["ts_code", "name", "trade_date", "q", "a", "pub_time"]
    qa = cp.normalize_investor_qa(
        "SH",
        names,
        [
            ["600010.SH", "x", "20250116", "问", "答", "2025-01-15 17:06:54"],
            ["600010.SH", "x", "20250115", "问", "答", "2025-01-15 17:06:54"],
        ],
    )
    assert qa.height == 1
    assert qa["trade_date"][0] == date(2025, 1, 15)
    _valid(qa, "investor_qa")
