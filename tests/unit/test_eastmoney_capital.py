"""Offline coverage for EastMoney capital helpers + mocked fetch_* paths."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl

from cn_market_lake.adapters.eastmoney import capital as cap
from cn_market_lake.config import Config


def test_channel_and_margin_symbol():
    assert cap._channel("001") == "SH"
    assert cap._channel(1) == "SH"
    assert cap._channel("沪股通") == "SH"
    assert cap._channel("SH") == "SH"
    assert cap._channel("002") == "SZ"
    assert cap._channel(None) == "SZ"

    assert cap._margin_symbol({"SECUCODE": "600519.SH"}) == "600519.SH"
    assert cap._margin_symbol({"SCODE": "000001", "TRADE_MARKET": "深交所"}) == "000001.SZ"
    assert cap._margin_symbol({"SCODE": "600000", "TRADE_MARKET": "沪市"}) == "600000.SH"
    assert cap._margin_symbol({"SCODE": "430047", "TRADE_MARKET": "北交所"}) == "430047.BJ"


def test_quarter_end_dates_order_and_cutoff():
    periods = cap._quarter_end_dates(date(2016, 7, 1))
    assert periods[0] == "2016-06-30"
    assert "2016-03-31" in periods
    assert "2016-09-30" not in periods
    assert periods == sorted(periods, reverse=True)


def test_fetch_fund_flow_and_margin(monkeypatch):
    monkeypatch.setattr(
        cap,
        "fetch_clist_pages",
        lambda client, fields: [
            {"f12": "600519", "f13": 1, "f62": 1, "f66": 2, "f72": 3, "f78": 4, "f84": 5}
        ],
    )
    monkeypatch.setattr(
        cap,
        "clist_rows_to_symbols",
        lambda rows: [("600519.SH", rows[0])],
    )
    client = SimpleNamespace(close=lambda: None)
    df = cap.fetch_fund_flow(date(2025, 1, 2), client=client)
    assert df.height == 1
    assert df["main_net_inflow"][0] == 1.0

    monkeypatch.setattr(
        cap,
        "fetch_datacenter",
        lambda *a, **k: [
            {
                "SECUCODE": "000001.SZ",
                "RZYE": 10,
                "RZMRE": 1,
                "RQYE": 2,
                "RQMCL": 3,
            }
        ],
    )
    mdf = cap.fetch_margin_trading(date(2025, 1, 2), client=client)
    assert mdf.height == 1
    assert mdf["margin_balance"][0] == 10.0


def test_fetch_northbound_holdings_and_flows(monkeypatch):
    client = SimpleNamespace(
        close=lambda: None,
        get=lambda url, **k: (_ for _ in ()).throw(RuntimeError("offline")),
    )
    monkeypatch.setattr(
        cap,
        "fetch_datacenter",
        lambda *a, **k: [
            {
                "SECUCODE": "600519.SH",
                "MUTUAL_TYPE": "001",
                "HOLD_SHARES": 100,
                "HOLD_MARKET_CAP": 200,
                "HOLD_SHARES_RATIO": 0.1,
            }
        ],
    )
    hdf = cap.fetch_northbound_holdings(date(2025, 3, 31), client=client)
    assert hdf.height >= 1
    assert set(hdf["channel"].to_list()) == {"SH"}


def _mutual_row(mutual_type: str, day: str, net, buy=None, sell=None) -> dict:
    return {
        "MUTUAL_TYPE": mutual_type,
        "TRADE_DATE": f"{day} 00:00:00",
        "NET_DEAL_AMT": net,
        "BUY_AMT": buy,
        "SELL_AMT": sell,
    }


def _patch_mutual_report(monkeypatch, rows: list[dict]) -> None:
    monkeypatch.setattr(cap, "fetch_datacenter", lambda *a, **k: rows)


def _offline_client() -> SimpleNamespace:
    return SimpleNamespace(
        close=lambda: None,
        get=lambda url, **k: (_ for _ in ()).throw(RuntimeError("offline")),
    )


def test_northbound_flows_keeps_only_northbound_legs_and_scales_to_yuan(monkeypatch):
    # 002/004/006 are the southbound legs — same report, wrong direction.
    _patch_mutual_report(
        monkeypatch,
        [
            _mutual_row("001", "2024-08-16", -2568.22, 22080.14, 24648.36),
            _mutual_row("003", "2024-08-16", -4206.77, 20561.17, 24767.94),
            _mutual_row("005", "2024-08-16", -6774.99, 42641.31, 49416.30),
            _mutual_row("002", "2024-08-16", 2820.59, 9200.12, 6379.53),
        ],
    )
    df = cap.fetch_northbound_flows(date(2024, 8, 16), client=_offline_client())
    assert df.height == 2
    assert set(df["channel"].to_list()) == {"SH", "SZ"}
    sh = df.filter(pl.col("channel") == "SH")
    assert sh["net_buy"][0] == -2568.22 * 1_000_000
    assert sh["buy_amount"][0] == 22080.14 * 1_000_000
    assert sh["sell_amount"][0] == 24648.36 * 1_000_000


def test_northbound_flows_drops_unpublished_days_instead_of_zero_filling(monkeypatch):
    # From 2024-08-19 the exchanges stopped publishing; the column is null.
    # A zero here would claim a flat session where no figure exists at all.
    _patch_mutual_report(
        monkeypatch,
        [
            _mutual_row("001", "2024-08-16", -2568.22, 22080.14, 24648.36),
            _mutual_row("001", "2024-08-19", None, None, None),
            _mutual_row("003", "2024-08-19", None, None, None),
        ],
    )
    df = cap.fetch_northbound_flows_range(
        date(2024, 8, 16), date(2024, 8, 19), client=_offline_client()
    )
    assert df["trade_date"].to_list() == [date(2024, 8, 16)]
    assert 0.0 not in df["net_buy"].to_list()


def test_northbound_flows_windows_client_side(monkeypatch):
    # The report rejects range predicates on TRADE_DATE, so the whole series
    # comes back and the window is applied here.
    _patch_mutual_report(
        monkeypatch,
        [
            _mutual_row("001", "2024-08-13", -862.15, 21148.25, 22010.40),
            _mutual_row("001", "2024-08-14", -3060.70, 18531.00, 21591.70),
            _mutual_row("001", "2024-08-15", 8865.01, 31345.92, 22480.91),
        ],
    )
    df = cap.fetch_northbound_flows_range(
        date(2024, 8, 14), date(2024, 8, 14), client=_offline_client()
    )
    assert df["trade_date"].to_list() == [date(2024, 8, 14)]


def test_northbound_flows_tolerates_unparseable_trade_date(monkeypatch):
    _patch_mutual_report(
        monkeypatch,
        [
            {"MUTUAL_TYPE": "001", "TRADE_DATE": "not-a-date", "NET_DEAL_AMT": 1.0},
            {"MUTUAL_TYPE": "001", "TRADE_DATE": None, "NET_DEAL_AMT": 1.0},
            _mutual_row("001", "2024-08-16", 1.0, 2.0, 1.0),
        ],
    )
    df = cap.fetch_northbound_flows(date(2024, 8, 16), client=_offline_client())
    assert df.height == 1


def test_em_datetime_to_date():
    assert cap._em_datetime_to_date("2024-08-16 00:00:00") == date(2024, 8, 16)
    assert cap._em_datetime_to_date("2024-08-16") == date(2024, 8, 16)
    assert cap._em_datetime_to_date("not-a-date") is None
    assert cap._em_datetime_to_date("") is None
    assert cap._em_datetime_to_date(None) is None


def test_fetch_dragon_tiger_and_block_trades(monkeypatch):
    client = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(
        cap,
        "fetch_datacenter",
        lambda client, report, columns, filter_expr=None: (
            [
                {
                    "SECURITY_CODE": "600519",
                    "EXPLANATION": "涨幅偏离值达7%",
                    "BILLBOARD_BUY_AMT": 1,
                    "BILLBOARD_SELL_AMT": 2,
                    "BILLBOARD_NET_AMT": -1,
                }
            ]
            if "BILLBOARD" in columns
            else [
                {
                    "SECURITY_CODE": "000001",
                    "AVERAGE_PRICE": 10,
                    "VOLUME": 100,
                    "DEAL_AMT": 1000,
                    "PREMIUM_RATIO": 0.01,
                }
            ]
        ),
    )

    ddf = cap.fetch_dragon_tiger(date(2025, 1, 2), client=client)
    assert ddf.height == 1
    assert ddf["symbol"][0] == "600519.SH"

    bdf = cap.fetch_block_trades(date(2025, 1, 2), client=client)
    assert bdf.height == 1
    assert bdf["symbol"][0] == "000001.SZ"


def test_fetch_fund_flow_and_margin_pass_config_to_client(monkeypatch, tmp_path):
    """Daily capital must not build a bare EastMoneyClient (1s in-process only)."""
    cfg = Config(data_root=tmp_path / "data")
    seen: list[object] = []

    class FakeClient:
        def __init__(self, *args, config=None, **kwargs):
            seen.append(config)

        def close(self) -> None:
            return None

    monkeypatch.setattr(cap, "EastMoneyClient", FakeClient)
    monkeypatch.setattr(cap, "fetch_clist_pages", lambda client, fields: [])
    monkeypatch.setattr(cap, "fetch_datacenter", lambda *a, **k: [])

    cap.fetch_fund_flow(date(2025, 1, 2), config=cfg)
    cap.fetch_margin_trading(date(2025, 1, 2), config=cfg)

    assert seen == [cfg, cfg]
