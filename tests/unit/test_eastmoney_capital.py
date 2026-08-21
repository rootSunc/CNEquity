"""Offline coverage for EastMoney capital helpers + mocked fetch_* paths."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from cnequity.adapters.eastmoney import capital as cap
from cnequity.config import Config


def test_channel_and_margin_symbol():
    assert cap._channel("001") == "SH"
    assert cap._channel(1) == "SH"
    assert cap._channel("沪股通") == "SH"
    assert cap._channel("SH") == "SH"
    assert cap._channel("003") == "SZ"
    assert cap._channel("深股通") == "SZ"
    assert cap._channel("002") is None
    assert cap._channel(None) is None

    assert cap._margin_symbol({"SECUCODE": "600519.SH"}) == "600519.SH"
    assert cap._margin_symbol({"SCODE": "000001", "TRADE_MARKET": "深交所"}) == "000001.SZ"
    assert cap._margin_symbol({"SCODE": "600000", "TRADE_MARKET": "沪市"}) == "600000.SH"
    assert cap._margin_symbol({"SCODE": "430047", "TRADE_MARKET": "北交所"}) == "430047.BJ"
    assert cap._margin_symbol({"SCODE": "600000"}) == "600000.SH"
    assert cap._margin_symbol({"SCODE": "688001"}) == "688001.SH"


def test_quarter_end_dates_order_and_cutoff():
    periods = cap._quarter_end_dates(date(2016, 7, 1))
    assert periods[0] == "2016-06-30"
    assert "2016-03-31" in periods
    assert "2016-09-30" not in periods
    assert periods == sorted(periods, reverse=True)


def test_quarter_end_dates_honor_explicit_backfill_window():
    periods = cap._quarter_end_dates(
        date(2026, 7, 1), start=date(2024, 1, 1), end=date(2024, 6, 30)
    )
    assert periods == ["2024-06-30", "2024-03-31"]


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
        "clist_rows_to_symbols_tolerant",
        lambda rows, **kwargs: [("600519.SH", rows[0])],
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
                "DATE": "2025-01-02 00:00:00",
                "RZYE": 10,
                "RZMRE": 1,
                "RQYE": 2,
                "RQMCL": 3,
            },
            {
                "SECUCODE": "000001.SZ",
                "DATE": "2025-01-02 00:00:00",
                "RZYE": 10,
                "RZMRE": 1,
                "RQYE": 2,
                "RQMCL": 3,
            },
        ],
    )
    mdf = cap.fetch_margin_trading(date(2025, 1, 2), client=client)
    assert mdf.height == 1
    assert mdf["margin_balance"][0] == 10.0


def test_fund_flow_closes_owned_client_when_clist_fails(monkeypatch):
    created = []

    class _Client:
        closed = False

        def close(self):
            self.closed = True

    def _factory(**kwargs):
        client = _Client()
        created.append(client)
        return client

    monkeypatch.setattr(cap, "EastMoneyClient", _factory)
    monkeypatch.setattr(
        cap,
        "fetch_clist_pages",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("clist down")),
    )
    with pytest.raises(RuntimeError, match="clist down"):
        cap.fetch_fund_flow(date(2025, 1, 2))
    assert created[0].closed is True


def test_fund_flow_skips_unmappable_clist_rows(monkeypatch):
    monkeypatch.setattr(
        cap,
        "fetch_clist_pages",
        lambda *args, **kwargs: [{"f12": "600519", "f13": 1}, {"f12": "123456"}],
    )
    df = cap.fetch_fund_flow(date(2025, 1, 2), client=SimpleNamespace(close=lambda: None))
    assert df.height == 1
    assert df["symbol"][0] == "600519.SH"


def test_capital_adapters_close_owned_client_when_parsing_fails(monkeypatch):
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

    monkeypatch.setattr(cap, "EastMoneyClient", _factory)
    monkeypatch.setattr(cap, "fetch_clist_pages", lambda *args, **kwargs: [{}])
    monkeypatch.setattr(
        cap,
        "clist_rows_to_symbols_tolerant",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("bad clist row")),
    )
    with pytest.raises(RuntimeError, match="bad clist row"):
        cap.fetch_fund_flow(date(2025, 1, 2))

    monkeypatch.setattr(cap, "fetch_datacenter", lambda *args, **kwargs: [{}])
    monkeypatch.setattr(
        cap,
        "_rows_for_report_date",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("bad report row")),
    )
    for fetcher in (cap.fetch_margin_trading, cap.fetch_dragon_tiger, cap.fetch_block_trades):
        with pytest.raises(RuntimeError, match="bad report row"):
            fetcher(date(2025, 1, 2))

    assert len(created) == 4
    assert all(client.closed for client in created)


def test_capital_missing_numeric_fields_remain_null(monkeypatch):
    client = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(
        cap,
        "fetch_datacenter",
        lambda client, report, columns, filter_expr=None: (
            [
                {
                    "SECUCODE": "000001.SZ",
                    "DATE": "2025-01-02 00:00:00",
                    "RZYE": "",
                    "RZMRE": "bad",
                    "RQYE": None,
                    "RQMCL": "0",
                }
            ]
            if "RPTA_WEB_RZRQ" in report
            else [
                {
                    "SECURITY_CODE": "600519",
                    "TRADE_DATE": "2025-01-02 00:00:00",
                    "BILLBOARD_BUY_AMT": "",
                    "BILLBOARD_SELL_AMT": None,
                    "BILLBOARD_NET_AMT": "0",
                }
            ]
        ),
    )

    margin = cap.fetch_margin_trading(date(2025, 1, 2), client=client)
    assert margin.row(0, named=True) == {
        "symbol": "000001.SZ",
        "trade_date": date(2025, 1, 2),
        "margin_balance": None,
        "margin_buy": None,
        "short_balance": None,
        "short_sell_volume": 0.0,
    }

    dragon = cap.fetch_dragon_tiger(date(2025, 1, 2), client=client)
    assert dragon["buy_amount"][0] is None
    assert dragon["sell_amount"][0] is None
    assert dragon["net_amount"][0] == 0.0


def test_capital_holdings_and_block_missing_numeric_fields_remain_null(monkeypatch):
    client = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(cap, "_quarter_end_dates", lambda *args, **kwargs: ["2025-03-31"])
    monkeypatch.setattr(
        cap,
        "fetch_datacenter",
        lambda *a, **k: (
            [
                {
                    "SECUCODE": "600519.SH",
                    "TRADE_DATE": "2025-03-31 00:00:00",
                    "MUTUAL_TYPE": "001",
                    "HOLD_SHARES": "",
                    "HOLD_MARKET_CAP": None,
                    "HOLD_SHARES_RATIO": "bad",
                }
            ]
            if "RPT_MUTUAL_HOLDSTOCKNORTH_STA" in a[1]
            else [
                {
                    "SECURITY_CODE": "000001",
                    "TRADE_DATE": "2025-01-02 00:00:00",
                    "AVERAGE_PRICE": "",
                    "VOLUME": None,
                    "DEAL_AMT": "bad",
                    "PREMIUM_RATIO": "0",
                }
            ]
        ),
    )

    holdings = cap.fetch_northbound_holdings(date(2025, 3, 31), client=client)
    assert holdings.row(0, named=True)["holding_shares"] is None
    assert holdings.row(0, named=True)["holding_mv"] is None
    assert holdings.row(0, named=True)["holding_ratio"] is None

    block = cap.fetch_block_trades(date(2025, 1, 2), client=client)
    assert block.row(0, named=True)["price"] is None
    assert block.row(0, named=True)["volume"] is None
    assert block.row(0, named=True)["amount"] is None
    assert block.row(0, named=True)["premium_ratio"] == 0.0


def test_fetch_northbound_holdings_and_flows(monkeypatch):
    client = SimpleNamespace(
        close=lambda: None,
        get=lambda url, **k: (_ for _ in ()).throw(RuntimeError("offline")),
    )
    monkeypatch.setattr(cap, "_quarter_end_dates", lambda _: ["2025-03-31"])
    monkeypatch.setattr(
        cap,
        "fetch_datacenter",
        lambda *a, **k: [
            {
                "SECUCODE": "600519.SH",
                "TRADE_DATE": "2025-03-31 00:00:00",
                "MUTUAL_TYPE": "001",
                "HOLD_SHARES": 100,
                "HOLD_MARKET_CAP": 200,
                "HOLD_SHARES_RATIO": 0.1,
            },
            {
                "SECUCODE": "600519.SH",
                "TRADE_DATE": "2025-03-31 00:00:00",
                "MUTUAL_TYPE": "001",
                "HOLD_SHARES": 100,
                "HOLD_MARKET_CAP": 200,
                "HOLD_SHARES_RATIO": 0.1,
            },
        ],
    )
    hdf = cap.fetch_northbound_holdings(date(2025, 3, 31), client=client)
    assert hdf.height == 1
    assert set(hdf["channel"].to_list()) == {"SH"}


def test_northbound_holdings_skips_unknown_mutual_channels(monkeypatch):
    client = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(cap, "_quarter_end_dates", lambda _: ["2025-03-31"])
    monkeypatch.setattr(
        cap,
        "fetch_datacenter",
        lambda *a, **k: [
            {
                "SECUCODE": "600519.SH",
                "TRADE_DATE": "2025-03-31 00:00:00",
                "MUTUAL_TYPE": "005",  # 北向合计，不是一个 exchange leg
                "HOLD_SHARES": 100,
                "HOLD_MARKET_CAP": 200,
                "HOLD_SHARES_RATIO": 0.1,
            },
            {
                "SECUCODE": "600519.SH",
                "TRADE_DATE": "2025-03-31 00:00:00",
                "MUTUAL_TYPE": "003",
                "HOLD_SHARES": 100,
                "HOLD_MARKET_CAP": 200,
                "HOLD_SHARES_RATIO": 0.1,
            },
        ],
    )

    holdings = cap.fetch_northbound_holdings(date(2025, 3, 31), client=client)

    assert holdings.height == 1
    assert holdings["channel"].to_list() == ["SZ"]


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


def test_northbound_flows_drops_scaled_overflow_rows(monkeypatch):
    _patch_mutual_report(
        monkeypatch,
        [
            _mutual_row("001", "2024-08-16", 1e308, 2.0, 1.0),
            _mutual_row("001", "2024-08-16", 1.0, 2.0, 1.0),
        ],
    )
    df = cap.fetch_northbound_flows(date(2024, 8, 16), client=_offline_client())
    assert df.height == 1
    assert df["net_buy"][0] == 1_000_000.0


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
                    "TRADE_DATE": "2025-01-02 00:00:00",
                    "EXPLANATION": "涨幅偏离值达7%",
                    "BILLBOARD_BUY_AMT": 1,
                    "BILLBOARD_SELL_AMT": 2,
                    "BILLBOARD_NET_AMT": -1,
                },
                {
                    "SECURITY_CODE": "600519",
                    "TRADE_DATE": "2025-01-02 00:00:00",
                    "EXPLANATION": "涨幅偏离值达7%",
                    "BILLBOARD_BUY_AMT": 1,
                    "BILLBOARD_SELL_AMT": 2,
                    "BILLBOARD_NET_AMT": -1,
                },
            ]
            if "BILLBOARD" in columns
            else [
                {
                    "SECURITY_CODE": "000001",
                    "TRADE_DATE": "2025-01-02 00:00:00",
                    "AVERAGE_PRICE": 10,
                    "VOLUME": 100,
                    "DEAL_AMT": 1000,
                    "PREMIUM_RATIO": 0.01,
                },
                {
                    "SECURITY_CODE": "000001",
                    "TRADE_DATE": "2025-01-02 00:00:00",
                    "AVERAGE_PRICE": 10,
                    "VOLUME": 100,
                    "DEAL_AMT": 1000,
                    "PREMIUM_RATIO": 0.01,
                },
            ]
        ),
    )

    ddf = cap.fetch_dragon_tiger(date(2025, 1, 2), client=client)
    assert ddf.height == 1
    assert ddf["symbol"][0] == "600519.SH"

    bdf = cap.fetch_block_trades(date(2025, 1, 2), client=client)
    assert bdf.height == 1
    assert bdf["symbol"][0] == "000001.SZ"


def test_capital_adapters_drop_rows_from_a_different_trade_date(monkeypatch):
    client = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(
        cap,
        "fetch_datacenter",
        lambda *a, **k: [
            {
                "SECUCODE": "000001.SZ",
                "DATE": "2025-01-01 00:00:00",
                "RZYE": 10,
            }
        ],
    )
    with pytest.raises(RuntimeError, match="no DATE row"):
        cap.fetch_margin_trading(date(2025, 1, 2), client=client)

    monkeypatch.setattr(
        cap,
        "fetch_datacenter",
        lambda *a, **k: [
            {
                "SECURITY_CODE": "600519",
                "TRADE_DATE": "2025-01-01 00:00:00",
            }
        ],
    )
    with pytest.raises(RuntimeError, match="no TRADE_DATE row"):
        cap.fetch_dragon_tiger(date(2025, 1, 2), client=client)
    with pytest.raises(RuntimeError, match="no TRADE_DATE row"):
        cap.fetch_block_trades(date(2025, 1, 2), client=client)


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
