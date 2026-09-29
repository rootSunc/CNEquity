from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import polars as pl
import pytest

from cnequity.adapters.qmt_bridge import (
    QmtBridgeSourceError,
    fetch_adj_factors_qmt,
    fetch_corporate_actions_qmt,
    fetch_daily_bars_qmt,
    fetch_financial_statement_items_qmt,
    fetch_index_bars_qmt,
    fetch_index_constituents_qmt,
    fetch_instruments_qmt,
    fetch_minute_bars_qmt,
    fetch_shareholder_counts_qmt,
    fetch_trading_calendar_qmt,
)
from cnequity.config import Config
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.orchestrator.worker_pool import _fetch_daily_bars_with_priority
from cnequity.steps import events, reference


class _ShareholderCountsFakeXtData:
    def __init__(self):
        self.calls: list[tuple[list[str], str, str, str]] = []

    def get_holder_num(self, symbols, start, end, report_type):
        self.calls.append((list(symbols), start, end, report_type))
        if report_type == "report_time":
            frame = pd.DataFrame(
                {
                    "stockCode": ["600000.SH", "600000.SH", "600000.SH", "000001.SZ", "600000.SH"],
                    "timetag": ["20231231", "20240331", "20240630", "20240331", "20240930"],
                    "holdNum": [200000.0, 200000.0, 190000.0, 180000.0, 150000.0],
                }
            )
        else:
            frame = pd.DataFrame(
                {
                    "stockCode": ["600000.SH", "600000.SH", "000001.SZ", "600000.SH"],
                    "timetag": ["20240420", "20240820", "20240421", "20241025"],
                    "holdNum": [200000.0, 190000.0, 180000.0, 160000.0],
                }
            )
        return frame[
            frame["stockCode"].isin(symbols) & frame["timetag"].between(str(start), str(end))
        ]


def test_fetch_shareholder_counts_qmt_pairs_dates_and_drops_unmatched():
    xt = _ShareholderCountsFakeXtData()
    metrics: dict = {}
    frame = fetch_shareholder_counts_qmt(
        ["600000.SH", "000001.SZ"],
        date(2024, 1, 1),
        date(2024, 6, 30),
        by="change_date",
        config=None,
        xtdata=xt,
        metrics=metrics,
    ).sort(["count_date", "symbol"])

    assert [(calls[3], calls[1], calls[2]) for calls in xt.calls] == [
        ("report_time", "20240101", "20240630"),
        ("announce_time", "20240101", "20251112"),
    ]
    assert frame.height == 3
    assert frame["symbol"].to_list() == ["000001.SZ", "600000.SH", "600000.SH"]
    for name in ("holder_count_change_pct", "avg_float_shares", "avg_holding_value"):
        assert frame[name].null_count() == frame.height
    assert frame["source"].unique().to_list() == ["qmt_bridge"]
    assert metrics["requests"] == 2
    assert metrics["rows_read"] == 3


def test_fetch_shareholder_counts_qmt_windows_on_notice_date():
    xt = _ShareholderCountsFakeXtData()
    frame = fetch_shareholder_counts_qmt(
        ["600000.SH"],
        date(2024, 4, 1),
        date(2024, 4, 30),
        by="notice_date",
        config=None,
        xtdata=xt,
    )

    assert xt.calls[1][3] == "announce_time"
    assert xt.calls[1][1] == "20240401"
    assert xt.calls[1][2] == "20240430"
    assert frame.height == 1
    assert frame["count_date"].to_list() == [date(2023, 12, 31)]
    assert frame["announce_date"].to_list() == [date(2024, 4, 20)]


def test_fetch_shareholder_counts_qmt_accepts_empty_responses():
    class _Empty:
        def get_holder_num(self, *args, **kwargs):
            return None

    frame = fetch_shareholder_counts_qmt(
        ["600000.SH"],
        date(2024, 1, 1),
        date(2024, 1, 31),
        config=None,
        xtdata=_Empty(),
    )
    assert frame.is_empty()


def test_fetch_shareholder_counts_qmt_raises_when_every_chunk_fails():
    class _Failing:
        def get_holder_num(self, *args, **kwargs):
            raise RuntimeError("bridge down")

    with pytest.raises(QmtBridgeSourceError, match="shareholder-count"):
        fetch_shareholder_counts_qmt(
            ["600000.SH"],
            date(2024, 1, 1),
            date(2024, 1, 31),
            config=None,
            xtdata=_Failing(),
        )


class _FakeXtData:
    def __init__(self, volume: int):
        self.volume = volume
        self.downloads: list[list[str]] = []

    def download_history_data2(self, stock_list, period, **kwargs):
        assert period == "1d"
        self.downloads.append(list(stock_list))
        return {"finished": len(stock_list), "total": len(stock_list)}

    def get_local_data(self, *, stock_list, **kwargs):
        return {
            symbol: pd.DataFrame(
                {
                    "open": [10.0],
                    "high": [11.0],
                    "low": [9.0],
                    "close": [10.0],
                    "volume": [self.volume],
                    "amount": [10.0 * self.volume],
                },
                index=pd.Index(["20240628"], name="date"),
            )
            for symbol in stock_list
        }


def test_fetch_daily_bars_qmt_parses_shares():
    xt = _FakeXtData(volume=1_000)
    frame = fetch_daily_bars_qmt(
        ["600000.SH"],
        date(2024, 6, 28),
        date(2024, 6, 28),
        config=None,
        xtdata=xt,
    )
    assert frame.height == 1
    assert frame["source"][0] == "qmt_bridge"
    assert frame["volume"][0] == 1_000
    assert xt.downloads == [["600000.SH"]]


class _LotsFakeXtData(_FakeXtData):
    def get_local_data(self, *, stock_list, **kwargs):
        return {
            symbol: pd.DataFrame(
                {
                    "open": [10.0],
                    "high": [11.0],
                    "low": [9.0],
                    "close": [10.0],
                    "volume": [10],
                    "amount": [10000.0],
                },
                index=pd.Index(["20240628"], name="date"),
            )
            for symbol in stock_list
        }


def test_fetch_daily_bars_qmt_infers_lots():
    frame = fetch_daily_bars_qmt(
        ["600000.SH"],
        date(2024, 6, 28),
        date(2024, 6, 28),
        config=None,
        xtdata=_LotsFakeXtData(volume=10),
    )
    assert frame["volume"][0] == 1_000


class _ZeroVolumeFakeXtData(_FakeXtData):
    def get_local_data(self, *, stock_list, **kwargs):
        return {
            symbol: pd.DataFrame(
                {
                    "open": [10.0, 11.0],
                    "high": [11.0, 12.0],
                    "low": [9.0, 10.0],
                    "close": [10.0, 11.0],
                    "volume": [0, 1_000],
                    "amount": [0.0, 11_000.0],
                },
                index=pd.Index(["20240627", "20240628"], name="date"),
            )
            for symbol in stock_list
        }


def test_fetch_daily_bars_qmt_skips_zero_volume_placeholder_bars():
    frame = fetch_daily_bars_qmt(
        ["600000.SH"],
        date(2024, 6, 27),
        date(2024, 6, 28),
        config=None,
        xtdata=_ZeroVolumeFakeXtData(volume=1_000),
    )
    assert frame.height == 1
    assert frame["trade_date"][0] == date(2024, 6, 28)


class _PeriodFakeXtData:
    def __init__(self, period: str):
        self.period = period
        self.periods: list[str] = []

    def download_history_data2(self, stock_list, period, **kwargs):
        self.periods.append(period)
        return {"finished": len(stock_list), "total": len(stock_list)}

    def get_local_data(self, *, stock_list, **kwargs):
        if self.period == "1d":
            index = pd.Index(["20240628"], name="date")
        else:
            index = pd.Index([datetime(2024, 6, 28, 9, 31)], name="date")
        return {
            symbol: pd.DataFrame(
                {
                    "open": [10.0],
                    "high": [11.0],
                    "low": [9.0],
                    "close": [10.0],
                    "volume": [1_000],
                    "amount": [10_000.0],
                },
                index=index,
            )
            for symbol in stock_list
        }


class _AdjFactorsFakeXtData:
    def __init__(self):
        self.downloads: list[str] = []

    def download_history_data2(self, stock_list, period, **kwargs):
        assert period == "1d"
        self.downloads.append(str(kwargs["dividend_type"]))
        return {"finished": len(stock_list), "total": len(stock_list)}

    def get_local_data(self, *, dividend_type, **kwargs):
        assert kwargs["stock_list"] == ["600000.SH"]
        assert kwargs["period"] == "1d"
        closes = [10.0, 20.0] if dividend_type == "none" else [10.0, 21.0]
        return {
            "600000.SH": pd.DataFrame(
                {
                    "close": closes,
                    "volume": [1_000, 1_100],
                    "amount": [10_000.0, 22_000.0],
                },
                index=pd.Index(["20240627", "20240628"], name="date"),
            )
        }


def test_fetch_adj_factors_qmt_divides_back_adjusted_by_raw_closes():
    xt = _AdjFactorsFakeXtData()
    metrics: dict = {}
    frame = fetch_adj_factors_qmt(
        ["600000.SH"],
        date(2024, 6, 27),
        date(2024, 6, 28),
        config=None,
        xtdata=xt,
        metrics=metrics,
    ).sort("trade_date")

    assert set(xt.downloads) == {"none", "back"}
    assert frame["symbol"].to_list() == ["600000.SH", "600000.SH"]
    assert frame["trade_date"].to_list() == [date(2024, 6, 27), date(2024, 6, 28)]
    assert frame["factor"].to_list() == [1.0, 1.05]
    assert frame["source"].unique().to_list() == ["qmt_bridge"]
    assert frame["data_version"].unique().to_list() == [data_version_for("adj_factors")]
    assert metrics["rows_read"] == 2


def test_fetch_index_bars_qmt_parses_daily_index_rows():
    xt = _PeriodFakeXtData("1d")
    frame = fetch_index_bars_qmt(
        ["000300.SH"],
        date(2024, 6, 28),
        date(2024, 6, 28),
        config=None,
        xtdata=xt,
    )
    assert xt.periods == ["1d"]
    assert frame["frequency"][0] == "1d"
    assert frame["volume"][0] == 1_000


def test_fetch_minute_bars_qmt_parses_timestamps():
    xt = _PeriodFakeXtData("1m")
    frame = fetch_minute_bars_qmt(
        ["600000.SH"],
        date(2024, 6, 28),
        date(2024, 6, 28),
        frequency="1m",
        config=None,
        xtdata=xt,
    )
    assert xt.periods == ["1m"]
    assert frame["frequency"][0] == "1m"
    assert frame["bar_time"][0] == datetime(2024, 6, 28, 9, 31)
    assert frame["trade_date"][0] == date(2024, 6, 28)


def test_fetch_daily_bars_qmt_requires_a_bridge_client(monkeypatch):
    config = Config(data_root=".")
    config.qmt_bridge_enabled = True
    config.qmt_bridge_src_path = "does-not-exist"
    monkeypatch.setattr(
        "cnequity.adapters.qmt_bridge._xtdata",
        lambda config: (_ for _ in ()).throw(QmtBridgeSourceError("bridge unavailable")),
    )
    with pytest.raises(QmtBridgeSourceError):
        fetch_daily_bars_qmt(["600000.SH"], date(2024, 6, 28), date(2024, 6, 28), config=config)


def test_priority_adapter_falls_back_to_tdx(monkeypatch):
    config = Config(data_root=".")
    config.qmt_bridge_enabled = True
    metrics: dict[str, int] = {}

    def _raise(*args, **kwargs):
        raise QmtBridgeSourceError("bridge down")

    def _tdx(symbols, start, end, **kwargs):
        return pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [end] * len(symbols),
                "open": [10.0] * len(symbols),
                "high": [10.0] * len(symbols),
                "low": [10.0] * len(symbols),
                "close": [10.0] * len(symbols),
                "volume": [100] * len(symbols),
                "amount": [1000.0] * len(symbols),
            }
        )

    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars_qmt", _raise)
    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _tdx)
    frame, source = _fetch_daily_bars_with_priority(
        ["600000.SH"],
        date(2024, 6, 28),
        date(2024, 6, 28),
        config=config,
        rate_limit=None,
        allow_mock=False,
        backfill=False,
        on_heartbeat=lambda: None,
        metrics=metrics,
    )
    assert source == "tdx_protocol"
    assert frame.height == 1
    assert metrics["fallback_requests"] == 1


def test_priority_adapter_keeps_mixed_source_provenance(monkeypatch):
    config = Config(data_root=".")
    config.qmt_bridge_enabled = True
    qmt_symbol = "600000.SH"
    tdx_symbol = "000001.SZ"
    trade_date = date(2024, 6, 28)
    qmt_frame = with_provenance(
        pl.DataFrame(
            {
                "symbol": [qmt_symbol],
                "trade_date": [trade_date],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.0],
                "volume": [1_000],
                "amount": [10_000.0],
            }
        ),
        source="qmt_bridge",
        data_version=data_version_for("daily_bars"),
    )

    def _tdx(symbols, start, end, **kwargs):
        assert symbols == [tdx_symbol]
        return pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [end] * len(symbols),
                "open": [20.0] * len(symbols),
                "high": [21.0] * len(symbols),
                "low": [19.0] * len(symbols),
                "close": [20.0] * len(symbols),
                "volume": [2_000] * len(symbols),
                "amount": [40_000.0] * len(symbols),
            }
        )

    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.fetch_daily_bars_qmt",
        lambda *args, **kwargs: qmt_frame,
    )
    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _tdx)
    frame, source = _fetch_daily_bars_with_priority(
        [qmt_symbol, tdx_symbol],
        trade_date,
        trade_date,
        config=config,
        rate_limit=None,
        allow_mock=False,
        backfill=False,
        on_heartbeat=lambda: None,
        metrics={},
    )
    assert source == "mixed"
    assert set(frame["source"]) == {"qmt_bridge", "tdx_protocol"}


def test_priority_adapter_fills_symbol_date_gaps(monkeypatch):
    config = Config(data_root=".")
    config.qmt_bridge_enabled = True
    session_one = date(2024, 6, 27)
    session_two = date(2024, 6, 28)
    qmt_symbol = "600000.SH"
    tdx_symbol = "000001.SZ"
    qmt_frame = with_provenance(
        pl.DataFrame(
            {
                "symbol": [qmt_symbol, tdx_symbol],
                "trade_date": [session_one, session_one],
                "open": [10.0, 20.0],
                "high": [11.0, 21.0],
                "low": [9.0, 19.0],
                "close": [10.0, 20.0],
                "volume": [1_000, 2_000],
                "amount": [10_000.0, 40_000.0],
            }
        ),
        source="qmt_bridge",
        data_version=data_version_for("daily_bars"),
    )
    calls: list[tuple[list[str], date, date]] = []

    def _tdx(symbols, start, end, **kwargs):
        calls.append((list(symbols), start, end))
        return pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [session_two] * len(symbols),
                "open": [11.0, 21.0],
                "high": [12.0, 22.0],
                "low": [10.0, 20.0],
                "close": [11.0, 21.0],
                "volume": [1_100, 2_100],
                "amount": [12_100.0, 44_100.0],
            }
        )

    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.list_trading_dates",
        lambda config, start, end: [session_one, session_two],
    )
    monkeypatch.setattr(
        "cnequity.orchestrator.worker_pool.fetch_daily_bars_qmt",
        lambda *args, **kwargs: qmt_frame,
    )
    monkeypatch.setattr("cnequity.orchestrator.worker_pool.fetch_daily_bars", _tdx)
    frame, source = _fetch_daily_bars_with_priority(
        [qmt_symbol, tdx_symbol],
        session_one,
        session_two,
        config=config,
        rate_limit=None,
        allow_mock=False,
        backfill=False,
        on_heartbeat=lambda: None,
        metrics={},
    )
    assert source == "mixed"
    assert calls == [([qmt_symbol, tdx_symbol], session_two, session_two)]
    assert frame.height == 4
    assert frame.filter(pl.col("trade_date") == session_two)["source"].unique().to_list() == [
        "tdx_protocol"
    ]


class _CorporateActionsFakeXtData:
    def __init__(self, failed_symbols: set[str] | None = None):
        self.failed_symbols = failed_symbols or set()

    def get_divid_factors(self, symbol):
        if symbol in self.failed_symbols:
            raise RuntimeError("bridge failed")
        return pd.DataFrame(
            {
                "time": [1.0, 2.0, 3.0, 4.0],
                "interest": [0.42, 0.0, 0.0, 0.0],
                "stockBonus": [0.0, 0.3, 0.0, 0.0],
                "stockGift": [0.0, 0.0, 0.2, 0.0],
                "allotNum": [0.0, 0.0, 0.0, 0.2],
                "allotPrice": [0.0, 0.0, 0.0, 8.5],
                "gugai": [0.0, 0.0, 0.0, 0.0],
                "dr": [1.047, 1.3, 1.2, 1.1],
            },
            index=pd.Index(["20260716", "20250716", "20240718", "20230721"], name="date"),
        )


def test_fetch_corporate_actions_qmt_splits_events_by_action_type():
    frame = fetch_corporate_actions_qmt(
        ["600000.SH"],
        date(2023, 1, 1),
        date(2026, 12, 31),
        config=None,
        xtdata=_CorporateActionsFakeXtData(),
    )
    assert frame.height == 4
    assert set(frame["action_type"]) == {"cash_dividend", "bonus", "transfer", "allotment"}
    cash = frame.filter(pl.col("action_type") == "cash_dividend")
    allotment = frame.filter(pl.col("action_type") == "allotment")
    assert cash["cash_dividend"][0] == 0.42
    assert allotment["allotment_ratio"][0] == 0.2
    assert allotment["allotment_price"][0] == 8.5
    assert frame["source"].unique().to_list() == ["qmt_bridge"]


def test_fetch_corporate_actions_qmt_raises_when_every_symbol_fails():
    with pytest.raises(QmtBridgeSourceError):
        fetch_corporate_actions_qmt(
            ["600000.SH"],
            date(2026, 1, 1),
            date(2026, 12, 31),
            config=None,
            xtdata=_CorporateActionsFakeXtData({"600000.SH"}),
        )


class _TradingCalendarFakeXtData:
    def get_trading_dates(self, market, start_time, end_time):
        assert market == "SH"
        return ["20240701", "20240702", "20240703", "20240704", "20240705"]


def test_fetch_trading_calendar_qmt_materialises_non_sessions():
    frame = fetch_trading_calendar_qmt(
        date(2024, 7, 1),
        date(2024, 7, 7),
        config=None,
        xtdata=_TradingCalendarFakeXtData(),
    )
    assert frame.height == 7
    assert frame.filter(pl.col("is_trading")).height == 5
    assert frame.filter(~pl.col("is_trading"))["trade_date"].to_list() == [
        date(2024, 7, 6),
        date(2024, 7, 7),
    ]
    assert frame["source"].unique().to_list() == ["qmt_bridge"]


class _FinancialsFakeXtData:
    def __init__(self, failed_tables: set[str] | None = None):
        self.failed_tables = failed_tables or set()

    def get_financial_data(self, symbols, tables, start, end, **kwargs):
        assert symbols == ["600000.SH"]
        assert start == "20240101"
        assert end == "20241231"
        assert kwargs["report_type"] == "announce_time"
        table = tables[0]
        if table in self.failed_tables:
            raise RuntimeError("bridge failed")
        fields = {
            "ASHAREBALANCESHEET": {"tot_assets": 1.0},
            "ASHAREINCOME": {"revenue": 2.0},
            "ASHARECASHFLOW": {"net_cash_flows_oper_act": 3.0},
            "PERSHAREINDEX": {"s_fa_eps_basic": 4.0},
        }[table]
        return pd.DataFrame(
            [
                {"m_timetag": "2024-03-31", "m_anntime": "2024-04-20", **fields},
                {"m_timetag": "2024-06-30", "m_anntime": None, **fields},
            ]
        )


def test_fetch_financial_statement_items_qmt_parses_canonical_items():
    metrics: dict = {}
    frame = fetch_financial_statement_items_qmt(
        ["600000.SH"],
        date(2024, 1, 1),
        date(2024, 12, 31),
        config=None,
        xtdata=_FinancialsFakeXtData(),
        metrics=metrics,
    )
    assert frame.height == 4
    assert set(frame["statement_type"]) == {"balance", "income", "cashflow", "indicator"}
    assert frame.filter(pl.col("statement_type") == "balance")["item_code"].to_list() == [
        "total_assets"
    ]
    assert frame.filter(pl.col("statement_type") == "income")["item_code"].to_list() == ["revenue"]
    assert frame.filter(pl.col("statement_type") == "cashflow")["item_code"].to_list() == [
        "net_cash_operate"
    ]
    assert frame.filter(pl.col("statement_type") == "indicator")["item_code"].to_list() == ["eps"]
    assert frame["announce_date"].unique().to_list() == [date(2024, 4, 20)]
    assert frame["source"].unique().to_list() == ["qmt_bridge"]
    assert metrics["requests"] == 4
    assert metrics["rows_read"] == 4


def test_fetch_financial_statement_items_qmt_raises_when_all_tables_fail():
    with pytest.raises(QmtBridgeSourceError):
        fetch_financial_statement_items_qmt(
            ["600000.SH"],
            date(2024, 1, 1),
            date(2024, 12, 31),
            config=None,
            xtdata=_FinancialsFakeXtData(
                {"ASHAREBALANCESHEET", "ASHAREINCOME", "ASHARECASHFLOW", "PERSHAREINDEX"}
            ),
        )


class _InstrumentsFakeXtData:
    def __init__(self):
        self._details = {
            "600000.SH": {
                "InstrumentName": "浦发银行",
                "OpenDate": 19991110,
                "ExpireDate": 99999999,
                "ExchangeID": "SH",
            },
            "000001.SZ": {
                "InstrumentName": "平安银行",
                "OpenDate": 19910403,
                "ExpireDate": 99999999,
                "ExchangeID": "SZ",
            },
            "510300.SH": {
                "InstrumentName": "沪深300ETF",
                "OpenDate": 20120528,
                "ExpireDate": 99999999,
                "ExchangeID": "SH",
            },
            "6000002.SH": {
                "InstrumentName": "old name",
                "OpenDate": 0,
                "ExpireDate": 20200101,
                "ExchangeID": "SH",
            },
        }

    def get_stock_list_in_sector(self, sector_name):
        if sector_name == "沪深A股":
            return ["600000.SH", "000001.SZ", "6000002.SH"]
        if sector_name == "沪深ETF":
            return ["510300.SH"]
        return []

    def get_instrument_detail(self, symbol):
        return self._details.get(symbol, {})


def test_fetch_instruments_qmt_parses_stocks_and_etfs():
    metrics: dict = {}
    frame = fetch_instruments_qmt(config=None, xtdata=_InstrumentsFakeXtData(), metrics=metrics)
    assert frame.height == 4
    assert set(frame["asset_type"]) == {"stock", "etf"}
    stocks = frame.filter(pl.col("asset_type") == "stock").sort("symbol")
    assert stocks["symbol"].to_list() == ["000001.SZ", "600000.SH", "6000002.SH"]
    assert stocks["name"].to_list() == ["平安银行", "浦发银行", "old name"]
    assert stocks.filter(pl.col("symbol") == "000001.SZ")["list_date"][0] == date(1991, 4, 3)
    assert stocks.filter(pl.col("symbol") == "6000002.SH")["delist_date"][0] == date(2020, 1, 1)
    assert stocks.filter(pl.col("symbol") == "600000.SH")["delist_date"][0] is None
    etfs = frame.filter(pl.col("asset_type") == "etf")
    assert etfs["symbol"].to_list() == ["510300.SH"]
    assert frame["source"].unique().to_list() == ["qmt_bridge"]
    assert metrics["rows_read"] == 4


class _IndexConstituentsFakeXtData:
    def get_stock_list_in_sector(self, sector_name):
        counts = {"上证50": 50, "沪深300": 300, "中证500": 500}
        n = counts.get(sector_name, 0)
        return [f"{i:06d}.SH" for i in range(600000, 600000 + n)]
        return []


def test_fetch_index_constituents_qmt_returns_membership():
    metrics: dict = {}
    frame = fetch_index_constituents_qmt(
        config=None, xtdata=_IndexConstituentsFakeXtData(), metrics=metrics
    )
    assert frame.height == 850
    assert set(frame["index_symbol"]) == {"000016.SH", "000300.SH", "000905.SH"}
    assert frame["weight"].null_count() == 850
    assert frame["source"].unique().to_list() == ["qmt_bridge"]
    assert metrics["requests"] == 13
    assert metrics["rows_read"] == 850


def test_instruments_step_falls_back_to_qmt_when_tdx_fails(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    cfg.staging_root.mkdir(parents=True)
    cfg.qmt_bridge_enabled = True

    def _tdx_boom(**kwargs):
        raise RuntimeError("TDX outage")

    monkeypatch.setattr(reference, "fetch_instruments", _tdx_boom)
    monkeypatch.setattr(
        reference,
        "fetch_instruments_qmt",
        lambda config=None, **kw: with_provenance(
            pl.DataFrame(
                {
                    "symbol": ["600000.SH", "000001.SZ"],
                    "name": ["浦发银行", "平安银行"],
                    "exchange": ["SH", "SZ"],
                    "asset_type": ["stock", "stock"],
                    "list_date": [date(1999, 11, 10), date(1991, 4, 3)],
                    "delist_date": [None, None],
                    "prev_symbol": [None, None],
                }
            ),
            source="qmt_bridge",
            data_version=data_version_for("instruments"),
        ),
    )
    monkeypatch.setattr(reference, "_merge_bse_instruments", lambda *a, **k: a[1])
    monkeypatch.setattr(reference, "_merge_untdxable_instruments", lambda *a, **k: a[1])
    monkeypatch.setattr(reference, "_require_beijing_instrument_scope", lambda *a, **k: None)
    monkeypatch.setattr(reference, "enrich_instrument_list_dates", lambda *a, **k: a[1])
    monkeypatch.setattr(reference, "_carry_lake_facts", lambda *a, **k: a[1])

    result = reference.step_instruments(cfg, date(2026, 9, 25), "run-qmt", {})
    assert result["source"] == "qmt_bridge"


def test_instruments_step_raises_when_tdx_fails_and_qmt_disabled(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    cfg.staging_root.mkdir(parents=True)
    cfg.qmt_bridge_enabled = False

    monkeypatch.setattr(
        reference, "fetch_instruments", lambda **kw: (_ for _ in ()).throw(RuntimeError("TDX down"))
    )

    with pytest.raises(RuntimeError, match="TDX down"):
        reference.step_instruments(cfg, date(2026, 9, 25), "run-no-qmt", {})


def test_corporate_actions_backfill_prefers_clean_qmt_sweep(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake", raw_archive_enabled=False)
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2026, 12, 31)
    cfg._backfill_symbols = ["600000.SH"]
    cfg.qmt_bridge_enabled = True
    qmt_frame = with_provenance(
        pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "ex_date": [date(2026, 7, 16)],
                "action_type": ["cash_dividend"],
                "cash_dividend": [0.42],
                "bonus_ratio": [0.0],
                "transfer_ratio": [0.0],
                "split_factor": [None],
                "allotment_ratio": [None],
                "allotment_price": [None],
            }
        ),
        source="qmt_bridge",
        data_version=data_version_for("corporate_actions"),
    )
    requested: list[list[str]] = []

    def _qmt(symbols, start, end, **kwargs):
        requested.append(list(symbols))
        assert start == date(2024, 1, 1)
        assert end == date(2026, 12, 31)
        return qmt_frame

    def _must_not_call(*args, **kwargs):
        raise AssertionError("TDX sweep should not run after a clean QMT sweep")

    monkeypatch.setattr(events, "fetch_corporate_actions_qmt", _qmt)
    monkeypatch.setattr(events, "fetch_corporate_actions", _must_not_call)
    result = events.step_corporate_actions(cfg, date(2026, 12, 31), "qmt-run", {})
    assert requested == [["600000.SH"]]
    assert result["rows_written"] == 1
    staged = list((cfg.staging_root / "corporate_actions").glob("**/*.parquet"))
    assert staged
    assert pl.read_parquet(staged[0])["source"].unique().to_list() == ["qmt_bridge"]
