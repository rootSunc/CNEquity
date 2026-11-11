"""The daily_bars tip from TDX batch quotes: 80 symbols per request, one session."""

from __future__ import annotations

import struct
from datetime import date

import polars as pl

from cnequity.adapters.tdx_protocol.quotes import quote_bars
from cnequity.config import Config

_D = date(2026, 9, 24)


def _quote(market, code, *, price, open_, high, low, vol, amount, last_close=None):
    return {
        "last_close_raw": last_close,
        "market": market,
        "code": code,
        "price_raw": price,
        "open_raw": open_,
        "high_raw": high,
        "low_raw": low,
        "vol": vol,
        "amount": amount,
    }


class _Client:
    def __init__(self, rows):
        self.rows = rows
        self.calls: list[list] = []

    def get_security_quotes(self, pairs):
        self.calls.append(list(pairs))
        codes = {code for _, code in pairs}
        return [r for r in self.rows if r["code"] in codes]


def test_quotes_become_bars_scaled_by_security_type():
    client = _Client(
        [
            _quote(
                1,
                "600519",
                price=123700,
                open_=125001,
                high=125613,
                low=123105,
                vol=31239,
                amount=3.8e9,
                last_close=124800,
            ),
            _quote(
                1, "510300", price=4515, open_=4578, high=4579, low=4512, vol=7102519, amount=3.2e9
            ),
        ]
    )
    bars = quote_bars(client, ["600519.SH", "510300.SH"], _D)
    assert bars["600519.SH"] == {
        "symbol": "600519.SH",
        "trade_date": _D,
        "open": 1250.01,
        "high": 1256.13,
        "low": 1231.05,
        "close": 1237.0,
        "pre_close": 1248.0,
        "volume": 3123900,
        "amount": 3.8e9,
    }
    assert bars["510300.SH"]["close"] == 4.515  # funds quote in 0.001
    # No previous close in the payload is a null, not a zero.
    assert bars["510300.SH"]["pre_close"] is None


def test_suspended_and_unpriced_symbols_stay_with_the_per_symbol_path():
    client = _Client(
        [
            _quote(0, "300096", price=0, open_=0, high=0, low=0, vol=0, amount=0.0),
            _quote(0, "000001", price=1130, open_=1135, high=1147, low=0, vol=100, amount=1.0),
        ]
    )
    assert quote_bars(client, ["300096.SZ", "000001.SZ", "920571.BJ"], _D) == {}


def test_requests_carry_at_most_80_symbols():
    client = _Client([])
    symbols = [f"{600000 + i:06d}.SH" for i in range(170)]
    paced: list[int] = []
    quote_bars(client, symbols, _D, pace=lambda: paced.append(1))
    assert [len(call) for call in client.calls] == [80, 80, 10]
    assert len(paced) == 3


def test_the_request_packet_names_every_security():
    from cnequity.adapters.tdx_protocol._wire.parser.std.get_security_quotes import (
        GetSecurityQuotesCmd,
    )

    cmd = GetSecurityQuotesCmd.__new__(GetSecurityQuotesCmd)
    cmd.setParams([(1, "600519"), (0, "000001")])
    header = struct.unpack("<HIHHIIHH", bytes(cmd.send_pkg[:22]))
    assert header == (0x10C, 0x02006320, 26, 26, 0x5053E, 0, 0, 2)
    assert bytes(cmd.send_pkg[22:]) == b"\x01600519\x00000001"


def test_the_tip_is_only_taken_while_the_quotes_describe_it(tmp_path, monkeypatch):
    from cnequity.steps import bars

    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.domain.market_time.last_closed_session", lambda now=None: date(2026, 9, 23)
    )
    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client._connect_with_retry",
        lambda config=None: (_ for _ in ()).throw(AssertionError("must not connect")),
    )
    assert (
        bars.tip_fetch._fetch_tip_via_tdx_quotes(cfg, ["600519.SH"], _D, "run-1")["covered"]
        == set()
    )


def test_the_tip_is_staged_under_its_own_label(tmp_path, monkeypatch):
    from cnequity.steps import bars

    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr("cnequity.domain.market_time.last_closed_session", lambda now=None: _D)

    class _Quotes(_Client):
        def close(self):
            pass

    client = _Quotes(
        [
            _quote(
                1,
                "600519",
                price=123700,
                open_=125001,
                high=125613,
                low=123105,
                vol=31239,
                amount=3.8e9,
            )
        ]
    )
    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client._connect_with_retry", lambda config=None: client
    )
    out = bars.tip_fetch._fetch_tip_via_tdx_quotes(cfg, ["600519.SH", "000001.SZ"], _D, "run-1")
    assert out["covered"] == {"600519.SH"}
    staged = pl.read_parquet(next((cfg.staging_root / "daily_bars").rglob("*.parquet")))
    assert staged["source"].to_list() == ["tdx_protocol_quote"]
    assert staged["volume"].to_list() == [3123900]
