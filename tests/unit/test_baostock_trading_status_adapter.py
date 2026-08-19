"""Baostock daily trading status adapter (offline)."""

from __future__ import annotations

from datetime import date

import pytest

from cnequity.adapters.baostock.trading_status import (
    _symbol_from_baostock,
    fetch_trading_status_baostock,
)


class _Rs:
    """Fake baostock result-set cursor (fields / next / get_row_data)."""

    def __init__(self, rows, fields=("code", "tradeStatus", "code_name"), error_code="0"):
        self._rows = list(rows)
        self.fields = list(fields)
        self.error_code = error_code
        self.error_msg = ""
        self._idx = 0

    def next(self):
        if self._idx >= len(self._rows):
            return False
        self._idx += 1
        return True

    def get_row_data(self):
        return list(self._rows[self._idx - 1])


class _FakeBs:
    def __init__(self, result: _Rs, login_errors: list[str] | None = None):
        self._result = result
        self.login_error = (login_errors or [None]).pop(0)
        self.logged_out = False

    def login(self):
        if self.login_error:

            class _Lg:
                error_code = "1"
                error_msg = self.login_error

            return _Lg()

        class _Lg:
            error_code = "0"
            error_msg = ""

        return _Lg()

    def query_all_stock(self, *, day):
        self.query_day = day
        return self._result

    def logout(self):
        self.logged_out = True


def test_symbol_from_baostock_mapping():
    assert _symbol_from_baostock("sh.600053") == "600053.SH"
    assert _symbol_from_baostock("sz.002084") == "002084.SZ"
    assert _symbol_from_baostock("bj.920001") is None
    assert _symbol_from_baostock("sh.600") is None


def _row(code, trade_status, name):
    return (code, trade_status, name)


def test_maps_st_normal_and_suspended():
    bs = _FakeBs(
        _Rs(
            [
                _row("sh.600053", "1", "*ST九鼎"),
                _row("sh.600079", "1", "ST人福"),
                _row("sh.600519", "1", "贵州茅台"),
                _row("sh.600984", "0", "建设机械"),
            ]
        )
    )
    df = fetch_trading_status_baostock(
        ["600053.SH", "600079.SH", "600519.SH", "600984.SH"],
        date(2026, 8, 18),
        bs=bs,
    )
    rows = {r["symbol"]: r for r in df.iter_rows(named=True)}
    assert rows["600053.SH"]["status"] == "st"
    assert rows["600053.SH"]["is_trading"] is True
    assert rows["600079.SH"]["status"] == "st"
    assert rows["600519.SH"]["status"] == "normal"
    assert rows["600984.SH"]["status"] == "suspended"
    assert rows["600984.SH"]["is_trading"] is False
    assert bs.query_day == "2026-08-18"


def test_suspension_beats_st_marker():
    bs = _FakeBs(_Rs([_row("sh.600984", "0", "ST建设")]))
    df = fetch_trading_status_baostock(["600984.SH"], date(2026, 8, 18), bs=bs)
    assert df.row(0, named=True)["status"] == "suspended"


def test_filters_to_requested_symbols_only():
    bs = _FakeBs(
        _Rs(
            [
                _row("sh.600519", "1", "贵州茅台"),
                _row("sz.000001", "1", "平安银行"),
                _row("sh.900901", "1", "某B股"),  # b-share not in scope
            ]
        )
    )
    df = fetch_trading_status_baostock(["600519.SH"], date(2026, 8, 18), bs=bs)
    assert df.height == 1
    assert df.row(0, named=True)["symbol"] == "600519.SH"


def test_unexpected_trade_status_fails_closed():
    bs = _FakeBs(_Rs([_row("sh.600519", "X", "贵州茅台")]))
    with pytest.raises(RuntimeError, match="unexpected tradeStatus"):
        fetch_trading_status_baostock(["600519.SH"], date(2026, 8, 18), bs=bs)


def test_error_code_from_server_raises():
    rs = _Rs([], error_code="10001011")
    with pytest.raises(RuntimeError, match="10001011"):
        fetch_trading_status_baostock(["600519.SH"], date(2026, 8, 18), bs=_FakeBs(rs))


def test_login_failure_raises():
    bs = _FakeBs(_Rs([]), login_errors=["bad login"])
    with pytest.raises(RuntimeError, match="bad login"):
        fetch_trading_status_baostock(["600519.SH"], date(2026, 8, 18), bs=bs)


def test_returns_empty_schema_for_no_rows():
    df = fetch_trading_status_baostock(["600519.SH"], date(2026, 8, 18), bs=_FakeBs(_Rs([])))
    assert df.is_empty()
    assert df.schema["symbol"] is not None
