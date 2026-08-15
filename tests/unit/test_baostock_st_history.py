"""Offline tests for the baostock historical ST-label backfill path (C4)."""

from __future__ import annotations

from datetime import date

import pytest

from cn_market_lake.adapters.baostock.st_history import fetch_st_history
from cn_market_lake.domain.schemas import PRIMARY_KEYS, TRADING_STATUS_SCHEMA


class _FakeResultSet:
    def __init__(self, rows: list[list[str]], error_code: str = "0"):
        self.error_code = error_code
        self.error_msg = "" if error_code == "0" else "boom"
        self._rows = rows
        self._i = -1

    def next(self) -> bool:
        self._i += 1
        return self._i < len(self._rows)

    def get_row_data(self) -> list[str]:
        return self._rows[self._i]


class _FakeBaostock:
    def __init__(self, per_symbol, login_ok=True, error_codes=None):
        self._per_symbol = per_symbol
        self._login_ok = login_ok
        self._error_codes = error_codes or {}
        self.logged_out = False
        self.logins = 0

    def login(self):
        self.logins += 1
        return _FakeResultSet([], error_code="0" if self._login_ok else "10001")

    def query_history_k_data_plus(self, code, fields, **kwargs):
        return _FakeResultSet(
            self._per_symbol.get(code, []), error_code=self._error_codes.get(code, "0")
        )

    def logout(self):
        self.logged_out = True


# k-data rows: [date, code, tradestatus, isST]
def _rows(code, days):
    return [[d, code, ts, st] for d, ts, st in days]


def test_emits_traded_st_and_normal_evidence():
    bs = _FakeBaostock(
        {
            "sz.000017": _rows(
                "sz.000017",
                [
                    ("2020-04-28", "1", "0"),  # not ST yet
                    ("2020-04-29", "1", "1"),  # ST day -> emitted
                    ("2020-04-30", "0", "1"),  # ST but suspended -> skipped
                    ("2020-05-06", "1", "1"),  # ST day -> emitted
                ],
            )
        }
    )
    df, failed = fetch_st_history(
        ["000017.SZ"], date(2020, 1, 1), date(2020, 12, 31), bs=bs, sleep=lambda _: None
    )

    assert bs.logged_out is True
    assert failed == []
    assert df.height == 3
    assert df.sort("trade_date")["status"].to_list() == ["normal", "st", "st"]
    assert df["is_trading"].unique().to_list() == [True]
    # columns are the curated trading_status contract minus provenance
    assert set(df.columns) == set(TRADING_STATUS_SCHEMA) - {"source", "data_version", "fetched_at"}
    # rows are unique on the trading_status primary key
    pk = PRIMARY_KEYS["trading_status"]
    assert df.unique(subset=pk).height == df.height


def test_never_st_symbol_emits_negative_evidence():
    bs = _FakeBaostock({"sz.000001": _rows("sz.000001", [("2020-01-02", "1", "0")])})
    df, failed = fetch_st_history(
        ["000001.SZ"], date(2020, 1, 1), date(2020, 12, 31), bs=bs, sleep=lambda _: None
    )
    assert failed == []
    assert df.height == 1
    assert df["status"].to_list() == ["normal"]


def test_unknown_is_st_value_fails_symbol_closed():
    bs = _FakeBaostock({"sz.000001": _rows("sz.000001", [("2020-01-02", "1", "")])})
    df, failed = fetch_st_history(
        ["000001.SZ"], date(2020, 1, 1), date(2020, 12, 31), bs=bs, sleep=lambda _: None
    )
    assert df.is_empty()
    assert failed == ["000001.SZ"]


def test_reports_failed_symbols_fail_loud():
    bs = _FakeBaostock(
        {"sz.000017": _rows("sz.000017", [("2020-04-29", "1", "1")])},
        error_codes={"sh.600145": "10002"},
    )
    df, failed = fetch_st_history(
        ["000017.SZ", "600145.SH"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        bs=bs,
        sleep=lambda _s: None,
    )
    assert df.height == 1
    assert failed == ["600145.SH"]
    assert bs.logins > 1  # relogin attempted on failure


def test_fails_loud_on_login_error():
    bs = _FakeBaostock({}, login_ok=False)
    with pytest.raises(RuntimeError, match="login failed"):
        fetch_st_history(
            ["000017.SZ"],
            date(2020, 1, 1),
            date(2020, 12, 31),
            bs=bs,
            sleep=lambda _s: None,
        )


class _StallingBaostock(_FakeBaostock):
    """A query that raises (a stalled/timed-out socket) instead of returning."""

    def __init__(self, per_symbol, stall_codes):
        super().__init__(per_symbol)
        self._stall_codes = stall_codes

    def query_history_k_data_plus(self, code, fields, **kwargs):
        if code in self._stall_codes:
            raise TimeoutError("timed out")
        return super().query_history_k_data_plus(code, fields, **kwargs)


def test_stalled_socket_is_retried_then_reported_failed():
    # A raised socket timeout must not crash the sweep: it is retried on a fresh
    # login and, if it never recovers, reported as failed (fail-loud) — not hung.
    bs = _StallingBaostock(
        {"sz.000017": _rows("sz.000017", [("2020-04-29", "1", "1")])},
        stall_codes={"sh.600145"},
    )
    df, failed = fetch_st_history(
        ["000017.SZ", "600145.SH"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        bs=bs,
        sleep=lambda _s: None,
    )
    assert df.height == 1
    assert failed == ["600145.SH"]
    assert bs.logins > 1  # relogin attempted after the stall
