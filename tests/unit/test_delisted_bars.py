"""Recovering delisted stocks — the survivorship repair.

`instruments` is a current-roster snapshot, so a stock delisted in 2019 has no
bars in the lake at all. These pin the two things that decide whether the repair
is trustworthy: which codes count as stocks, and that an unauthenticated roster
query cannot be mistaken for "nothing was trading".
"""

from __future__ import annotations

from datetime import date

from cn_market_lake.adapters.baostock.delisted_bars import (
    _fetch_one,
    _is_stock,
    roster_on,
    to_lake_symbol,
)


class _Rs:
    def __init__(self, rows, error_code="0"):
        self._rows = list(rows)
        self.error_code = error_code
        self.error_msg = ""

    def next(self):
        return bool(self._rows)

    def get_row_data(self):
        return self._rows.pop(0)


class _Bs:
    """Minimal baostock stand-in that records whether it was logged in."""

    def __init__(self, rows, authed_rows=None):
        self._rows = rows
        self._authed_rows = authed_rows
        self.logged_in = False
        self.logout_called = False

    def login(self):
        self.logged_in = True
        return _Rs([], error_code="0")

    def logout(self):
        self.logout_called = True
        return _Rs([])

    def query_all_stock(self, day):
        # Unauthenticated baostock answers with an empty set, not an error.
        if self._authed_rows is not None and not self.logged_in:
            return _Rs([])
        return _Rs([[c] for c in self._rows])


def test_shanghai_000_is_an_index_shenzhen_000_is_a_stock():
    """The prefix has to be read per exchange — sh.000001 is the composite
    index, sz.000001 is 平安银行. Getting this wrong pulls indices into the
    stock universe and inflates the measured gap."""
    assert not _is_stock("sh.000001")
    assert _is_stock("sz.000001")
    assert _is_stock("sh.600519")
    assert _is_stock("sh.688981")
    assert _is_stock("sz.300104")
    assert not _is_stock("sh.999999")
    assert not _is_stock("bj.430047")  # 北交所 has no adjustment factors


def test_symbol_conversion_round_trip():
    assert to_lake_symbol("sh.600519") == "600519.SH"
    assert to_lake_symbol("sz.000001") == "000001.SZ"


def test_roster_logs_in_before_querying(monkeypatch):
    """An unauthenticated roster query returns empty rather than failing, which
    would silently report a survivorship gap of zero — the exact failure this
    repair exists to detect."""
    bs = _Bs(["sh.600519", "sz.000001"], authed_rows=True)
    got = roster_on(date(2018, 6, 29), bs=bs)
    assert got == {"600519.SH", "000001.SZ"}
    assert bs.logged_in and bs.logout_called


def test_roster_skips_login_when_the_caller_holds_a_session():
    bs = _Bs(["sh.600519"])
    bs.logged_in = True
    got = roster_on(date(2018, 6, 29), bs=bs, login=False)
    assert got == {"600519.SH"}
    assert not bs.logout_called  # the caller's session stays open


def test_roster_filters_indices_out():
    bs = _Bs(["sh.000001", "sh.600519", "sz.399001", "sz.300104"])
    bs.logged_in = True
    assert roster_on(date(2018, 6, 29), bs=bs, login=False) == {"600519.SH", "300104.SZ"}


def test_fetch_skips_suspended_sessions_rather_than_writing_zeros():
    """A halted session comes back with blank prices; writing them as 0.0 would
    read as a -100% move on a stock that simply did not trade."""

    class _B:
        def query_history_k_data_plus(self, *a, **k):
            return _Rs(
                [
                    ["2019-01-02", "7.60", "7.72", "7.19", "7.24", "92853327", "686428182.91", "1"],
                    ["2019-01-03", "", "", "", "", "0", "0", "0"],  # suspended
                ]
            )

    rows = _fetch_one(_B(), "002450.SZ", date(2019, 1, 1), date(2019, 12, 31))
    assert [r["trade_date"] for r in rows] == [date(2019, 1, 2)]
    assert rows[0]["close"] == 7.24


def test_query_error_is_retryable_not_an_empty_result():
    """`None` tells the session driver to relogin and retry; `[]` would record
    the symbol as legitimately having no data."""

    class _B:
        def query_history_k_data_plus(self, *a, **k):
            return _Rs([], error_code="10001")

    assert _fetch_one(_B(), "002450.SZ", date(2019, 1, 1), date(2019, 12, 31)) is None
