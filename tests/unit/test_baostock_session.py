"""Offline tests for the shared baostock session driver (retry + watchdog)."""

from __future__ import annotations

import threading
from datetime import date

from cnequity.adapters.baostock._session import fetch_per_symbol


class _NoQueryBaostock:
    """A bs stub with just login/logout — queries are driven by the injected fetch."""

    def __init__(self):
        self.logins = 0
        self.logged_out = False

    def login(self):
        self.logins += 1
        return type("R", (), {"error_code": "0", "error_msg": ""})()

    def logout(self):
        self.logged_out = True


def test_completes_normally_without_tripping_the_watchdog():
    bs = _NoQueryBaostock()

    def fetch(_bs, symbol, _s, _e):
        return [{"symbol": symbol}]

    fired = []
    rows, failed = fetch_per_symbol(
        ["600000.SH", "600001.SH"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        fetch,
        bs=bs,
        sleep=lambda _s: None,
        deadline=5.0,
        on_deadline=lambda: fired.append(1),
    )
    assert failed == []
    assert {r["symbol"] for r in rows} == {"600000.SH", "600001.SH"}
    assert fired == []  # fast fetches never reach the deadline
    assert bs.logged_out is True


def test_main_thread_deadline_interrupts_a_blocking_fetch():
    bs = _NoQueryBaostock()
    calls = {"fetch": 0, "deadline": 0}

    def stalled_fetch(_bs, _symbol, _s, _e):
        calls["fetch"] += 1
        threading.Event().wait(1.0)
        return [{"symbol": _symbol}]

    def on_deadline():
        calls["deadline"] += 1

    rows, failed = fetch_per_symbol(
        ["600000.SH"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        stalled_fetch,
        bs=bs,
        sleep=lambda _s: None,
        deadline=0.01,
        on_deadline=on_deadline,
    )

    assert rows == []
    assert failed == ["600000.SH"]
    assert calls == {"fetch": 3, "deadline": 3}


def test_mid_sweep_login_failure_returns_partial(monkeypatch):
    """A dead baostock session mid-sweep must not discard already-fetched rows."""
    from cnequity.adapters.baostock import _session as sess

    monkeypatch.setattr(sess, "_RELOGIN_EVERY", 2)
    monkeypatch.setattr(sess, "_LOGIN_RETRIES", 2)
    monkeypatch.setattr(sess, "_LOGIN_BACKOFF_SECONDS", (0.0, 0.0))

    class _FlakyLogin(_NoQueryBaostock):
        def login(self):
            self.logins += 1
            # First login ok; periodic relogin at i=2 fails forever.
            if self.logins == 1:
                return type("R", (), {"error_code": "0", "error_msg": ""})()
            return type("R", (), {"error_code": "1", "error_msg": "网络接收错误。"})()

    bs = _FlakyLogin()

    def fetch(_bs, symbol, _s, _e):
        return [{"symbol": symbol}]

    rows, failed = fetch_per_symbol(
        ["A.SH", "B.SH", "C.SH", "D.SH"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        fetch,
        bs=bs,
        sleep=lambda _s: None,
    )
    assert {r["symbol"] for r in rows} == {"A.SH", "B.SH"}
    assert failed == ["C.SH", "D.SH"]

    # fetch_one blocks as if on a slowloris recv; the watchdog "closes the socket"
    # (on_deadline), which unblocks it into a raise — retried, then reported failed.
    bs = _NoQueryBaostock()
    unblock = threading.Event()
    calls = {"fetch": 0, "deadline": 0}

    def stalled_fetch(_bs, _symbol, _s, _e):
        calls["fetch"] += 1
        if unblock.wait(timeout=2.0):
            unblock.clear()
            raise ConnectionError("socket closed by watchdog")
        return [{"symbol": _symbol}]  # would mean the stall never resolved

    def on_deadline():
        calls["deadline"] += 1
        unblock.set()

    rows, failed = fetch_per_symbol(
        ["600000.SH"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        stalled_fetch,
        bs=bs,
        sleep=lambda _s: None,
        deadline=0.05,
        on_deadline=on_deadline,
    )
    assert calls["deadline"] >= 1  # watchdog fired on the stall
    assert calls["fetch"] == 3  # retried the full _MAX_RETRIES
    assert failed == ["600000.SH"]
    assert rows == []


def test_login_deadline_is_not_swallowed_and_restores_socket_default(monkeypatch):
    import socket

    import pytest

    from cnequity.adapters.baostock import _session as sess

    monkeypatch.setattr(sess, "_LOGIN_DEADLINE_SECONDS", 0.01)
    monkeypatch.setattr(sess, "_force_close_baostock_socket", lambda: None)

    class SlowLogin(_NoQueryBaostock):
        def login(self):
            self.logins += 1
            try:
                threading.Event().wait(1)
            except Exception:
                # The real SDK catches the watchdog exception around recv.
                pass
            return type("R", (), {"error_code": "0"})()

    bs = SlowLogin()
    previous = socket.getdefaulttimeout()
    with pytest.raises(RuntimeError, match="login exceeded"):
        sess.fetch_per_symbol([], date(2020, 1, 1), date(2020, 1, 2), lambda *a: [], bs=bs)
    assert socket.getdefaulttimeout() == previous
    assert bs.logins == 1
    assert not bs.logged_out


def test_logout_deadline_does_not_lose_completed_rows(monkeypatch):
    from cnequity.adapters.baostock import _session as sess

    monkeypatch.setattr(sess, "_LOGOUT_DEADLINE_SECONDS", 0.01)
    monkeypatch.setattr(sess, "_force_close_baostock_socket", lambda: None)

    class SlowLogout(_NoQueryBaostock):
        def logout(self):
            threading.Event().wait(1)

    rows, failed = sess.fetch_per_symbol(
        ["600000.SH"],
        date(2020, 1, 1),
        date(2020, 1, 2),
        lambda *a: [{"symbol": "600000.SH"}],
        bs=SlowLogout(),
        sleep=lambda _: None,
    )
    assert rows == [{"symbol": "600000.SH"}]
    assert failed == []


def test_relogin_does_not_overlap_a_timed_out_logout(monkeypatch):
    import pytest

    from cnequity.adapters.baostock import _session as sess

    monkeypatch.setattr(sess, "_LOGOUT_DEADLINE_SECONDS", 0.01)
    monkeypatch.setattr(sess, "_force_close_baostock_socket", lambda: None)

    class SlowLogout(_NoQueryBaostock):
        def logout(self):
            threading.Event().wait(1)

    bs = SlowLogout()
    with pytest.raises(RuntimeError, match="logout exceeded"):
        sess._relogin(bs)
    assert bs.logins == 0


def test_sdk_exception_handler_cannot_swallow_query_deadline():
    import pytest

    from cnequity.adapters.baostock import _session as sess

    def vendor():
        try:
            threading.Event().wait(1)
        except Exception:
            # An SDK may retry or return an apparently valid object here.
            return "deadline was swallowed"
        return "too late"

    with pytest.raises(TimeoutError, match="exceeded"):
        sess._fetch_with_deadline(vendor, 0.01, lambda: None)
