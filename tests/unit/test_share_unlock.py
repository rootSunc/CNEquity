"""share_unlock_schedule paging.

EastMoney's datacenter rejects range comparisons on date columns
("参数预处理错误: org.antlr.v4.runtime.InputMismatchException", code=9501), which
broke this step every run with no change on our side. Re-measured 2026-09-17 the
predicate is honoured again, so the window is asked for and the walk is kept as
the fallback — these pin both halves, and that only the documented refusal earns
the slow path.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest

import cnequity.adapters.eastmoney.share_unlock as su
from cnequity.adapters.eastmoney.datacenter import fetch_datacenter


def _row(code: str, day: date, exch_suffix: str = "SH") -> dict:
    return {
        "SECURITY_CODE": code,
        "SECURITY_TYPE_CODE": "058001001",
        "SECUCODE": f"{code}.{exch_suffix}",
        "FREE_DATE": f"{day.isoformat()} 00:00:00",
        "ABLE_FREE_SHARES": 1000.0,
        "FREE_RATIO": 0.01,
        "FREE_SHARES_TYPE": "首发原股东限售股份",
        "CURRENT_FREE_SHARES": 1000.0,
    }


def test_the_window_is_asked_for_rather_than_walked(monkeypatch):
    """Re-measured 2026-09-17: EastMoney honours the FREE_DATE range exactly.

    It refused it once — code=9501, which is why this adapter was rewritten to
    page the whole report — and the walk is kept as a fallback below. Asking is
    not a micro-optimisation: the walk reads all of 2010..2035 on *every* call,
    so a backfill's strides each restart it and one page timing out fails the
    run. A year comes back in three pages when asked for.
    """
    seen: dict = {}

    def _fake(client, report, columns, **kwargs):
        seen.update(kwargs)
        return []

    monkeypatch.setattr(su, "fetch_datacenter", _fake)
    su.fetch_share_unlock_schedule(date(2026, 8, 7), client=MagicMock())

    assert "FREE_DATE>=" in seen["filter_expr"]
    assert "FREE_DATE<=" in seen["filter_expr"]
    assert "stop_after" not in seen, "a windowed request has nothing to stop early"


def test_a_refused_range_falls_back_to_walking_the_report(monkeypatch):
    """This upstream has broken this way before; a slow answer beats none."""
    from cnequity.adapters.eastmoney.datacenter import EastMoneyDatacenterError

    calls: list[dict] = []

    def _fake(client, report, columns, **kwargs):
        calls.append(kwargs)
        if "filter_expr" in kwargs and kwargs["filter_expr"]:
            raise EastMoneyDatacenterError("rejected schema: 参数预处理错误 (code=9501)")
        return []

    monkeypatch.setattr(su, "fetch_datacenter", _fake)
    su.fetch_share_unlock_schedule(date(2026, 8, 7), client=MagicMock())

    assert len(calls) == 2, "the refusal must be retried as a walk"
    walk = calls[1]
    assert not walk.get("filter_expr")
    assert walk["sort_columns"] == "FREE_DATE"
    assert walk["sort_types"] == "-1"
    assert callable(walk["stop_after"])


def test_an_unrelated_datacenter_error_is_not_swallowed(monkeypatch):
    """Only the documented refusal earns the slow path."""
    from cnequity.adapters.eastmoney.datacenter import EastMoneyDatacenterError

    def _fake(client, report, columns, **kwargs):
        raise EastMoneyDatacenterError("rejected schema: FREE_RATIO返回字段不存在 (code=9999)")

    monkeypatch.setattr(su, "fetch_datacenter", _fake)
    with pytest.raises(EastMoneyDatacenterError):
        su.fetch_share_unlock_schedule(date(2026, 8, 7), client=MagicMock())


def test_window_is_applied_client_side(monkeypatch):
    d = date(2026, 8, 7)
    rows = [
        _row("600001", d - timedelta(days=1)),  # before the window
        _row("600002", d),  # first day, inclusive
        _row("600003", d + timedelta(days=90)),
        _row("600004", d + timedelta(days=180)),  # last day, inclusive
        _row("600005", d + timedelta(days=181)),  # past the horizon
    ]
    monkeypatch.setattr(su, "fetch_datacenter", lambda *a, **k: rows)
    df = su.fetch_share_unlock_schedule(d, horizon_days=180, client=MagicMock())
    assert sorted(df["symbol"].to_list()) == ["600002.SH", "600003.SH", "600004.SH"]


def test_stop_after_halts_paging_and_skips_the_count_guard():
    """A short read is the point here, so the completeness guard must not fire."""
    pages = [
        {"result": {"pages": 60, "count": 30000, "data": [{"FREE_DATE": "2030-01-01"}] * 500}},
        {"result": {"pages": 60, "count": 30000, "data": [{"FREE_DATE": "2020-01-01"}] * 500}},
    ]
    calls = {"n": 0}

    class _Client:
        def get(self, url):
            resp = MagicMock()
            resp.json.return_value = pages[min(calls["n"], len(pages) - 1)]
            resp.raise_for_status.return_value = None
            calls["n"] += 1
            return resp

    rows = fetch_datacenter(
        _Client(),
        "RPT_LIFT_STAGE",
        "FREE_DATE",
        stop_after=lambda batch: batch[-1]["FREE_DATE"] < "2025",
    )
    assert calls["n"] == 2, "must stop at the first page that ends before the window"
    assert len(rows) == 1000


def test_without_stop_after_the_count_guard_still_fires():
    """The early-stop escape hatch must not weaken the truncation guard."""
    import pytest

    from cnequity.adapters.eastmoney.datacenter import EastMoneyDatacenterError

    class _Client:
        def get(self, url):
            resp = MagicMock()
            resp.json.return_value = {"result": {"pages": 1, "count": 9999, "data": [{"a": 1}]}}
            resp.raise_for_status.return_value = None
            return resp

    with pytest.raises(EastMoneyDatacenterError, match="declared count"):
        fetch_datacenter(_Client(), "RPT_X", "a")
