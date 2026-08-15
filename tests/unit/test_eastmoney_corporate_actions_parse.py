"""EastMoney corporate actions parsing + fetch flow (backup for TDX xdxr)."""

from __future__ import annotations

from datetime import date

import pytest

from cn_market_lake.adapters.eastmoney.corporate_actions import (
    _map_action_type,
    _num,
    _parse_row,
    fetch_corporate_actions_eastmoney,
)
from cn_market_lake.adapters.eastmoney.datacenter import EastMoneyDatacenterError


def test_num_handles_bad_values():
    assert _num("1.5") == 1.5
    assert _num(None) == 0.0
    assert _num("not-a-number") == 0.0


def test_map_action_type_all_branches():
    assert _map_action_type({"IMPL_PLAN_PROFILE": "10配3"}) == "allotment"
    assert _map_action_type({"IMPL_PLAN_PROFILE": "10转3"}) == "transfer"
    assert _map_action_type({"IMPL_PLAN_PROFILE": "10送3"}) == "bonus"
    assert _map_action_type({"IMPL_PLAN_PROFILE": "10派1.5元(现金)"}) == "cash_dividend"
    assert _map_action_type({"IMPL_PLAN_PROFILE": "", "PRETAX_BONUS_RMB": "1.0"}) == "cash_dividend"
    assert _map_action_type({"IMPL_PLAN_PROFILE": "", "IT_RATIO": "3"}) == "transfer"
    assert _map_action_type({"IMPL_PLAN_PROFILE": "", "BONUS_RATIO": "3"}) == "bonus"
    assert _map_action_type({"IMPL_PLAN_PROFILE": ""}) is None


def test_parse_row_returns_none_when_no_ex_date():
    assert _parse_row({}) is None


def test_parse_row_returns_none_when_no_action_type():
    row = {
        "SECUCODE": "600519.SH",
        "SECURITY_CODE": "600519",
        "EX_DIVIDEND_DATE": "2024-06-28",
        "IMPL_PLAN_PROFILE": "",
    }
    assert _parse_row(row) is None


def test_parse_row_uses_equity_record_date_fallback():
    row = {
        "SECURITY_CODE": "000001",
        "EQUITY_RECORD_DATE": "2024-06-27 00:00:00",
        "IMPL_PLAN_PROFILE": "10送3",
        "BONUS_RATIO": "3",
    }
    parsed = _parse_row(row)
    assert parsed is not None
    assert parsed["ex_date"] == date(2024, 6, 27)
    assert parsed["symbol"] == "000001.SZ"
    assert parsed["bonus_ratio"] == 0.3


def test_parse_row_exchange_from_suffix_and_prefix_bands():
    bj_row = {
        "SECUCODE": "430047.BJ",
        "SECURITY_CODE": "430047",
        "EX_DIVIDEND_DATE": "2024-06-28",
        "IMPL_PLAN_PROFILE": "10派1元",
    }
    parsed = _parse_row(bj_row)
    assert parsed["symbol"] == "430047.BJ"

    sh_by_prefix = {
        "SECURITY_CODE": "601988",
        "EX_DIVIDEND_DATE": "2024-06-28",
        "IMPL_PLAN_PROFILE": "10派1元",
    }
    assert _parse_row(sh_by_prefix)["symbol"] == "601988.SH"


def test_parse_row_cash_dividend_divides_per_10_shares():
    row = {
        "SECURITY_CODE": "600519",
        "EX_DIVIDEND_DATE": "2024-06-28",
        "IMPL_PLAN_PROFILE": "10派245.66元",
        "PRETAX_BONUS_RMB": "245.66",
    }
    parsed = _parse_row(row)
    assert parsed["action_type"] == "cash_dividend"
    assert parsed["cash_dividend"] == pytest.approx(24.566)
    assert parsed["bonus_ratio"] == 0.0
    assert parsed["transfer_ratio"] == 0.0
    assert parsed["allotment_ratio"] is None
    assert parsed["allotment_price"] is None


class _Client:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_fetch_corporate_actions_eastmoney_backfill_filter_and_dedupe(monkeypatch):
    seen_filters = []

    seen_stop = []

    def _fake_fetch_datacenter(client, report, columns, *, filter_expr, **kwargs):
        seen_filters.append(filter_expr)
        seen_stop.append(kwargs.get("stop_after"))
        return [
            {
                "SECURITY_CODE": "600519",
                "EX_DIVIDEND_DATE": "2024-06-28",
                "IMPL_PLAN_PROFILE": "10送3",
                "BONUS_RATIO": "3",
            },
            {
                "SECURITY_CODE": "600519",
                "EX_DIVIDEND_DATE": "2024-06-28",
                "IMPL_PLAN_PROFILE": "10送5",
                "BONUS_RATIO": "5",
            },
            {"SECURITY_CODE": "000001", "IMPL_PLAN_PROFILE": ""},  # no ex-date, dropped
        ]

    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.corporate_actions.fetch_datacenter",
        _fake_fetch_datacenter,
    )
    client = _Client()
    df = fetch_corporate_actions_eastmoney(date(2024, 6, 28), backfill=True, client=client)
    # No range predicate: EastMoney rejects those on date columns now
    # (InputMismatchException, code=9501), which took `cml backfill
    # corporate_actions` from working to failing outright. The window is bounded
    # by early-stopping the newest-first paging instead.
    assert seen_filters[0] == ""
    assert callable(seen_stop[0]), "backfill must bound itself via stop_after"
    assert client.closed is False
    # unique(keep="last") on (symbol, ex_date, action_type) keeps the second row.
    assert df.height == 1
    assert df.row(0, named=True)["bonus_ratio"] == 0.5


def test_fetch_corporate_actions_eastmoney_tip_filter_and_empty(monkeypatch):
    seen_filters = []

    def _fake_fetch_datacenter(client, report, columns, *, filter_expr, **kwargs):
        seen_filters.append(filter_expr)
        return []

    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.corporate_actions.fetch_datacenter",
        _fake_fetch_datacenter,
    )
    df = fetch_corporate_actions_eastmoney(date(2024, 6, 28), backfill=False, client=_Client())
    assert "2024-06-28" in seen_filters[0]
    assert df.is_empty()


def test_fetch_corporate_actions_eastmoney_owns_and_closes_default_client(monkeypatch):
    created: list[_Client] = []

    def _factory(**kwargs):
        client = _Client()
        created.append(client)
        return client

    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.corporate_actions.EastMoneyClient", _factory
    )
    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.corporate_actions.fetch_datacenter",
        lambda *a, **k: [],
    )
    fetch_corporate_actions_eastmoney(date(2024, 6, 28))
    assert created[0].closed is True


def test_fetch_corporate_actions_eastmoney_reraises_datacenter_error_and_closes(monkeypatch):
    def _boom(*a, **k):
        raise EastMoneyDatacenterError("boom")

    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.corporate_actions.fetch_datacenter", _boom
    )
    client = _Client()
    with pytest.raises(EastMoneyDatacenterError):
        fetch_corporate_actions_eastmoney(date(2024, 6, 28), client=client)


def test_fetch_corporate_actions_eastmoney_wraps_unexpected_error(monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("timeout")

    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.corporate_actions.fetch_datacenter", _boom
    )
    with pytest.raises(EastMoneyDatacenterError, match="corporate_actions failed"):
        fetch_corporate_actions_eastmoney(date(2024, 6, 28), client=_Client())


def test_fetch_corporate_actions_eastmoney_uses_config_retries_and_rate_limit(monkeypatch):
    seen_kwargs = {}
    rate_limit_calls = []

    class _Cfg:
        max_retries = 7
        retry_backoff_seconds = 2

        def rate_limit(self, source):
            rate_limit_calls.append(source)

    def _fake_fetch_datacenter(client, report, columns, *, max_retries, retry_backoff_seconds, **k):
        seen_kwargs["max_retries"] = max_retries
        seen_kwargs["retry_backoff_seconds"] = retry_backoff_seconds
        return []

    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.corporate_actions.fetch_datacenter",
        _fake_fetch_datacenter,
    )
    fetch_corporate_actions_eastmoney(date(2024, 6, 28), client=_Client(), config=_Cfg())
    assert seen_kwargs == {"max_retries": 7, "retry_backoff_seconds": 2.0}
    assert rate_limit_calls == ["eastmoney"]


def test_backfill_stops_paging_once_past_the_floor(monkeypatch):
    """The floor is enforced by early-stopping, since the filter cannot express it."""
    from datetime import date as _date

    pages = [
        [{"SECURITY_CODE": "600519", "EX_DIVIDEND_DATE": "2026-06-26", "BONUS_RATIO": "1"}],
        [{"SECURITY_CODE": "600519", "EX_DIVIDEND_DATE": "2010-06-26", "BONUS_RATIO": "1"}],
    ]
    captured = {}

    def _fake(client, report, columns, *, filter_expr, stop_after=None, **kwargs):
        captured["stop_after"] = stop_after
        return pages[0]

    monkeypatch.setattr(
        "cn_market_lake.adapters.eastmoney.corporate_actions.fetch_datacenter", _fake
    )
    fetch_corporate_actions_eastmoney(_date(2026, 8, 7), backfill=True, client=_Client())
    stop = captured["stop_after"]
    assert stop(pages[0]) is False, "a page inside the window must not stop paging"
    assert stop(pages[1]) is True, "a page ending before the floor must stop it"
    assert stop([{"SECURITY_CODE": "x"}]) is False, "no parseable date — keep going"
