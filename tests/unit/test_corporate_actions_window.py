"""The daily dividend window is one query, not one per day."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.steps import events

_D = date(2026, 9, 24)
_WINDOW = [date(2026, 9, 22), date(2026, 9, 23), date(2026, 9, 24)]


def _rows(days: list[date]) -> pl.DataFrame:
    return pl.DataFrame({"symbol": [f"60000{i}.SH" for i in range(len(days))], "ex_date": days})


def test_one_query_serves_the_whole_window(tmp_path, monkeypatch):
    calls: list[dict] = []

    def _fetch(d, **kwargs):
        calls.append({"day": d, "dates": kwargs.get("dates")})
        return _rows([date(2026, 9, 22), date(2026, 9, 24)])

    monkeypatch.setattr(events, "fetch_corporate_actions_eastmoney", _fetch)
    monkeypatch.setattr(events, "incremental_trade_dates", lambda config, ds, td: list(_WINDOW))
    fetch = events._window_fetcher(Config(data_root=tmp_path / "data"), _D, "run-1")
    got = [fetch(d).height for d in _WINDOW]
    assert got == [1, 0, 1]
    assert calls == [{"day": _D, "dates": _WINDOW}]


def test_a_failed_window_query_falls_back_to_one_query_per_day(tmp_path, monkeypatch):
    calls: list[date | None] = []

    def _fetch(d, **kwargs):
        if kwargs.get("dates"):
            raise RuntimeError("code=9501")
        calls.append(d)
        return _rows([d])

    monkeypatch.setattr(events, "fetch_corporate_actions_eastmoney", _fetch)
    monkeypatch.setattr(events, "incremental_trade_dates", lambda config, ds, td: list(_WINDOW))
    fetch = events._window_fetcher(Config(data_root=tmp_path / "data"), _D, "run-1")
    assert [fetch(d).height for d in _WINDOW] == [1, 1, 1]
    assert calls == _WINDOW


def test_the_adapter_builds_an_in_list_and_keeps_only_window_days(monkeypatch):
    from cnequity.adapters.eastmoney import corporate_actions as adapter

    seen: dict = {}

    def _datacenter(client, report, columns, **kwargs):
        seen["filter"] = kwargs["filter_expr"]
        return [
            {"SECUCODE": "600000.SH", "EX_DIVIDEND_DATE": "2026-09-22 00:00:00"},
            {"SECUCODE": "600001.SH", "EX_DIVIDEND_DATE": "2026-09-20 00:00:00"},
        ]

    monkeypatch.setattr(adapter, "fetch_datacenter", _datacenter)
    monkeypatch.setattr(
        adapter,
        "_parse_rows",
        lambda item: [
            {
                "symbol": item["SECUCODE"],
                "ex_date": date.fromisoformat(item["EX_DIVIDEND_DATE"][:10]),
                "action_type": "cash",
            }
        ],
    )
    out = adapter.fetch_corporate_actions_eastmoney(_D, client=object(), dates=_WINDOW)
    assert seen["filter"] == "(EX_DIVIDEND_DATE in ('2026-09-22','2026-09-23','2026-09-24'))"
    assert out["symbol"].to_list() == ["600000.SH"]
