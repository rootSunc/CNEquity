"""Baostock free-API pacing: per-symbol interval + batch rest."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from cn_market_lake.adapters.baostock._session import fetch_per_symbol


class _NoQueryBaostock:
    def __init__(self):
        self.logins = 0

    def login(self):
        self.logins += 1
        return type("R", (), {"error_code": "0", "error_msg": ""})()

    def logout(self):
        pass


def test_paces_each_symbol_and_batch_rests():
    sleeps: list[float] = []
    rate_calls: list[str] = []

    cfg = SimpleNamespace(
        baostock_batch_size=2,
        baostock_batch_rest_seconds=9.0,
        rate_limit=lambda source: rate_calls.append(source),
    )

    def fetch(_bs, symbol, _s, _e):
        return [{"symbol": symbol}]

    rows, failed = fetch_per_symbol(
        ["A.SH", "B.SH", "C.SH"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        fetch,
        bs=_NoQueryBaostock(),
        sleep=sleeps.append,
        config=cfg,
    )
    assert failed == []
    assert len(rows) == 3
    assert rate_calls == ["baostock", "baostock", "baostock"]
    # Batch rest after completing 2 symbols (before starting the 3rd).
    assert 9.0 in sleeps


def test_default_interval_without_config():
    sleeps: list[float] = []

    def fetch(_bs, symbol, _s, _e):
        return [{"symbol": symbol}]

    fetch_per_symbol(
        ["A.SH", "B.SH"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        fetch,
        bs=_NoQueryBaostock(),
        sleep=sleeps.append,
        config=None,
    )
    # Two per-symbol default intervals (1.0) plus one batch rest (45) after first
    # batch boundary only when index % 50 == 0 — with 2 symbols, no batch rest.
    assert sleeps.count(1.0) == 2
