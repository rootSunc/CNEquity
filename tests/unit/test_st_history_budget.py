"""A fresh lake must not wait ten hours behind a free API's pacing.

baostock's rests are deliberate — exceeding its limits blacklists the IP — but
on a fresh lake there is no ST signal to narrow the universe with, so the sweep
inherits all 5,557 symbols. A measured first `cne init` spent 1.8h reaching
that step and would have spent 10.4h more inside it.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.steps import reference


@pytest.fixture
def swept(monkeypatch):
    """Record which symbols each sweep actually asked the source for."""
    calls: list[list[str]] = []

    def _fake_fetch(batch, start, end, **kwargs):
        calls.append(list(batch))
        return pl.DataFrame(schema={"symbol": pl.Utf8}), []

    monkeypatch.setattr("cnequity.adapters.baostock.st_history.fetch_st_history", _fake_fetch)
    return calls


def _universe(n: int) -> list[str]:
    return [f"{i:06d}.SZ" for i in range(1, n + 1)]


def test_the_budget_bounds_one_run(config, swept, monkeypatch, tmp_path):
    config.st_history_symbols_per_run = 40
    monkeypatch.setattr(
        reference, "_resolve_st_backfill_universe", lambda c, s, e: (_universe(100), "all_a")
    )
    monkeypatch.setattr(
        reference, "write_fetched", lambda *a, **k: {"rows_read": 0, "rows_written": 0}
    )

    out = reference._backfill_trading_status_st_source(
        config,
        date(2026, 9, 17),
        "run-1",
        universe=_universe(100),
        universe_name="all_a",
        source="baostock",
    )

    assert sum(len(c) for c in swept) == 40, "the sweep must stop at the budget"
    assert out["status"] == "warning", "a paused sweep is not a clean success"
    assert out["deferred_symbols"] == 60


def test_a_paused_sweep_is_not_reported_as_unresolved(config, swept, monkeypatch):
    """Deferred and unresolved mean different things: one had no turn, the
    other the source would not answer for. Summing them sends an operator
    hunting a vendor outage that is not happening."""
    config.st_history_symbols_per_run = 20
    monkeypatch.setattr(
        reference, "write_fetched", lambda *a, **k: {"rows_read": 0, "rows_written": 0}
    )

    out = reference._backfill_trading_status_st_source(
        config,
        date(2026, 9, 17),
        "run-1",
        universe=_universe(100),
        universe_name="all_a",
        source="baostock",
    )

    assert out["failed_symbols"] == 0, "nothing failed — the rest simply had no turn"
    message = out["context_updates"]["audit_findings"][0]["message"]
    assert "not yet swept" in message
    assert "unresolved" not in message
    assert "cne backfill trading_status" in message


def test_zero_removes_the_bound(config, swept, monkeypatch):
    """`cne backfill trading_status` sets this: asking for the sweep by name is
    asking to sit through it, and stopping at 400 would look like completion."""
    config.st_history_symbols_per_run = 0
    monkeypatch.setattr(
        reference, "write_fetched", lambda *a, **k: {"rows_read": 0, "rows_written": 0}
    )

    reference._backfill_trading_status_st_source(
        config,
        date(2026, 9, 17),
        "run-1",
        universe=_universe(100),
        universe_name="all_a",
        source="baostock",
    )

    assert sum(len(c) for c in swept) == 100
