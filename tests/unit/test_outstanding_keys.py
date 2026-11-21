"""A tolerated gap owes the lake keys, and something has to remember which.

Once the gate stops refusing, the hole is inside a published revision: the
watermark has moved over those sessions, so no incremental run will ever ask
for them again. The ledger is the only thing that knows.
"""

from __future__ import annotations

from datetime import date

import pytest

from cnequity.config import Config
from cnequity.storage.layout import init_data_layout
from cnequity.storage.state import StateStore


@pytest.fixture
def store(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    init_data_layout(cfg)
    return StateStore(cfg.meta_root)


PAIRS = [
    ("000001.SZ", date(2026, 9, 15)),
    ("000001.SZ", date(2026, 9, 16)),
    ("600519.SH", date(2026, 9, 16)),
]


def test_a_whole_symbol_gap_is_clipped_to_its_listing_window(monkeypatch):
    """A symbol listed halfway through the sweep must not be owed the sessions
    before it existed — a debt nothing can pay off drowns the real ones."""
    from cnequity.steps import bars

    sessions = [date(2026, 9, d) for d in (14, 15, 16)]
    monkeypatch.setattr(
        bars.admission, "list_trading_dates", lambda cfg, s, e: [d for d in sessions if s <= d <= e]
    )
    monkeypatch.setattr(
        bars.bar_scope, "list_trading_dates", lambda cfg, s, e: [d for d in sessions if s <= d <= e]
    )
    monkeypatch.setattr(
        bars.daily, "list_trading_dates", lambda cfg, s, e: [d for d in sessions if s <= d <= e]
    )
    monkeypatch.setattr(
        bars.finish, "list_trading_dates", lambda cfg, s, e: [d for d in sessions if s <= d <= e]
    )
    monkeypatch.setattr(
        bars.gapfill, "list_trading_dates", lambda cfg, s, e: [d for d in sessions if s <= d <= e]
    )
    monkeypatch.setattr(
        bars.gapfill_chain,
        "list_trading_dates",
        lambda cfg, s, e: [d for d in sessions if s <= d <= e],
    )
    monkeypatch.setattr(
        bars.index_history,
        "list_trading_dates",
        lambda cfg, s, e: [d for d in sessions if s <= d <= e],
    )
    monkeypatch.setattr(
        bars.sina_bars, "list_trading_dates", lambda cfg, s, e: [d for d in sessions if s <= d <= e]
    )
    monkeypatch.setattr(
        bars.staging, "list_trading_dates", lambda cfg, s, e: [d for d in sessions if s <= d <= e]
    )
    monkeypatch.setattr(
        bars.tip_fetch, "list_trading_dates", lambda cfg, s, e: [d for d in sessions if s <= d <= e]
    )
    monkeypatch.setattr(
        bars.bar_scope,
        "_instrument_spans",
        lambda cfg: {
            "LATE.SZ": (date(2026, 9, 16), None, "stock"),
            "GONE.SZ": (None, date(2026, 9, 14), "stock"),
            "FULL.SZ": (None, None, "stock"),
        },
    )

    owed = bars.admission._owed_keys_for_symbols(
        None, ["LATE.SZ", "GONE.SZ", "FULL.SZ"], date(2026, 9, 14), date(2026, 9, 16)
    )

    assert {d for s, d in owed if s == "LATE.SZ"} == {date(2026, 9, 16)}
    assert {d for s, d in owed if s == "GONE.SZ"} == {date(2026, 9, 14)}
    assert {d for s, d in owed if s == "FULL.SZ"} == set(sessions)


def test_an_unknown_symbol_is_owed_the_whole_window(monkeypatch):
    """No listing record is not a reason to owe less — the gap is still real."""
    from cnequity.steps import bars

    sessions = [date(2026, 9, d) for d in (15, 16)]
    monkeypatch.setattr(bars.admission, "list_trading_dates", lambda cfg, s, e: sessions)
    monkeypatch.setattr(bars.bar_scope, "list_trading_dates", lambda cfg, s, e: sessions)
    monkeypatch.setattr(bars.daily, "list_trading_dates", lambda cfg, s, e: sessions)
    monkeypatch.setattr(bars.finish, "list_trading_dates", lambda cfg, s, e: sessions)
    monkeypatch.setattr(bars.gapfill, "list_trading_dates", lambda cfg, s, e: sessions)
    monkeypatch.setattr(bars.gapfill_chain, "list_trading_dates", lambda cfg, s, e: sessions)
    monkeypatch.setattr(bars.index_history, "list_trading_dates", lambda cfg, s, e: sessions)
    monkeypatch.setattr(bars.sina_bars, "list_trading_dates", lambda cfg, s, e: sessions)
    monkeypatch.setattr(bars.staging, "list_trading_dates", lambda cfg, s, e: sessions)
    monkeypatch.setattr(bars.tip_fetch, "list_trading_dates", lambda cfg, s, e: sessions)
    monkeypatch.setattr(bars.bar_scope, "_instrument_spans", lambda cfg: {})

    owed = bars.admission._owed_keys_for_symbols(
        None, ["MYSTERY.SZ"], date(2026, 9, 15), date(2026, 9, 16)
    )

    assert owed == {("MYSTERY.SZ", d) for d in sessions}
