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


def test_keys_are_recorded_with_the_run_that_owed_them(store):
    assert (
        store.record_outstanding_keys("daily_bars", PAIRS, run_id="r1", reason="interior_gap") == 3
    )
    rows = store.get_outstanding_keys("daily_bars")
    assert [(r["symbol"], r["trade_date"]) for r in rows] == [
        ("000001.SZ", "2026-09-15"),
        ("000001.SZ", "2026-09-16"),
        ("600519.SH", "2026-09-16"),
    ]
    assert rows[0]["run_id"] == "r1"
    assert rows[0]["reason"] == "interior_gap"


def test_recording_the_same_gap_twice_does_not_grow_the_ledger(store):
    """A sweep that fails the same way every night must not accumulate."""
    store.record_outstanding_keys("daily_bars", PAIRS, run_id="r1", reason="interior_gap")
    assert (
        store.record_outstanding_keys("daily_bars", PAIRS, run_id="r2", reason="interior_gap") == 3
    )
    # The first run keeps the claim: it is when the debt was incurred.
    assert store.get_outstanding_keys("daily_bars")[0]["run_id"] == "r1"


def test_filled_keys_are_struck_off_and_the_rest_stay_owed(store):
    store.record_outstanding_keys("daily_bars", PAIRS, run_id="r1", reason="interior_gap")
    left = store.clear_outstanding_keys("daily_bars", [("000001.SZ", date(2026, 9, 15))])
    assert left == 2
    assert ("000001.SZ", "2026-09-15") not in {
        (r["symbol"], r["trade_date"]) for r in store.get_outstanding_keys("daily_bars")
    }


def test_an_empty_ledger_leaves_no_key_behind(store):
    store.record_outstanding_keys("daily_bars", PAIRS, run_id="r1", reason="interior_gap")
    assert store.clear_outstanding_keys("daily_bars") == 0
    assert store.get_outstanding_keys("daily_bars") == []
    # The section is dropped rather than left as an empty list, so a reader
    # cannot mistake "nothing owed" for "never recorded".
    assert "outstanding_keys" not in store.get_payload("daily_bars")


def test_a_dataset_that_never_owed_anything_reads_empty(store):
    assert store.get_outstanding_keys("index_bars") == []


def test_a_repair_that_misses_counts_the_attempt(store):
    """Nothing here expires — so the count is the only way to tell last
    night's blip from a vendor that has stopped serving the symbol."""
    store.record_outstanding_keys("daily_bars", PAIRS, run_id="r1", reason="interior_gap")
    for _ in range(3):
        store.note_repair_attempt("daily_bars", [("600519.SH", date(2026, 9, 16))])

    rows = {(r["symbol"], r["trade_date"]): r for r in store.get_outstanding_keys("daily_bars")}
    tried = rows[("600519.SH", "2026-09-16")]
    assert tried["attempts"] == 3
    assert "last_attempt_at" in tried
    # Keys the repair never reached for are untouched.
    assert "attempts" not in rows[("000001.SZ", "2026-09-15")]


def test_an_attempt_never_retires_the_debt(store):
    """Expiry would be exactly the silent loss the ledger exists to prevent."""
    store.record_outstanding_keys("daily_bars", PAIRS, run_id="r1", reason="interior_gap")
    for _ in range(50):
        store.note_repair_attempt("daily_bars", PAIRS)

    assert len(store.get_outstanding_keys("daily_bars")) == 3


def test_noting_an_attempt_on_an_empty_ledger_is_harmless(store):
    store.note_repair_attempt("daily_bars", PAIRS)
    assert store.get_outstanding_keys("daily_bars") == []


def test_a_whole_symbol_gap_is_clipped_to_its_listing_window(monkeypatch):
    """A symbol listed halfway through the sweep must not be owed the sessions
    before it existed — a debt nothing can pay off drowns the real ones."""
    from cnequity.steps import bars

    sessions = [date(2026, 9, d) for d in (14, 15, 16)]
    monkeypatch.setattr(
        bars, "list_trading_dates", lambda cfg, s, e: [d for d in sessions if s <= d <= e]
    )
    monkeypatch.setattr(
        bars,
        "_instrument_spans",
        lambda cfg: {
            "LATE.SZ": (date(2026, 9, 16), None, "stock"),
            "GONE.SZ": (None, date(2026, 9, 14), "stock"),
            "FULL.SZ": (None, None, "stock"),
        },
    )

    owed = bars._owed_keys_for_symbols(
        None, ["LATE.SZ", "GONE.SZ", "FULL.SZ"], date(2026, 9, 14), date(2026, 9, 16)
    )

    assert {d for s, d in owed if s == "LATE.SZ"} == {date(2026, 9, 16)}
    assert {d for s, d in owed if s == "GONE.SZ"} == {date(2026, 9, 14)}
    assert {d for s, d in owed if s == "FULL.SZ"} == set(sessions)


def test_an_unknown_symbol_is_owed_the_whole_window(monkeypatch):
    """No listing record is not a reason to owe less — the gap is still real."""
    from cnequity.steps import bars

    sessions = [date(2026, 9, d) for d in (15, 16)]
    monkeypatch.setattr(bars, "list_trading_dates", lambda cfg, s, e: sessions)
    monkeypatch.setattr(bars, "_instrument_spans", lambda cfg: {})

    owed = bars._owed_keys_for_symbols(None, ["MYSTERY.SZ"], date(2026, 9, 15), date(2026, 9, 16))

    assert owed == {("MYSTERY.SZ", d) for d in sessions}
