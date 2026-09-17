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
