"""StateStore gap ledger: the part of incremental state nothing tested directly.

``record_missing_units``, ``clear_missing_units``, ``mark_staged_units``,
``clear_missing_dates`` and ``transaction`` have no test references at all.
They are what keeps a failed fundamentals/macro unit, or a failed by-date
request, owed until a *published* revision covers it. The invariants below are
the ones an interrupted run depends on:

* a gap is cleared only when the run that fetched it is published, never by
  staging alone and never by another run;
* a transaction that raises writes nothing and does not strand the lock;
* concurrent writers (parallel wave threads) do not lose each other's gaps;
* a damaged state file fails loud instead of reading as "no watermark".
"""

from __future__ import annotations

from datetime import date

import pytest

from cnequity.storage.state import StateStore

D1, D2, D3 = date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28)


@pytest.fixture
def store(tmp_path) -> StateStore:
    return StateStore(tmp_path / "meta")


# ---------------------------------------------------------------- transaction


def test_a_transaction_that_raises_writes_nothing_and_releases_the_lock(store):
    store.set_date("margin_trading", date(2024, 6, 25))
    path = store.root / "margin_trading.json"
    before = path.read_text(encoding="utf-8")

    with pytest.raises(RuntimeError, match="died mid-update"):
        with store.transaction("margin_trading") as payload:
            payload["last_success_trade_date"] = "2099-01-01"
            payload["missing_ranges"] = [{"start": D1.isoformat()}]
            raise RuntimeError("step died mid-update")

    assert path.read_text(encoding="utf-8") == before
    # A second acquisition would block (or fail) if the lock were stranded.
    assert store.get_date("margin_trading") == date(2024, 6, 25)
    assert not list(store.root.glob(".*.tmp"))


# ---------------------------------------------------------- date-level ledger


# ---------------------------------------------------------- unit-level ledger


# ---------------------------------------------------------------- concurrency


# ------------------------------------------------------------- damaged state


@pytest.mark.parametrize("damage", ["", "{", '{"last_success_trade_date": "2024-06-'])
def test_a_damaged_state_file_fails_loud_instead_of_reading_as_no_watermark(store, damage):
    """Reading damage as "no watermark" would silently shrink the next window.

    With no watermark, ``incremental_window`` falls back to a five-day lookback,
    so a lake whose watermark was weeks behind would skip the gap without a
    word. Failing loud is the safe behaviour; this pins it.
    """
    (store.root / "margin_trading.json").write_text(damage, encoding="utf-8")
    with pytest.raises(ValueError):
        store.get_date("margin_trading")
    with pytest.raises(ValueError):
        store.get_missing_dates("margin_trading")
