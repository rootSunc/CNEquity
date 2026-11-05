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

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import pytest

from cnequity.config import Config
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


def test_a_recorded_gap_pulls_complete_through_back_and_counts_attempts(store):
    with store.transaction("margin_trading") as payload:
        payload["complete_through"] = D3.isoformat()

    store.record_missing_dates("margin_trading", [D2], reason="timeout")
    store.record_missing_dates("margin_trading", [D2], reason="timeout again")

    payload = store.get_payload("margin_trading")
    assert payload["complete_through"] == (D2 - timedelta(days=1)).isoformat()
    assert payload["coverage_status"] == "incomplete"
    assert len(payload["missing_ranges"]) == 1
    row = payload["missing_ranges"][0]
    assert row["attempts"] == 2
    assert row["last_failure"] == "timeout again"


def test_staged_days_clear_a_gap_only_when_their_own_run_publishes(store):
    store.record_missing_dates("margin_trading", [D1, D2], reason="timeout")
    store.mark_staged_request_days("margin_trading", "run-a", [D1])
    store.mark_staged_request_days("margin_trading", "run-b", [D2])

    # Staging alone settles nothing: run-a may still die before compact.
    assert store.get_missing_dates("margin_trading") == {D1, D2}

    store.commit_staged_request_days("margin_trading", "run-b")
    assert store.get_missing_dates("margin_trading") == {D1}
    payload = store.get_payload("margin_trading")
    assert payload["staged_request_days"] == {"run-a": [D1.isoformat()]}
    assert payload["coverage_status"] == "incomplete"

    # Committing a run that staged nothing is a no-op, not a clear-all.
    store.commit_staged_request_days("margin_trading", "run-never-staged")
    assert store.get_missing_dates("margin_trading") == {D1}

    store.commit_staged_request_days("margin_trading", "run-a")
    payload = store.get_payload("margin_trading")
    assert store.get_missing_dates("margin_trading") == set()
    assert "staged_request_days" not in payload
    assert payload["coverage_status"] == "unknown"


def test_a_day_the_same_sweep_reported_as_a_source_gap_is_not_marked_staged(tmp_path):
    """A partial response that recorded its own gap must not clear it on publish."""
    from cnequity.orchestrator.source_gaps import record_source_gap, source_gap_scope

    cfg = Config(data_root=tmp_path / "data")
    store = StateStore(cfg.meta_root)
    with source_gap_scope(cfg):
        record_source_gap("margin_trading", "SZSE half missing", dates=[D1])
        store.mark_staged_request_days("margin_trading", "run-a", [D1, D2])

    assert store.get_payload("margin_trading")["staged_request_days"] == {"run-a": [D2.isoformat()]}
    store.commit_staged_request_days("margin_trading", "run-a")
    assert store.get_missing_dates("margin_trading") == {D1}


def test_clearing_dates_keeps_the_dataset_incomplete_while_units_are_owed(store):
    store.record_missing_dates("macro_indicators", [D1, D2], reason="timeout")
    store.record_missing_units("macro_indicators", [("cpi:2024-05", "timeout")])

    store.clear_missing_dates("macro_indicators", [D1])
    payload = store.get_payload("macro_indicators")
    assert [row["start"] for row in payload["missing_ranges"]] == [D2.isoformat()]
    assert payload["coverage_status"] == "incomplete"

    store.clear_missing_dates("macro_indicators", [D2])
    assert store.get_payload("macro_indicators")["coverage_status"] == "incomplete"

    store.clear_missing_units("macro_indicators", ["cpi:2024-05"])
    payload = store.get_payload("macro_indicators")
    assert "missing_units" not in payload
    assert payload["coverage_status"] == "unknown"


# ---------------------------------------------------------- unit-level ledger


def test_unit_gaps_are_cleared_only_by_the_run_that_staged_them(store):
    dataset = "financial_statement_items"
    store.record_missing_units(
        dataset, [("600519.SH:2024Q1", "timeout"), ("000001.SZ:2024Q1", "timeout")]
    )
    store.mark_staged_units(dataset, "run-a", ["600519.SH:2024Q1"])

    store.commit_staged_units(dataset, "run-other")
    assert set(store.get_payload(dataset)["missing_units"]) == {
        "600519.SH:2024Q1",
        "000001.SZ:2024Q1",
    }

    store.commit_staged_units(dataset, "run-a")
    payload = store.get_payload(dataset)
    assert set(payload["missing_units"]) == {"000001.SZ:2024Q1"}
    assert "staged_units" not in payload
    assert payload["coverage_status"] == "incomplete"

    store.record_missing_units(dataset, [("000001.SZ:2024Q1", "still down")])
    unit = store.get_payload(dataset)["missing_units"]["000001.SZ:2024Q1"]
    assert unit["attempts"] == 2
    assert unit["reason"] == "still down"


def test_empty_unit_inputs_do_not_touch_state(store):
    store.record_missing_units("macro_indicators", [])
    store.mark_staged_units("macro_indicators", "run-a", [])
    store.clear_missing_units("macro_indicators", [])
    assert not (store.root / "macro_indicators.json").exists()


# ---------------------------------------------------------------- concurrency


def test_parallel_writers_do_not_lose_each_others_gaps(store):
    """A lost update here silently forgets a gap: the watermark has moved past it."""
    days = [date(2024, 6, 1) + timedelta(days=i) for i in range(24)]
    symbols = [f"{600000 + i:06d}.SH" for i in range(24)]

    def work(i: int) -> None:
        store.record_missing_dates("margin_trading", [days[i]], reason=f"t{i}")
        store.record_outstanding_keys(
            "daily_bars", [(symbols[i], D3)], run_id=f"run-{i}", reason="source_unavailable"
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(work, range(len(days))))

    assert store.get_missing_dates("margin_trading") == set(days)
    owed = {row["symbol"] for row in store.get_outstanding_keys("daily_bars")}
    assert owed == set(symbols)
    # Every write went through mkstemp + replace; none may be left behind.
    assert not list(store.root.glob(".*.tmp"))


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


def test_state_on_disk_is_plain_json_after_every_ledger_operation(store):
    """Operators read and occasionally hand-edit these files."""
    store.record_missing_dates("margin_trading", [D1], reason="timeout")
    store.mark_staged_request_days("margin_trading", "run-a", [D1])
    store.record_missing_units("margin_trading", [("u", "timeout")])
    json.loads((store.root / "margin_trading.json").read_text(encoding="utf-8"))
