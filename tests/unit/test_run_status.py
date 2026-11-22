"""One word for how a run, batch or result ended; why is its reason code."""

from __future__ import annotations

import sqlite3

import pytest

from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.outcomes import RunStatus, execution_exit_code, run_status


def test_old_words_map_onto_the_enum_and_their_reason():
    assert run_status("warning") == (RunStatus.DEGRADED, None)
    assert run_status("skipped_non_trading_day") == (RunStatus.SKIPPED, "non_trading_day")
    assert run_status("skipped", "locked") == (RunStatus.SKIPPED, "locked")
    with pytest.raises(ValueError):
        run_status("ok")


@pytest.mark.parametrize(
    ("status", "reason", "code"),
    [
        ("success", None, 0),
        ("degraded", None, 0),
        ("warning", None, 0),
        ("skipped", "non_trading_day", 0),
        ("skipped_non_trading_day", None, 0),
        ("skipped", "locked", 0),
        ("skipped", None, 1),
        ("failed", None, 1),
        ("nonsense", None, 1),
    ],
)
def test_exit_codes_follow_status_and_reason(status, reason, code):
    assert execution_exit_code(status, reason) == code


def test_the_manifest_rewrites_old_words_and_keeps_the_reason(tmp_path):
    manifest = Manifest(tmp_path / "manifest.db")
    old = manifest.start_run("daily:core")
    weekend = manifest.start_run("daily:core")
    manifest.start_batch(old, "b0", task_id="t", dataset="daily_bars")
    with sqlite3.connect(tmp_path / "manifest.db") as conn:
        # As an older release left them.
        conn.execute("UPDATE ingestion_runs SET status = 'warning' WHERE run_id = ?", (old,))
        conn.execute(
            "UPDATE ingestion_runs SET status = 'skipped_non_trading_day' WHERE run_id = ?",
            (weekend,),
        )
        conn.execute("UPDATE ingestion_batches SET status = 'warning' WHERE run_id = ?", (old,))
        # An older release never stamped a schema version.
        conn.execute("PRAGMA user_version = 0")
    reopened = Manifest(tmp_path / "manifest.db")
    assert reopened.get_run(old)["status"] == "degraded"
    run = reopened.get_run(weekend)
    assert (run["status"], run["reason_code"]) == ("skipped", "non_trading_day")
    assert reopened.get_batch(old, "b0")["status"] == "degraded"


def test_a_run_finished_with_an_old_word_is_stored_in_the_new_one(tmp_path):
    manifest = Manifest(tmp_path / "manifest.db")
    run_id = manifest.start_run("daily:core")
    manifest.finish_run(run_id, "skipped_not_scheduled")
    run = manifest.get_run(run_id)
    assert (run["status"], run["reason_code"]) == ("skipped", "not_scheduled")


def test_a_missing_state_file_is_not_opened(tmp_path):
    from cnequity.config import Config
    from cnequity.storage.revisions import RevisionStore

    cfg = Config(data_root=tmp_path / "data")
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    store._drop_state_revision("hot_rank")
    assert store.receipt_file("hot_rank") is None
    state = cfg.meta_root / "state"
    assert not state.exists()


def test_the_state_file_drops_a_revision_copy_the_catalog_keeps(tmp_path):
    from cnequity.config import Config
    from cnequity.storage.revisions import STATE_REVISION_FIELDS, RevisionStore
    from cnequity.storage.state import StateStore

    cfg = Config(data_root=tmp_path / "data")
    state = StateStore(cfg.meta_root)
    with state.transaction("hot_rank") as payload:
        payload.update({"revision": 26, "revision_id": "abc", "content_digest": "d"})
        payload["last_success_trade_date"] = "2026-11-19"
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    store._drop_state_revision("hot_rank")
    payload = state.get_payload("hot_rank")
    assert not set(STATE_REVISION_FIELDS) & set(payload)
    assert payload["last_success_trade_date"] == "2026-11-19"
