"""JobEngine: Ctrl-C mid-run, malformed step results, and missing inputs.

The engine's own comments record two regressions on the interrupt path that no
test drives end-to-end:

* `cne backfill` runs with ``finalize_run=False``. That flag was once read as
  "do not close the run on interrupt", so every Ctrl-C left the run and its
  batches ``running`` and unretryable until the hour-long stale window expired.
* `cne run compact` / backfill finalize reach the manifest through
  ``run_step``. Without its own interrupt handler a Ctrl-C there left the run
  ``running`` until the orphan reconciler noticed.

The existing suite only interrupts at the CLI layer (``JobEngine.run_job`` is
monkeypatched to raise) or inside init phases, so neither of these paths is
exercised. These tests raise ``KeyboardInterrupt`` from inside a real step.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.config.loader import WaveConfig
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.registry import StepEntry
from cnequity.orchestrator.run_lock import is_run_locked

DAY = date(2024, 6, 28)  # a Friday session; the seed calendar marks it trading
ACTIVE = {"running", "queued", "stale"}


def _config(tmp_path) -> Config:
    return Config(data_root=tmp_path / "data", publication_gate="off", audit_gate="off")


def _install_steps(monkeypatch, behaviour, *, input_datasets=()):
    """Route every step name to a fake whose result is ``behaviour(name)``."""
    seen: list[str] = []

    def entry(name: str) -> StepEntry:
        def run(config, trade_date, run_id, context):
            seen.append(name)
            return behaviour(name)

        return StepEntry(fn=run, group="core", input_datasets=tuple(input_datasets))

    monkeypatch.setattr("cnequity.orchestrator.engine.get_step", entry)
    return seen


def _interrupt_on(target: str):
    def behaviour(name: str) -> dict:
        if name == target:
            raise KeyboardInterrupt
        return {"rows_written": 1}

    return behaviour


# ---------------------------------------------------------------------------
# Ctrl-C inside run_job
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("finalize_run", [False, True], ids=["backfill-caller", "daily-caller"])
def test_ctrl_c_inside_a_step_closes_the_run_and_its_batches(tmp_path, monkeypatch, finalize_run):
    cfg = _config(tmp_path)
    engine = JobEngine(cfg)
    seen = _install_steps(monkeypatch, _interrupt_on("instruments"))

    with pytest.raises(KeyboardInterrupt):
        engine.run_job(
            "backfill",
            DAY,
            waves=[
                WaveConfig(name="write", parallel=False, steps=["instruments", "trading_calendar"])
            ],
            finalize_run=finalize_run,
        )

    # The interrupt stops the wave: the next step never starts.
    assert seen == ["instruments"]

    run = engine.manifest.latest_run("backfill")
    assert run is not None
    assert run["status"] == "failed"
    assert run["execution_status"] == "interrupted"
    # The first handler to see the interrupt names it; the `finally` fallback
    # must not overwrite that with the generic "worker exited" text.
    assert run["error_message"] == "interrupted by operator"

    batches = engine.manifest.get_batches_for_run(run["run_id"])
    assert batches, "the interrupted attempt must leave evidence in the ledger"
    assert not [b["batch_id"] for b in batches if b["status"] in ACTIVE]
    step_batch = next(b for b in batches if b["dataset"] == "instruments")
    assert step_batch["status"] == "failed"
    assert "interrupted" in (step_batch["error_message"] or "")

    # Retryable now, not after `batch_stale_seconds`: nothing looks like an
    # orphan, the run lock is gone, and the failed attempt is still owed.
    assert not is_run_locked(cfg.meta_root, run["run_id"])
    assert (
        engine.manifest.count_stale_running_runs(stale_after_seconds=0, locks_root=cfg.meta_root)
        == 0
    )
    assert engine.manifest.incomplete_batch_count(run["run_id"]) >= 1


def test_ctrl_c_in_a_single_step_run_closes_the_run(tmp_path, monkeypatch):
    """`cne run compact` and backfill's finalize go through ``run_step``."""
    cfg = _config(tmp_path)
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("backfill")
    _install_steps(monkeypatch, _interrupt_on("compact"))

    with pytest.raises(KeyboardInterrupt):
        engine.run_step("compact", DAY, run_id)

    run = engine.manifest.get_run(run_id)
    assert run["status"] == "failed"
    assert run["execution_status"] == "interrupted"
    batches = engine.manifest.get_batches_for_run(run_id)
    assert batches
    assert all(b["status"] == "failed" for b in batches)


# ---------------------------------------------------------------------------
# A step that returns something the engine does not understand
# ---------------------------------------------------------------------------


def test_a_step_returning_an_unknown_status_is_failed_not_trusted(tmp_path, monkeypatch):
    """`status="ok"` (or any typo) must not be recorded as a successful step.

    ``_run_step`` raises ValueError for an unknown status inside its own try,
    so the attempt is recorded as failed. Nothing tested that branch.
    """
    cfg = _config(tmp_path)
    engine = JobEngine(cfg)
    _install_steps(monkeypatch, lambda name: {"status": "ok", "rows_written": 3})

    result = engine.run_job(
        "backfill", DAY, waves=[WaveConfig(name="write", parallel=False, steps=["instruments"])]
    )

    step = result["results"][0]
    assert step["status"] == "failed"
    assert "invalid status" in step["error"]
    assert result["status"] == "failed"
    batch = next(
        b
        for b in engine.manifest.get_batches_for_run(result["run_id"])
        if b["dataset"] == "instruments"
    )
    assert batch["status"] == "failed"


# ---------------------------------------------------------------------------
# input_datasets: a derive must not run on a lake without its inputs
# ---------------------------------------------------------------------------


def test_a_step_without_its_committed_input_is_skipped_without_running(tmp_path, monkeypatch):
    cfg = _config(tmp_path)
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("backfill")
    seen = _install_steps(
        monkeypatch, lambda name: {"rows_written": 1}, input_datasets=("daily_bars",)
    )

    out = engine.run_step("instruments", DAY, run_id)

    assert seen == [], "the step body must not run against a missing input"
    assert out["status"] == "skipped"
    assert out["execution_status"] == "skipped"
    assert out["reason_code"] == "input_unavailable"
    assert "missing committed input: daily_bars" in out["error"]


def test_the_same_step_runs_once_its_input_is_committed(tmp_path, monkeypatch):
    cfg = _config(tmp_path)
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run("backfill")
    seen = _install_steps(
        monkeypatch, lambda name: {"rows_written": 1}, input_datasets=("daily_bars",)
    )
    root = cfg.curated_root / "daily_bars"
    root.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [DAY]}).write_parquet(
        root / "part.parquet"
    )

    out = engine.run_step("instruments", DAY, run_id)

    assert seen == ["instruments"]
    assert out["status"] == "success"
