"""Pure helper branches in orchestrator/init_phases.py not exercised elsewhere."""

from __future__ import annotations

from cn_market_lake.orchestrator.init_phases import (
    FINALIZE_STEPS,
    INIT_PHASE_STEPS,
    expected_steps,
    init_run_complete,
    needs_finalize,
    pending_phases,
    phases_never_started,
    step_backfill,
    step_incomplete,
)


def test_expected_steps_dedupes_repeated_phase():
    # Same phase listed twice: the second pass's steps are already seen.
    steps = expected_steps(["phase1_reference", "phase1_reference"])
    assert steps == ["instruments", "trading_calendar"]


def test_step_backfill_all_branches():
    assert step_backfill("corporate_actions", ["phase2a_corporate_actions"]) is True
    assert step_backfill("corporate_actions", []) is False
    assert step_backfill("daily_bars", ["phase2c_daily_bars_backfill"]) is True
    assert step_backfill("daily_bars", []) is False
    assert step_backfill("compact", ["phase4_finalize"]) is False


def test_step_incomplete_true_and_false():
    batches = [{"dataset": "daily_bars", "status": "failed"}]
    assert step_incomplete(batches, "daily_bars") is True
    ok_batches = [{"dataset": "daily_bars", "status": "success"}]
    assert step_incomplete(ok_batches, "daily_bars") is False
    assert step_incomplete([], "daily_bars") is False


def test_pending_phases_skips_completed_and_unknown_phase():
    phases = ["phase1_reference", "not_a_real_phase", "phase3_index_and_status"]
    batches = [
        {"dataset": "instruments", "status": "success"},
        {"dataset": "trading_calendar", "status": "success"},
    ]
    assert pending_phases(phases, batches) == ["phase3_index_and_status"]


def test_pending_phases_all_done():
    phases = ["phase1_reference"]
    batches = [
        {"dataset": "instruments", "status": "success"},
        {"dataset": "trading_calendar", "status": "success"},
    ]
    assert pending_phases(phases, batches) == []


def test_phases_never_started_ignores_unknown_phase_and_partial_phase():
    phases = ["not_a_real_phase", "phase1_reference"]
    batches = [{"dataset": "instruments", "status": "success"}]
    # instruments started (success) but trading_calendar never did → phase is
    # "partial", not "never started" — left alone so retry resumes just it.
    assert phases_never_started(phases, batches) == []


def test_phases_never_started_fully_untouched_phase():
    phases = ["phase1_reference"]
    assert phases_never_started(phases, []) == ["phase1_reference"]


def test_needs_finalize_false_when_finalize_not_requested():
    assert needs_finalize(["phase1_reference"], []) is False


def test_needs_finalize_false_until_prior_phases_complete():
    phases = ["phase1_reference", "phase4_finalize"]
    assert needs_finalize(phases, []) is False


def test_needs_finalize_true_when_prior_done_and_finalize_incomplete():
    phases = ["phase1_reference", "phase4_finalize"]
    batches = [
        {"dataset": "instruments", "status": "success"},
        {"dataset": "trading_calendar", "status": "success"},
    ]
    assert needs_finalize(phases, batches) is True


def test_needs_finalize_false_when_finalize_already_done():
    phases = ["phase1_reference", "phase4_finalize"]
    batches = [{"dataset": "instruments", "status": "success"}] + [
        {"dataset": step, "status": "success"} for step in sorted(FINALIZE_STEPS)
    ]
    batches.append({"dataset": "trading_calendar", "status": "success"})
    assert init_run_complete(phases, batches) is True
    assert needs_finalize(phases, batches) is False


def test_finalize_steps_match_the_phase_definition():
    """Regression: the two lists drifted apart when derive_industry_index landed.

    ``FINALIZE_STEPS`` gates ``needs_finalize`` and ``phase4_finalize`` gates
    ``init_run_complete``. Adding a step to one and not the other leaves init
    either re-running finalize forever or calling an incomplete run complete,
    and every fixture that spells the steps out by hand goes stale silently.
    """
    assert set(INIT_PHASE_STEPS["phase4_finalize"]) == set(FINALIZE_STEPS)
