"""A fallback table the schedule depends on daily is held to daily freshness."""

from __future__ import annotations

from datetime import date

from cnequity.config.loader import ScheduleGroup
from cnequity.quality.freshness import is_stale


def _with_groups(config, steps):
    config.schedule_groups = {"capital": ScheduleGroup(at="16:30", steps=list(steps))}
    return config


def test_unscheduled_fallback_is_never_stale(config):
    _with_groups(config, ["fund_flow", "compact"])
    assert not is_stale("fund_flow_ths", date(2026, 1, 5), date(2026, 10, 23), config)


def test_scheduled_fallback_missing_a_session_is_stale(config):
    _with_groups(config, ["fund_flow_ths", "compact"])
    # A snapshot cannot be fetched later by date, so the closed session itself is due.
    assert not is_stale("fund_flow_ths", date(2026, 10, 23), date(2026, 10, 23), config)
    assert is_stale("fund_flow_ths", date(2026, 10, 22), date(2026, 10, 23), config)


def test_a_weekend_is_not_a_missed_session(config):
    _with_groups(config, ["sector_fund_flow_ths"])
    # Friday's rows on Saturday: no session has closed since.
    assert not is_stale("sector_fund_flow_ths", date(2026, 10, 23), date(2026, 10, 24), config)
    # Friday's rows once Monday has closed: Monday is missing.
    assert is_stale("sector_fund_flow_ths", date(2026, 10, 23), date(2026, 10, 26), config)


def test_the_gate_skips_a_dataset_the_config_removed_on_purpose(config):
    from dataclasses import replace

    from cnequity.cli.quality_cmds import _gates_on_dataset

    _with_groups(config, ["fund_flow_ths", "compact"])
    assert _gates_on_dataset(config, "fund_flow", {"capital"})  # unknown owner still gates
    opted = replace(config, opted_out_steps=frozenset({"fund_flow"}))
    assert not _gates_on_dataset(opted, "fund_flow", {"capital"})


def test_a_dataset_removed_on_purpose_is_a_separate_state_not_stale(config):
    from dataclasses import replace

    from cnequity.quality.freshness import freshness_state

    _with_groups(config, ["fund_flow_ths", "compact"])
    old, anchor = date(2026, 9, 21), date(2026, 10, 23)
    # Unscheduled without an explicit opt-out: still owed, still stale.
    assert freshness_state("fund_flow", old, anchor, config) == "stale"
    removed = replace(config, opted_out_steps=frozenset({"fund_flow"}))
    assert freshness_state("fund_flow", old, anchor, removed) == "removed"
    assert not is_stale("fund_flow", old, anchor, removed)
    # Another group running it again makes it due.
    _with_groups(removed, ["fund_flow", "compact"])
    assert freshness_state("fund_flow", old, anchor, removed) == "stale"
