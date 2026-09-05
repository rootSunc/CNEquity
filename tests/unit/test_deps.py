import uuid

import pytest

import cnequity.steps  # noqa: F401 — register steps
from cnequity.orchestrator.deps import (
    CyclicDependencyError,
    UnknownStepError,
    step_execution_levels,
    validate_steps_registered,
)
from cnequity.orchestrator.registry import register_step


def test_reference_wave_steps_are_single_parallel_level():
    levels = step_execution_levels(["instruments", "trading_calendar", "trading_status"])
    assert len(levels) == 1
    assert set(levels[0]) == {"instruments", "trading_calendar", "trading_status"}


def test_corp_actions_before_daily_bars_in_sequential_wave():
    levels = step_execution_levels(["corporate_actions", "daily_bars"])
    assert levels[0] == ["corporate_actions"]
    assert levels[1] == ["daily_bars"]


def test_unknown_step_raises():
    with pytest.raises(UnknownStepError, match="not_a_step"):
        validate_steps_registered(["instruments", "not_a_step"])


def test_cyclic_dependency_raises():
    suffix = uuid.uuid4().hex[:8]
    name_a = f"cycle_a_{suffix}"
    name_b = f"cycle_b_{suffix}"

    @register_step(name_a, depends_on=[name_b])
    def _cycle_a(config, trade_date, run_id, context):
        return {}

    @register_step(name_b, depends_on=[name_a])
    def _cycle_b(config, trade_date, run_id, context):
        return {}

    with pytest.raises(CyclicDependencyError):
        step_execution_levels([name_a, name_b])


def test_core_group_level1_has_concurrent_tdx_steps():
    """corporate_actions (xdxr) and index_bars must not run in parallel without TDX lock."""
    levels = step_execution_levels(["corporate_actions", "index_bars"])
    assert len(levels) == 1
    assert set(levels[0]) == {"corporate_actions", "index_bars"}


def test_compact_runs_after_fetch_steps_in_core_group():
    steps = [
        "instruments",
        "trading_calendar",
        "trading_status",
        "corporate_actions",
        "daily_bars",
        "index_bars",
        "compact",
        "derive_adj_factors",
        "derive_industry_index",
    ]
    levels = step_execution_levels(steps)
    flat = [s for level in levels for s in level]
    assert flat.index("compact") > flat.index("daily_bars")
    assert flat.index("derive_adj_factors") > flat.index("compact")
    assert flat.index("derive_industry_index") > flat.index("derive_adj_factors")


def test_capital_group_runs_compact_last():
    steps = [
        "fund_flow",
        "northbound_holdings",
        "northbound_flows",
        "margin_trading",
        "valuation_metrics",
        "sector_members",
        "compact",
    ]
    levels = step_execution_levels(steps)
    assert levels[-1] == ["compact"]


def test_corporate_events_group_runs_compact_last():
    steps = ["announcement_index", "regulatory_events", "compact"]
    levels = step_execution_levels(steps)
    assert levels[-1] == ["compact"]


def test_news_wire_group_runs_compact_last():
    steps = ["flash_news_wire", "news_headlines", "compact"]
    levels = step_execution_levels(steps)
    assert levels[-1] == ["compact"]


def test_finalize_wave_order():
    levels = step_execution_levels(
        ["compact", "derive_adj_factors", "derive_industry_index", "audit"]
    )
    assert levels == [
        ["compact"],
        ["derive_adj_factors"],
        ["derive_industry_index"],
        ["audit"],
    ]


def test_finalize_subset_skips_missing_steps():
    levels = step_execution_levels(["compact", "audit"])
    assert levels[-2:] == [["compact"], ["audit"]]
