"""`cne check` judges only the core datasets."""

from __future__ import annotations

from types import SimpleNamespace

from cnequity.cli.quality_cmds import _gates_on_dataset, _gating_finding


def test_only_core_datasets_gate_when_core_only():
    cfg = SimpleNamespace()
    assert _gates_on_dataset(cfg, "daily_bars", None, core_only=True)
    assert not _gates_on_dataset(cfg, "hot_rank", None, core_only=True)
    assert not _gates_on_dataset(cfg, "industry_index", None, core_only=True)
    # Without the flag every dataset still gates, as `cne status --gate` always has.
    assert _gates_on_dataset(cfg, "hot_rank", None)


def test_audit_errors_count_for_core_and_lake_wide_findings_only():
    assert _gating_finding({"dataset": "financial_statement_items"})
    assert _gating_finding({"dataset": "sources"})
    assert _gating_finding({})
    assert not _gating_finding({"dataset": "minute_bars"})
