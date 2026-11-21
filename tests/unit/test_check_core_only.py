"""`cne check` judges only the core datasets."""

from __future__ import annotations

from types import SimpleNamespace

from cnequity.cli.quality_cmds import _gates_on_dataset
from cnequity.quality.gate import is_core


def test_only_core_datasets_gate_when_core_only():
    cfg = SimpleNamespace()
    assert _gates_on_dataset(cfg, "daily_bars", None, core_only=True)
    assert not _gates_on_dataset(cfg, "hot_rank", None, core_only=True)
    assert not _gates_on_dataset(cfg, "industry_index", None, core_only=True)
    # Without the flag every dataset still gates, as `cne status --gate` always has.
    assert _gates_on_dataset(cfg, "hot_rank", None)


def test_audit_errors_count_for_core_and_lake_wide_findings_only():
    assert is_core("financial_statement_items")
    assert is_core("sources")
    assert is_core("")
    assert not is_core("minute_bars")


def test_trading_status_scope_excuses_a_never_traded_undated_code(monkeypatch):
    from datetime import date

    import cnequity.quality.planned_coverage as planned_coverage
    import cnequity.steps.common as common
    from cnequity.cli.quality_cmds import _proven_absent_on

    day = date(2026, 10, 26)
    # The bar probe answered "no rows" for each of these on the day.
    monkeypatch.setattr(
        planned_coverage,
        "answered_cells",
        lambda _cfg, _ds: {
            (day.isoformat(), "001381.SZ"),  # IPO, never traded
            (day.isoformat(), "600001.SH"),  # undated but has traded
            (day.isoformat(), "000002.SZ"),  # dated: not a candidate
            ("2026-10-23", "300001.SZ"),  # another day
        },
    )
    monkeypatch.setattr(common, "load_bar_universe", lambda _cfg: {"600001.SH", "000002.SZ"})

    undated = {"001381.SZ", "600001.SH"}
    assert _proven_absent_on(object(), "trading_status", day, undated=undated) == {"001381.SZ"}
    # daily_bars honours every cell answered on the day.
    assert len(_proven_absent_on(object(), "daily_bars", day)) == 3
