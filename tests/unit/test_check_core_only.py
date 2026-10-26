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


def test_trading_status_scope_excuses_a_never_traded_undated_code(monkeypatch):
    from datetime import date

    import cnequity.steps.common as common
    from cnequity.cli.quality_cmds import _proven_absent_on

    day = date(2026, 10, 26)

    def evidence(symbol, reason):
        return {
            "symbol": symbol,
            "window_start": "2026-10-19",
            "window_end": "2026-10-26",
            "reason": reason,
        }

    monkeypatch.setattr(
        common,
        "load_negative_evidence",
        lambda _cfg, _ds: [
            evidence("001381.SZ", "source_empty"),  # IPO, never traded
            evidence("600001.SH", "source_empty"),  # undated but has traded
            evidence("000002.SZ", "source_empty"),  # dated: not a candidate
        ],
    )
    monkeypatch.setattr(common, "negative_evidence_covers", lambda item, sym, s, e: True)
    monkeypatch.setattr(common, "load_bar_universe", lambda _cfg: {"600001.SH", "000002.SZ"})

    undated = {"001381.SZ", "600001.SH"}
    assert _proven_absent_on(object(), "trading_status", day, undated=undated) == {"001381.SZ"}
    # daily_bars has always honoured every live record.
    assert len(_proven_absent_on(object(), "daily_bars", day)) == 3
