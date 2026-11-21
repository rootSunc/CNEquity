"""Bars owed per security and session: what the source answered is kept per cell."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl

from cnequity.config import Config
from cnequity.quality import planned_coverage
from cnequity.steps import common

HALT = date(2025, 10, 9)
FAILED = date(2025, 10, 10)


def _lake(tmp_path) -> Config:
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core import coverage as cov
    from equitylake_core.catalog import Catalog

    cfg = Config(data_root=tmp_path / "data")
    with Catalog(lake_root(cfg), create=True) as catalog:
        cov.prepare(catalog)
    return cfg


def _gaps(*cells: tuple[str, str]):
    frame = pl.DataFrame(
        {
            "period": [period for period, _ in cells],
            "entity": [entity for _, entity in cells],
            "state": ["gap"] * len(cells),
        },
        schema={"period": pl.String, "entity": pl.String, "state": pl.String},
    )

    def planned(config, catalog, scope, wanted=None):
        return {name: SimpleNamespace(cells=frame) for name in wanted}

    return planned


def test_a_source_empty_symbol_is_answered_per_cell_and_a_proof_is_not(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    monkeypatch.setattr(common, "list_trading_dates", lambda config, start, end: [HALT])
    common.record_negative_evidence(
        cfg, "daily_bars", ["600001.SH"], HALT, HALT, reason="source_empty", source="fallback"
    )
    # Listings and trading status already excuse these cells in the planner.
    common.record_negative_evidence(
        cfg,
        "daily_bars",
        ["600002.SH"],
        HALT,
        HALT,
        reason="verified_no_data",
        source="instruments_or_trading_status",
    )
    assert planned_coverage.answered_cells(cfg, "daily_bars") == {("2025-10-09", "600001.SH")}
    # Nothing reaches meta/state.
    assert not (cfg.meta_root / "state" / "daily_bars.json").exists()


def test_a_failed_fetch_is_unanswered_and_an_empty_answer_is_not(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    planned_coverage.record_cell_answers(
        cfg, "daily_bars", [("2025-10-09", "600001.SH")], run="step:source_empty"
    )
    monkeypatch.setattr(
        planned_coverage,
        "planned",
        _gaps(("2025-10-09", "600001.SH"), ("2025-10-10", "600001.SH")),
    )
    unknown = planned_coverage.unanswered_keys(cfg, "daily_bars", HALT, FAILED)
    assert unknown.rows() == [("600001.SH", FAILED)]


def test_the_switch_carries_what_the_old_ledger_holds_answered(tmp_path, monkeypatch):
    import json

    cfg = _lake(tmp_path)
    # What an older lake's meta/state/daily_bars.json held.
    legacy = cfg.meta_root / "state" / "daily_bars.json"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(
        json.dumps(
            {
                "missing_ranges": [{"start": "2025-10-08", "end": "2025-10-08"}],
                "outstanding_keys": [{"symbol": "600003.SH", "trade_date": "2025-10-09"}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        planned_coverage,
        "planned",
        _gaps(
            ("2025-10-08", "600001.SH"),  # its day is still owed
            ("2025-10-09", "600003.SH"),  # an outstanding key
            ("2025-10-09", "501005.SH"),  # a fund that never prints: answered
            ("2025-11-19", "501005.SH"),  # not settled yet
        ),
    )
    found = planned_coverage.entity_seeds(cfg, date(2025, 11, 20))
    assert found == {"daily_bars": [("2025-10-09", "501005.SH")]}


def test_an_index_session_short_of_some_index_is_answered_in_part(tmp_path, monkeypatch):
    from cnequity.steps.bars import index_history

    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(index_history, "incremental_window", lambda *args: HALT)
    monkeypatch.setattr(
        index_history.bar_scope, "_reject_unfinished_daily_bar_window", lambda *a: None
    )
    monkeypatch.setattr(index_history, "fetch_index_bars", lambda *args, **kwargs: pl.DataFrame())
    monkeypatch.setattr(index_history, "normalize_with_source", lambda df, source: df)
    monkeypatch.setattr(
        index_history, "_index_bar_missing_keys", lambda *args: [("000001.SH", HALT)]
    )
    monkeypatch.setattr(index_history, "list_trading_dates", lambda config, a, b: [HALT, FAILED])
    monkeypatch.setattr(common, "write_simple", lambda *args, **kwargs: {"status": "success"})
    result = index_history.step_index_bars(cfg, FAILED, "r1", {})
    answers = {
        item["outcome"]: item["periods"]
        for item in result["context_updates"]["audit_findings"]
        if item["check"] == planned_coverage.ANSWER_CHECK
    }
    assert answers == {"partial": ["2025-10-09"], "complete": ["2025-10-10"]}
    assert not (cfg.meta_root / "state" / "index_bars.json").exists()
