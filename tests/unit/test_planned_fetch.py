"""The planner decides what a fetch asks, and fetch steps report what came back empty."""

from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from cnequity.config import Config
from cnequity.quality import planned_coverage
from cnequity.steps.common import fetch_incremental_daily, incremental_trade_dates
from cnequity.storage.state import StateStore

DAY = date(2025, 10, 9)
LATE = date(2025, 9, 26)  # a session the watermark already passed


def test_compact_records_each_answer_once(tmp_path):
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core import coverage as cov
    from equitylake_core.catalog import Catalog

    cfg = Config(data_root=tmp_path / "data")
    with Catalog(lake_root(cfg), create=True) as catalog:
        cov.prepare(catalog)
    asked = datetime(2026, 11, 18, 10, tzinfo=UTC)
    answers = [
        planned_coverage.answer_finding("margin_trading", [LATE], asked),
        {"check": "row_count", "dataset": "margin_trading"},
    ]
    assert planned_coverage.record_answers(cfg, "r1", answers) == 1
    # The run compacts again with the same findings.
    assert planned_coverage.record_answers(cfg, "r1", answers) == 0
    with Catalog(lake_root(cfg), read_only=True) as catalog:
        rows = cov.attempted(catalog, "margin_trading")
    assert rows.select("period", "outcome").rows() == [("2025-09-26", "empty")]


def _alone(tmp_path, dataset="margin_trading") -> Config:
    return Config(data_root=tmp_path / "data")


def _no_state(cfg, dataset="margin_trading") -> bool:
    """Nothing about coverage is written to ``meta/state``."""
    return not (cfg.meta_root / "state" / f"{dataset}.json").exists()


def test_a_dataset_fetches_the_planner_s_days_and_reads_no_state(tmp_path, monkeypatch):
    monkeypatch.setattr(planned_coverage, "gap_days", lambda config, dataset, day: [LATE])
    # No reconcile tail: the run day, and the day the planner owes.
    assert incremental_trade_dates(_alone(tmp_path), "margin_trading", DAY) == [LATE, DAY]


def test_a_fetch_writes_no_state_and_records_its_answers(tmp_path, monkeypatch):
    from cnequity.orchestrator.outcomes import SourceUnavailableError

    monkeypatch.setattr(planned_coverage, "gap_days", lambda config, dataset, day: [LATE])
    cfg = _alone(tmp_path)

    def fetch(day):
        if day == LATE:
            raise SourceUnavailableError("busy")
        return pl.DataFrame(schema={"trade_date": pl.Date, "symbol": pl.String})

    _, findings = fetch_incremental_daily(cfg, "margin_trading", DAY, fetch, allow_empty=True)
    # The failed day is not written anywhere: it is simply still a gap.
    assert _no_state(cfg)
    by_check = {item["check"]: item for item in findings}
    assert by_check[planned_coverage.ANSWER_CHECK]["periods"] == [DAY.isoformat()]
    assert "planner_fetch_split" not in by_check


def test_a_clean_tiered_sweep_is_recorded_as_a_recheck(tmp_path, monkeypatch):
    from cnequity.steps import common

    monkeypatch.setattr(planned_coverage, "gap_days", lambda config, dataset, day: [])
    monkeypatch.setattr(common, "_reconciliation_depth", lambda config, dataset, day: "tail")
    cfg = _alone(tmp_path, "announcement_index")

    def fetch(day):
        return pl.DataFrame({"announce_date": [day], "symbol": ["600519.SH"]})

    _, findings = fetch_incremental_daily(
        cfg, "announcement_index", DAY, fetch, allow_empty=True, date_col="announce_date"
    )
    recheck = next(item for item in findings if item["check"] == planned_coverage.RECHECK_CHECK)
    assert recheck["depth"] == "tail" and recheck["periods"][-1] == DAY.isoformat()
    assert _no_state(cfg, "announcement_index")


def test_a_recorded_recheck_dates_the_last_sweep_by_the_day_it_ran_for(tmp_path):
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core import coverage as cov
    from equitylake_core.catalog import Catalog

    cfg = Config(data_root=tmp_path / "data")
    with Catalog(lake_root(cfg), create=True) as catalog:
        cov.prepare(catalog)
    assert planned_coverage.last_recheck(cfg, "announcement_index", "deep") is None
    # A catch-up run asked a year later: the sweep is the run day's (its window's end).
    asked = datetime(2026, 11, 19, 17, tzinfo=UTC)
    found = planned_coverage.recheck_finding("announcement_index", [LATE, DAY], "deep", asked)
    assert planned_coverage.record_answers(cfg, "r1", [found]) == 2
    assert planned_coverage.last_recheck(cfg, "announcement_index", "deep") == DAY
    assert planned_coverage.last_recheck(cfg, "announcement_index", "tail") is None
    with Catalog(lake_root(cfg), read_only=True) as catalog:
        assert set(cov.attempted(catalog, "announcement_index")["outcome"]) == {"complete"}


def test_a_snapshot_is_covered_through_its_last_capture(tmp_path):
    from cnequity.quality.verify import covered_through
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core import coverage as cov
    from equitylake_core.catalog import Catalog

    cfg = Config(data_root=tmp_path / "data")
    with Catalog(lake_root(cfg), create=True) as catalog:
        cov.prepare(catalog)
        # A capture that found nothing new: no revision, one refresh attempt.
        cov.record(
            catalog,
            pl.DataFrame(
                {
                    "dataset": ["market_holidays"],
                    "period": ["2026-11-19"],
                    "entity": [""],
                    "run": ["run:r1:refresh"],
                    "asked_at": [datetime(2026, 11, 19, 10, tzinfo=UTC)],
                    "outcome": ["empty"],
                }
            ),
            message="r1",
        )
    assert covered_through(cfg, "market_holidays") == date(2026, 11, 19)


def test_the_switch_carries_the_last_sweeps_into_the_ledger_once(tmp_path):
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core import coverage as cov
    from equitylake_core.catalog import Catalog

    cfg = Config(data_root=tmp_path / "data")
    with Catalog(lake_root(cfg), create=True) as catalog:
        cov.prepare(catalog)
    state = StateStore(cfg.meta_root)
    state.set_date("daily_bars", date(2026, 11, 16), field="deep_reconciled_on")
    state.set_date("daily_bars", date(2026, 11, 19), field="tail_reconciled_on")
    assert planned_coverage.seed_rechecks(cfg, ["daily_bars", "margin_trading"]) == 2
    assert planned_coverage.last_recheck(cfg, "daily_bars", "deep") == date(2026, 11, 16)
    assert planned_coverage.last_recheck(cfg, "daily_bars", "tail") == date(2026, 11, 19)
    assert planned_coverage.seed_rechecks(cfg, ["daily_bars"]) == 0


def test_a_backfill_skips_settled_days_and_records_its_empty_answers(tmp_path, monkeypatch):
    from cnequity.run_scope import set_backfill
    from cnequity.steps import common

    cfg = _alone(tmp_path)
    set_backfill(cfg, active=True, start=date(2025, 10, 6), end=date(2025, 10, 9))
    days = [date(2025, 10, 6), date(2025, 10, 7), date(2025, 10, 8), date(2025, 10, 9)]
    monkeypatch.setattr(common, "list_trading_dates", lambda config, s, e: days)
    monkeypatch.setattr(
        planned_coverage, "settled_days", lambda config, ds, s, e, **k: {date(2025, 10, 6)}
    )
    asked = []

    def fetch(day):
        asked.append(day)
        if day == date(2025, 10, 7):
            return pl.DataFrame(schema={"trade_date": pl.Date, "symbol": pl.String})
        return pl.DataFrame(
            {
                "trade_date": [day],
                "symbol": ["600519.SH"],
                "margin_balance": [1.0],
                "margin_buy": [1.0],
                "short_balance": [0.0],
                "short_sell_volume": [0.0],
            }
        )

    out = common.walk_day_backfill(
        cfg, DAY, "r1", "margin_trading", fetch, source="sse", allow_empty_days=True
    )
    # The settled day is skipped.
    assert asked == days[1:]
    answers = [
        item
        for item in out["context_updates"]["audit_findings"]
        if item["check"] == planned_coverage.ANSWER_CHECK
    ]
    assert answers[0]["periods"] == ["2025-10-07"]
    assert _no_state(cfg)


def test_a_backfill_asks_again_what_redo_names(tmp_path, monkeypatch):
    from cnequity.run_scope import set_backfill
    from cnequity.steps import common

    cfg = _alone(tmp_path)
    set_backfill(cfg, active=True, start=date(2025, 10, 6), end=date(2025, 10, 6), redo="empty")
    monkeypatch.setattr(common, "list_trading_dates", lambda config, s, e: [date(2025, 10, 6)])
    seen = {}

    def settled(config, ds, s, e, *, redo="gaps"):
        seen["redo"] = redo
        return {date(2025, 10, 6)}

    monkeypatch.setattr(planned_coverage, "settled_days", settled)
    out = common.walk_day_backfill(cfg, DAY, "r1", "margin_trading", lambda d: None, source="sse")
    assert seen["redo"] == "empty" and out["already_covered"] == 1


def test_a_partial_day_is_recorded_partial_and_shown_as_missing(tmp_path):
    from cnequity.orchestrator.source_gaps import record_source_gap, source_gap_scope
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core import coverage as cov
    from equitylake_core.catalog import Catalog

    cfg = _alone(tmp_path)
    with Catalog(lake_root(cfg), create=True) as catalog:
        cov.prepare(catalog)
    with source_gap_scope(cfg) as findings:
        record_source_gap("margin_trading", "SZ side missing", dates=[DAY])
    answer = next(item for item in findings if item["check"] == planned_coverage.ANSWER_CHECK)
    # The same shape compact records from ``answer_finding``.
    expected = planned_coverage.answer_finding("margin_trading", [DAY], DAY, "partial")
    assert set(answer) == set(expected) and answer["outcome"] == "partial"
    # Nothing goes to meta/state; the coverage ledger has it.
    assert _no_state(cfg)
    assert planned_coverage.record_answers(cfg, "r1", findings) == 1
    view = planned_coverage.ledger_view(cfg, "margin_trading")
    assert [row["start"] for row in view["missing_ranges"]] == [DAY.isoformat()]
    # A clean answer later settles it.
    clean = planned_coverage.answer_finding("margin_trading", [DAY], datetime.now(UTC), "complete")
    planned_coverage.record_answers(cfg, "r2", [clean])
    assert planned_coverage.ledger_view(cfg, "margin_trading")["missing_ranges"] == []


def test_a_dataset_compaction_never_watermarks_has_no_computed_watermark(tmp_path):
    from cnequity.domain.datasets import DATASETS, PARTITION_COLS, WATERMARK_SKIP
    from cnequity.quality.verify import covered_through

    # top_holders keeps no watermark (WATERMARK_SKIP); its window is the default one.
    assert "top_holders" in WATERMARK_SKIP and PARTITION_COLS.get("top_holders")
    cfg = Config(data_root=tmp_path / "data")
    assert covered_through(cfg, "top_holders") is None
    # A derived dataset without a derive watermark neither.
    assert DATASETS["delisting_events"].watermark is False
    assert covered_through(cfg, "delisting_events") is None


def test_the_retired_planner_switches_load_and_say_so(tmp_path, caplog):
    import logging

    from cnequity.config import load_config, loader
    from cnequity.config.bootstrap import path_for_toml

    loader._WARNED_RENAMES.clear()
    path = tmp_path / "c.toml"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "data")}"\n'
        '[datasets]\nplanned = ["margin_trading"]\nplanner_only = ["daily_bars"]\n'
    )
    with caplog.at_level(logging.WARNING, logger="cnequity.config.loader"):
        load_config(path)
    said = " ".join(record.getMessage() for record in caplog.records)
    assert "[datasets] planned no longer does anything" in said
    assert "[datasets] planner_only no longer does anything" in said
