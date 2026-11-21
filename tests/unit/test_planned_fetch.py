"""The planner adds days to fetch beside the old ledgers, and fetch steps report what came back empty."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.quality import planned_coverage
from cnequity.steps.common import fetch_incremental_daily, incremental_trade_dates
from cnequity.storage.state import StateStore

DAY = date(2025, 10, 9)
LATE = date(2025, 9, 26)  # a session the watermark already passed


def _lake(tmp_path, *, planned: bool = True) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    StateStore(cfg.meta_root).set_date("margin_trading", date(2025, 9, 30))
    if planned:
        cfg = replace(cfg, planned_datasets=frozenset({"margin_trading"}))
    return cfg


def test_the_planner_adds_its_gaps_and_never_drops_a_day(tmp_path, monkeypatch):
    monkeypatch.setattr(planned_coverage, "gap_days", lambda config, dataset, day: [LATE])
    assert incremental_trade_dates(_lake(tmp_path), "margin_trading", DAY) == [LATE, DAY]
    # Not planned: the old ledgers alone decide.
    assert incremental_trade_dates(_lake(tmp_path, planned=False), "margin_trading", DAY) == [DAY]


def test_a_planner_that_cannot_plan_leaves_the_old_ledgers_deciding(tmp_path, monkeypatch):
    def broken(config, dataset, day):
        raise OSError("catalog busy")

    monkeypatch.setattr(planned_coverage, "gap_days", broken)
    assert incremental_trade_dates(_lake(tmp_path), "margin_trading", DAY) == [DAY]


def test_the_fetch_says_where_the_two_sides_differ_and_what_came_back_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(planned_coverage, "gap_days", lambda config, dataset, day: [LATE])
    fetched = []

    def fetch(day):
        fetched.append(day)
        if day == LATE:
            return pl.DataFrame(schema={"trade_date": pl.Date, "symbol": pl.String})
        return pl.DataFrame({"trade_date": [day], "symbol": ["600519.SH"]})

    _, findings = fetch_incremental_daily(
        _lake(tmp_path), "margin_trading", DAY, fetch, allow_empty=True
    )
    assert fetched == [LATE, DAY]
    by_check = {item["check"]: item for item in findings}
    split = by_check["planner_fetch_split"]
    assert (split["planner_only"], split["old_only"]) == (["2025-09-26"], ["2025-10-09"])
    assert by_check[planned_coverage.ANSWER_CHECK]["periods"] == ["2025-09-26"]


def test_an_unplanned_fetch_reports_no_answers(tmp_path):
    _, findings = fetch_incremental_daily(
        _lake(tmp_path, planned=False),
        "margin_trading",
        DAY,
        lambda day: pl.DataFrame(schema={"trade_date": pl.Date, "symbol": pl.String}),
        allow_empty=True,
    )
    assert planned_coverage.ANSWER_CHECK not in {item["check"] for item in findings}


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


def test_planned_names_known_datasets(tmp_path):
    from cnequity.config import load_config
    from cnequity.config.bootstrap import path_for_toml
    from cnequity.config.errors import ConfigError

    path = tmp_path / "c.toml"
    data = tmp_path / "data"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(data)}"\n[datasets]\nplanned = ["margin_trading"]\n'
    )
    assert load_config(path).planned_datasets == {"margin_trading"}
    path.write_text(f'[data]\nroot = "{path_for_toml(data)}"\n[datasets]\nplanned = ["nope"]\n')
    with pytest.raises(ConfigError, match="unknown datasets"):
        load_config(path)


def _alone(tmp_path, dataset="margin_trading") -> Config:
    cfg = Config(data_root=tmp_path / "data")
    state = StateStore(cfg.meta_root)
    # What the old ledgers would say; a planner-only dataset reads none of it.
    state.set_date(dataset, date(2025, 9, 1))
    state.record_missing_dates(dataset, [date(2025, 8, 1)], reason="old")
    return replace(cfg, planner_only=frozenset({dataset}))


def test_a_planner_only_dataset_fetches_the_planner_s_days_and_reads_no_state(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(planned_coverage, "gap_days", lambda config, dataset, day: [LATE])
    # No reconcile tail: the run day, and the day the planner owes.
    assert incremental_trade_dates(_alone(tmp_path), "margin_trading", DAY) == [LATE, DAY]


def test_a_planner_only_fetch_writes_no_ledger_and_records_its_answers(tmp_path, monkeypatch):
    from cnequity.orchestrator.outcomes import SourceUnavailableError

    monkeypatch.setattr(planned_coverage, "gap_days", lambda config, dataset, day: [LATE])
    cfg = _alone(tmp_path)

    def fetch(day):
        if day == LATE:
            raise SourceUnavailableError("busy")
        return pl.DataFrame(schema={"trade_date": pl.Date, "symbol": pl.String})

    _, findings = fetch_incremental_daily(cfg, "margin_trading", DAY, fetch, allow_empty=True)
    # The failed day is not written anywhere: it is simply still a gap.
    assert StateStore(cfg.meta_root).get_missing_dates("margin_trading") == {date(2025, 8, 1)}
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
    assert (
        StateStore(cfg.meta_root).get_date("announcement_index", field="tail_reconciled_on") is None
    )


def test_a_recorded_recheck_dates_the_last_sweep_by_the_market_day(tmp_path):
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core import coverage as cov
    from equitylake_core.catalog import Catalog

    cfg = Config(data_root=tmp_path / "data")
    with Catalog(lake_root(cfg), create=True) as catalog:
        cov.prepare(catalog)
    assert planned_coverage.last_recheck(cfg, "announcement_index", "deep") is None
    # 17:00 UTC on the 19th is already the 20th in Shanghai.
    asked = datetime(2026, 11, 19, 17, tzinfo=UTC)
    found = planned_coverage.recheck_finding("announcement_index", [DAY], "deep", asked)
    assert planned_coverage.record_answers(cfg, "r1", [found]) == 1
    assert planned_coverage.last_recheck(cfg, "announcement_index", "deep") == date(2026, 11, 20)
    assert planned_coverage.last_recheck(cfg, "announcement_index", "tail") is None
    with Catalog(lake_root(cfg), read_only=True) as catalog:
        assert cov.attempted(catalog, "announcement_index")["outcome"].to_list() == ["complete"]


def test_compact_keeps_no_watermark_for_a_planner_only_dataset(tmp_path):
    from cnequity.steps.finalize import _update_watermarks

    cfg = _alone(tmp_path)
    _update_watermarks(cfg, frozenset({"margin_trading"}), DAY)
    assert StateStore(cfg.meta_root).get_date("margin_trading") == date(2025, 9, 1)


def test_planner_only_names_known_datasets_that_are_not_derived(tmp_path):
    from cnequity.config import load_config
    from cnequity.config.bootstrap import path_for_toml
    from cnequity.config.errors import ConfigError

    path = tmp_path / "c.toml"
    data = tmp_path / "data"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(data)}"\n[datasets]\nplanner_only = ["margin_trading"]\n'
    )
    assert load_config(path).planner_only == {"margin_trading"}
    path.write_text(
        f'[data]\nroot = "{path_for_toml(data)}"\n[datasets]\nplanner_only = ["adj_factors"]\n'
    )
    with pytest.raises(ConfigError, match="derived datasets"):
        load_config(path)


def test_a_planner_only_snapshot_is_covered_through_its_last_capture(tmp_path):
    from cnequity.quality.verify import covered_through
    from cnequity.steps.http_common import _mark_snapshot_capture
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core import coverage as cov
    from equitylake_core.catalog import Catalog

    cfg = replace(Config(data_root=tmp_path / "data"), planner_only=frozenset({"market_holidays"}))
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
    _mark_snapshot_capture(cfg, "market_holidays", date(2026, 11, 20))
    assert StateStore(cfg.meta_root).get_date("market_holidays", field="last_snapshot_date") is None


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


def test_a_planner_only_backfill_skips_settled_days_and_records_its_empty_answers(
    tmp_path, monkeypatch
):
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
    # The ledger's old missing day (2025-08-01) is not asked; the settled day is skipped.
    assert asked == days[1:]
    answers = [
        item
        for item in out["context_updates"]["audit_findings"]
        if item["check"] == planned_coverage.ANSWER_CHECK
    ]
    assert answers[0]["periods"] == ["2025-10-07"]
    assert StateStore(cfg.meta_root).get_missing_dates("margin_trading") == {date(2025, 8, 1)}


def test_a_planner_only_backfill_asks_again_what_redo_names(tmp_path, monkeypatch):
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
