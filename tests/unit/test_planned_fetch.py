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
