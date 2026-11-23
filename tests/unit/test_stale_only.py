"""`cne run daily --stale-only` — the same-day second attempt.

The gap it closes: a ``snapshot`` dataset fetches only the run day, so an outage
during the one scheduled window loses that day for good. valuation_metrics lost
2026-07-30 and 07-31 to a push2 clist outage; per-host retries already existed
and were exhausted, and no later run could have replayed a snapshot.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.cli.run_cmds import stale_fetch_steps

ANCHOR = date(2026, 7, 31)
BEHIND = date(2026, 7, 29)
FETCHED = datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc)


def _write(config, dataset: str, day: date, column: str = "trade_date", **extra) -> None:
    """Rows of *dataset* through *day*, published into the lake's catalog."""
    from tests.catalog_seed import publish_rows

    publish_rows(
        config,
        dataset,
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                column: [day],
                **{name: [value] for name, value in extra.items()},
                "source": ["tdx_protocol"],
                "data_version": ["v1"],
                "fetched_at": [FETCHED],
            }
        ),
        run_id=f"seed-{dataset}",
    )


def _watermark(config, dataset: str, day: date) -> None:
    """The watermark is read off the rows ``_write`` published."""


def _snapshot_capture(config, dataset: str, day: date) -> None:
    """A capture of a rolling snapshot: a refresh on the coverage ledger."""
    from datetime import time
    from zoneinfo import ZoneInfo

    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core import coverage as cov
    from equitylake_core.catalog import Catalog

    asked = datetime.combine(day, time(12), ZoneInfo("Asia/Shanghai"))
    with Catalog(lake_root(config), create=True) as catalog:
        cov.record(
            catalog,
            pl.DataFrame(
                {
                    "dataset": [dataset],
                    "period": [day.isoformat()],
                    "entity": [""],
                    "run": [f"run:seed:{dataset}:refresh"],
                    "asked_at": [asked],
                    "outcome": ["empty"],
                }
            ),
            message=f"seed {dataset}",
        )


@pytest.fixture
def lake(config):
    _write(config, "daily_bars", ANCHOR)
    _watermark(config, "daily_bars", ANCHOR)
    _write(config, "valuation_metrics", BEHIND)
    _watermark(config, "valuation_metrics", BEHIND)
    return config


def test_only_datasets_behind_the_anchor_are_selected(lake):
    steps = stale_fetch_steps(lake, ANCHOR)
    assert "valuation_metrics" in steps
    assert "daily_bars" not in steps


def test_a_feed_the_events_job_owns_is_left_to_that_job(lake):
    """Two jobs, two locks: the daily stale pass must not re-fetch an event feed."""
    from cnequity.config.loader import ScheduleGroup

    _write(lake, "announcement_index", BEHIND, "announce_date", announcement_id="A-1")
    _watermark(lake, "announcement_index", BEHIND)

    assert "announcement_index" in stale_fetch_steps(lake, ANCHOR)

    lake.events_groups = {
        "disclosures": ScheduleGroup(at="20:00", steps=["announcement_index", "compact"])
    }
    assert "announcement_index" not in stale_fetch_steps(lake, ANCHOR)


def test_derived_datasets_are_left_to_cne_derive(lake):
    """Re-fetching is not what a computed dataset needs."""
    _write(lake, "adj_factors", BEHIND)
    _watermark(lake, "adj_factors", BEHIND)
    assert "adj_factors" not in stale_fetch_steps(lake, ANCHOR)


def test_datasets_with_no_data_are_not_called_stale(lake):
    """An empty opt-in dataset is not behind; it was never started."""
    assert "minute_bars" not in stale_fetch_steps(lake, ANCHOR)


def test_per_dataset_tolerance_is_respected(lake):
    """margin_trading is T+1 tolerant; a one-day lag is not stale."""
    _write(lake, "margin_trading", date(2026, 7, 30))
    _watermark(lake, "margin_trading", date(2026, 7, 30))
    assert "margin_trading" not in stale_fetch_steps(lake, ANCHOR)


@pytest.mark.usefixtures("chinese_output")
def test_nothing_stale_is_a_clean_no_op(config, monkeypatch):
    """Safe on a timer: it must not create a run when there is nothing to do."""
    _write(config, "daily_bars", ANCHOR)
    _watermark(config, "daily_bars", ANCHOR)
    # Watermarkless rolling snapshots are considered stale until a capture
    # marker proves that their finite window was observed.
    _snapshot_capture(config, "economic_calendar", ANCHOR)
    _snapshot_capture(config, "share_unlock_schedule", ANCHOR)
    _snapshot_capture(config, "etf_profiles", ANCHOR)
    monkeypatch.setattr("cnequity.cli.run_cmds.current_anchor", lambda _cfg: ANCHOR)

    result = CliRunner().invoke(
        cli, ["run", "daily", "--stale-only", "--config", str(config.config_path)]
    )
    assert result.exit_code == 0
    assert "没有落后的数据集" in result.output


def test_stale_only_refuses_to_be_combined_with_group(config):
    result = CliRunner().invoke(
        cli,
        ["run", "daily", "--stale-only", "--group", "core", "--config", str(config.config_path)],
    )
    assert result.exit_code != 0
    assert "--group" in result.output


def test_scoped_retry_derives_core_outputs_after_compaction(config, monkeypatch):
    from cnequity.cli.run_cmds import _run_stale_only
    from cnequity.config import ScheduleGroup

    config.schedule_groups = {
        "core": ScheduleGroup(
            at="16:00",
            steps=["daily_bars", "compact", "derive_adj_factors", "derive_industry_index"],
        )
    }
    monkeypatch.setattr("cnequity.cli.run_cmds.last_trading_day", lambda *_a, **_k: ANCHOR)
    monkeypatch.setattr(
        "cnequity.cli.run_cmds.stale_fetch_plan",
        lambda *args, **kwargs: [{"dataset": "daily_bars", "priority": 1}],
    )
    captured = []

    class Engine:
        def run_job(self, *args, **kwargs):
            captured.extend(kwargs["waves"])
            return {"run_id": "isolated", "status": "success"}

    _run_stale_only(config, Engine(), ANCHOR, backfill=False, groups={"core"})
    assert [w.steps for w in captured] == [
        ["daily_bars"],
        ["compact"],
        ["derive_adj_factors", "derive_industry_index"],
    ]


@pytest.mark.parametrize(
    ("selection", "configured_events", "expected"),
    [
        ("news_headlines", True, [("events:news", [["news_headlines"], ["compact"]])]),
        ("news_headlines", False, [("events:stale", [["news_headlines"], ["compact"]])]),
        ("daily_bars", True, [("daily:stale", [["daily_bars"], ["compact"]])]),
        ("adj_factors", True, [("daily:stale", [["derive_adj_factors"]])]),
        (
            "daily_bars,adj_factors,news_headlines",
            True,
            [
                ("daily:stale", [["daily_bars"], ["compact"], ["derive_adj_factors"]]),
                ("events:news", [["news_headlines"], ["compact"]]),
            ],
        ),
        ("index_bars", True, []),
    ],
)
def test_explicit_stale_datasets_control_executed_steps(
    config, monkeypatch, selection, configured_events, expected
):
    from cnequity.config import ScheduleGroup

    config.best_effort_enabled = frozenset({"news_headlines", "flash_news_wire"})
    config.schedule_groups = {
        "core": ScheduleGroup(at="16:00", steps=["daily_bars", "compact", "derive_adj_factors"])
    }
    if configured_events:
        config.events_groups = {
            "news": ScheduleGroup(
                at="21:00",
                steps=["news_headlines", "flash_news_wire", "announcement_index", "compact"],
            )
        }
    names = ["daily_bars", "adj_factors", "news_headlines", "announcement_index", "index_bars"]
    marks = [BEHIND] * 4 + [ANCHOR]
    rows = pl.DataFrame(
        {
            "dataset": names,
            "has_data": [True] * len(names),
            "watermarked": [True] * len(names),
            "watermark": marks,
            "coverage_end": marks,
        }
    )
    monkeypatch.setattr("cnequity.cli.run_cmds._cfg", lambda _path: config)
    monkeypatch.setattr("cnequity.query.reader.list_datasets", lambda **_kwargs: rows)
    monkeypatch.setattr("cnequity.cli.run_cmds.last_trading_day", lambda *_a, **_k: ANCHOR)

    weekend = date(2026, 8, 2)
    jobs = []

    class Engine:
        def __init__(self, _config):
            pass

        def run_job(self, name, *, trade_date, waves, **_kwargs):
            assert trade_date == (weekend if name.startswith("events:") else ANCHOR)
            jobs.append((name, [wave.steps for wave in waves]))
            return {"run_id": name, "status": "success"}

    monkeypatch.setattr("cnequity.cli.run_cmds.JobEngine", Engine)
    result = CliRunner().invoke(
        cli,
        [
            "run",
            "daily",
            "--stale-only",
            "--trade-date",
            weekend.isoformat(),
            "--datasets",
            selection,
        ],
    )
    assert result.exit_code == 0, (result.output, result.exception)
    assert jobs == expected
