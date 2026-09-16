"""A bare `cne run daily` must say what it did not run.

Without `--group` the job runs `[[job.daily.waves]]` — the core spine — while
two thirds of the registered datasets live in `[job.daily.groups.*]`. That
combination has no failure mode: the run exits 0 and most datasets are simply
never updated, which is what the README used to recommend as the whole daily job.
"""

from __future__ import annotations

from cnequity.cli.run_cmds import datasets_outside_the_daily_waves
from cnequity.config import ScheduleGroup, WaveConfig, load_config


class _Cfg:
    def __init__(self, waves, groups, events=None):
        self.daily_waves = [WaveConfig(name="w", parallel=True, steps=list(waves))]
        self.schedule_groups = {
            name: ScheduleGroup(at="16:00", steps=list(steps)) for name, steps in groups.items()
        }
        self.events_groups = {
            name: ScheduleGroup(at="16:00", steps=list(steps))
            for name, steps in (events or {}).items()
        }


def test_group_steps_outside_the_waves_are_reported():
    cfg = _Cfg(
        waves=["daily_bars", "compact"],
        groups={"core": ["daily_bars", "compact"], "capital": ["valuation_metrics", "fund_flow"]},
    )
    assert datasets_outside_the_daily_waves(cfg) == ["fund_flow", "valuation_metrics"]


def test_waves_covering_every_group_report_nothing():
    cfg = _Cfg(
        waves=["daily_bars", "valuation_metrics"],
        groups={"core": ["daily_bars"], "capital": ["valuation_metrics"]},
    )
    assert datasets_outside_the_daily_waves(cfg) == []


def test_event_owned_feeds_are_not_counted_as_missed():
    """`cne run events` owns these on the natural calendar."""
    cfg = _Cfg(
        waves=["daily_bars"],
        groups={"core": ["daily_bars"], "news": ["news_headlines"]},
        events={"news_wire": ["news_headlines"]},
    )
    assert datasets_outside_the_daily_waves(cfg) == []


def test_unregistered_step_names_are_ignored():
    cfg = _Cfg(waves=["daily_bars"], groups={"core": ["daily_bars", "not_a_real_step"]})
    assert datasets_outside_the_daily_waves(cfg) == []


def test_the_shipped_example_config_leaves_most_datasets_to_the_groups():
    """The real config is the case this notice exists for."""
    uncovered = datasets_outside_the_daily_waves(load_config("configs/cnequity.example.toml"))

    assert len(uncovered) > 20
    assert {"valuation_metrics", "financial_statement_items", "margin_trading"} <= set(uncovered)
    assert "daily_bars" not in uncovered


def test_a_retired_source_is_labelled_retired_not_fresh():
    """`fresh` on a 2024 watermark reads as current data.

    `northbound_flows` stopped being published on 2024-08-16, so it is
    correctly not stale — there is nothing left to fetch — but the freshness
    column has to say why rather than implying the rows are current.
    """
    from datetime import date

    from cnequity.domain.datasets import DATASETS, is_stale

    spec = DATASETS["northbound_flows"]
    assert spec.source_retired_date == date(2024, 8, 16)
    # Not stale, by design — the label is what has to carry the distinction.
    assert is_stale("northbound_flows", spec.source_retired_date, date(2026, 9, 15)) is False
