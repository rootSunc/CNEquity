"""Tushare-only backfills are not a daily freshness obligation.

The open-source default leaves ``[sources.tushare]`` off. A one-time pull, or
an empty table, must not show up as stale in ``cne serve``.
"""

from __future__ import annotations

from datetime import date

from cnequity.config import Config
from cnequity.config.loader import ScheduleGroup
from cnequity.domain.datasets import daily_freshness_owed
from cnequity.quality.freshness import freshness_owed, freshness_state, is_stale
from cnequity.serve.lake import LakeView

_OLD = date(2026, 11, 4)
_ANCHOR = date(2026, 11, 6)


def _token(config: Config) -> Config:
    config.sources = {"tushare": True}
    config.tushare_token = "test-token"
    return config


def test_a_default_lake_does_not_owe_a_tushare_backfill(config):
    assert not daily_freshness_owed(config, "limit_prices")
    assert freshness_state("limit_prices", _OLD, _ANCHOR, config) == "n/a"
    assert not is_stale("limit_prices", _OLD, _ANCHOR, config)
    # The registry rule still applies when no lake is in hand.
    assert is_stale("limit_prices", _OLD, _ANCHOR)


def test_a_token_without_a_schedule_is_still_a_one_time_pull(config):
    _token(config)
    assert freshness_state("fund_flow_tushare", _OLD, _ANCHOR, config) == "n/a"
    assert not is_stale("chip_distribution", _OLD, _ANCHOR, config)


def test_scheduling_the_step_makes_it_due(config):
    _token(config)
    config.schedule_groups = {
        "research": ScheduleGroup(at="18:00", steps=["limit_prices", "compact"])
    }
    assert daily_freshness_owed(config, "limit_prices")
    assert freshness_state("limit_prices", _OLD, _ANCHOR, config) == "stale"
    assert is_stale("limit_prices", _OLD, _ANCHOR, config)


def test_an_eastmoney_dataset_with_a_tushare_backfill_stays_due(config):
    _token(config)
    config.schedule_groups = {"capital": ScheduleGroup(at="16:30", steps=["fund_flow"])}
    assert daily_freshness_owed(config, "fund_flow")
    assert freshness_state("fund_flow", _OLD, _ANCHOR, config) == "stale"


def test_serve_does_not_list_an_unscheduled_tushare_dataset_as_stale(config):
    _token(config)
    view = LakeView(config)
    row = {
        "dataset": "limit_prices",
        "has_data": True,
        "watermarked": True,
        "watermark": _OLD,
        "coverage_end": _OLD,
    }
    assert view._freshness_of(row, _ANCHOR) == "n/a"
    assert view._freshness_of({**row, "has_data": False}, _ANCHOR) == "n/a"


def test_a_rejected_key_takes_a_scheduled_dataset_off_the_stale_list(config):
    from cnequity.storage.key_checks import last_check, save_check

    _token(config)
    config.schedule_groups = {
        "research": ScheduleGroup(at="18:00", steps=["limit_prices", "compact"])
    }
    save_check(config, "tushare", "rejected", "token 不对", "2026-11-09T08:00:00+00:00")
    assert daily_freshness_owed(config, "limit_prices"), "the registry rule alone"
    assert not freshness_owed(config, "limit_prices")
    assert freshness_state("limit_prices", _OLD, _ANCHOR, config) == "n/a"
    # A new key has not been checked, so the old verdict does not stick to it.
    config.tushare_token = "another-token"
    assert last_check(config, "tushare") is None
    assert freshness_owed(config, "limit_prices")


def test_the_key_check_record_never_holds_the_key(config):
    from cnequity.storage.key_checks import last_check, save_check

    _token(config)
    save_check(config, "tushare", "ok", "", "2026-11-09T08:00:00+00:00")
    written = (config.meta_root / "sources" / "key-checks.json").read_text(encoding="utf-8")
    assert "test-token" not in written
    assert last_check(config, "tushare")["state"] == "ok"
