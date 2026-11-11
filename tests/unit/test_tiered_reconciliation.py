"""A full reconciliation window that is too expensive to walk every run.

`announcement_index` re-reads 30 days of CNINFO on every sweep — ~40,000
records over ~1,350 pages, because the endpoint fixes its page at 30 rows and
ignores `pageSize` — to pick up the few records indexed late. Measured against
the source on 2026-09-15: a 3-, 7- and 14-day-old day each returned exactly
what the lake already held, while a 21-day-old day returned 21 more rows out of
5,932. The deep tail earns its keep; it just does not need walking every run.
"""

from __future__ import annotations

from datetime import date

import pytest

from cnequity.config import Config, load_config, validate_config
from cnequity.domain.datasets import DATASETS, DatasetSpec
from cnequity.steps.common import incremental_window

# 2026-09-15 is a Tuesday; 2026-09-19 is the Saturday after it.
TUESDAY = date(2026, 9, 15)
SATURDAY = date(2026, 9, 19)


def _cfg(tmp_path, **kwargs) -> Config:
    return Config(data_root=tmp_path / "data", **kwargs)


def _deep_done(cfg: Config, dataset: str = "announcement_index", day: date = date(2026, 9, 12)):
    """The previous Saturday's deep sweep, so a weekday run is an ordinary one."""
    from cnequity.steps.common import _DEEP_RECONCILED_FIELD
    from cnequity.storage.state import StateStore

    StateStore(cfg.meta_root).set_date(dataset, day, field=_DEEP_RECONCILED_FIELD)


def test_announcement_index_declares_both_windows():
    spec = DATASETS["announcement_index"]
    assert spec.reconciliation_lookback_days == 30
    assert spec.shallow_reconciliation_lookback_days == 7


def test_the_near_tail_is_walked_on_an_ordinary_run(tmp_path):
    cfg = _cfg(tmp_path)
    _deep_done(cfg)
    window = incremental_window(cfg, "announcement_index", TUESDAY)
    assert (TUESDAY - window).days + 1 == 7


def test_the_deep_tail_is_walked_on_its_own_day(tmp_path):
    cfg = _cfg(tmp_path)
    window = incremental_window(cfg, "announcement_index", SATURDAY)
    assert (SATURDAY - window).days + 1 == 30


def test_zero_restores_the_full_window_on_every_run(tmp_path):
    """The escape hatch: pay the full cost daily, as before."""
    cfg = _cfg(tmp_path, deep_reconciliation_dow=0)
    assert (TUESDAY - incremental_window(cfg, "announcement_index", TUESDAY)).days + 1 == 30


def test_a_dataset_without_a_shallow_window_is_untouched(tmp_path):
    cfg = _cfg(tmp_path)
    spec = DATASETS["regulatory_events"]
    assert spec.shallow_reconciliation_lookback_days == 0
    assert (TUESDAY - incremental_window(cfg, "regulatory_events", TUESDAY)).days + 1 == 30


def test_a_shallow_window_must_be_shorter_than_the_full_one():
    """Otherwise 'tiering' silently widens the cheap path."""
    with pytest.raises(ValueError, match="shorter than the full window"):
        DatasetSpec(
            "x",
            primary_source="cninfo",
            tier="L2",
            partition_col="announce_date",
            reconciliation_lookback_days=7,
            shallow_reconciliation_lookback_days=7,
        )


def test_the_weekday_knob_is_validated(tmp_path):
    path = tmp_path / "bad.toml"
    path.write_text(
        f'[data]\nroot = "{(tmp_path / "lake").as_posix()}"\n'
        "[incremental]\ndeep_reconciliation_dow = 9\n"
    )
    errors = validate_config(load_config(path))
    assert any("deep_reconciliation_dow" in error for error in errors)


def _sweep(cfg, day, fetch):
    from cnequity.steps.common import fetch_incremental_daily

    return fetch_incremental_daily(cfg, "announcement_index", day, fetch, allow_empty=True)


def test_the_deep_tail_is_walked_once_per_deep_day(tmp_path):
    """The 6-hourly events job used to repeat the 30-day sweep on every Saturday run."""
    import polars as pl

    cfg = _cfg(tmp_path)
    asked: list[date] = []

    def _fetch(d):
        asked.append(d)
        return pl.DataFrame()

    _sweep(cfg, SATURDAY, _fetch)
    first = len(asked)
    asked.clear()
    _sweep(cfg, SATURDAY, _fetch)
    # The deep sweep covered the tail too: the next run only reaches the watermark day.
    assert first == 30 and len(asked) == 1
    # The next deep day walks the full window again.
    assert (
        date(2026, 9, 26) - incremental_window(cfg, "announcement_index", date(2026, 9, 26))
    ).days + 1 == 30


def test_a_failed_deep_sweep_is_retried_deep(tmp_path):
    cfg = _cfg(tmp_path)

    def _boom(d):
        raise RuntimeError("cninfo 502")

    with pytest.raises(RuntimeError):
        _sweep(cfg, SATURDAY, _boom)
    assert (SATURDAY - incremental_window(cfg, "announcement_index", SATURDAY)).days + 1 == 30


def test_the_near_tail_is_walked_once_a_day_then_only_the_tip(tmp_path):
    """The events job re-walked 7 days on each of its four daily runs."""
    import polars as pl

    cfg = _cfg(tmp_path)
    _deep_done(cfg)
    asked: list[date] = []

    def _fetch(d):
        asked.append(d)
        return pl.DataFrame()

    _sweep(cfg, TUESDAY, _fetch)
    first = len(asked)
    asked.clear()
    _sweep(cfg, TUESDAY, _fetch)
    assert (first, len(asked)) == (7, 1)
    # The next day walks its tail again.
    assert (
        date(2026, 9, 16) - incremental_window(cfg, "announcement_index", date(2026, 9, 16))
    ).days + 1 == 7


def test_a_failed_tail_is_walked_again(tmp_path):
    cfg = _cfg(tmp_path)
    _deep_done(cfg)

    def _boom(d):
        raise RuntimeError("cninfo 502")

    with pytest.raises(RuntimeError):
        _sweep(cfg, TUESDAY, _boom)
    assert (TUESDAY - incremental_window(cfg, "announcement_index", TUESDAY)).days + 1 == 7


def test_a_fresh_lake_keeps_the_weekly_rhythm(tmp_path):
    """No record yet: an ordinary weekday stays shallow, the deep day goes deep."""
    cfg = _cfg(tmp_path)
    assert (TUESDAY - incremental_window(cfg, "announcement_index", TUESDAY)).days + 1 == 7
    fresh = _cfg(tmp_path / "other")
    assert (SATURDAY - incremental_window(fresh, "announcement_index", SATURDAY)).days + 1 == 30


def test_a_sessions_only_feed_goes_deep_on_its_first_run_after_the_deep_day(tmp_path):
    """daily_bars never runs on a Saturday; its weekly tail used to never run."""
    from cnequity.steps.common import _deep_reconciliation_due

    cfg = _cfg(tmp_path)
    _deep_done(cfg, "daily_bars", date(2026, 9, 14))  # the Monday after 9-12
    assert not _deep_reconciliation_due(cfg, "daily_bars", date(2026, 9, 18))  # Friday
    assert _deep_reconciliation_due(cfg, "daily_bars", date(2026, 9, 21))  # next Monday


def test_the_daily_bar_tip_does_not_re_read_the_settled_session(tmp_path):
    """shallow=1 is the tip alone: the window starts after the watermark."""
    from cnequity.storage.state import StateStore

    cfg = _cfg(tmp_path)
    _deep_done(cfg, "daily_bars", date(2026, 9, 14))
    StateStore(cfg.meta_root).set_date("daily_bars", date(2026, 9, 17))
    assert incremental_window(cfg, "daily_bars", date(2026, 9, 18)) == date(2026, 9, 18)
    # Behind by a few sessions: every missed session is still in the window.
    StateStore(cfg.meta_root).set_date("daily_bars", date(2026, 9, 15))
    assert incremental_window(cfg, "daily_bars", date(2026, 9, 18)) == date(2026, 9, 16)


def test_a_finished_daily_bar_deep_sweep_lets_the_next_session_go_tip_only(tmp_path):
    """daily_bars has its own fetch path and never recorded its deep sweep, so
    from 2026-10-08 every daily run was a deep day (~30 min, not ~8)."""
    from cnequity.steps.common import mark_reconciled, reconciliation_depth
    from cnequity.storage.state import StateStore

    cfg = _cfg(tmp_path)
    _deep_done(cfg, "daily_bars", date(2026, 9, 14))
    StateStore(cfg.meta_root).set_date("daily_bars", date(2026, 9, 18))
    monday = date(2026, 9, 21)
    assert reconciliation_depth(cfg, "daily_bars", monday) == "deep"

    mark_reconciled(cfg, "daily_bars", monday, "deep")
    StateStore(cfg.meta_root).set_date("daily_bars", monday)
    tuesday = date(2026, 9, 22)
    assert reconciliation_depth(cfg, "daily_bars", tuesday) == "tail"
    assert incremental_window(cfg, "daily_bars", tuesday) == tuesday


def test_the_daily_bar_step_records_its_deep_sweep(tmp_path, monkeypatch):
    import cnequity.steps.bars as bars
    from cnequity.steps.common import _DEEP_RECONCILED_FIELD
    from cnequity.storage.state import StateStore

    cfg = _cfg(tmp_path)
    monday = date(2026, 9, 21)
    monkeypatch.setattr(bars.daily, "incremental_window", lambda *_a: date(2026, 9, 15))
    monkeypatch.setattr(bars.index_history, "incremental_window", lambda *_a: date(2026, 9, 15))
    monkeypatch.setattr(bars.daily, "reconciliation_depth", lambda *_a: "deep")
    monkeypatch.setattr(bars.bar_scope, "_reject_unfinished_daily_bar_window", lambda *_a: None)
    monkeypatch.setattr(bars.bar_scope, "_daily_fund_bar_symbols", lambda *_a, **_k: [])
    monkeypatch.setattr(bars.bar_scope, "load_symbols", lambda _c: [])
    monkeypatch.setattr(bars.daily, "load_symbols", lambda _c: [])
    monkeypatch.setattr(bars.index_history, "load_symbols", lambda _c: [])
    monkeypatch.setattr(
        bars.tip_fetch,
        "_fetch_tip_via_exchange",
        lambda *_a: {"rows_read": 0, "rows_written": 0, "covered": set()},
    )
    monkeypatch.setattr(
        bars.daily,
        "fetch_daily_bars_parallel",
        lambda *_a, **_k: {"rows_read": 0, "rows_written": 0},
    )
    monkeypatch.setattr(
        bars.tip_fetch,
        "fetch_daily_bars_parallel",
        lambda *_a, **_k: {"rows_read": 0, "rows_written": 0},
    )
    monkeypatch.setattr(bars.finish, "_finish_daily_bars", lambda *_a, **_k: {"status": "warning"})
    monkeypatch.setattr(bars.bar_scope, "_merge_ownership_result", lambda out, *_a: out)

    bars.daily._step_daily_bars(cfg, monday, "run-1", {})
    assert StateStore(cfg.meta_root).get_date("daily_bars", field=_DEEP_RECONCILED_FIELD) == monday

    # An execution failure leaves the deep sweep owed.
    later = date(2026, 9, 28)
    monkeypatch.setattr(bars.finish, "_finish_daily_bars", lambda *_a, **_k: {"status": "failed"})
    bars.daily._step_daily_bars(cfg, later, "run-2", {})
    assert StateStore(cfg.meta_root).get_date("daily_bars", field=_DEEP_RECONCILED_FIELD) == monday
