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


def test_announcement_index_declares_both_windows():
    spec = DATASETS["announcement_index"]
    assert spec.reconciliation_lookback_days == 30
    assert spec.shallow_reconciliation_lookback_days == 7


def test_the_near_tail_is_walked_on_an_ordinary_run(tmp_path):
    cfg = _cfg(tmp_path)
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
