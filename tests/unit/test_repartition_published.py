"""Repartitioning a published dataset publishes a revision; it never swaps
the mutable tree behind the readers' back.

The old repair rewrote ``curated/<dataset>`` in place. Readers follow
``current.json`` to a committed generation, so they kept seeing the old layout,
and the next compact rebuilt the mutable tree from that generation.
"""

from __future__ import annotations

import dataclasses
from datetime import date, timedelta

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from cnequity.config import Config
from cnequity.domain import datasets as registry
from cnequity.domain.schemas import with_provenance
from cnequity.query.reader import load
from cnequity.storage.layout import init_data_layout
from cnequity.storage.repairs.repartition import repartition_dataset
from cnequity.storage.revisions import RevisionStore

DAYS = [date(2024, 1, 29) + timedelta(days=i) for i in range(10)]
DAYS = [d for d in DAYS if d.weekday() < 5]


def _bars(day: date, *, with_pre_close: bool) -> pl.DataFrame:
    columns = {
        "symbol": ["600519.SH", "000001.SZ"],
        "trade_date": [day, day],
        "open": [10.0, 20.0],
        "high": [11.0, 21.0],
        "low": [9.0, 19.0],
        "close": [10.5, 20.5],
        "volume": [100, 200],
        "amount": [1000.0, 4000.0],
    }
    if with_pre_close:
        columns["pre_close"] = [10.0, 20.0]
    return with_provenance(pl.DataFrame(columns), source="tdx_protocol", data_version="v1")


@pytest.fixture
def lake(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    for i, day in enumerate(DAYS):
        out = cfg.curated_root / "daily_bars" / f"trade_date={day}"
        out.mkdir(parents=True)
        _bars(day, with_pre_close=i % 2 == 1).write_parquet(out / "part-0.parquet")
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    store.ensure_current("daily_bars")
    monkeypatch.setitem(
        registry.DATASETS,
        "daily_bars",
        dataclasses.replace(registry.DATASETS["daily_bars"], partition_granularity="month"),
    )
    return cfg, store


def test_a_published_dataset_is_repartitioned_as_a_new_revision(lake):
    cfg, store = lake
    before = load("daily_bars", config=cfg).sort("symbol", "trade_date")
    revision_before = store.current_pointer("daily_bars")["revision"]

    result = repartition_dataset(cfg, "daily_bars")

    assert result.changed and result.partitions_after == 2  # January and February
    assert store.current_pointer("daily_bars")["revision"] == revision_before + 1
    committed = store.current_root("daily_bars")
    assert sorted(p.name for p in committed.iterdir() if p.is_dir()) == [
        "trade_date=2024-01",
        "trade_date=2024-02",
    ]
    after = load("daily_bars", config=cfg).sort("symbol", "trade_date")
    assert_frame_equal(after, before, check_column_order=False)


def test_a_dry_run_reports_without_publishing(lake):
    cfg, store = lake
    revision_before = store.current_pointer("daily_bars")["revision"]
    result = repartition_dataset(cfg, "daily_bars", dry_run=True)
    assert not result.changed and result.partitions_after == 2
    assert store.current_pointer("daily_bars")["revision"] == revision_before
    assert all(
        "-" in p.name[len("trade_date=") :][5:] for p in store.current_root("daily_bars").iterdir()
    )


def test_a_layout_that_already_matches_is_left_alone(lake):
    cfg, store = lake
    repartition_dataset(cfg, "daily_bars")
    revision = store.current_pointer("daily_bars")["revision"]
    assert not repartition_dataset(cfg, "daily_bars").changed
    assert store.current_pointer("daily_bars")["revision"] == revision


def test_the_upgrade_migration_previews_applies_and_then_has_nothing_left(lake):
    from cnequity.storage.migrations import MIGRATIONS, affected

    cfg, store = lake
    migration = next(m for m in MIGRATIONS if m.id == "daily-bars-month-partitions")
    revision = store.current_pointer("daily_bars")["revision"]

    preview = migration.apply(cfg, apply=False)
    assert affected(preview) == 2 * len(DAYS)
    assert store.current_pointer("daily_bars")["revision"] == revision

    migration.apply(cfg, apply=True)
    assert store.current_pointer("daily_bars")["revision"] == revision + 1
    assert affected(migration.apply(cfg, apply=False)) == 0
