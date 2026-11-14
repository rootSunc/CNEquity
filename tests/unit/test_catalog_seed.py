"""The test helper that seeds a lake by publishing into its catalog."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from tests.catalog_seed import publish_rows
from tests.unit.test_lake_write import _bars

from cnequity.config.loader import Config


@pytest.fixture
def cfg(tmp_path):
    from cnequity.query import catalog_read
    from equitylake_core.extensions import ExtensionUnavailableError, ensure_extensions

    try:
        import duckdb

        ensure_extensions(duckdb.connect())
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))
    yield Config(data_root=tmp_path / "data")
    catalog_read.close_all()


def _load(cfg, dataset):
    from cnequity.query import catalog_read, load

    catalog_read.close_all()
    return load(dataset, config=cfg)


def test_rows_land_in_their_partitions_and_a_later_seed_replaces_only_its_own(cfg):
    first = publish_rows(
        cfg,
        "daily_bars",
        pl.concat(
            [
                _bars(date(2024, 1, 2), {"600000.SH": 10.0, "000001.SZ": 9.0}),
                _bars(date(2024, 2, 1), {"600000.SH": 11.0}),
            ]
        ),
    )
    assert sorted(first.changed_partitions) == [
        "daily_bars/trade_date=2024-01",
        "daily_bars/trade_date=2024-02",
    ]
    assert (cfg.meta_root / "storage.json").is_file()

    publish_rows(cfg, "daily_bars", _bars(date(2024, 2, 1), {"600000.SH": 12.0}), run_id="again")
    rows = _load(cfg, "daily_bars").sort("trade_date", "symbol")
    assert rows.select("symbol", "trade_date", "close").rows() == [
        ("000001.SZ", date(2024, 1, 2), 9.0),
        ("600000.SH", date(2024, 1, 2), 10.0),
        ("600000.SH", date(2024, 2, 1), 12.0),
    ]


def test_an_unpartitioned_dataset_is_one_partition(cfg):
    from cnequity.storage.revisions import RevisionStore

    frame = pl.DataFrame({"symbol": ["600000.SH", "000001.SZ"], "name": ["浦发银行", "平安银行"]})
    revision = publish_rows(cfg, "instruments", frame)
    assert revision.changed_partitions == ("instruments",)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    assert store.current_pointer("instruments")["revision_id"] == revision.revision_id


def test_a_lake_that_still_holds_generations_is_refused(cfg):
    path = cfg.curated_root / "daily_bars" / "trade_date=2024-01" / "part-merged.parquet"
    path.parent.mkdir(parents=True)
    _bars(date(2024, 1, 2), {"600000.SH": 10.0}).write_parquet(path)
    with pytest.raises(ValueError, match="holds generations"):
        publish_rows(cfg, "daily_bars", _bars(date(2024, 1, 2), {"600000.SH": 1.0}))
