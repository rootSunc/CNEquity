"""A lake on its catalog cannot be written as folders, and its read caches stay bounded."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.schemas import validate_dataframe, with_provenance
from cnequity.storage import StagingWriter
from cnequity.storage.derived_output import DerivedOutput
from cnequity.storage.lake_record import start_on_catalog
from cnequity.storage.parquet import CuratedWriter, compact_dataset
from cnequity.storage.revisions import LakeFolderWrite, refuse_lake_folder_write
from equitylake_core.catalog_cache import OpenCatalogs


def _bars() -> pl.DataFrame:
    return validate_dataframe(
        with_provenance(
            pl.DataFrame(
                {
                    "symbol": ["600519.SH"],
                    "trade_date": [date(2024, 6, 3)],
                    "open": [1.0],
                    "high": [1.0],
                    "low": [1.0],
                    "close": [1.0],
                    "volume": [1],
                    "amount": [1.0],
                }
            ),
            source="tdx_protocol",
            data_version="v1",
        ),
        "daily_bars",
    )


def _catalog_lake(tmp_path) -> Config:
    cfg = Config(data_root=tmp_path / "lake")
    cfg.meta_root.mkdir(parents=True)
    start_on_catalog(cfg)
    return cfg


def test_a_write_into_a_catalog_lakes_folders_is_refused(tmp_path):
    cfg = _catalog_lake(tmp_path)
    StagingWriter(cfg.staging_root).write_batch("daily_bars", "run", "b0", _bars())

    for target in (cfg.curated_root, cfg.derived_root / "adj_factors" / "trade_date=2024"):
        with pytest.raises(LakeFolderWrite, match="reads only its catalog"):
            refuse_lake_folder_write(target)
    with pytest.raises(LakeFolderWrite):
        CuratedWriter(cfg.curated_root)
    with pytest.raises(LakeFolderWrite):
        compact_dataset(
            cfg.staging_root, cfg.curated_root, "daily_bars", "run", out_root=cfg.curated_root
        )
    assert not cfg.curated_root.exists()


def test_other_folders_are_written_as_before(tmp_path):
    cfg = _catalog_lake(tmp_path)
    # A checkout, a scratch folder, and the folders of a lake not on its catalog.
    for target in (
        cfg.staging_root / "_checkout" / "run" / "curated",
        tmp_path / "scratch" / "curated",
        Config(data_root=tmp_path / "old-lake").curated_root,
    ):
        refuse_lake_folder_write(target)


def test_a_derivation_and_a_compact_name_where_they_write():
    with pytest.raises(TypeError):
        DerivedOutput(Config(), "adj_factors")  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="out_root"):
        compact_dataset("staging", "curated", "daily_bars", "run")  # type: ignore[call-arg]


class _Closable:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_open_catalogs_closes_the_least_recently_used_beyond_its_limit(tmp_path):
    cache = OpenCatalogs(limit=2)
    a, b, c = _Closable(), _Closable(), _Closable()
    cache.put(tmp_path / "a", a)
    cache.put(tmp_path / "b", b)
    assert cache.get(tmp_path / "a") is a  # now the most recent
    cache.put(tmp_path / "c", c)
    assert b.closed and not a.closed and not c.closed
    assert cache.roots() == [tmp_path / "a", tmp_path / "c"]


def test_open_catalogs_reopens_when_the_options_differ(tmp_path):
    cache = OpenCatalogs(limit=2)
    first = _Closable()
    cache.put(tmp_path / "a", first, "4GB")
    assert cache.get(tmp_path / "a", "8GB") is None
    assert first.closed and tmp_path / "a" not in cache


def test_reading_many_lakes_keeps_only_a_few_catalogs_open(tmp_path):
    from tests.catalog_seed import publish_rows

    from cnequity.query import catalog_read
    from cnequity.query.reader import load
    from cnequity.storage import revision_records

    for index in range(6):
        cfg = Config(data_root=tmp_path / f"lake-{index}")
        publish_rows(cfg, "daily_bars", _bars())
        assert load("daily_bars", config=cfg).height == 1
    assert len(catalog_read._open) <= catalog_read._open.limit
    assert len(revision_records._readers) <= revision_records._readers.limit
