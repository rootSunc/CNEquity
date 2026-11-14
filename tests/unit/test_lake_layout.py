# ruff: noqa: F811 - the imported fixture is requested by name
"""The lake keeps a dataset as the files its writers produce: one file set per partition."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from tests.unit.test_lake_write import (  # noqa: F401 - cfg is a fixture
    PUBLISH,
    _bars,
    _load,
    _pointer,
    _replace,
    cfg,
)

from cnequity.storage.catalog_bridge import lake_root
from cnequity.storage.lake_write import LakeWrite, logical_name
from equitylake_core.catalog import Catalog
from equitylake_core.write import Write, WriteRejected, rows_digest

JAN, FEB = "trade_date=2024-01", "trade_date=2024-02"


def _lake_files(cfg) -> list[str]:
    with Catalog(lake_root(cfg), read_only=True) as lake:
        return sorted(lake.data_files("daily_bars"))


def _folders(cfg) -> dict[str, list[str]]:
    """Each partition folder of the lake with the partition files it holds."""
    table = lake_root(cfg).resolve() / "data" / "main" / "daily_bars"
    out: dict[str, list[str]] = {}
    for path in _lake_files(cfg):
        relative = type(table)(path).relative_to(table)
        out.setdefault(relative.parts[0], []).append(logical_name(path))
    return out


def test_a_migrated_lake_keeps_the_generation_files_themselves(cfg):
    assert _folders(cfg) == {"2024-01": ["part-merged.parquet"], "2024-02": ["part-merged.parquet"]}
    january = cfg.curated_root / "daily_bars/trade_date=2024-01/part-merged.parquet"
    generation = RevisionStore_root(cfg) / "trade_date=2024-01" / "part-merged.parquet"
    lake_file = next(path for path in _lake_files(cfg) if "/2024-01/" in path)
    # Linked, not written again: the lake file is the generation's file.
    assert type(january)(lake_file).stat().st_ino == generation.stat().st_ino


def RevisionStore_root(cfg):
    from cnequity.storage.revisions import RevisionStore

    return RevisionStore(cfg.meta_root, cfg.curated_root).current_root("daily_bars")


def test_a_checkout_is_the_published_files_and_a_publish_registers_what_was_written(cfg):
    before = _lake_files(cfg)
    with LakeWrite(cfg, "r1") as write:
        january = write.checkout("daily_bars").partition(JAN)
        linked = january / "part-merged.parquet"
        lake_file = next(path for path in before if "/2024-01/" in path)
        assert linked.stat().st_ino == type(linked)(lake_file).stat().st_ino
        _replace(january, _bars(date(2024, 1, 2), {"600000.SH": 10.5}))
        written = linked.stat().st_ino
        write.publish({"daily_bars": PUBLISH})

    after = _lake_files(cfg)
    february = [path for path in before if "/2024-02/" in path]
    # February was not touched: its file is the same file.
    assert [path for path in after if "/2024-02/" in path] == february
    new = next(path for path in after if "/2024-01/" in path)
    # January's file is the one the writer wrote, registered as it is.
    assert new != lake_file and type(linked)(new).stat().st_ino == written
    assert _load(cfg).sort("trade_date")["close"].to_list() == [10.5, 11.0]


def test_only_the_rewritten_file_of_a_partition_is_replaced(cfg):
    with LakeWrite(cfg, "r1") as write:
        january = write.checkout("daily_bars").partition(JAN)
        # A second file in the partition, as a bucketed or appended partition has.
        _bars(date(2024, 1, 3), {"600000.SH": 10.2}).write_parquet(january / "part-new.parquet")
        write.publish({"daily_bars": PUBLISH})
    assert _folders(cfg)["2024-01"] == ["part-merged.parquet", "part-new.parquet"]
    kept = next(path for path in _lake_files(cfg) if "part-merged" in path and "/2024-01/" in path)

    with LakeWrite(cfg, "r2") as write:
        january = write.checkout("daily_bars").partition(JAN)
        # As the writers do it: a new file replaces the old one, never written in place.
        (january / "part-new.parquet").unlink()
        _bars(date(2024, 1, 3), {"600000.SH": 10.3}).write_parquet(january / "part-new.parquet")
        swaps = write.checkout("daily_bars").swaps()
        assert [(len(swap.add), len(swap.remove)) for swap in swaps] == [(1, 1)]
        receipt = write.publish({"daily_bars": PUBLISH})["daily_bars"]

    # The receipt names the one file this revision wrote.
    assert [item.path for item in receipt.files] == [
        "daily_bars/trade_date=2024-01/part-new.parquet"
    ]
    assert kept in _lake_files(cfg)
    january_rows = _load(cfg).filter(pl.col("trade_date") < date(2024, 2, 1))
    assert sorted(january_rows["close"].to_list()) == [9.0, 10.0, 10.3]


def test_rows_written_into_the_wrong_partition_are_refused(cfg):
    before = _pointer(cfg)
    with LakeWrite(cfg, "r1") as write:
        january = write.checkout("daily_bars").partition(JAN)
        _replace(january, _bars(date(2024, 2, 5), {"600000.SH": 10.5}))
        with pytest.raises(WriteRejected, match="do not belong to it"):
            write.publish({"daily_bars": PUBLISH})
    assert _pointer(cfg) == before
    assert _load(cfg).height == 3
    # Nothing was left behind in the lake for a publish that did not happen.
    assert _folders(cfg) == {"2024-01": ["part-merged.parquet"], "2024-02": ["part-merged.parquet"]}


def test_a_file_whose_types_the_table_cannot_read_is_written_once_more(cfg):
    with LakeWrite(cfg, "r1") as write:
        january = write.checkout("daily_bars").partition(JAN)
        wide = _bars(date(2024, 1, 2), {"600000.SH": 10.5}).with_columns(
            pl.col("volume").cast(pl.Float64)
        )
        _replace(january, wide)
        write.publish({"daily_bars": PUBLISH})
    assert _load(cfg).sort("trade_date")["volume"].to_list() == [100, 100]


def _catalog_laid_out(cfg) -> None:
    """Replace the table by one the catalog wrote itself: a single file holding both months."""
    with Catalog(lake_root(cfg)) as lake:
        rows = lake.relation("SELECT * FROM daily_bars").pl()
        source = lake_root(cfg) / "tmp" / "all.parquet"
        source.parent.mkdir(parents=True, exist_ok=True)
        rows.write_parquet(source)
        write = Write(lake)
        write.replace("daily_bars", [source])
        write.commit()
        assert len(lake.data_files("daily_bars")) == 1


def test_a_file_holding_several_partitions_is_split_the_first_time_one_changes(cfg):
    _catalog_laid_out(cfg)
    with LakeWrite(cfg, "r1") as write:
        checkout = write.checkout("daily_bars")
        _replace(checkout.partition(JAN), _bars(date(2024, 1, 2), {"600000.SH": 10.5}))
        # February shares the file that is dropped, so it is written out too.
        assert sorted(checkout.changed()) == [JAN, FEB]
        write.publish({"daily_bars": PUBLISH})

    assert _folders(cfg) == {"2024-01": ["part-merged.parquet"], "2024-02": ["part-merged.parquet"]}
    assert _load(cfg).sort("trade_date")["close"].to_list() == [10.5, 11.0]
    with LakeWrite(cfg, "r2") as write:
        checkout = write.checkout("daily_bars")
        _replace(checkout.partition(JAN), _bars(date(2024, 1, 2), {"600000.SH": 10.6}))
        assert sorted(checkout.changed()) == [JAN]


def test_laying_a_table_out_changes_its_files_and_none_of_its_rows(cfg):
    from cnequity.query import catalog_read
    from cnequity.storage.catalog_layout import datasets_to_lay_out, lay_out_dataset
    from cnequity.storage.published import published

    _catalog_laid_out(cfg)
    pointer = _pointer(cfg)
    assert datasets_to_lay_out(cfg) == ["daily_bars"]
    with Catalog(lake_root(cfg)) as lake:
        before = rows_digest(lake, "daily_bars")
        report = lay_out_dataset(cfg, lake, "daily_bars")
        assert report == {"dataset": "daily_bars", "rows": 3, "files_before": 1, "files": 2}
        assert rows_digest(lake, "daily_bars").digest == before.digest
        assert lay_out_dataset(cfg, lake, "daily_bars") is None
    assert datasets_to_lay_out(cfg) == []
    assert _pointer(cfg) == pointer

    catalog_read.close_all()
    groups = published(cfg, "daily_bars").groups
    # A reader gets one group per partition, bounded by the partition's period.
    assert [(group.key, group.low, group.high, group.rows) for group in groups] == [
        ("2024-01", date(2024, 1, 1), date(2024, 1, 31), 2),
        ("2024-02", date(2024, 2, 1), date(2024, 2, 29), 1),
    ]
    assert _load(cfg).height == 3


def test_a_rehearsal_copies_no_data_and_leaves_nothing_in_the_lake(cfg, tmp_path):
    from dataclasses import replace

    from equitylake_core.catalog import copy_catalog

    data = lake_root(cfg) / "data"
    before = sorted(path for path in data.rglob("*.parquet"))
    view = replace(cfg, data_root=tmp_path / "view")
    copy_catalog(lake_root(cfg), lake_root(view))
    with LakeWrite(cfg, "r1") as write:
        _replace(
            write.checkout("daily_bars").partition(JAN),
            _bars(date(2024, 1, 2), {"600000.SH": 10.5}),
        )
        write.rehearse(view, {"daily_bars": PUBLISH})
        # The copy holds the publish and has no data files of its own.
        with Catalog(lake_root(view), read_only=True) as copy:
            closes = copy.execute("SELECT close FROM daily_bars ORDER BY trade_date").fetchall()
        assert closes == [(10.5,), (11.0,)]
        assert not (lake_root(view) / "data").exists()
        assert sorted(_load(cfg)["close"].to_list()) == [9.0, 10.0, 11.0]
    # The file the rehearsal registered in the copy is gone with the write.
    assert sorted(path for path in data.rglob("*.parquet")) == before


def test_a_generation_file_with_rows_of_another_partition_is_not_filed_under_its_folder(tmp_path):
    from cnequity.config.loader import Config
    from cnequity.storage.catalog_bridge import sync_dataset
    from cnequity.storage.revisions import RevisionStore
    from equitylake_core.extensions import ExtensionUnavailableError

    config = Config(data_root=tmp_path / "data")
    # An old writer left a February row in January's file.
    stray = pl.concat(
        [
            _bars(date(2024, 1, 2), {"600000.SH": 10.0}),
            _bars(date(2024, 2, 1), {"600000.SH": 11.0}),
        ]
    )
    path = config.curated_root / "daily_bars/trade_date=2024-01/part-merged.parquet"
    path.parent.mkdir(parents=True)
    stray.write_parquet(path)
    RevisionStore(config.meta_root, config.curated_root).commit(
        "daily_bars",
        run_id="seed",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    try:
        with Catalog(lake_root(config), create=True) as lake:
            sync_dataset(config, lake, "daily_bars")
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))
    assert list(_folders(config)) == ["_mixed"]

    with LakeWrite(config, "r1") as write:
        checkout = write.checkout("daily_bars")
        _replace(checkout.partition(FEB), _bars(date(2024, 2, 1), {"600000.SH": 11.5}))
        # January shares the file that is dropped, so it is written out too.
        assert sorted(checkout.changed()) == [JAN, FEB]
        write.publish({"daily_bars": PUBLISH})

    assert _folders(config) == {
        "2024-01": ["part-merged.parquet"],
        "2024-02": ["part-merged.parquet"],
    }
    assert _load(config).sort("trade_date")["close"].to_list() == [10.0, 11.5]
