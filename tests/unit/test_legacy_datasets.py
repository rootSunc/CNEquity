"""Upgrading a lake whose datasets an earlier release wrote without a revision."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.schemas import validate_dataframe, with_provenance
from cnequity.ops import upgrade as upgrade_ops
from cnequity.query.reader import load
from cnequity.storage.catalog_bridge import lake_root
from cnequity.storage.lake_record import _write_lake
from cnequity.storage.migrations import owed
from equitylake_core.catalog import Catalog


def _frame(dataset: str, columns: dict) -> pl.DataFrame:
    return validate_dataframe(
        with_provenance(pl.DataFrame(columns), source="mock", data_version="v1"), dataset
    )


def _pre_025_sample_lake(root) -> Config:
    """What `cne init --profile sample` left before 0.25: Parquet folders, no revisions."""
    cfg = Config(data_root=root)
    cfg.meta_root.mkdir(parents=True)
    _write_lake(cfg, {"created_with": "0.24.0", "migrations": {}})
    days = [date(2024, 5, 31), date(2024, 6, 3), date(2024, 6, 4)]
    bars = _frame(
        "daily_bars",
        {
            "symbol": ["600519.SH"] * 3,
            "trade_date": days,
            "open": [10.0, 11.0, 12.0],
            "high": [11.0, 12.0, 13.0],
            "low": [9.0, 10.0, 11.0],
            "close": [10.5, 11.5, 12.5],
            "volume": [1000, 1100, 1200],
            "amount": [1.0e4, 1.1e4, 1.2e4],
        },
    )
    for month, rows in bars.group_by(pl.col("trade_date").dt.strftime("%Y-%m")):
        folder = cfg.curated_root / "daily_bars" / f"trade_date={month[0]}"
        folder.mkdir(parents=True)
        rows.write_parquet(folder / "part-sample.parquet")
    instruments = _frame(
        "instruments",
        {
            "symbol": ["600519.SH"],
            "name": ["贵州茅台"],
            "exchange": ["SH"],
            "asset_type": ["stock"],
            "list_date": [date(2001, 8, 27)],
            "delist_date": [None],
            "prev_symbol": [None],
        },
    )
    (cfg.curated_root / "instruments").mkdir(parents=True)
    instruments.write_parquet(cfg.curated_root / "instruments" / "part-sample.parquet")
    return cfg


def test_upgrade_publishes_unversioned_folders_into_the_catalog(tmp_path):
    cfg = _pre_025_sample_lake(tmp_path / "lake")

    plan = upgrade_ops.plan(cfg)
    assert "legacy-datasets" in plan["migrations"]
    result = upgrade_ops.run(cfg)
    assert result["done"], result

    with Catalog(lake_root(cfg), read_only=True) as catalog:
        assert catalog.execute("SELECT count(*) FROM daily_bars").fetchone()[0] == 3
        assert catalog.execute("SELECT count(*) FROM instruments").fetchone()[0] == 1
    # The folders the catalog replaced are gone, and nothing is owed any more.
    assert not cfg.curated_root.exists()
    assert owed(cfg) == []
    assert load("daily_bars", config=cfg, symbols=["600519.SH"]).height == 3


def test_a_lake_upgraded_without_the_move_gets_it_on_the_next_upgrade(tmp_path, monkeypatch):
    """A lake whose 0.25 upgrade left its folders outside the catalog is finished by a rerun."""
    cfg = _pre_025_sample_lake(tmp_path / "lake")
    from cnequity.storage import legacy_datasets

    with monkeypatch.context() as patch:
        patch.setattr(
            legacy_datasets, "adopt_legacy_datasets", lambda config, *, apply: {"rows_changed": 0}
        )
        assert upgrade_ops.run(cfg)["done"]
    # Pretend that upgrade did not know the migration: forget it was recorded.
    from cnequity.storage.lake_record import read_lake

    record = read_lake(cfg)
    record["migrations"].pop("legacy-datasets")
    _write_lake(cfg, record)
    assert (cfg.curated_root / "daily_bars").is_dir()

    assert [m.id for m in owed(cfg)] == ["legacy-datasets"]
    assert upgrade_ops.run(cfg)["done"]
    assert not cfg.curated_root.exists()
    with Catalog(lake_root(cfg), read_only=True) as catalog:
        assert catalog.execute("SELECT count(*) FROM daily_bars").fetchone()[0] == 3


def test_a_lake_on_its_catalog_never_reads_a_dataset_folder(tmp_path):
    """Before the upgrade a folder is the data; after it only the catalog is."""
    from contextlib import closing

    import pytest

    from cnequity.query.parquet_scan import dataset_has_parquet
    from cnequity.query.reader import ReaderError
    from cnequity.query.views import views_connection

    cfg = _pre_025_sample_lake(tmp_path / "lake")
    # An earlier release's lake is read from its folders, with no upgrade.
    assert load("daily_bars", config=cfg, symbols=["600519.SH"]).height == 3
    assert upgrade_ops.run(cfg)["done"]

    # A folder the catalog does not record: a stray copy, a hand-made file.
    stray = cfg.curated_root / "valuation_metrics" / "trade_date=2024-06"
    stray.mkdir(parents=True)
    (cfg.curated_root / "daily_bars" / "trade_date=2024-06").mkdir(parents=True)
    pl.DataFrame({"symbol": ["X"], "trade_date": [date(2024, 6, 3)]}).write_parquet(
        stray / "part.parquet"
    )
    pl.DataFrame({"symbol": ["X"], "trade_date": [date(2024, 6, 3)]}).write_parquet(
        cfg.curated_root / "daily_bars" / "trade_date=2024-06" / "part.parquet"
    )

    assert not dataset_has_parquet(cfg.curated_root / "valuation_metrics", meta_root=cfg.meta_root)
    with pytest.raises(ReaderError, match="in this lake"):
        load("valuation_metrics", config=cfg)
    # The published dataset still comes only from the catalog: the stray row is not in it.
    assert load("daily_bars", config=cfg)["symbol"].to_list() == ["600519.SH"] * 3
    with closing(views_connection(cfg)) as con:
        assert con.execute("SELECT count(*) FROM valuation_metrics").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM daily_bars").fetchone()[0] == 3


def test_reading_a_lake_without_its_directory_creates_nothing(tmp_path):
    from equitylake_core.lock import lake_read

    cfg = _pre_025_sample_lake(tmp_path / "lake")
    with lake_read(lake_root(cfg)):
        pass
    load("daily_bars", config=cfg)
    assert not lake_root(cfg).exists()


def test_the_commands_that_did_nothing_are_gone():
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    runner = CliRunner()
    for args in (
        ["storage", "sync-catalog"],
        ["storage", "merge-files"],
        ["storage", "apply"],
        ["repair", "layout", "daily_bars"],
        ["repair", "repartition"],
    ):
        result = runner.invoke(cli, args)
        assert result.exit_code == 2, args
        assert "No such command" in result.output, args
    listed = (
        runner.invoke(cli, ["storage", "--help"]).output
        + runner.invoke(cli, ["repair", "--help"]).output
    )
    for name in (" apply", "sync-catalog", "merge-files", " layout", "repartition"):
        assert name not in listed, name
    root = runner.invoke(cli, ["--help"])
    assert root.exit_code == 0
    assert "storage" in root.output
