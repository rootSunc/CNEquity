"""The opt-in migration that moves an existing lake onto the lake catalog."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tests.unit.test_compact_to_catalog import FIRST, SECOND, _flows, _legacy_run, _margins, _run

from cnequity.config import Config
from cnequity.storage.catalog_bridge import catalog_is_current, lake_root
from cnequity.storage.catalog_migration import CatalogMigrationError, migrate_to_catalog
from cnequity.storage.migrations import MIGRATIONS, owed
from cnequity.storage.revisions import RevisionStore
from equitylake_core.catalog import CATALOG_FILE, Catalog
from equitylake_core.extensions import ExtensionUnavailableError


@pytest.fixture
def lake(tmp_path, monkeypatch):
    from cnequity.query import catalog_read

    cfg = Config(data_root=tmp_path / "lake")
    _legacy_run(
        cfg,
        "r1",
        FIRST,
        _flows(FIRST, {"600519.SH": 1.0, "000001.SZ": 2.0}),
        _margins(FIRST, {"600519.SH": 10.0}),
    )
    try:
        Catalog(tmp_path / "probe", create=True).close()
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))
    yield cfg
    catalog_read.close_all()


def test_a_lake_built_before_owes_the_migration_first(lake):
    assert [migration.id for migration in owed(lake)][0] == "lake-catalog"
    assert MIGRATIONS[0].id == "lake-catalog" and not MIGRATIONS[0].optional


def test_nothing_writes_a_lake_that_has_not_moved(lake, monkeypatch):
    from cnequity.orchestrator.engine import JobEngine
    from cnequity.query import catalog_read
    from cnequity.storage.catalog_migration import require_catalog_lake
    from cnequity.storage.lake_record import LakeMigrationRequired
    from cnequity.storage.repairs.publish import publish_repair

    store = RevisionStore(lake.meta_root, lake.curated_root)
    before = store.current_pointer("fund_flow")

    with pytest.raises(LakeMigrationRequired, match="cne upgrade --apply"):
        require_catalog_lake(lake)
    with pytest.raises(LakeMigrationRequired):
        JobEngine(lake).run_job("daily", SECOND)
    with pytest.raises(LakeMigrationRequired):
        publish_repair(
            lake, store, "fund_flow", keep=lambda frame: frame.head(0), reason="x", metadata={}
        )
    assert store.current_pointer("fund_flow") == before
    assert not (lake.meta_root / "storage.json").exists()

    migrate_to_catalog(lake, apply=True)
    catalog_read.close_all()
    require_catalog_lake(lake)


def test_a_lake_that_holds_nothing_starts_on_the_catalog(tmp_path, monkeypatch):
    from cnequity.query import catalog_read, load
    from cnequity.storage.catalog_migration import require_catalog_lake

    cfg = Config(data_root=tmp_path / "new")
    try:
        Catalog(tmp_path / "probe", create=True).close()
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))

    require_catalog_lake(cfg)
    assert json.loads((cfg.meta_root / "storage.json").read_text()) == {
        "publish": "catalog",
        "revisions": "catalog",
    }
    _run(
        cfg,
        "r1",
        FIRST,
        _flows(FIRST, {"600519.SH": 1.0}),
        _margins(FIRST, {"600519.SH": 10.0}),
    )
    catalog_read.close_all()

    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    pointer = store.current_pointer("fund_flow")
    # Its first revision is already the catalog's: no generation was written.
    assert pointer["storage"] == "catalog" and pointer["revision"] == 1
    assert not (cfg.meta_root / "revisions" / "data" / "fund_flow").exists()
    assert load("fund_flow", config=cfg).height == 1
    catalog_read.close_all()


def test_creating_a_lake_records_that_it_publishes_into_its_catalog(tmp_path):
    from cnequity.storage.layout import init_data_layout

    cfg = Config(data_root=tmp_path / "fresh")
    init_data_layout(cfg)
    assert json.loads((cfg.meta_root / "storage.json").read_text()) == {
        "publish": "catalog",
        "revisions": "catalog",
    }


def test_a_preview_reports_and_creates_nothing(lake):
    report = migrate_to_catalog(lake, apply=False)
    assert report["to_copy"] == ["fund_flow", "margin_trading"]
    assert report["switched"] is False and report["rows_changed"] == 3
    assert not (lake_root(lake) / CATALOG_FILE).exists()
    assert not (lake.meta_root / "storage.json").exists()


def test_applying_copies_verifies_and_switches_the_lake(lake):
    from cnequity.query import catalog_read, load

    report = migrate_to_catalog(lake, apply=True)
    assert report["copied"] == ["fund_flow", "margin_trading"]
    assert report["verified"] == 2 and report["switched"] is True
    assert json.loads((lake.meta_root / "storage.json").read_text()) == {
        "publish": "catalog",
        "revisions": "catalog",
    }
    with Catalog(lake_root(lake), read_only=True) as catalog:
        assert catalog_is_current(lake, catalog, "fund_flow")
    # A second run has nothing left to do.
    assert migrate_to_catalog(lake, apply=True)["rows_changed"] == 0

    # The config still says nothing about the catalog; the lake's own record
    # sends the next compaction there.
    _run(lake, "r2", SECOND, _flows(SECOND, {"600519.SH": 3.0}), _margins(SECOND, {}))
    catalog_read.close_all()
    pointer = RevisionStore(lake.meta_root, lake.curated_root).current_pointer("fund_flow")
    assert pointer["storage"] == "catalog"
    assert load("fund_flow", config=lake).height == 3


def test_a_copy_that_differs_leaves_the_lake_unswitched(lake, monkeypatch):
    monkeypatch.setattr(
        "cnequity.storage.catalog_migration._mismatched",
        lambda config, catalog, datasets, progress=None: (
            ["fund_flow"] if "fund_flow" in datasets else []
        ),
    )
    with pytest.raises(CatalogMigrationError, match="differs.*fund_flow"):
        migrate_to_catalog(lake, apply=True)
    assert not (lake.meta_root / "storage.json").exists()
    # The generations are untouched and still what readers get.
    pointer = RevisionStore(lake.meta_root, lake.curated_root).current_pointer("fund_flow")
    assert pointer.get("storage") != "catalog"


def test_a_repair_publishes_into_the_catalog_on_a_lake_that_lives_there(lake):
    from cnequity.query import catalog_read, load
    from cnequity.storage.repairs.publish import publish_repair, repair_root

    migrate_to_catalog(lake, apply=True)
    _run(
        lake,
        "r2",
        SECOND,
        _flows(SECOND, {"600519.SH": 3.0}),
        _margins(SECOND, {"600519.SH": 11.0}),
    )
    catalog_read.close_all()
    store = RevisionStore(lake.meta_root, lake.curated_root)
    base = repair_root(lake, store, "fund_flow", register=True)
    assert base is not None

    result = publish_repair(
        lake,
        store,
        "fund_flow",
        keep=lambda frame: frame.filter(frame["symbol"] != "000001.SZ"),
        reason="drop_symbol",
        metadata={},
    )
    catalog_read.close_all()

    assert result["revision"] == 3
    pointer = store.current_pointer("fund_flow")
    assert pointer["storage"] == "catalog"
    assert sorted(load("fund_flow", config=lake)["symbol"].unique()) == ["600519.SH"]
    # The dataset the repair did not touch is as it was.
    assert load("margin_trading", config=lake).height == 2


def test_a_repair_the_gate_blocks_publishes_nothing_and_keeps_its_checkout(lake, monkeypatch):
    from cnequity.quality import publication
    from cnequity.query import catalog_read, load
    from cnequity.storage.repairs.publish import publish_repair

    migrate_to_catalog(lake, apply=True)
    _run(
        lake,
        "r2",
        SECOND,
        _flows(SECOND, {"600519.SH": 3.0}),
        _margins(SECOND, {"600519.SH": 11.0}),
    )
    catalog_read.close_all()
    store = RevisionStore(lake.meta_root, lake.curated_root)
    before = store.current_pointer("fund_flow")["revision_id"]
    monkeypatch.setattr(
        publication,
        "evaluate_publication",
        lambda *args, **kwargs: {"blocked": True, "report_path": "report.json"},
    )

    with pytest.raises(RuntimeError, match="repair publication gate blocked fund_flow") as raised:
        publish_repair(
            lake,
            store,
            "fund_flow",
            keep=lambda frame: frame.filter(frame["symbol"] != "000001.SZ"),
            reason="drop_symbol",
            metadata={},
        )
    catalog_read.close_all()

    assert store.current_pointer("fund_flow")["revision_id"] == before
    assert "000001.SZ" in load("fund_flow", config=lake)["symbol"].to_list()
    kept = raised.value.args[0].partition("candidate: ")[2]
    # The partitions the repair would have published, kept for inspection.
    assert list(Path(kept).rglob("*.parquet"))


def _built_by(lake, version: str):
    """Record that *lake* was built by *version*, and return a config file for it."""
    from cnequity.storage.lake_record import _write_lake

    _write_lake(lake, {"created_with": version, "migrations": {}})
    path = lake.data_root.parent / "cnequity.toml"
    path.write_text(f'[data]\nroot = "{lake.data_root.as_posix()}"\n', encoding="utf-8")
    return str(path)


def test_one_command_shows_then_does_the_whole_upgrade(lake, monkeypatch):
    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.query import catalog_read, load
    from cnequity.storage.catalog_migration import require_catalog_lake

    config = _built_by(lake, "0.24.0")

    preview = CliRunner().invoke(cli, ["upgrade", "--config", config])
    assert preview.exit_code == 0, preview.output
    assert "lake-catalog" in preview.output and "cne upgrade --apply" in preview.output
    assert not (lake.meta_root / "storage.json").exists()
    assert not (lake.data_root / "backups").exists()

    done = CliRunner().invoke(cli, ["upgrade", "--apply", "--config", config])
    catalog_read.close_all()
    assert done.exit_code == 0, done.output
    # Each dataset is named as it is registered and verified.
    assert "1/2 fund_flow" in done.output and "2/2 margin_trading" in done.output
    backup = next((lake.data_root / "backups").glob("upgrade-*"))
    assert (backup / "meta" / "revisions" / "fund_flow" / "current.json").is_file()
    require_catalog_lake(lake)
    assert load("fund_flow", config=lake).height == 2
    catalog_read.close_all()

    again = CliRunner().invoke(cli, ["upgrade", "--apply", "--config", config])
    assert again.exit_code == 0 and "lake-catalog" not in again.output
    assert len(list((lake.data_root / "backups").glob("upgrade-*"))) == 1


def test_a_stopped_upgrade_continues_with_what_it_had_not_done(lake, monkeypatch):
    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.query import catalog_read
    from cnequity.storage import catalog_bridge

    config = _built_by(lake, "0.24.0")
    real_sync = catalog_bridge.sync_dataset

    def stop_at_margins(cfg, catalog, dataset):
        if dataset == "margin_trading":
            raise KeyboardInterrupt
        return real_sync(cfg, catalog, dataset)

    monkeypatch.setattr(catalog_bridge, "sync_dataset", stop_at_margins)
    CliRunner().invoke(cli, ["upgrade", "--apply", "--config", config])
    monkeypatch.setattr(catalog_bridge, "sync_dataset", real_sync)
    catalog_read.close_all()
    assert not (lake.meta_root / "storage.json").exists()

    done = CliRunner().invoke(cli, ["upgrade", "--apply", "--config", config])
    catalog_read.close_all()
    assert done.exit_code == 0, done.output
    # Only what the stopped run had not registered is registered, and counted.
    assert "registering 1/1 margin_trading" in done.output
    assert "fund_flow" not in done.output.split("registering", 1)[1].split("verifying", 1)[0]
    assert (lake.meta_root / "storage.json").exists()


def test_an_upgrade_this_machine_cannot_do_changes_nothing(lake, monkeypatch):
    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.ops import upgrade as upgrading

    config = _built_by(lake, "0.24.0")
    monkeypatch.setattr(
        upgrading,
        "_check_extensions",
        lambda: upgrading.Check(
            "extensions", False, "DuckDB extension 'ducklake' is not available"
        ),
    )
    result = CliRunner().invoke(cli, ["upgrade", "--apply", "--config", config])
    assert result.exit_code == 1
    assert "ducklake" in result.output
    assert not (lake.meta_root / "storage.json").exists()
    assert not (lake.data_root / "backups").exists()


def test_migrate_to_catalog_handles_legacy_null_receipt_and_empty_legacy_pointers(tmp_path):
    """Pre-0.25 lakes with revision=0 / receipt=None pointers (populated or empty) migrate cleanly."""
    import json
    from datetime import date, datetime, timezone

    import polars as pl

    from cnequity.domain.schemas import DATASET_SCHEMAS
    from cnequity.storage.catalog_migration import migrate_to_catalog
    from cnequity.storage.revisions import RevisionStore

    lake = Config(data_root=tmp_path / "legacy-null-receipt")
    # Populated legacy pointer (receipt: null), like 0.24.0 demo instruments.
    inst_dir = lake.meta_root / "revisions" / "data" / "instruments" / "legacy"
    inst_dir.mkdir(parents=True)
    inst_schema = DATASET_SCHEMAS["instruments"]
    inst_row = {name: None for name in inst_schema}
    inst_row.update(
        symbol="600519.SH",
        name="贵州茅台",
        exchange="SH",
        asset_type="stock",
        list_date=date(2001, 8, 27),
        status="listed",
        source="test",
        data_version="v1",
        fetched_at=datetime(2024, 6, 18, 8, 0, tzinfo=timezone.utc),
    )
    pl.DataFrame([inst_row], schema=inst_schema).write_parquet(inst_dir / "part-merged.parquet")
    (lake.meta_root / "revisions" / "instruments").mkdir(parents=True)
    (lake.meta_root / "revisions" / "instruments" / "current.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": "instruments",
                "revision": 0,
                "revision_id": "legacy",
                "generation_path": "revisions/data/instruments/legacy",
                "receipt": None,
            }
        ),
        encoding="utf-8",
    )
    # Empty legacy pointer (receipt: null, 0 parquet files), like 0.24.0 untouched datasets.
    (lake.meta_root / "revisions" / "data" / "corporate_actions" / "legacy").mkdir(parents=True)
    (lake.meta_root / "revisions" / "corporate_actions").mkdir(parents=True)
    (lake.meta_root / "revisions" / "corporate_actions" / "current.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": "corporate_actions",
                "revision": 0,
                "revision_id": "legacy",
                "generation_path": "revisions/data/corporate_actions/legacy",
                "receipt": None,
            }
        ),
        encoding="utf-8",
    )

    result = migrate_to_catalog(lake, apply=True)
    assert result["switched"] is True
    store = RevisionStore(lake.meta_root, lake.curated_root, lake.derived_root, create=False)
    assert store.publishes_to_catalog()
    assert store.latest("instruments") is not None
    assert store.latest("instruments").revision_id == "legacy"
