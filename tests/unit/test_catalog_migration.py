"""The opt-in migration that moves an existing lake onto the lake catalog."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tests.unit.test_compact_to_catalog import FIRST, SECOND, _flows, _margins, _run

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

    monkeypatch.setenv(catalog_read.ENV_SWITCH, "1")
    cfg = Config(data_root=tmp_path / "lake")
    _run(
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
    from cnequity.storage.catalog_migration import GENERATION_LAKE_SWITCH, require_catalog_lake
    from cnequity.storage.lake_record import LakeMigrationRequired
    from cnequity.storage.repairs.publish import publish_repair

    monkeypatch.delenv(GENERATION_LAKE_SWITCH)
    store = RevisionStore(lake.meta_root, lake.curated_root)
    before = store.current_pointer("fund_flow")

    with pytest.raises(LakeMigrationRequired, match="cne migrate --apply"):
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
    from cnequity.storage.catalog_migration import GENERATION_LAKE_SWITCH, require_catalog_lake

    monkeypatch.delenv(GENERATION_LAKE_SWITCH)
    cfg = Config(data_root=tmp_path / "new")
    try:
        Catalog(tmp_path / "probe", create=True).close()
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))

    require_catalog_lake(cfg)
    assert json.loads((cfg.meta_root / "storage.json").read_text()) == {"publish": "catalog"}
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
    assert json.loads((cfg.meta_root / "storage.json").read_text()) == {"publish": "catalog"}


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
    assert json.loads((lake.meta_root / "storage.json").read_text()) == {"publish": "catalog"}
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
        lambda config, catalog, datasets: ["fund_flow"] if "fund_flow" in datasets else [],
    )
    with pytest.raises(CatalogMigrationError, match="differs.*fund_flow"):
        migrate_to_catalog(lake, apply=True)
    assert not (lake.meta_root / "storage.json").exists()
    # The generations are untouched and still what readers get.
    pointer = RevisionStore(lake.meta_root, lake.curated_root).current_pointer("fund_flow")
    assert pointer.get("storage") != "catalog"


def test_the_repair_gate_audits_a_lake_whose_datasets_live_only_in_the_catalog(lake, monkeypatch):
    import polars as pl

    from cnequity.quality.publication import check_repair_publication
    from cnequity.query import catalog_read, load

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
    assert store.current_pointer("margin_trading")["storage"] == "catalog"

    # A repair rewrites one partition of the verified working tree.
    store.materialize_current("fund_flow")
    target = sorted((lake.curated_root / "fund_flow").rglob("trade_date=2024-07*/*.parquet"))[-1]
    pl.read_parquet(target).with_columns(pl.lit(7.0).alias("main_net_inflow")).write_parquet(target)
    audited = []

    def errors(view, day, scope=None):
        flows = load("fund_flow", config=view).sort("trade_date")
        audited.append(
            (flows["main_net_inflow"].to_list()[-1], load("margin_trading", config=view).height)
        )
        return []

    monkeypatch.setattr("cnequity.quality.publication._errors", errors)
    report = check_repair_publication(lake, "fund_flow", "repair-1", [target])

    # The published rows first, then the candidate; the other dataset, which
    # exists only in the catalog, is readable in both.
    assert audited == [(3.0, 2), (7.0, 2)]
    assert report["blocked"] is False


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


def test_file_layout_tools_leave_a_catalog_dataset_alone(lake):
    from cnequity.storage.merge_files import merge_candidates
    from cnequity.storage.repairs.layout import repair_layout
    from cnequity.storage.repairs.repartition import repartition_dataset

    migrate_to_catalog(lake, apply=True)

    # The catalog lays out and merges its own files: none of these builds a
    # working tree or publishes anything.
    assert merge_candidates(lake, "fund_flow", min_files=1) == {}
    assert repair_layout(lake, "fund_flow", apply=True)["applied"] is False
    assert repartition_dataset(lake, "fund_flow").changed is False
    store = RevisionStore(lake.meta_root, lake.curated_root)
    assert store.current_pointer("fund_flow")["revision"] == 1
