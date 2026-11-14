"""The opt-in migration that moves an existing lake onto the lake catalog."""

from __future__ import annotations

import json

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


def test_the_migration_is_never_owed(lake):
    assert "lake-catalog" in {migration.id for migration in MIGRATIONS}
    assert "lake-catalog" not in {migration.id for migration in owed(lake)}


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
