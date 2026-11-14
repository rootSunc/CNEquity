"""The lake catalog as the record of a lake's revisions (the ``revision-records`` migration)."""

# ruff: noqa: F811 — the ``cfg`` fixture is shared with test_lake_write.

from __future__ import annotations

import json
from datetime import date

import pytest
from tests.unit.test_lake_write import PUBLISH, _bars, _replace, cfg  # noqa: F401

from cnequity.storage.lake_write import LakeWrite
from cnequity.storage.revision_records import import_receipts, records_for
from cnequity.storage.revisions import RevisionStore


def _store(cfg):
    return RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)


def _publish(cfg, run_id: str, close: float) -> None:
    with LakeWrite(cfg, run_id) as write:
        january = write.checkout("daily_bars").partition("trade_date=2024-01")
        _replace(january, _bars(date(2024, 1, 2), {"600000.SH": close}))
        write.publish({"daily_bars": PUBLISH})


def _switch(cfg):
    (cfg.meta_root / "storage.json").write_text('{"publish": "catalog"}', encoding="utf-8")


def _record_migration(cfg, report):
    from cnequity.storage.migrations import MIGRATIONS, record

    (migration,) = [m for m in MIGRATIONS if m.id == "revision-records"]
    record(cfg, migration, report)


@pytest.fixture
def lake(cfg):
    """A generation revision synced into the catalog, then two published there."""
    from cnequity.storage import revision_records
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core.catalog import Catalog

    _switch(cfg)
    first = _store(cfg).pointer_file("daily_bars")["revision_id"]
    with Catalog(lake_root(cfg)) as catalog:
        # As 0.25.0 synced it: the snapshot, but not what the receipt says.
        catalog.execute("UPDATE _revision_history SET revision = NULL, metadata = NULL")
    _publish(cfg, "second", 2.0)
    _publish(cfg, "third", 3.0)
    yield cfg, first
    revision_records.forget()


def test_reads_stay_on_the_files_until_the_receipts_are_imported(lake):
    cfg, _ = lake
    assert records_for(cfg.meta_root) is None
    report = import_receipts(cfg, apply=False)
    # The generation revision is the only one the catalog has no receipt for.
    assert report["receipts"] == 1 and report["rows_changed"] == 1
    assert records_for(cfg.meta_root) is None


def test_after_the_import_the_catalog_answers_as_the_files_do(lake):
    cfg, first = lake
    store = _store(cfg)
    # A publish that stopped after writing its receipt is not a revision.
    stray = cfg.meta_root / "revisions" / "daily_bars" / f"{99:08d}-{'f' * 32}.json"
    stray.write_text(json.dumps({"dataset": "daily_bars", "revision": 99, "revision_id": "f" * 32}))

    report = import_receipts(cfg, apply=True)
    assert (report["receipts"], report["unpublished"]) == (1, 1)
    _record_migration(cfg, report)
    records = records_for(cfg.meta_root)
    assert records is not None

    files = store.pointer_file("daily_bars")
    assert store.current_pointer("daily_bars") == {
        key: files.get(key)
        for key in (
            "schema_version",
            "dataset",
            "revision",
            "revision_id",
            "generation_path",
            "receipt",
            "content_digest",
            "committed_at",
            "storage",
        )
    }
    assert store.current_root("daily_bars") == store.current_root_file("daily_bars")
    assert store.current_root("daily_bars", revision=first) == store.current_root_file(
        "daily_bars", revision=first
    )

    latest, receipt = store.latest("daily_bars"), store.receipt_file("daily_bars")
    assert latest.to_dict() | {"files": [], "generation_files": []} == receipt.to_dict() | {
        "files": [],
        "generation_files": [],
    }
    after = [r.revision_id for r in store.history_after("daily_bars", first)]
    assert after == [r.revision_id for r in store._history_after_file("daily_bars", first)]
    assert len(after) == 2
    assert records.find("daily_bars", "f" * 32) is None

    # Run again: nothing left to import.
    assert import_receipts(cfg, apply=True)["receipts"] == 0


def test_a_publish_is_seen_at_once_and_a_revision_the_catalog_let_go_has_no_snapshot(lake):
    from cnequity.storage.catalog_bridge import lake_root, revision_snapshot
    from equitylake_core.catalog import Catalog

    cfg, first = lake
    _record_migration(cfg, import_receipts(cfg, apply=True))
    store = _store(cfg)
    before = store.current_pointer("daily_bars")["revision"]
    _publish(cfg, "fourth", 4.0)
    assert store.current_pointer("daily_bars")["revision"] == before + 1
    assert store.current_pointer("daily_bars") == {
        **store.pointer_file("daily_bars"),
        "storage": "catalog",
    }

    with Catalog(lake_root(cfg)) as catalog:
        catalog.execute(
            "UPDATE _revision_history SET snapshot = NULL WHERE revision_id = ?", [first]
        )
        assert revision_snapshot(catalog, "daily_bars", first) is None
