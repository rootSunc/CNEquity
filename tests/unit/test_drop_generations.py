"""The ``drop-generations`` migration: delete what the generation publish layer left behind."""

# ruff: noqa: F811 — the ``cfg`` fixture is shared with test_lake_write.

from __future__ import annotations

from datetime import date

import polars as pl
from tests.unit.test_lake_write import PUBLISH, _bars, _load, _replace, cfg  # noqa: F401

from cnequity.storage import drop_generations as module
from cnequity.storage.drop_generations import drop_generations
from cnequity.storage.lake_write import LakeWrite
from cnequity.storage.revisions import RevisionStore


def _store(cfg):
    return RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)


def _publish(cfg, close: float) -> None:
    with LakeWrite(cfg, "later") as write:
        january = write.checkout("daily_bars").partition("trade_date=2024-01")
        _replace(january, _bars(date(2024, 1, 2), {"600000.SH": close}))
        write.publish({"daily_bars": PUBLISH})


def _rows(cfg):
    return _load(cfg).sort("trade_date", "symbol").select("symbol", "trade_date", "close").rows()


def test_a_preview_deletes_nothing_and_apply_leaves_only_the_catalog(cfg):
    first = _store(cfg).pointer_file("daily_bars")["revision_id"]
    generation = cfg.meta_root / "revisions" / "data" / "daily_bars" / first
    _publish(cfg, 12.0)
    before = _rows(cfg)

    preview = drop_generations(cfg, apply=False)

    # The first revision and the baseline it was built on.
    assert preview["generations"] == 2
    assert preview["dataset_directories"] == ["curated/daily_bars"]
    assert preview["rows_changed"] > 0 and preview["skipped_datasets"] == []
    assert generation.is_dir() and (cfg.curated_root / "daily_bars").is_dir()

    applied = drop_generations(cfg, apply=True)

    assert applied["rows_changed"] == preview["rows_changed"]
    assert not cfg.curated_root.exists()
    assert not [p for p in (cfg.meta_root / "revisions").rglob("*") if p.is_file()]
    assert _rows(cfg) == before
    assert _store(cfg).current_pointer("daily_bars")["revision"] == 2
    assert drop_generations(cfg, apply=True)["rows_changed"] == 0


def test_a_held_generation_stays_with_its_receipt_and_reads_by_revision(cfg, monkeypatch):
    from cnequity.query import catalog_read
    from cnequity.query.parquet_scan import scan_parquet_root

    first = _store(cfg).pointer_file("daily_bars")["revision_id"]
    _publish(cfg, 12.0)
    monkeypatch.setattr(module, "_held_revisions", lambda meta: {f"revision/daily_bars/{first}"})

    report = drop_generations(cfg, apply=True)

    assert (report["generations"], report["kept_generations"]) == (1, 1)
    assert report["kept"][0]["kept_because"] == ["explicit_hold"]
    folder = cfg.meta_root / "revisions" / "daily_bars"
    assert [path.name for path in folder.iterdir()] == [f"{1:08d}-{first}.json"]
    assert not cfg.curated_root.exists()
    catalog_read.close_all()
    held = scan_parquet_root(
        cfg.curated_root / "daily_bars", meta_root=cfg.meta_root, revision=first
    ).collect()
    assert sorted(held["close"].to_list()) == [9.0, 10.0, 11.0]


def test_what_the_catalog_does_not_hold_and_what_is_not_a_dataset_stay(cfg):
    orphan = cfg.curated_root / "valuation_metrics" / "part.parquet"
    orphan.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600000.SH"]}).write_parquet(orphan)
    backup = cfg.curated_root / "daily_bars.bak"
    backup.mkdir()

    report = drop_generations(cfg, apply=True)

    assert report["skipped_datasets"] == ["valuation_metrics"]
    assert report["left_in_place"] == ["curated/daily_bars.bak"]
    assert orphan.is_file() and backup.is_dir()
    assert not (cfg.curated_root / "daily_bars").exists()


def test_a_lake_whose_catalog_does_not_record_revisions_is_left_alone(cfg):
    from cnequity.storage import revision_records

    (cfg.meta_root / "storage.json").write_text('{"publish": "catalog"}', encoding="utf-8")
    revision_records.forget()

    report = drop_generations(cfg, apply=True)

    assert report["rows_changed"] == 0 and "skipped" in report
    assert (cfg.curated_root / "daily_bars").is_dir()
