"""Writes to the lake catalog: check out partitions, change them, publish one snapshot."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config.loader import Config
from cnequity.storage.catalog_bridge import (
    REVISIONS_TABLE,
    catalog_is_current,
    lake_root,
    retained_snapshots,
    revision_snapshot,
    sync_dataset,
)
from cnequity.storage.lake_write import DatasetPublish, LakeWrite
from cnequity.storage.revisions import RevisionStore
from equitylake_core.catalog import Catalog
from equitylake_core.extensions import ExtensionUnavailableError
from equitylake_core.write import WriteRejected

FETCHED = datetime(2024, 3, 1, tzinfo=timezone.utc)
PUBLISH = DatasetPublish(schema_version=1, contract_fingerprint="contract")


def _bars(day: date, closes: dict[str, float]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "symbol": symbol,
                "trade_date": day,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 100,
                "amount": close * 100,
                "source": "test",
                "data_version": "v1",
                "fetched_at": FETCHED,
            }
            for symbol, close in closes.items()
        ]
    )


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    from cnequity.query import catalog_read

    monkeypatch.setenv(catalog_read.ENV_SWITCH, "1")
    monkeypatch.setattr(
        "cnequity.storage.catalog_bridge.sync_published", lambda store, dataset: None
    )
    config = Config(data_root=tmp_path / "data")
    files = []
    for month, frame in {
        "2024-01": _bars(date(2024, 1, 2), {"600000.SH": 10.0, "000001.SZ": 9.0}),
        "2024-02": _bars(date(2024, 2, 1), {"600000.SH": 11.0}),
    }.items():
        path = config.curated_root / f"daily_bars/trade_date={month}/part-merged.parquet"
        path.parent.mkdir(parents=True)
        frame.write_parquet(path)
        files.append(path)
    RevisionStore(config.meta_root, config.curated_root).commit(
        "daily_bars",
        run_id="seed",
        changed_files=files,
        schema_version=1,
        contract_fingerprint="contract",
    )
    try:
        with Catalog(lake_root(config), create=True) as lake:
            sync_dataset(config, lake, "daily_bars")
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))
    yield config
    catalog_read.close_all()


def _pointer(cfg, dataset="daily_bars"):
    return RevisionStore(cfg.meta_root, cfg.curated_root).current_pointer(dataset)


def _load(cfg, dataset="daily_bars"):
    from cnequity.query import catalog_read, load

    catalog_read.close_all()
    return load(dataset, config=cfg)


def _replace(directory, frame):
    for old in directory.glob("*.parquet"):
        old.unlink()
    frame.write_parquet(directory / "part-merged.parquet")


def test_a_checkout_holds_the_published_rows_and_an_untouched_one_publishes_nothing(cfg):
    with LakeWrite(cfg, "r1") as write:
        january = write.checkout("daily_bars").partition("trade_date=2024-01")
        rows = pl.read_parquet(list(january.glob("*.parquet")))
        assert sorted(rows["symbol"]) == ["000001.SZ", "600000.SH"]
        assert write.publish({"daily_bars": PUBLISH}) == {}
    assert not write.root.exists()


def test_a_changed_partition_publishes_into_the_catalog_and_nothing_else_moves(cfg):
    before = _pointer(cfg)
    with LakeWrite(cfg, "r2") as write:
        january = write.checkout("daily_bars").partition("trade_date=2024-01")
        _replace(january, _bars(date(2024, 1, 2), {"600000.SH": 10.5, "000001.SZ": 9.0}))
        march = write.checkout("daily_bars").partition("trade_date=2024-03")
        assert not list(march.glob("*.parquet"))
        _replace(march, _bars(date(2024, 3, 1), {"600000.SH": 12.0}))
        (receipt,) = write.publish({"daily_bars": PUBLISH}).values()

    pointer = _pointer(cfg)
    assert pointer["storage"] == "catalog" and pointer["revision"] == before["revision"] + 1
    assert not (cfg.meta_root / pointer["generation_path"]).exists()
    assert set(receipt.changed_partitions) == {
        "daily_bars/trade_date=2024-01",
        "daily_bars/trade_date=2024-03",
    }
    rows = _load(cfg).sort("trade_date", "symbol")
    assert rows.select("symbol", "trade_date", "close").rows() == [
        ("000001.SZ", date(2024, 1, 2), 9.0),
        ("600000.SH", date(2024, 1, 2), 10.5),
        ("600000.SH", date(2024, 2, 1), 11.0),
        ("600000.SH", date(2024, 3, 1), 12.0),
    ]
    with Catalog(lake_root(cfg), read_only=True) as lake:
        assert catalog_is_current(cfg, lake, "daily_bars")
        assert lake.execute(f"SELECT revision_id FROM {REVISIONS_TABLE}").fetchall() == [
            (receipt.revision_id,)
        ]
        # The publish is one snapshot: it holds the rows and names the revision,
        # it can be read from its files, and retention keeps it.
        snapshot = lake.snapshot()
        assert revision_snapshot(lake, "daily_bars", receipt.revision_id) == (snapshot, True)
        assert lake.data_files("daily_bars", snapshot) is not None
        assert snapshot in retained_snapshots(lake, keep=1, held=set())


def test_a_refused_check_publishes_nothing_and_keeps_the_checkout(cfg):
    before = _pointer(cfg)
    write = LakeWrite(cfg, "r3")
    january = write.checkout("daily_bars").partition("trade_date=2024-01")
    _replace(january, _bars(date(2024, 1, 2), {"600000.SH": -1.0}))
    write.check(
        "positive prices",
        lambda lake: (
            "a close is negative"
            if lake.execute("SELECT count(*) FROM daily_bars WHERE close < 0").fetchone()[0]
            else None
        ),
    )
    with pytest.raises(WriteRejected, match="positive prices: a close is negative"):
        write.publish({"daily_bars": PUBLISH})
    kept = write.close(keep=True)
    assert kept is not None and list(kept.rglob("*.parquet"))
    assert _pointer(cfg) == before
    assert _load(cfg).height == 3
    with Catalog(lake_root(cfg), read_only=True) as lake:
        assert not lake.has_table(REVISIONS_TABLE)


def test_two_datasets_publish_in_one_snapshot(cfg):
    with LakeWrite(cfg, "r4") as write:
        january = write.checkout("daily_bars").partition("trade_date=2024-01")
        _replace(january, _bars(date(2024, 1, 2), {"600000.SH": 10.5}))
        instruments = write.checkout("instruments").partition()
        pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "name": ["浦发银行"],
                "exchange": ["SH"],
                "asset_type": ["stock"],
                "list_date": [date(1999, 11, 10)],
                "delist_date": [None],
                "prev_symbol": [None],
                "source": ["test"],
                "data_version": ["v1"],
                "fetched_at": [FETCHED],
            },
            schema_overrides={"delist_date": pl.Date, "prev_symbol": pl.Utf8},
        ).write_parquet(instruments / "part-merged.parquet")
        published = write.publish(
            {"daily_bars": PUBLISH, "instruments": PUBLISH}, message="bars and listings"
        )
    assert set(published) == {"daily_bars", "instruments"}
    with Catalog(lake_root(cfg), read_only=True) as lake:
        snapshots = lake.execute(
            "SELECT DISTINCT snapshot_id FROM _revisions WHERE revision_id IN (?, ?)",
            [published["daily_bars"].revision_id, published["instruments"].revision_id],
        ).fetchall()
    assert len(snapshots) == 1
    assert _load(cfg, "instruments")["symbol"].to_list() == ["600000.SH"]


def test_a_removed_partition_leaves_the_dataset(cfg):
    with LakeWrite(cfg, "r5") as write:
        write.checkout("daily_bars").remove("trade_date=2024-02")
        (receipt,) = write.publish({"daily_bars": PUBLISH}).values()
    assert receipt.metadata["removed_partitions"] == ["daily_bars/trade_date=2024-02"]
    assert _load(cfg)["trade_date"].max() == date(2024, 1, 2)


def test_a_dataset_published_since_the_checkout_is_refused(cfg):
    first = LakeWrite(cfg, "slow")
    january = first.checkout("daily_bars").partition("trade_date=2024-01")
    with LakeWrite(cfg, "fast") as second:
        other = second.checkout("daily_bars").partition("trade_date=2024-02")
        _replace(other, _bars(date(2024, 2, 1), {"600000.SH": 11.5}))
        second.publish({"daily_bars": PUBLISH})
    _replace(january, _bars(date(2024, 1, 2), {"600000.SH": 1.0}))
    with pytest.raises(WriteRejected, match="published by another write"):
        first.publish({"daily_bars": PUBLISH})
    first.close()
    assert _load(cfg).filter(pl.col("trade_date") == date(2024, 1, 2)).height == 2


def test_sweep_moves_the_pointer_to_what_the_catalog_published(cfg, monkeypatch):
    before = _pointer(cfg)
    monkeypatch.setattr(
        RevisionStore,
        "finish_catalog_revision",
        lambda self, receipt: (_ for _ in ()).throw(SystemExit("crash")),
    )
    write = LakeWrite(cfg, "r6")
    january = write.checkout("daily_bars").partition("trade_date=2024-01")
    _replace(january, _bars(date(2024, 1, 2), {"600000.SH": 10.5}))
    with pytest.raises(SystemExit):
        write.publish({"daily_bars": PUBLISH})
    write.close()
    monkeypatch.undo()
    assert _pointer(cfg) == before

    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    report = store.sweep(apply=True)
    assert report["pointer_restored"] == ["daily_bars"]
    after = _pointer(cfg)
    assert after["revision"] == before["revision"] + 1 and after["storage"] == "catalog"


def test_a_generation_writer_publishes_into_the_catalog_only_from_a_verified_tree(cfg):
    from cnequity.storage.revisions import CatalogOnlyDatasetError

    with LakeWrite(cfg, "r7") as write:
        january = write.checkout("daily_bars").partition("trade_date=2024-01")
        _replace(january, _bars(date(2024, 1, 2), {"600000.SH": 10.5}))
        write.publish({"daily_bars": PUBLISH})
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    april = cfg.curated_root / "daily_bars/trade_date=2024-04"

    def commit():
        april.mkdir(parents=True, exist_ok=True)
        _bars(date(2024, 4, 1), {"600000.SH": 13.0}).write_parquet(april / "part-merged.parquet")
        return store.commit(
            "daily_bars",
            run_id="old-writer",
            changed_files=[april / "part-merged.parquet"],
            schema_version=1,
            contract_fingerprint="contract",
        )

    # The publish carried January into the working tree and kept it verified.
    tree_january = pl.read_parquet(
        list((cfg.curated_root / "daily_bars/trade_date=2024-01").glob("*.parquet"))
    )
    assert tree_january["close"].to_list() == [10.5]
    # A tree the store can no longer vouch for is never committed from.
    (cfg.meta_root / "working_trees" / "daily_bars.json").unlink()
    with pytest.raises(CatalogOnlyDatasetError, match="not verified"):
        commit()
    assert _load(cfg)["trade_date"].max() == date(2024, 2, 1)

    store.materialize_current("daily_bars")
    receipt = commit()
    assert receipt.changed_partitions == ("daily_bars/trade_date=2024-04",)
    assert _pointer(cfg)["storage"] == "catalog"
    rows = _load(cfg).sort("trade_date", "symbol").select("trade_date", "close").rows()
    assert rows == [
        (date(2024, 1, 2), 10.5),
        (date(2024, 2, 1), 11.0),
        (date(2024, 4, 1), 13.0),
    ]


def test_the_working_tree_of_a_catalog_revision_is_rebuilt_unless_verified(cfg):
    with LakeWrite(cfg, "r8") as write:
        january = write.checkout("daily_bars").partition("trade_date=2024-01")
        _replace(january, _bars(date(2024, 1, 2), {"600000.SH": 10.5}))
        write.publish({"daily_bars": PUBLISH})
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    tree = cfg.curated_root / "daily_bars"
    # The tree still holds the old January: it must be rebuilt from the catalog.
    store.materialize_current("daily_bars")
    rows = pl.read_parquet(list(tree.rglob("*.parquet"))).sort("trade_date", "symbol")
    assert rows.select("symbol", "close").rows() == [("600000.SH", 10.5), ("600000.SH", 11.0)]
    # Now recorded as current: a second call keeps it as it is.
    before = {path: path.stat().st_mtime_ns for path in tree.rglob("*.parquet")}
    store.materialize_current("daily_bars")
    assert {path: path.stat().st_mtime_ns for path in tree.rglob("*.parquet")} == before
    # A file changed behind the store's back is never trusted.
    stray = tree / "trade_date=2024-02" / "part-stray.parquet"
    _bars(date(2024, 2, 1), {"000001.SZ": 1.0}).write_parquet(stray)
    store.materialize_current("daily_bars")
    assert not stray.exists()


def test_a_lake_switched_to_the_catalog_publishes_every_writer_there(cfg):
    import json

    (cfg.meta_root / "storage.json").write_text(json.dumps({"publish": "catalog"}))
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    assert _pointer(cfg).get("storage") is None  # still a generation revision
    store.materialize_current("daily_bars")
    february = cfg.curated_root / "daily_bars/trade_date=2024-02"
    _replace(february, _bars(date(2024, 2, 1), {"600000.SH": 11.5}))
    receipt = store.commit(
        "daily_bars",
        run_id="derive",
        changed_files=[february / "part-merged.parquet"],
        schema_version=1,
        contract_fingerprint="contract",
    )
    pointer = _pointer(cfg)
    assert pointer["storage"] == "catalog" and pointer["revision_id"] == receipt.revision_id
    assert not (cfg.meta_root / pointer["generation_path"]).exists()
    closes = _load(cfg).sort("trade_date", "symbol")["close"].to_list()
    assert closes == [9.0, 10.0, 11.5]


def test_a_published_revision_records_a_digest_of_the_rows_the_catalog_holds(cfg):
    from cnequity.storage.lake_write import verify_revisions

    with LakeWrite(cfg, "r7") as write:
        january = write.checkout("daily_bars").partition("trade_date=2024-01")
        _replace(january, _bars(date(2024, 1, 2), {"600000.SH": 10.5, "000001.SZ": 9.0}))
        (receipt,) = write.publish({"daily_bars": PUBLISH}).values()

    with Catalog(lake_root(cfg)) as lake:
        (check,) = verify_revisions(lake)
        assert (check.revision_id, check.status, check.rows) == (receipt.revision_id, "match", 2)
        # A later publish does not disturb the earlier revision's proof: it is
        # checked at its own snapshot.
        lake.execute("UPDATE daily_bars SET close = 99 WHERE symbol = '000001.SZ'")
        assert [c.status for c in verify_revisions(lake)] == ["match"]
        # Rows that differ from what was published are reported.
        lake.execute(f"UPDATE {REVISIONS_TABLE} SET rows_digest = '2:0:0'")
        assert [c.status for c in verify_revisions(lake, "daily_bars")] == ["mismatch"]
