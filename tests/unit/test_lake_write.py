"""Writes to the lake catalog: check out partitions, change them, publish one snapshot."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest
from tests.legacy_lake import commit_generation

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
from equitylake_core.write import Write, WriteRejected

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
    commit_generation(
        RevisionStore(config.meta_root, config.curated_root),
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
    # As `cne upgrade` leaves a lake: publishing into its catalog, which records its revisions.
    from cnequity.storage import revision_records
    from cnequity.storage.catalog_migration import _record_revisions

    _record_revisions(config)
    yield config
    catalog_read.close_all()
    revision_records.forget()


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


def test_a_failure_reading_the_commit_result_keeps_published_files(cfg, monkeypatch):
    original = Write.commit

    def result_unavailable(write, **kwargs):
        def fail():
            raise OSError("cannot read committed snapshot")

        with monkeypatch.context() as patch:
            patch.setattr(write.catalog, "snapshot", fail)
            return original(write, **kwargs)

    monkeypatch.setattr(Write, "commit", result_unavailable)
    with LakeWrite(cfg, "commit-result") as write:
        checkout = write.checkout("daily_bars")
        _replace(
            checkout.partition("trade_date=2024-01"), _bars(date(2024, 1, 2), {"600000.SH": 20})
        )
        with pytest.raises(OSError, match="cannot read committed snapshot"):
            write.publish({"daily_bars": PUBLISH})
    rows = _load(cfg)
    assert rows.filter(pl.col("trade_date") == date(2024, 1, 2))["close"].to_list() == [20]


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
        assert lake.execute(f"SELECT count(*) FROM {REVISIONS_TABLE}").fetchone()[0] == 0


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


def _publish_elsewhere(cfg, run_id: str, close: float) -> None:
    """Another writer, outside any scope this test opened, publishes February."""
    import contextvars

    def publish():
        with LakeWrite(cfg, run_id) as write:
            february = write.checkout("daily_bars").partition("trade_date=2024-02")
            _replace(february, _bars(date(2024, 2, 1), {"600000.SH": close}))
            write.publish({"daily_bars": PUBLISH})

    contextvars.Context().run(publish)


def test_a_write_planned_before_another_publish_is_refused_though_its_checkout_is_later(cfg):
    from cnequity.storage.lake_write import InputChanged, planned_on

    with planned_on(cfg):
        planned = _load(cfg)  # what a repair decides from
        _publish_elsewhere(cfg, "meanwhile", 11.5)
        with LakeWrite(cfg, "repair") as write:
            january = write.checkout("daily_bars").partition("trade_date=2024-01")
            _replace(january, planned.filter(pl.col("trade_date") == date(2024, 1, 2)).head(1))
            with pytest.raises(InputChanged, match="daily_bars was published by another write"):
                write.publish({"daily_bars": PUBLISH})
    assert _load(cfg).filter(pl.col("trade_date") == date(2024, 1, 2)).height == 2


def test_a_scope_may_publish_a_dataset_twice_and_takes_no_lock_while_it_reads(cfg):
    from cnequity.file_lock import lake_mutation_lock
    from cnequity.storage.lake_write import planned_on

    with planned_on(cfg):
        # Nothing holds the lake lock while the writer reads and computes.
        with lake_mutation_lock(cfg.meta_root, blocking=False):
            pass
        for run_id, close in (("one", 1.0), ("two", 2.0)):
            with LakeWrite(cfg, run_id) as write:
                january = write.checkout("daily_bars").partition("trade_date=2024-01")
                _replace(january, _bars(date(2024, 1, 2), {"600000.SH": close}))
                write.publish({"daily_bars": PUBLISH})
    assert _load(cfg).filter(pl.col("trade_date") == date(2024, 1, 2))["close"].to_list() == [2.0]


def test_an_input_published_since_it_was_read_refuses_the_commit(cfg):
    from cnequity.storage.lake_write import InputChanged

    read = _pointer(cfg)["revision_id"]
    _publish_elsewhere(cfg, "meanwhile", 11.5)
    with LakeWrite(cfg, "derive") as write:
        write.expect("daily_bars", read)
        january = write.checkout("daily_bars").partition("trade_date=2024-01")
        _replace(january, _bars(date(2024, 1, 2), {"600000.SH": 3.0}))
        with pytest.raises(InputChanged) as refused:
            write.publish({"daily_bars": PUBLISH})
    assert refused.value.dataset == "daily_bars"


def test_a_publish_writes_no_pointer_or_receipt_file(cfg):
    import json

    directory = cfg.meta_root / "revisions" / "daily_bars"
    files = {path.name: path.read_bytes() for path in directory.glob("*.json")}
    before = _pointer(cfg)
    with LakeWrite(cfg, "r6") as write:
        january = write.checkout("daily_bars").partition("trade_date=2024-01")
        _replace(january, _bars(date(2024, 1, 2), {"600000.SH": 10.5}))
        (receipt,) = write.publish({"daily_bars": PUBLISH}).values()

    # The catalog is the record: the revision store's files are as they were.
    assert {path.name: path.read_bytes() for path in directory.glob("*.json")} == files
    after = _pointer(cfg)
    assert after["revision"] == before["revision"] + 1 == receipt.revision
    assert after["revision_id"] == receipt.revision_id and after["storage"] == "catalog"
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    assert store.latest("daily_bars").metadata["base_revision_id"] == before["revision_id"]
    # The state file keeps no copy of the revision the catalog records.
    state = json.loads((cfg.meta_root / "state" / "daily_bars.json").read_text())
    assert "revision_id" not in state
    assert "revision_receipt" not in state and "revision_pointer" not in state


def test_a_publish_that_stops_before_the_state_file_is_still_published(cfg, monkeypatch):
    import json

    before = _pointer(cfg)
    monkeypatch.setattr(
        RevisionStore,
        "index_catalog_revision",
        lambda self, receipt: (_ for _ in ()).throw(SystemExit("crash")),
    )
    write = LakeWrite(cfg, "r6")
    january = write.checkout("daily_bars").partition("trade_date=2024-01")
    _replace(january, _bars(date(2024, 1, 2), {"600000.SH": 10.5}))
    with pytest.raises(SystemExit):
        write.publish({"daily_bars": PUBLISH})
    write.close()
    monkeypatch.undo()
    # The catalog committed the rows and the revision together.
    after = _pointer(cfg)
    assert after["revision"] == before["revision"] + 1 and after["storage"] == "catalog"
    # A copy an older release left in the state file is now stale; the sweep
    # removes it, and the catalog alone names the revision.
    state_path = cfg.meta_root / "state" / "daily_bars.json"
    assert json.loads(state_path.read_text())["revision_id"] == before["revision_id"]

    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    assert store.sweep()["state_synced"] == ["daily_bars"]
    assert store.sweep(apply=True)["state_synced"] == ["daily_bars"]
    assert "revision_id" not in json.loads(state_path.read_text())
    assert store.latest("daily_bars").revision_id == after["revision_id"]
    assert store.sweep(apply=True)["state_synced"] == []


def test_a_publish_does_not_recreate_a_removed_state_directory(cfg):
    import shutil

    state = cfg.meta_root / "state"
    shutil.rmtree(state)
    with LakeWrite(cfg, "r-nostate") as write:
        january = write.checkout("daily_bars").partition("trade_date=2024-01")
        _replace(january, _bars(date(2024, 1, 2), {"600000.SH": 10.5}))
        write.publish({"daily_bars": PUBLISH})

    assert not state.exists()
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    assert store.sweep(apply=True)["state_synced"] == []
    assert not state.exists()


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


def test_export_writes_plain_parquet_a_hive_reader_cannot_misread(cfg, tmp_path):
    import duckdb

    from cnequity.storage.catalog_bridge import ExportRefused, export_dataset

    out = tmp_path / "export"
    result = export_dataset(cfg, "daily_bars", out)
    assert (result["rows"], result["path"]) == (3, str(out / "daily_bars"))
    files = (out / "daily_bars" / "**" / "*.parquet").as_posix()
    # With or without directory names read as columns, the dates are the rows' own.
    for hive in ("true", "false"):
        rows = duckdb.sql(
            f"SELECT symbol, trade_date, close FROM read_parquet('{files}', "
            f"hive_partitioning = {hive}) ORDER BY trade_date, symbol"
        ).fetchall()
        assert rows == [
            ("000001.SZ", date(2024, 1, 2), 9.0),
            ("600000.SH", date(2024, 1, 2), 10.0),
            ("600000.SH", date(2024, 2, 1), 11.0),
        ]
    # A second export never overwrites the first.
    with pytest.raises(ExportRefused, match="not empty"):
        export_dataset(cfg, "daily_bars", out)
    ranged = export_dataset(cfg, "daily_bars", tmp_path / "feb", start=date(2024, 2, 1))
    assert ranged["rows"] == 1


def test_the_first_publish_of_a_dataset_keyed_by_a_period_label_checks_its_rows(tmp_path):
    """A table not created yet takes the partition column's type from the schema."""
    from tests.catalog_seed import publish_rows

    from cnequity.domain.schemas import DATASET_SCHEMAS

    cfg = Config(data_root=tmp_path / "fresh")
    schema = DATASET_SCHEMAS["financial_statement_items"]
    row = {name: None for name in schema}
    row.update(
        symbol="600519.SH",
        report_period="2024Q1",
        statement_type="income",
        item_code="net_profit",
        item_value=1.0,
        source="test",
        data_version="v1",
        fetched_at=FETCHED,
    )
    revision = publish_rows(cfg, "financial_statement_items", pl.DataFrame([row], schema=schema))
    assert revision is not None and revision.changed_partitions
