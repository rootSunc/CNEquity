"""Copying a committed generation into the lake catalog, and noticing when the copies differ."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from cnequity.config.loader import Config
from cnequity.domain.datasets import DATASETS
from cnequity.storage.catalog_bridge import (
    catalog_is_current,
    committed_source,
    compare_dataset,
    import_dataset,
    lake_root,
    sync_dataset,
)
from cnequity.storage.revisions import RevisionStore
from equitylake_core.catalog import Catalog
from equitylake_core.extensions import ExtensionUnavailableError

FETCHED = datetime(2026, 7, 23, tzinfo=timezone.utc)


def _rows(day: date, closes: dict[str, float], **extra) -> pl.DataFrame:
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
                **extra,
            }
            for symbol, close in closes.items()
        ]
    )


@pytest.fixture
def cfg(tmp_path):
    config = Config(data_root=tmp_path)
    files = []
    for month, frame in {
        # Older files lack a column newer ones have, as in the real lake.
        "2024-01": _rows(date(2024, 1, 2), {"600000.SH": 10.0, "000001.SZ": 9.0}),
        "2024-02": _rows(date(2024, 2, 1), {"600000.SH": 11.0}, pre_close=10.0),
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
    return config


@pytest.fixture
def lake(cfg):
    try:
        catalog = Catalog(lake_root(cfg), create=True)
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))
    yield catalog
    catalog.close()


def test_a_dataset_without_a_committed_generation_is_skipped(cfg, lake):
    assert committed_source(cfg, "index_bars") is None
    assert import_dataset(cfg, lake, "index_bars") is None
    assert compare_dataset(cfg, lake, "index_bars") is None


def test_the_imported_table_matches_the_committed_generation(cfg, lake):
    result = import_dataset(cfg, lake, "daily_bars")
    assert result.rows == 3
    comparison = compare_dataset(cfg, lake, "daily_bars")
    assert comparison.matches and comparison.catalog_rows == 3
    # The real date column survives a directory named like a Hive partition of it.
    rows = lake.execute(
        'SELECT symbol, trade_date, pre_close FROM "daily_bars" ORDER BY 2, 1'
    ).fetchall()
    assert rows == [
        ("000001.SZ", date(2024, 1, 2), None),
        ("600000.SH", date(2024, 1, 2), None),
        ("600000.SH", date(2024, 2, 1), 10.0),
    ]


def test_a_changed_value_is_a_mismatch(cfg, lake):
    import_dataset(cfg, lake, "daily_bars")
    lake.execute("UPDATE \"daily_bars\" SET close = close + 0.01 WHERE symbol = '000001.SZ'")
    comparison = compare_dataset(cfg, lake, "daily_bars")
    assert comparison.revision_rows == comparison.catalog_rows
    assert not comparison.same_content and not comparison.matches


def test_a_missing_row_is_a_mismatch(cfg, lake):
    import_dataset(cfg, lake, "daily_bars")
    lake.execute("DELETE FROM \"daily_bars\" WHERE symbol = '000001.SZ'")
    comparison = compare_dataset(cfg, lake, "daily_bars")
    assert (comparison.revision_rows, comparison.catalog_rows) == (3, 2)
    assert not comparison.matches


def _commit_month(cfg, month: str, frame: pl.DataFrame) -> None:
    path = cfg.curated_root / f"daily_bars/trade_date={month}/part-merged.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)
    RevisionStore(cfg.meta_root, cfg.curated_root).commit(
        "daily_bars",
        run_id="next",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )


def test_the_first_sync_imports_and_a_second_does_nothing(cfg, lake):
    assert sync_dataset(cfg, lake, "daily_bars").action == "imported"
    assert catalog_is_current(cfg, lake, "daily_bars")
    before = lake.snapshot()
    assert sync_dataset(cfg, lake, "daily_bars").action == "current"
    assert lake.snapshot() == before
    assert sync_dataset(cfg, lake, "index_bars") is None


def test_a_new_revision_replaces_only_its_changed_partitions(cfg, lake):
    sync_dataset(cfg, lake, "daily_bars")
    # February is corrected and gains a row; March is new; January is untouched.
    _commit_month(
        cfg,
        "2024-02",
        _rows(date(2024, 2, 1), {"600000.SH": 11.5, "000001.SZ": 9.5}, pre_close=10.0),
    )
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))
    assert not catalog_is_current(cfg, lake, "daily_bars")

    result = sync_dataset(cfg, lake, "daily_bars")
    assert (result.action, result.partitions) == ("partitions", 2)
    assert catalog_is_current(cfg, lake, "daily_bars")
    assert compare_dataset(cfg, lake, "daily_bars").matches
    assert lake.execute('SELECT count(*) FROM "daily_bars"').fetchone()[0] == 5


def test_a_column_new_to_the_dataset_is_added_on_sync(cfg, lake):
    sync_dataset(cfg, lake, "daily_bars")
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, turnover=0.5))
    assert sync_dataset(cfg, lake, "daily_bars").action == "partitions"
    assert compare_dataset(cfg, lake, "daily_bars").matches
    assert lake.execute('SELECT count(turnover) FROM "daily_bars"').fetchone()[0] == 1


def test_an_unknown_previous_state_replaces_the_whole_table(cfg, lake):
    import_dataset(cfg, lake, "daily_bars")  # imported by hand: no sync record
    lake.execute("DELETE FROM \"daily_bars\" WHERE symbol = '000001.SZ'")
    assert sync_dataset(cfg, lake, "daily_bars").action == "replaced"
    assert compare_dataset(cfg, lake, "daily_bars").matches


@pytest.fixture
def catalog_reads(monkeypatch):
    from cnequity.query import catalog_read

    monkeypatch.setenv(catalog_read.ENV_SWITCH, "1")
    yield catalog_read
    catalog_read.close_all()


def test_the_catalog_answers_unless_switched_off(cfg, lake, monkeypatch):
    from cnequity.query import catalog_read

    sync_dataset(cfg, lake, "daily_bars")
    monkeypatch.setenv(catalog_read.ENV_SWITCH, "0")
    assert catalog_read.read_published(cfg, "daily_bars", partition_col="trade_date") is None
    monkeypatch.delenv(catalog_read.ENV_SWITCH)
    rows = catalog_read.read_published(cfg, "daily_bars", partition_col="trade_date")
    catalog_read.close_all()
    # One row per key: the reader has nothing left to dedupe.
    assert rows.unique_keys and rows.frame.height == 3


def test_catalog_reads_filter_like_the_partition_scan(cfg, lake, catalog_reads):
    sync_dataset(cfg, lake, "daily_bars")
    everything = catalog_reads.read_published(cfg, "daily_bars", partition_col="trade_date").frame
    assert everything.height == 3
    assert everything.schema["fetched_at"] == pl.Datetime("us", "UTC")
    ranged = catalog_reads.read_published(
        cfg,
        "daily_bars",
        partition_col="trade_date",
        start=date(2024, 1, 2),
        end=date(2024, 1, 31),
        symbols=["600000.SH"],
    ).frame
    assert ranged.select("symbol", "trade_date").rows() == [("600000.SH", date(2024, 1, 2))]


def test_a_catalog_behind_the_revision_store_is_not_read(cfg, lake, catalog_reads):
    sync_dataset(cfg, lake, "daily_bars")
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))
    assert catalog_reads.read_published(cfg, "daily_bars", partition_col="trade_date") is None
    sync_dataset(cfg, lake, "daily_bars")
    assert (
        catalog_reads.read_published(cfg, "daily_bars", partition_col="trade_date").frame.height
        == 4
    )


@pytest.fixture(autouse=True)
def _publish_does_not_sync(request, monkeypatch):
    """Most tests here drive the sync by hand; ``auto_sync`` keeps the publish hook."""
    if "auto_sync" not in request.fixturenames:
        monkeypatch.setattr(
            "cnequity.storage.catalog_bridge.sync_published", lambda store, dataset: None
        )


@pytest.fixture
def auto_sync():
    return True


def test_a_publish_carries_the_new_revision_into_the_catalog(cfg, lake, auto_sync):
    sync_dataset(cfg, lake, "daily_bars")
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))
    assert catalog_is_current(cfg, lake, "daily_bars")
    assert compare_dataset(cfg, lake, "daily_bars").matches


def test_a_lake_without_a_catalog_publishes_as_before(cfg, auto_sync):
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))
    assert not lake_root(cfg).exists()


def test_a_catalog_that_cannot_be_updated_does_not_fail_the_publish(
    cfg, lake, auto_sync, monkeypatch
):
    sync_dataset(cfg, lake, "daily_bars")

    def broken(config, catalog, dataset):
        raise RuntimeError("catalog is locked")

    monkeypatch.setattr("cnequity.storage.catalog_bridge.sync_dataset", broken)
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))
    monkeypatch.undo()
    assert not catalog_is_current(cfg, lake, "daily_bars")
    assert sync_dataset(cfg, lake, "daily_bars").action == "partitions"


def test_a_synced_table_is_file_complete(cfg, lake):
    sync_dataset(cfg, lake, "daily_bars")
    _commit_month(
        cfg, "2024-01", _rows(date(2024, 1, 2), {"600000.SH": 10.5}, pre_close=None)
    )  # January loses a symbol: a partial delete in a small, unpartitioned table
    sync_dataset(cfg, lake, "daily_bars")
    files = lake.data_files("daily_bars")
    assert files
    scanned = pl.scan_parquet(
        files, hive_partitioning=False, extra_columns="ignore", missing_columns="insert"
    ).collect()
    assert scanned.height == lake.execute('SELECT count(*) FROM "daily_bars"').fetchone()[0] == 2


def test_the_generation_scan_reads_the_catalog_files(cfg, lake, catalog_reads, monkeypatch):
    from cnequity.query import parquet_scan
    from cnequity.storage.read_context import read_root

    sync_dataset(cfg, lake, "daily_bars")
    root = read_root(cfg, "daily_bars")
    used = []
    real = parquet_scan._scan_catalog_files
    monkeypatch.setattr(
        parquet_scan,
        "_scan_catalog_files",
        lambda *a, **k: used.append(1) or real(*a, **k),
    )
    via_catalog = parquet_scan.scan_parquet_root(
        root, partition_col="trade_date", start=date(2024, 2, 1), committed=False
    ).collect()
    assert used and via_catalog.select("symbol", "trade_date").rows() == [
        ("600000.SH", date(2024, 2, 1))
    ]
    monkeypatch.setenv(catalog_reads.ENV_SWITCH, "0")
    via_files = parquet_scan.scan_parquet_root(
        root, partition_col="trade_date", start=date(2024, 2, 1), committed=False
    ).collect()
    assert via_files.equals(via_catalog.select(via_files.columns))


def test_sql_views_read_the_catalog_files_and_stay_inside_the_sandbox(cfg, lake, catalog_reads):
    from cnequity.mcp_server.tools import _configure_sql_connection
    from cnequity.query.views import views_connection

    sync_dataset(cfg, lake, "daily_bars")
    con = views_connection(cfg)
    try:
        sql = con.execute(
            "SELECT sql FROM duckdb_views() WHERE view_name = 'daily_bars_including_otc'"
        ).fetchone()[0]
        assert (lake_root(cfg) / "data").as_posix() in sql
        _configure_sql_connection(con, cfg)
        rows = con.execute(
            "SELECT symbol, trade_date, pre_close FROM daily_bars ORDER BY trade_date, symbol"
        ).fetchall()
    finally:
        con.close()
    assert rows == [
        ("000001.SZ", date(2024, 1, 2), None),
        ("600000.SH", date(2024, 1, 2), None),
        ("600000.SH", date(2024, 2, 1), 10.0),
    ]


def test_a_read_pinned_to_an_earlier_revision_uses_that_snapshot(cfg, lake, catalog_reads):
    from cnequity.storage.revisions import RevisionStore

    sync_dataset(cfg, lake, "daily_bars")
    first = RevisionStore(cfg.meta_root, cfg.curated_root).current_pointer("daily_bars")[
        "revision_id"
    ]
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))
    sync_dataset(cfg, lake, "daily_bars")

    earlier = catalog_reads.published_files(cfg, "daily_bars", first)
    assert earlier is not None
    files, schema = earlier
    rows = pl.scan_parquet(files, schema=schema, extra_columns="ignore", missing_columns="insert")
    assert rows.collect().height == 3  # March is not in the first revision

    # Once the catalog has let that snapshot's files go, it no longer answers for it.
    lake.execute("CALL ducklake_expire_snapshots('lake', older_than => now() + INTERVAL 1 DAY)")
    lake.execute("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)")
    catalog_reads.close_all()
    assert catalog_reads.published_files(cfg, "daily_bars", first) is None


def test_every_read_path_serves_a_revision_published_only_into_the_catalog(
    cfg, lake, catalog_reads, publish_to_catalog
):
    from cnequity.mcp_server.tools import _configure_sql_connection
    from cnequity.quality.verify import _covered_days
    from cnequity.query import load
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.query.read_budget import oversized_read
    from cnequity.query.views import views_connection
    from cnequity.storage.published import day_digests, published
    from cnequity.storage.read_context import read_root

    sync_dataset(cfg, lake, "daily_bars")
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))
    catalog_reads.close_all()
    root = read_root(cfg, "daily_bars")
    assert not root.exists()

    assert dataset_has_parquet(root, committed=False)
    assert published(cfg, "daily_bars").from_catalog
    assert load("daily_bars", config=cfg).height == 4
    assert (
        scan_parquet_root(root, partition_col="trade_date", committed=False).collect().height == 4
    )
    assert len(day_digests(cfg, "daily_bars")) == 3
    assert oversized_read(cfg, "daily_bars", start=None, end=None, symbols=None) is None
    assert _covered_days(cfg, DATASETS["daily_bars"]) == [
        date(2024, 1, 2),
        date(2024, 2, 1),
        date(2024, 3, 1),
    ]
    con = views_connection(cfg)
    try:
        _configure_sql_connection(con, cfg)
        assert con.execute("SELECT count(*) FROM daily_bars").fetchone()[0] == 4
    finally:
        con.close()


@pytest.fixture
def publish_to_catalog(monkeypatch):
    from cnequity.storage.catalog_bridge import PUBLISH_SWITCH

    monkeypatch.setenv(PUBLISH_SWITCH, "daily_bars")


def _pointer(cfg):
    return RevisionStore(cfg.meta_root, cfg.curated_root).current_pointer("daily_bars")


def test_a_commit_publishes_into_the_catalog_without_a_generation(
    cfg, lake, catalog_reads, publish_to_catalog
):
    from cnequity.query import load

    sync_dataset(cfg, lake, "daily_bars")
    first = _pointer(cfg)["revision_id"]
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))

    pointer = _pointer(cfg)
    assert pointer["storage"] == "catalog" and pointer["revision_id"] != first
    assert not (cfg.meta_root / pointer["generation_path"]).exists()
    catalog_reads.close_all()
    assert catalog_is_current(cfg, lake, "daily_bars")
    rows = load("daily_bars", config=cfg)
    assert rows.height == 4 and rows["trade_date"].max() == date(2024, 3, 1)
    # The revision it was built on stays readable from its snapshot.
    assert catalog_reads.published_files(cfg, "daily_bars", first) is not None


def test_a_catalog_commit_never_publishes_an_undeclared_partition(
    cfg, lake, catalog_reads, publish_to_catalog
):
    sync_dataset(cfg, lake, "daily_bars")
    # Bytes an interrupted run left in January, which this commit does not declare.
    stray = cfg.curated_root / "daily_bars/trade_date=2024-01/part-stray.parquet"
    _rows(date(2024, 1, 3), {"300750.SZ": 99.0}).write_parquet(stray)
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))
    symbols = {r[0] for r in lake.execute('SELECT symbol FROM "daily_bars"').fetchall()}
    assert "300750.SZ" not in symbols


def test_a_commit_falls_back_to_a_generation_when_the_catalog_is_behind(
    cfg, lake, publish_to_catalog
):
    # No sync: the catalog does not hold the revision this commit builds on.
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))
    pointer = _pointer(cfg)
    assert "storage" not in pointer
    assert (cfg.meta_root / pointer["generation_path"]).is_dir()


def test_sweep_moves_the_pointer_to_what_the_catalog_published(
    cfg, lake, catalog_reads, publish_to_catalog, monkeypatch
):
    from cnequity.storage import revisions

    sync_dataset(cfg, lake, "daily_bars")
    before = _pointer(cfg)
    real = revisions.write_json_atomic

    def lose_the_pointer(path, *args, **kwargs):
        if Path(path).name == "current.json":
            raise RuntimeError("crash after the catalog commit")
        return real(path, *args, **kwargs)

    monkeypatch.setattr(revisions, "write_json_atomic", lose_the_pointer)
    with pytest.raises(RuntimeError, match="crash"):
        _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))
    monkeypatch.setattr(revisions, "write_json_atomic", real)
    assert _pointer(cfg)["revision_id"] == before["revision_id"]

    report = RevisionStore(cfg.meta_root, cfg.curated_root).sweep(apply=True)
    assert report["pointer_restored"] == ["daily_bars"]
    after = _pointer(cfg)
    assert after["revision"] == before["revision"] + 1 and after["storage"] == "catalog"
    catalog_reads.close_all()
    assert catalog_is_current(cfg, lake, "daily_bars")


def test_retention_keeps_recent_and_held_revisions_and_deletes_after_the_grace(
    cfg, lake, catalog_reads, publish_to_catalog
):
    from cnequity.storage.catalog_bridge import apply_retention

    sync_dataset(cfg, lake, "daily_bars")
    revisions = [_pointer(cfg)["revision_id"]]
    for month in range(3, 9):
        _commit_month(
            cfg, f"2024-{month:02d}", _rows(date(2024, month, 1), {"600000.SH": 1.0 * month})
        )
        revisions.append(_pointer(cfg)["revision_id"])
    catalog_reads.close_all()
    oldest, held, recent = revisions[0], revisions[1], revisions[-2]

    preview = apply_retention(lake, keep=2, held={f"revision/daily_bars/{held}"}, apply=False)
    assert preview.snapshots_expired > 0 and preview.files_deleted == 0
    assert (
        catalog_reads.published_files(cfg, "daily_bars", oldest) is not None
    )  # nothing changed yet

    # Expired, but their files wait out the grace period.
    kept = apply_retention(lake, keep=2, held={f"revision/daily_bars/{held}"}, apply=True)
    assert kept.files_deleted == 0
    gone = apply_retention(
        lake, keep=2, held={f"revision/daily_bars/{held}"}, apply=True, grace_hours=-1
    )
    assert gone.files_deleted > 0
    catalog_reads.close_all()
    assert catalog_reads.published_files(cfg, "daily_bars", oldest) is None
    assert catalog_reads.published_files(cfg, "daily_bars", held) is not None
    assert catalog_reads.published_files(cfg, "daily_bars", recent) is not None
    assert catalog_reads.published_files(cfg, "daily_bars", revisions[-1]) is not None


def test_a_snapshot_of_a_catalog_only_revision_restores_as_a_generation(
    cfg, lake, catalog_reads, publish_to_catalog, tmp_path, monkeypatch
):
    from cnequity.query import load
    from cnequity.storage.catalog_bridge import PUBLISH_SWITCH
    from cnequity.storage.snapshots import SnapshotStore

    sync_dataset(cfg, lake, "daily_bars")
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))
    catalog_reads.close_all()
    expected = load("daily_bars", config=cfg)

    store = SnapshotStore(cfg)
    store.create("native", ["daily_bars"])
    assert store.verify("native").passed
    assert not (store.path("native") / ".export").exists()

    monkeypatch.delenv(PUBLISH_SWITCH)
    restored = Config(data_root=store.restore("native", tmp_path / "restored"))
    pointer = RevisionStore(restored.meta_root, restored.curated_root).current_pointer("daily_bars")
    assert "storage" not in pointer and pointer["revision"] == _pointer(cfg)["revision"]
    assert load("daily_bars", config=restored).equals(expected)


def test_a_catalog_only_revision_can_be_held_and_retention_keeps_it(
    cfg, lake, catalog_reads, publish_to_catalog
):
    from cnequity.storage.catalog_bridge import apply_retention
    from cnequity.storage.lifecycle import LifecycleStore
    from cnequity.storage.lifecycle.resources import resource_exists

    sync_dataset(cfg, lake, "daily_bars")
    _commit_month(cfg, "2024-03", _rows(date(2024, 3, 1), {"600000.SH": 12.0}, pre_close=11.5))
    held = _pointer(cfg)["revision_id"]
    for month in range(4, 8):
        _commit_month(
            cfg, f"2024-{month:02d}", _rows(date(2024, month, 1), {"600000.SH": 1.0 * month})
        )
    assert resource_exists(cfg.meta_root, f"revision/daily_bars/{held}")
    assert not resource_exists(cfg.meta_root, "revision/daily_bars/" + "0" * 32)
    from cnequity.storage.lifecycle import reference_fingerprint

    store = LifecycleStore(cfg.meta_root)
    evidence = cfg.data_root.parent / "evidence"
    evidence.mkdir()
    roots = [{"path": str(evidence)}]
    store.import_manifest(
        {
            "schema_version": 1,
            "meta_root": str(store.meta),
            "holds": {},
            "reference_roots": roots,
            "reference_fingerprint": reference_fingerprint(roots),
        }
    )
    store.hold(f"revision/daily_bars/{held}", "paper figures")

    from cnequity.storage.catalog_bridge import catalog_retention

    result = catalog_retention(cfg, apply=True)
    assert result["snapshots_expired"] > 0
    apply_retention(lake, keep=1, held={f"revision/daily_bars/{held}"}, apply=True, grace_hours=-1)
    catalog_reads.close_all()
    assert catalog_reads.published_files(cfg, "daily_bars", held) is not None
