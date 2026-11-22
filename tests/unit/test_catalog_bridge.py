"""Copying a committed generation into the lake catalog, and noticing when the copies differ."""

from __future__ import annotations

from datetime import date, datetime, timezone
from multiprocessing import get_context
from pathlib import Path

import polars as pl
import pytest
from tests.legacy_lake import commit_generation

from cnequity.config.loader import Config
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
    commit_generation(
        RevisionStore(config.meta_root, config.curated_root),
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
    commit_generation(
        RevisionStore(cfg.meta_root, cfg.curated_root),
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

    yield catalog_read
    catalog_read.close_all()


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


def _pointer(cfg):
    return RevisionStore(cfg.meta_root, cfg.curated_root).current_pointer("daily_bars")


def test_retention_keeps_recent_and_held_revisions_and_deletes_after_the_grace(
    cfg, lake, catalog_reads
):
    from cnequity.storage.catalog_bridge import apply_retention

    sync_dataset(cfg, lake, "daily_bars")
    revisions = [_pointer(cfg)["revision_id"]]
    for month in range(3, 9):
        _commit_month(
            cfg, f"2024-{month:02d}", _rows(date(2024, month, 1), {"600000.SH": 1.0 * month})
        )
        sync_dataset(cfg, lake, "daily_bars")
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
    # Each revision here only added a month's file, so no file was replaced
    # and expiring the old snapshots frees none.
    assert gone.files_deleted == 0
    catalog_reads.close_all()
    assert catalog_reads.published_files(cfg, "daily_bars", oldest) is None
    assert catalog_reads.published_files(cfg, "daily_bars", held) is not None
    assert catalog_reads.published_files(cfg, "daily_bars", recent) is not None
    assert catalog_reads.published_files(cfg, "daily_bars", revisions[-1]) is not None


def _stray(lake, name: str, *, days_old: float) -> Path:
    import os
    import time

    path = lake.table_dir("daily_bars") / "2024-01" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    _rows(date(2024, 1, 2), {"600000.SH": 99.0}).write_parquet(path)
    stamp = time.time() - days_old * 86400
    os.utime(path, (stamp, stamp))
    return path


def test_retention_reads_holds_after_entering_the_writer_lock(tmp_path, monkeypatch):
    from contextlib import contextmanager

    from tests.catalog_seed import publish_rows

    from cnequity import file_lock
    from cnequity.storage.catalog_bridge import catalog_retention, revision_snapshot
    from cnequity.storage.lifecycle import LifecycleStore

    config = Config(data_root=tmp_path, revision_keep_generations=1)
    day = date(2024, 1, 2)
    older = publish_rows(config, "daily_bars", _rows(day, {"600000.SH": 10.0}), run_id="older")
    publish_rows(config, "daily_bars", _rows(day, {"600000.SH": 11.0}), run_id="newer")
    assert older is not None
    store = LifecycleStore(config.meta_root)
    store._save(
        {
            "schema_version": 1,
            "meta_root": str(store.meta),
            "lake_id": "test-lake",
            "holds": {},
        }
    )
    lock = file_lock.lake_mutation_lock

    @contextmanager
    def hold_before_retention_acquires(*args, **kwargs):
        store.hold(f"revision/daily_bars/{older.revision_id}", "keep historical revision")
        with lock(*args, **kwargs):
            yield

    monkeypatch.setattr(file_lock, "lake_mutation_lock", hold_before_retention_acquires)
    catalog_retention(config, apply=True)
    with Catalog(lake_root(config), read_only=True) as catalog:
        version, _ = revision_snapshot(catalog, "daily_bars", older.revision_id)
        assert catalog.execute(
            f"SELECT close FROM daily_bars AT (VERSION => {version})"
        ).fetchall() == [(10.0,)]


def test_catalog_retention_ignores_a_note_citing_a_revision_and_keeps_a_hold(tmp_path):
    from tests.catalog_seed import publish_rows

    from cnequity.storage.catalog_bridge import catalog_retention, revision_snapshot
    from cnequity.storage.lifecycle import LifecycleStore

    config = Config(data_root=tmp_path / "lake", revision_keep_generations=1)
    day = date(2024, 1, 2)
    cited = publish_rows(config, "daily_bars", _rows(day, {"600000.SH": 10.0}), run_id="a")
    held = publish_rows(config, "daily_bars", _rows(day, {"600000.SH": 11.0}), run_id="b")
    publish_rows(config, "daily_bars", _rows(day, {"600000.SH": 12.0}), run_id="c")
    notes = config.meta_root / "research"
    notes.mkdir()
    (notes / "report.md").write_text(f"revision {cited.revision_id}", encoding="utf-8")
    LifecycleStore(config.meta_root).hold(
        f"revision/daily_bars/{held.revision_id}", "kept for review", until=date.today()
    )
    catalog_retention(config, apply=True)
    with Catalog(lake_root(config), read_only=True) as catalog:
        live = {
            int(row[0])
            for row in catalog.execute(
                "SELECT snapshot_id FROM ducklake_snapshots('lake')"
            ).fetchall()
        }
        assert revision_snapshot(catalog, "daily_bars", cited.revision_id)[0] not in live
        version, _ = revision_snapshot(catalog, "daily_bars", held.revision_id)
        assert catalog.execute(
            f"SELECT close FROM daily_bars AT (VERSION => {version})"
        ).fetchall() == [(11.0,)]


def _read_pinned_snapshot(root, channel):
    with channel, Catalog(root, read_only=True) as reader, reader.pin_snapshot() as held:
        channel.send(held)
        if channel.poll(30):
            channel.recv()
            rows = reader.execute(f"SELECT close FROM daily_bars AT (VERSION => {held})").fetchall()
            channel.send(rows)


def test_retention_skips_an_active_snapshot_while_publication_can_continue(tmp_path):
    from tests.catalog_seed import publish_rows

    from cnequity.storage.catalog_bridge import catalog_retention
    from equitylake_core.lock import lake_read

    config = Config(data_root=tmp_path, revision_keep_generations=1)
    day = date(2024, 1, 2)
    root = lake_root(config)
    publish_rows(config, "daily_bars", _rows(day, {"600000.SH": 10.0}), run_id="first")
    ctx = get_context("spawn")
    parent, child = ctx.Pipe()
    process = ctx.Process(target=_read_pinned_snapshot, args=(root, child))
    process.start()
    child.close()
    try:
        with parent:
            assert parent.poll(20)
            held = parent.recv()
            publish_rows(config, "daily_bars", _rows(day, {"600000.SH": 11.0}), run_id="second")
            with lake_read(root):
                outcome = catalog_retention(config, apply=True)
                assert outcome["active_readers"] and not outcome["applied"]
            outcome = catalog_retention(config, apply=True)
            assert outcome["active_readers"] and outcome["applied"]
            parent.send("read")
            assert parent.poll(20)
            assert parent.recv() == [(10.0,)]
    finally:
        process.join(5)
        if process.is_alive():
            process.terminate()
            process.join(5)
    assert process.exitcode == 0
    catalog_retention(config, apply=True)
    with Catalog(root, read_only=True) as reader:
        from equitylake_core.catalog import CatalogError

        with (
            pytest.raises(CatalogError, match="snapshot .* has expired"),
            reader.pin_snapshot(held),
        ):
            pytest.fail("retention should expire the snapshot after the reader exits")


def test_zero_retention_never_applies_catalog_cleanup(cfg, lake, monkeypatch):
    from cnequity.storage import catalog_bridge

    sync_dataset(cfg, lake, "daily_bars")
    old = _stray(lake, "left-by-a-crash.parquet", days_old=3)
    lake.close()
    cfg.revision_keep_generations = 0

    def refused(*args, **kwargs):
        pytest.fail("disabled retention attempted to delete orphans")

    monkeypatch.setattr(catalog_bridge, "delete_orphans", refused)
    preview = catalog_bridge.catalog_retention(cfg, apply=False)
    assert catalog_bridge.catalog_retention(cfg, apply=True) == preview
    assert preview["keep_revisions"] == 0
    assert preview["snapshots_expired"] == preview["files_deleted"] == 0
    assert not preview["applied"]
    assert old.exists()


def test_retention_deletes_files_no_snapshot_registers_once_past_the_grace(cfg, lake):
    from cnequity.storage.catalog_bridge import catalog_retention

    sync_dataset(cfg, lake, "daily_bars")
    registered = {
        Path(row[0])
        for row in lake.execute(
            "SELECT data_file FROM ducklake_list_files('lake', 'daily_bars')"
        ).fetchall()
    }
    old = _stray(lake, "left-by-a-crash.parquet", days_old=3)
    fresh = _stray(lake, "placed-just-now.parquet", days_old=0)
    lake.close()

    # Before the lake switches, the migration links old files in without the lock.
    assert catalog_retention(cfg, apply=True)["orphans_deleted"] == 0
    assert old.exists()

    (cfg.meta_root / "storage.json").write_text('{"publish": "catalog"}', encoding="utf-8")
    assert catalog_retention(cfg, apply=False)["orphans_deleted"] == 0
    result = catalog_retention(cfg, apply=True)
    assert result["orphans_deleted"] == 1
    assert not old.exists() and fresh.exists()
    assert all(path.exists() for path in registered)


def _factors(day: date, factor: float) -> pl.DataFrame:
    from cnequity.domain.schemas import ADJ_FACTORS_SCHEMA

    return pl.DataFrame(
        [
            {
                "symbol": "600000.SH",
                "trade_date": day,
                "adjust_type": "hfq",
                "factor": factor,
                "source": "test",
                "data_version": "v1",
                "fetched_at": FETCHED,
            }
        ],
        schema=ADJ_FACTORS_SCHEMA,
    )


def _factor_at(root, revision_id: str) -> tuple[int, list]:
    from cnequity.storage.catalog_bridge import revision_snapshot

    with Catalog(root, read_only=True) as catalog:
        version, _ = revision_snapshot(catalog, "adj_factors", revision_id)
        rows = catalog.execute(f"SELECT factor FROM adj_factors AT (VERSION => {version})")
        return version, rows.fetchall()


def _live(root) -> set[int]:
    with Catalog(root, read_only=True) as catalog:
        rows = catalog.execute("SELECT snapshot_id FROM ducklake_snapshots('lake')").fetchall()
    return {int(row[0]) for row in rows}


def test_retention_keeps_no_old_snapshot_for_a_dataset_that_has_not_changed(tmp_path):
    from tests.catalog_seed import publish_rows

    from cnequity.storage.catalog_bridge import apply_retention, plan_retention

    config = Config(data_root=tmp_path)
    root = lake_root(config)
    day = date(2024, 1, 2)
    publish_rows(config, "daily_bars", _rows(day, {"600000.SH": 10.0}), run_id="bars-0")
    factors = publish_rows(config, "adj_factors", _factors(day, 1.0), run_id="factors")
    for close in (11.0, 12.0, 13.0):
        publish_rows(config, "daily_bars", _rows(day, {"600000.SH": close}), run_id=f"{close}")
    recorded, _ = _factor_at(root, factors.revision_id)

    with Catalog(root) as catalog:
        # Each dataset keeps only its current revision: the latest snapshot holds both.
        assert plan_retention(catalog, keep=1).keep == {catalog.snapshot()}
        preview = apply_retention(catalog, keep=1)
        assert preview.bytes_freed > 0 and preview.snapshots_expired > 0
        result = apply_retention(catalog, keep=1, apply=True, grace_hours=-1)
    assert result.applied and result.files_deleted > 0
    assert recorded not in _live(root)
    version, rows = _factor_at(root, factors.revision_id)
    assert version != recorded and rows == [(1.0,)]


def test_retention_moves_a_kept_revision_to_a_later_snapshot_with_the_same_rows(tmp_path):
    from tests.catalog_seed import publish_rows

    from cnequity.storage.catalog_bridge import apply_retention

    config = Config(data_root=tmp_path)
    root = lake_root(config)
    day = date(2024, 1, 2)
    publish_rows(config, "daily_bars", _rows(day, {"600000.SH": 10.0}), run_id="bars-0")
    first = publish_rows(config, "adj_factors", _factors(day, 1.0), run_id="f1")
    first_recorded, _ = _factor_at(root, first.revision_id)
    publish_rows(config, "daily_bars", _rows(day, {"600000.SH": 11.0}), run_id="bars-1")
    second = publish_rows(config, "adj_factors", _factors(day, 2.0), run_id="f2")
    publish_rows(config, "daily_bars", _rows(day, {"600000.SH": 12.0}), run_id="bars-2")

    with Catalog(root) as catalog:
        assert apply_retention(catalog, keep=2, apply=True).applied
    assert first_recorded not in _live(root)
    assert _factor_at(root, first.revision_id)[1] == [(1.0,)]

    # The revision that was current when retention ran stays readable once replaced.
    publish_rows(config, "adj_factors", _factors(day, 3.0), run_id="f3")
    assert _factor_at(root, second.revision_id)[1] == [(2.0,)]
    with Catalog(root) as catalog:
        assert apply_retention(catalog, keep=2, apply=True).applied
    assert _factor_at(root, second.revision_id)[1] == [(2.0,)]
    with Catalog(root, read_only=True) as catalog:
        from cnequity.storage.catalog_bridge import revision_snapshot

        # Out of the newest two: no longer readable, and no longer pointed at.
        version, _ = revision_snapshot(catalog, "adj_factors", first.revision_id)
    assert version not in _live(root)


def test_retention_keeps_a_replaced_revision_only_within_the_days_unless_held(tmp_path):
    import time

    from tests.catalog_seed import publish_rows

    from cnequity.storage.catalog_bridge import plan_retention

    config = Config(data_root=tmp_path)
    root = lake_root(config)
    day = date(2024, 1, 2)
    publish_rows(config, "daily_bars", _rows(day, {"600000.SH": 10.0}), run_id="bars")
    first = publish_rows(config, "adj_factors", _factors(day, 1.0), run_id="f1")
    publish_rows(config, "adj_factors", _factors(day, 2.0), run_id="f2")
    first_recorded, _ = _factor_at(root, first.revision_id)
    later = time.time() + 3 * 86400
    held = {f"revision/adj_factors/{first.revision_id}"}
    with Catalog(root, read_only=True) as catalog:
        assert first_recorded in plan_retention(catalog, keep=5, keep_days=7, now=later).keep
        assert first_recorded not in plan_retention(catalog, keep=5, keep_days=1, now=later).keep
        assert first_recorded in plan_retention(catalog, keep=5, keep_days=0, now=later).keep
        assert (
            first_recorded
            in plan_retention(catalog, keep=5, held=held, keep_days=1, now=later).keep
        )
