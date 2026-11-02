"""A write touches the files its rows belong to, not the whole partition.

Every partition used to be read whole, merged and rewritten as one file, so
forty symbols' month of daily bars rewrote 159,192 rows to add 840. New keys
now land in a file of their own, an update rewrites only the file holding its
key, revisions link what did not change, and `cne run maintain` folds the
extra files back in.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.query.parquet_scan import collect_parquet_root
from cnequity.storage.merge_files import merge_candidates, merge_dataset_files
from cnequity.storage.parquet import (
    MAX_PARTITION_FILES,
    MERGED_FILE,
    StagingWriter,
    compact_dataset,
    write_partition_rows,
)
from cnequity.storage.revisions import RevisionStore

_DATASET = "index_bars"
_YEAR = "trade_date=2024"


def _bars(
    symbols: list[str],
    day: date = date(2024, 6, 3),
    close: float = 1.0,
    fetched_at: str = "2026-07-21T00:00:00+00:00",
) -> pl.DataFrame:
    n = len(symbols)
    return pl.DataFrame(
        {
            "symbol": symbols,
            "trade_date": [day] * n,
            "open": [close] * n,
            "high": [close] * n,
            "low": [close] * n,
            "close": [close] * n,
            "volume": [1] * n,
            "amount": [1.0] * n,
            "frequency": ["d"] * n,
            "source": ["tdx"] * n,
            "data_version": ["v1"] * n,
            "fetched_at": [fetched_at] * n,
        }
    ).with_columns(pl.col("fetched_at").str.to_datetime(time_unit="us", time_zone="UTC"))


def _compact(cfg: Config, run_id: str, frame: pl.DataFrame) -> list:
    StagingWriter(cfg.staging_root).write_batch(_DATASET, run_id, "batch-0", frame)
    changed: list = []
    compact_dataset(cfg.staging_root, cfg.curated_root, _DATASET, run_id, changed_files=changed)
    return changed


def _stamp(path):
    info = path.stat()
    return info.st_ino, info.st_mtime_ns


def test_new_keys_land_in_a_new_file_and_leave_the_old_one_untouched(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    part = cfg.curated_root / _DATASET / _YEAR
    _compact(cfg, "run-1", _bars(["000001.SH", "000016.SH"]))
    base = part / MERGED_FILE
    before = _stamp(base)

    changed = _compact(cfg, "run-2", _bars(["000300.SH"]))

    assert _stamp(base) == before  # never read in full, never rewritten
    assert len(changed) == 1 and changed[0] != base
    rows = collect_parquet_root(cfg.curated_root / _DATASET, partition_col="trade_date")
    assert sorted(rows["symbol"].to_list()) == ["000001.SH", "000016.SH", "000300.SH"]


def test_an_update_rewrites_only_the_file_holding_its_key(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    part = cfg.curated_root / _DATASET / _YEAR
    _compact(cfg, "run-1", _bars(["000001.SH"]))
    _compact(cfg, "run-2", _bars(["000300.SH"]))
    other = next(path for path in part.glob("*.parquet") if path.name != MERGED_FILE)
    before = _stamp(other)

    _compact(cfg, "run-3", _bars(["000001.SH"], close=2.0, fetched_at="2026-07-22T00:00:00+00:00"))

    assert _stamp(other) == before
    merged = pl.read_parquet(part / MERGED_FILE)
    assert merged["close"].to_list() == [2.0]


def test_an_unchanged_refetch_writes_nothing(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _compact(cfg, "run-1", _bars(["000001.SH"]))
    assert _compact(cfg, "run-2", _bars(["000001.SH"])) == []


def test_a_key_split_across_files_is_repaired_when_its_partition_is_written(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    part = cfg.curated_root / _DATASET / _YEAR
    part.mkdir(parents=True)
    _bars(["000001.SH"], close=1.0).write_parquet(part / MERGED_FILE)
    _bars(["000001.SH"], close=1.0).write_parquet(part / "part-legacy.parquet")

    _compact(cfg, "run-1", _bars(["000300.SH"]))

    rows = collect_parquet_root(cfg.curated_root / _DATASET, partition_col="trade_date")
    assert sorted(rows["symbol"].to_list()) == ["000001.SH", "000300.SH"]
    assert [path.name for path in part.glob("*.parquet")] == [MERGED_FILE]


def test_a_partition_past_the_file_cap_folds_its_small_files(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    part = cfg.curated_root / _DATASET / _YEAR
    _compact(cfg, "run-0", _bars([f"{i:06d}.SH" for i in range(50)]))
    base = part / MERGED_FILE
    before = _stamp(base)
    for i in range(MAX_PARTITION_FILES + 2):
        _compact(cfg, f"run-{i + 1}", _bars([f"{900 + i:06d}.SH"]))

    files = list(part.glob("*.parquet"))
    assert len(files) <= MAX_PARTITION_FILES
    assert _stamp(base) == before  # the big file is never part of the fold
    rows = collect_parquet_root(cfg.curated_root / _DATASET, partition_col="trade_date")
    assert rows.height == 50 + MAX_PARTITION_FILES + 2


def test_replace_by_drops_stored_rows_the_recompute_no_longer_has(tmp_path):
    part = tmp_path / "part"
    part.mkdir()
    pl.DataFrame({"symbol": ["a", "b"], "day": [1, 1], "v": [1, 1]}).write_parquet(
        part / MERGED_FILE
    )
    pl.DataFrame({"symbol": ["a"], "day": [2], "v": [1]}).write_parquet(part / "part-x.parquet")

    write_partition_rows(
        pl.DataFrame({"symbol": ["a"], "day": [1], "v": [9]}),
        "industry_index",  # any dataset; the group keys are passed through replace_by
        read_dir=part,
        out_dir=part,
        load=pl.read_parquet,
        dedupe=lambda frame: frame.unique(subset=["symbol", "day"], keep="last"),
        replace_by=["day"],
        incoming_first=False,
    )

    rows = pl.concat([pl.read_parquet(p) for p in part.glob("*.parquet")]).sort("day")
    assert rows.rows() == [("a", 1, 9), ("a", 2, 1)]  # "b" left day 1 with the recompute


def _publish(cfg: Config, store: RevisionStore, run_id: str, frame: pl.DataFrame):
    store.ensure_current(_DATASET)
    store.materialize_current(_DATASET)
    changed = _compact(cfg, run_id, frame)
    if not changed:
        return None
    return store.commit(
        _DATASET,
        run_id=run_id,
        changed_files=changed,
        schema_version=1,
        contract_fingerprint="test",
    )


def test_a_new_generation_links_the_files_a_write_did_not_touch(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    _publish(cfg, store, "run-1", _bars([f"{i:06d}.SH" for i in range(20)]))
    first = store.current_root(_DATASET) / _YEAR / MERGED_FILE

    _publish(cfg, store, "run-2", _bars(["999999.SH"]))
    second = store.current_root(_DATASET) / _YEAR / MERGED_FILE

    assert first != second
    assert first.stat().st_ino == second.stat().st_ino  # linked, not copied
    assert len(list((store.current_root(_DATASET) / _YEAR).glob("*.parquet"))) == 2


def test_materialize_copies_only_what_differs(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    _publish(cfg, store, "run-1", _bars(["000001.SH"]))
    _publish(cfg, store, "run-2", _bars(["000002.SH"], day=date(2025, 1, 6)))
    mutable = cfg.curated_root / _DATASET
    kept = mutable / _YEAR / MERGED_FILE
    before = _stamp(kept)
    stray = mutable / _YEAR / "part-interrupted.parquet"
    _bars(["000003.SH"]).write_parquet(stray)

    store.materialize_current(_DATASET)

    assert _stamp(kept) == before
    assert not stray.exists()


def test_maintain_merges_a_partition_with_many_files(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    _publish(cfg, store, "run-0", _bars(["000000.SH"]))
    for i in range(1, 5):
        _publish(cfg, store, f"run-{i}", _bars([f"{i:06d}.SH"]))
    assert merge_candidates(cfg, _DATASET)

    plan = merge_dataset_files(cfg, _DATASET)
    assert plan["applied"] is False and plan["files_before"] == 5

    result = merge_dataset_files(cfg, _DATASET, apply=True)

    assert result["applied"] is True
    current = store.current_root(_DATASET) / _YEAR
    assert [path.name for path in current.glob("*.parquet")] == [MERGED_FILE]
    assert pl.read_parquet(current / MERGED_FILE).height == 5
    assert not merge_candidates(cfg, _DATASET)


def test_a_merge_audits_only_when_it_changes_rows(tmp_path, monkeypatch):
    """Same rows in fewer files skip the whole-lake audit; a collapsed duplicate does not."""
    import cnequity.quality.publication as publication

    audited: list[str] = []

    def gate(config, dataset, run_id, changed, **kwargs):
        audited.append(dataset)
        return {"report_path": None}

    monkeypatch.setattr(publication, "check_repair_publication", gate)
    cfg = Config(data_root=tmp_path / "data")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    for i in range(4):
        _publish(cfg, store, f"run-{i}", _bars([f"{i:06d}.SH"]))
    assert merge_dataset_files(cfg, _DATASET, apply=True)["content_changed"] is False
    assert audited == []

    store.materialize_current(_DATASET)
    part = cfg.curated_root / _DATASET / _YEAR
    for i in range(4):
        _bars(["000000.SH"]).write_parquet(part / f"part-dup{i}.parquet")
    store.commit(
        _DATASET,
        run_id="dup",
        changed_files=sorted(part.glob("part-dup*.parquet")),
        schema_version=1,
        contract_fingerprint="test",
    )
    assert merge_dataset_files(cfg, _DATASET, apply=True)["content_changed"] is True
    assert audited == [_DATASET]


def test_a_large_partition_is_bucketed_and_an_update_rewrites_one_bucket(tmp_path, monkeypatch):
    import cnequity.storage.parquet as storage

    monkeypatch.setattr(storage, "BUCKET_ROWS", 10)
    cfg = Config(data_root=tmp_path / "data")
    part = cfg.curated_root / _DATASET / _YEAR
    symbols = [f"{i:06d}.SH" for i in range(50)]
    _compact(cfg, "run-1", _bars(symbols))
    files = sorted(part.glob("*.parquet"))
    assert len(files) > 1 and all(storage.parse_bucket(p.name) for p in files)
    stamps = {p.name: _stamp(p) for p in files}

    _compact(cfg, "run-2", _bars(["000007.SH"], close=3.0, fetched_at="2026-07-22T00:00:00+00:00"))

    moved = [p.name for p in part.glob("*.parquet") if _stamp(p) != stamps.get(p.name)]
    assert len(moved) == 1  # one bucket, not the partition
    rows = collect_parquet_root(cfg.curated_root / _DATASET, partition_col="trade_date")
    assert rows.height == 50
    assert rows.filter(pl.col("symbol") == "000007.SH")["close"].to_list() == [3.0]


def test_a_merge_folds_appended_rows_into_their_buckets_only(tmp_path, monkeypatch):
    import cnequity.storage.parquet as storage

    monkeypatch.setattr(storage, "BUCKET_ROWS", 10)
    cfg = Config(data_root=tmp_path / "data")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    _publish(cfg, store, "run-0", _bars([f"{i:06d}.SH" for i in range(50)]))
    for i in range(3):
        _publish(cfg, store, f"run-{i + 1}", _bars([f"{900 + i:06d}.SH"]))
    current = store.current_root(_DATASET) / _YEAR
    before = {p.name: _stamp(p) for p in current.glob("*.parquet")}
    assert sum(not storage.is_base_file(name) for name in before) == 3

    merge_dataset_files(cfg, _DATASET, apply=True)

    current = store.current_root(_DATASET) / _YEAR
    after = {p.name: p for p in current.glob("*.parquet")}
    assert all(storage.parse_bucket(name) for name in after)
    untouched = [name for name, path in after.items() if before.get(name) == _stamp(path)]
    assert untouched  # buckets the appended rows did not belong to were not rewritten
    rows = collect_parquet_root(cfg.curated_root / _DATASET, partition_col="trade_date")
    assert rows.height == 53


def test_views_are_rebuilt_only_when_a_pointer_moves(tmp_path):
    from cnequity.query.views import ensure_duckdb_views

    cfg = Config(data_root=tmp_path / "data")
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    _publish(cfg, store, "run-1", _bars(["000001.SH"]))
    db = ensure_duckdb_views(cfg)
    built = _stamp(db)

    assert _stamp(ensure_duckdb_views(cfg)) == built  # nothing moved

    _publish(cfg, store, "run-2", _bars(["000002.SH"]))
    db = ensure_duckdb_views(cfg)  # only index_bars is redefined
    assert _stamp(db) != built
    import duckdb

    with duckdb.connect(str(db), read_only=True) as con:
        assert con.execute("SELECT count(*) FROM index_bars").fetchone() == (2,)
        assert con.execute("SELECT count(*) FROM daily_bars").fetchone() == (0,)
    incremental = _stamp(db)
    assert _stamp(ensure_duckdb_views(cfg, force=True)) != incremental


def test_partition_answers_are_recomputed_only_where_files_changed(tmp_path):
    from cnequity.storage.partition_cache import cached_partition_values

    root = tmp_path / "root"
    for part in ("d=1", "d=2"):
        (root / part).mkdir(parents=True)
        pl.DataFrame({"x": [1]}).write_parquet(root / part / "f.parquet")
    calls: list[str] = []

    def compute(files):
        calls.append(files[0].parent.name)
        return [files[0].parent.name]

    def parts():
        return {p.name: sorted(p.glob("*.parquet")) for p in sorted(root.iterdir())}

    assert cached_partition_values(tmp_path, root, "q", parts(), compute) == {
        "d=1": ["d=1"],
        "d=2": ["d=2"],
    }
    cached_partition_values(tmp_path, root, "q", parts(), compute)
    pl.DataFrame({"x": [1, 2]}).write_parquet(root / "d=2" / "f.parquet")
    cached_partition_values(tmp_path, root, "q", parts(), compute)
    assert calls == ["d=1", "d=2", "d=2"]


def test_the_bj_observation_receipt_is_rebuilt_only_when_its_inputs_move(tmp_path, monkeypatch):
    import cnequity.quality.st_coverage as st

    cfg = Config(data_root=tmp_path / "data")
    receipt = tmp_path / "receipt.json"
    receipt.write_text("{}")
    inputs = {"instruments": "a", "daily_bars": "b", "trading_status": "c"}
    built: list[int] = []
    monkeypatch.setattr(st, "_bse_observation_inputs", lambda config, universe: dict(inputs))

    def publish(config, universe):
        built.append(1)
        return receipt

    monkeypatch.setattr(st, "_publish_bse_st_observation_receipt", publish)
    assert st.publish_bse_st_observation_receipt(cfg) == receipt
    assert st.publish_bse_st_observation_receipt(cfg) == receipt
    inputs["trading_status"] = "d"
    st.publish_bse_st_observation_receipt(cfg)
    assert len(built) == 2
