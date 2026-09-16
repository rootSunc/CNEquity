from datetime import date

import polars as pl

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.query import dataset_state
from cnequity.steps.finalize import step_compact
from cnequity.storage import StagingWriter, compact_dataset


def test_compact_only_merges_datasets_staged_in_run(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    run_id = "run-compact-test"
    writer = StagingWriter(cfg.staging_root)

    writer.write_batch(
        "fund_flow",
        run_id,
        "batch-0",
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 28)],
                "main_net_inflow": [1.0],
                "super_large_net_inflow": [0.0],
                "large_net_inflow": [0.0],
                "medium_net_inflow": [0.0],
                "small_net_inflow": [0.0],
                "source": ["eastmoney"],
                "data_version": ["v1"],
                "fetched_at": ["2024-06-28T00:00:00+00:00"],
            }
        ),
    )

    out = step_compact(cfg, date(2024, 6, 28), run_id, {})
    assert out["rows_written"] == 1
    curated = cfg.curated_root / "fund_flow" / "trade_date=2024-06-28" / "part-merged.parquet"
    assert curated.exists()
    assert not (cfg.curated_root / "daily_bars").exists()
    assert out["dataset_revisions"]["fund_flow"]["revision"] == 1
    state = dataset_state("fund_flow", config=cfg)
    assert state.revision == 1
    assert state.revision_id == out["dataset_revisions"]["fund_flow"]["revision_id"]


def test_staging_writer_lists_nested_run_fragments(tmp_path):
    writer = StagingWriter(tmp_path / "staging")
    nested = tmp_path / "staging" / "fund_flow" / "run_id=run-nested" / ".recovered"
    nested.mkdir(parents=True)
    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "main_net_inflow": [1.0],
            "super_large_net_inflow": [0.0],
            "large_net_inflow": [0.0],
            "medium_net_inflow": [0.0],
            "small_net_inflow": [0.0],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    )
    frame.write_parquet(nested / "part-recovered.parquet")

    assert writer.list_run_files("fund_flow", "run-nested") == [nested / "part-recovered.parquet"]


def test_compact_removes_stale_partition_fragments(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    run_id = "run-fragment-cleanup"
    part = cfg.curated_root / "fund_flow" / "trade_date=2024-06-28"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "main_net_inflow": [1.0],
            "super_large_net_inflow": [0.0],
            "large_net_inflow": [0.0],
            "medium_net_inflow": [0.0],
            "small_net_inflow": [0.0],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-27T00:00:00+00:00"],
        }
    ).write_parquet(part / "part-old.parquet")
    nested = part / ".old-fragments"
    nested.mkdir()
    pl.read_parquet(part / "part-old.parquet").with_columns(
        pl.lit("000001.SZ").alias("symbol"),
        pl.lit(0.5).alias("main_net_inflow"),
    ).write_parquet(nested / "part-old.parquet")

    StagingWriter(cfg.staging_root).write_batch(
        "fund_flow",
        run_id,
        "batch-0",
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 28)],
                "main_net_inflow": [2.0],
                "super_large_net_inflow": [0.0],
                "large_net_inflow": [0.0],
                "medium_net_inflow": [0.0],
                "small_net_inflow": [0.0],
                "source": ["eastmoney"],
                "data_version": ["v1"],
                "fetched_at": ["2024-06-28T00:00:00+00:00"],
            }
        ),
    )

    step_compact(cfg, date(2024, 6, 28), run_id, {})

    assert [p.name for p in part.rglob("*.parquet")] == ["part-merged.parquet"]
    result = pl.read_parquet(part / "part-merged.parquet")
    assert set(result["symbol"]) == {"000001.SZ", "600519.SH"}
    assert result.filter(pl.col("symbol") == "600519.SH")["main_net_inflow"].to_list() == [2.0]


def test_compact_merge_style_dataset_preserves_prior_runs(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    dataset = "regulatory_events"
    first = "run-regulatory-1"
    second = "run-regulatory-2"

    def event(event_id: str, day: date) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "event_id": [event_id],
                "symbol": ["600519.SH"],
                "event_date": [day],
                "event_type": ["notice"],
                "title": [event_id],
                "source": ["cninfo"],
                "data_version": ["v1"],
                "fetched_at": [f"{day.isoformat()}T00:00:00+00:00"],
            }
        )

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(dataset, first, "batch-0", event("e-1", date(2024, 6, 27)))
    compact_dataset(
        cfg.staging_root,
        cfg.curated_root,
        dataset,
        first,
        partition_col=None,
    )

    canonical = cfg.curated_root / dataset / "part-merged.parquet"
    nested = canonical.parent / ".old-fragments"
    nested.mkdir()
    pl.read_parquet(canonical).write_parquet(nested / "part-old.parquet")

    writer.write_batch(dataset, second, "batch-0", event("e-2", date(2024, 6, 28)))
    compact_dataset(
        cfg.staging_root,
        cfg.curated_root,
        dataset,
        second,
        partition_col=None,
    )

    assert pl.read_parquet(canonical)["event_id"].sort().to_list() == ["e-1", "e-2"]
    assert [path.name for path in canonical.parent.rglob("*.parquet")] == ["part-merged.parquet"]


def test_unpartitioned_reconciliation_from_committed_root_is_physical_noop(tmp_path):
    import shutil

    cfg = Config(data_root=tmp_path)
    writer = StagingWriter(cfg.staging_root)
    frame = pl.DataFrame(
        {
            "exchange": ["SSE"],
            "trade_date": [date(2026, 9, 15)],
            "is_trading": [True],
            "source": ["calendar"],
            "data_version": ["v1"],
            "fetched_at": ["2026-09-15T00:00:00+00:00"],
        }
    )
    writer.write_batch("trading_calendar", "first", "one", frame)
    compact_dataset(
        cfg.staging_root, cfg.curated_root, "trading_calendar", "first", partition_col=None
    )
    canonical = cfg.curated_root / "trading_calendar/part-merged.parquet"
    base = tmp_path / "committed"
    shutil.copytree(canonical.parent, base)
    original = canonical.stat()
    writer.write_batch(
        "trading_calendar",
        "retry",
        "one",
        frame.with_columns(pl.lit("2026-09-16T00:00:00+00:00").alias("fetched_at")),
    )
    changed = []
    compact_dataset(
        cfg.staging_root,
        cfg.curated_root,
        "trading_calendar",
        "retry",
        partition_col=None,
        base_root=base,
        changed_files=changed,
    )
    assert changed == []
    assert canonical.stat().st_ino == original.st_ino
    assert canonical.stat().st_mtime_ns == original.st_mtime_ns


def test_business_comparison_preserves_values_types_and_provenance():
    from cnequity.storage.parquet import _business_equal

    a = pl.DataFrame(
        {
            "key": [1, 2],
            "value": [None, 2.0],
            "source": ["a", "b"],
            "available_at": ["2026-01-01"] * 2,
            "fetched_at": ["old"] * 2,
        }
    )
    b = a.reverse().with_columns(pl.lit("new").alias("fetched_at"))
    assert _business_equal(a, b)
    for column, value in [("source", "c"), ("value", 3.0), ("available_at", "2026-01-02")]:
        assert not _business_equal(a, b.with_columns(pl.lit(value).alias(column)))
    assert not _business_equal(a, b.with_columns(pl.col("key").cast(pl.Int32)))
    assert not _business_equal(a, pl.concat([b, b]))
    assert not _business_equal(a, b.drop("source"))
