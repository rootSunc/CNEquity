"""The lake measurement tables under meta/stats."""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest

from cn_market_lake.file_lock import exclusive_lock
from cn_market_lake.storage import stats as stats_module
from cn_market_lake.storage.stats import (
    load_partition_stats,
    load_provenance_stats,
    load_summary,
    partition_stats_path,
    rebuild_stats,
    refresh_stats_if_stale,
    stats_freshness,
    stats_root,
)

FETCHED = datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc)


def _write(root, partition: str | None, rows: list[dict], *, name: str = "part-0") -> None:
    target = root if partition is None else root / partition
    target.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target / f"{name}.parquet")


def _bar(symbol: str, source: str = "tdx_protocol", version: str = "v2", hours: int = 0) -> dict:
    return {
        "symbol": symbol,
        "source": source,
        "data_version": version,
        "fetched_at": FETCHED.replace(hour=12 + hours),
    }


def test_rebuild_measures_partitions_and_provenance(config):
    root = config.curated_root / "daily_bars"
    _write(root, "trade_date=2026-07-30", [_bar("600519.SH"), _bar("000001.SZ")])
    _write(root, "trade_date=2026-07-31", [_bar("600519.SH", source="ths", hours=1)])

    result = rebuild_stats(config, datasets=["daily_bars"])
    assert result.datasets == ["daily_bars"]
    assert (result.partitions, result.rows, result.files) == (2, 3, 2)
    assert result.bytes > 0

    partitions = load_partition_stats(config).sort("partition")
    assert partitions["partition"].to_list() == ["2026-07-30", "2026-07-31"]
    assert partitions["row_count"].to_list() == [2, 1]
    assert partitions["granularity"].to_list() == ["day", "day"]

    provenance = load_provenance_stats(config).sort("source")
    assert provenance["source"].to_list() == ["tdx_protocol", "ths"]
    assert provenance["row_count"].to_list() == [2, 1]
    assert provenance["fetched_at_max"].max() == FETCHED.replace(hour=13)


def test_row_counts_split_by_source_within_one_partition(config):
    """The provenance grain is finer than the partition grain, not equal to it."""
    root = config.curated_root / "daily_bars"
    _write(
        root,
        "trade_date=2026-07-31",
        [_bar("600519.SH"), _bar("000001.SZ"), _bar("000002.SZ", source="sina")],
    )

    rebuild_stats(config, datasets=["daily_bars"])
    partitions = load_partition_stats(config)
    provenance = load_provenance_stats(config).sort("source")

    assert partitions.height == 1
    assert partitions["row_count"].item() == 3
    assert provenance["source"].to_list() == ["sina", "tdx_protocol"]
    assert provenance["row_count"].to_list() == [1, 2]
    assert provenance["row_count"].sum() == partitions["row_count"].item()


def test_provenance_rollup_is_preserved_across_bounded_batches(config, monkeypatch):
    """Batching is an execution detail; identical provenance still rolls up."""
    monkeypatch.setattr(stats_module, "PROVENANCE_SCAN_BATCH_ROWS", 2)
    calls = 0
    original = stats_module._scan_provenance_batch

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(stats_module, "_scan_provenance_batch", counted)
    root = config.curated_root / "daily_bars"
    _write(root, "trade_date=2026-07-31", [_bar("600519.SH"), _bar("000001.SZ")])
    _write(
        root,
        "trade_date=2026-07-31",
        [_bar("000002.SZ"), _bar("000003.SZ", source="sina")],
        name="part-1",
    )

    rebuild_stats(config, datasets=["daily_bars"])

    assert calls == 2
    assert load_partition_stats(config)["row_count"].item() == 4
    provenance = load_provenance_stats(config).sort("source")
    assert provenance["source"].to_list() == ["sina", "tdx_protocol"]
    assert provenance["row_count"].to_list() == [1, 3]


def test_footer_counts_dataset_without_provenance_columns(config):
    """Row counts do not depend on scanning or having attribution columns."""
    root = config.curated_root / "daily_bars"
    _write(root, "trade_date=2026-07-31", [{"symbol": "600519.SH"}, {"symbol": "000001.SZ"}])

    result = rebuild_stats(config, datasets=["daily_bars"])

    assert result.rows == 2
    assert load_partition_stats(config)["row_count"].item() == 2
    assert load_provenance_stats(config).is_empty()


def test_provenance_without_fetched_at_gets_null_time_span(config):
    root = config.curated_root / "daily_bars"
    _write(
        root,
        "trade_date=2026-07-31",
        [{"symbol": "600519.SH", "source": "tdx_protocol", "data_version": "v2"}],
    )

    rebuild_stats(config, datasets=["daily_bars"])

    row = load_provenance_stats(config).row(0, named=True)
    assert row["row_count"] == 1
    assert row["fetched_at_min"] is None
    assert row["fetched_at_max"] is None


@pytest.mark.parametrize(
    ("partition", "granularity", "start", "end"),
    [
        ("trade_date=2026-07-31", "day", "2026-07-31", "2026-07-31"),
        ("trade_date=2026-07", "month", "2026-07-01", "2026-07-31"),
        ("trade_date=2026", "year", "2026-01-01", "2026-12-31"),
    ],
)
def test_every_granularity_reads_back_its_period(config, partition, granularity, start, end):
    root = config.curated_root / "daily_bars"
    _write(root, partition, [_bar("600519.SH")])

    rebuild_stats(config, datasets=["daily_bars"])
    row = load_partition_stats(config).row(0, named=True)
    assert row["granularity"] == granularity
    assert (row["period_start"].isoformat(), row["period_end"].isoformat()) == (start, end)


def test_quarter_partitions_are_measured_as_quarters(config):
    """report_period datasets partition by quarter; the period must say so."""
    root = config.curated_root / "financial_statement_items"
    _write(root, "report_period=2026Q2", [_bar("600519.SH")])

    rebuild_stats(config, datasets=["financial_statement_items"])
    row = load_partition_stats(config).row(0, named=True)
    assert row["partition"] == "2026Q2"
    assert row["granularity"] == "quarter"
    assert row["period_start"].isoformat() == "2026-04-01"
    assert row["period_end"].isoformat() == "2026-06-30"


def test_merge_style_datasets_get_a_null_partition(config):
    """instruments is one file with no partition directory, not zero rows."""
    _write(config.curated_root / "instruments", None, [_bar("600519.SH")])

    rebuild_stats(config, datasets=["instruments"])
    row = load_partition_stats(config).row(0, named=True)
    assert row["partition"] is None
    assert row["granularity"] is None
    assert row["period_start"] is None
    assert row["row_count"] == 1


def test_stray_root_files_are_counted_not_dropped(config):
    """A partitioned dataset's loose files still belong to the lake's totals."""
    root = config.curated_root / "daily_bars"
    _write(root, "trade_date=2026-07-31", [_bar("600519.SH")])
    _write(root, None, [_bar("000001.SZ")], name="orphan")

    result = rebuild_stats(config, datasets=["daily_bars"])
    assert result.rows == 2
    partitions = load_partition_stats(config)
    assert sorted(partitions["partition"].to_list(), key=lambda v: (v is not None, v)) == [
        None,
        "2026-07-31",
    ]


def test_datasets_without_parquet_are_reported_not_rowed(config):
    result = rebuild_stats(config, datasets=["daily_bars"])
    assert result.datasets == []
    assert result.empty == ["daily_bars"]
    assert load_partition_stats(config).is_empty()


def test_partial_rebuild_keeps_the_other_datasets(config):
    """`--dataset` must not delete what it did not look at."""
    _write(config.curated_root / "daily_bars", "trade_date=2026-07-31", [_bar("600519.SH")])
    _write(config.curated_root / "instruments", None, [_bar("000001.SZ")])
    rebuild_stats(config)
    assert set(load_partition_stats(config)["dataset"]) == {"daily_bars", "instruments"}

    # daily_bars gains a partition; instruments is not rescanned.
    _write(config.curated_root / "daily_bars", "trade_date=2026-07-30", [_bar("000002.SZ")])
    rebuild_stats(config, datasets=["daily_bars"])

    partitions = load_partition_stats(config)
    assert set(partitions["dataset"]) == {"daily_bars", "instruments"}
    assert partitions.filter(pl.col("dataset") == "daily_bars").height == 2
    assert partitions.filter(pl.col("dataset") == "instruments").height == 1


def test_rebuild_replaces_rather_than_appends(config):
    root = config.curated_root / "daily_bars"
    _write(root, "trade_date=2026-07-31", [_bar("600519.SH")])
    rebuild_stats(config, datasets=["daily_bars"])
    rebuild_stats(config, datasets=["daily_bars"])

    assert load_partition_stats(config).height == 1
    assert load_provenance_stats(config).height == 1


def test_unknown_dataset_is_rejected_before_any_write(config):
    with pytest.raises(ValueError, match="nonesuch"):
        rebuild_stats(config, datasets=["nonesuch"])
    assert not partition_stats_path(config).exists()


def test_summary_records_when_and_against_which_run(config):
    _write(config.curated_root / "daily_bars", "trade_date=2026-07-31", [_bar("600519.SH")])
    rebuild_stats(config, datasets=["daily_bars"])

    summary = load_summary(config)
    assert summary["rows"] == 1
    assert summary["rebuilt_datasets"] == ["daily_bars"]
    # Parses as a timestamp — a reader compares it against the manifest to tell
    # whether the tables predate the last ingestion.
    assert datetime.fromisoformat(summary["generated_at"]).tzinfo is not None


def test_missing_stats_load_as_empty_frames_not_errors(config):
    assert load_partition_stats(config).is_empty()
    assert load_provenance_stats(config).is_empty()
    assert load_summary(config) is None


# --- staleness ---------------------------------------------------------------


def _start_run(config) -> str:
    from cn_market_lake.orchestrator.manifest import Manifest

    return Manifest(config.manifest_path).start_run("daily")


def _seed(config) -> None:
    _write(config.curated_root / "daily_bars", "trade_date=2026-07-31", [_bar("600519.SH")])


def test_no_stats_is_stale(config):
    freshness = stats_freshness(config)
    assert freshness.stale
    assert freshness.reason == "no stats yet"


def test_stats_without_any_ingestion_run_are_current(config):
    """Nothing has run, so nothing can have invalidated them."""
    _seed(config)
    rebuild_stats(config, datasets=["daily_bars"])

    freshness = stats_freshness(config)
    assert not freshness.stale
    assert freshness.latest_run_id is None
    assert freshness.generated_at.tzinfo is not None


def test_a_run_landing_after_the_rebuild_makes_them_stale(config):
    _seed(config)
    _start_run(config)
    rebuild_stats(config, datasets=["daily_bars"])
    assert not stats_freshness(config).stale

    run_id = _start_run(config)
    freshness = stats_freshness(config)
    assert freshness.stale
    assert run_id in freshness.reason
    assert freshness.latest_run_id == run_id
    assert freshness.stats_run_id != run_id


def test_refresh_is_a_no_op_while_current(config):
    _seed(config)
    rebuild_stats(config, datasets=["daily_bars"])
    before = load_summary(config)["generated_at"]

    assert refresh_stats_if_stale(config) is None
    assert load_summary(config)["generated_at"] == before


def test_refresh_rebuilds_after_a_run(config):
    _seed(config)
    rebuild_stats(config, datasets=["daily_bars"])
    _write(config.curated_root / "daily_bars", "trade_date=2026-07-30", [_bar("000001.SZ")])
    _start_run(config)

    result = refresh_stats_if_stale(config)
    assert result is not None
    # A refresh covers the whole lake, not just what a caller happened to name.
    assert result.rows == 2
    assert not stats_freshness(config).stale


def test_force_rebuilds_a_current_table(config):
    _seed(config)
    rebuild_stats(config, datasets=["daily_bars"])

    assert refresh_stats_if_stale(config, force=True) is not None


def test_refresh_yields_to_a_rebuild_already_running(config):
    """Losing the lock returns None rather than queueing behind a full scan."""
    _seed(config)
    stats_root(config).mkdir(parents=True, exist_ok=True)

    with exclusive_lock(stats_root(config) / ".rebuild.lock"):
        assert refresh_stats_if_stale(config) is None

    # Lock released — the same call now does the work.
    assert refresh_stats_if_stale(config) is not None


# --- partition mutation proration -------------------------------------------
# A month partition on the 8th holds eight days against a full prior month, so
# the raw period-over-period ratio read ~26% and tripped the shrink threshold —
# for every month-partitioned dataset, for most of every month. Two of those
# fired in a real audit (sentiment_scores, sector_bars) purely from the calendar.


def test_period_elapsed_fraction_tracks_the_calendar():
    from datetime import date

    from cn_market_lake.quality.dataset_checks import period_elapsed_fraction as frac

    assert frac("2026-08", "month", date(2026, 8, 8)) == 8 / 31
    assert frac("2026-08", "month", date(2026, 8, 31)) == 1.0
    assert frac("2026-07", "month", date(2026, 8, 8)) == 1.0, "a finished period is whole"
    assert frac("2026-08-08", "day", date(2026, 8, 8)) == 1.0, "a day partition is never partial"
    assert 0.4 < frac("2026Q3", "quarter", date(2026, 8, 8)) < 0.45
    assert frac("garbage", "month", date(2026, 8, 8)) == 1.0, "unparseable must not warn"


def test_partial_month_is_not_flagged_as_a_shrink():
    from cn_market_lake.quality.dataset_checks import check_partition_row_mutation

    # Real numbers from the audit that surfaced this: sector_bars, 8 days in.
    finding = check_partition_row_mutation(
        "sector_bars",
        "trade_date",
        current_value="2026-08",
        previous_value="2026-07",
        current_stats={"rows": 2160, "symbols": None},
        previous_stats={"rows": 9935, "symbols": None},
        elapsed_fraction=8 / 31,
    )
    assert finding is None, "on pace for the month — 2160 vs a prorated ~2564"


def test_symbol_counts_are_prorated_too():
    """Leaving symbols raw kept three datasets warning after the row fix.

    dragon_tiger, block_trades and sentiment_scores are event-driven: distinct
    names accumulate over the month exactly like rows, so an 8-day partition
    holds ~26% of them and tripped the threshold on the symbol ratio alone.
    """
    from cn_market_lake.quality.dataset_checks import check_partition_row_mutation

    for dataset, cur_rows, prev_rows, cur_syms, prev_syms in [
        ("dragon_tiger", 355, 1977, 235, 900),
        ("block_trades", 218, 1098, 138, 499),
        ("sentiment_scores", 2700, 12140, 2090, 4582),
    ]:
        finding = check_partition_row_mutation(
            dataset,
            "trade_date",
            current_value="2026-08",
            previous_value="2026-07",
            current_stats={"rows": cur_rows, "symbols": cur_syms},
            previous_stats={"rows": prev_rows, "symbols": prev_syms},
            elapsed_fraction=8 / 31,
        )
        assert finding is None, f"{dataset} is on pace, not shrinking"


def test_a_real_shrink_still_fires_mid_period():
    from cn_market_lake.quality.dataset_checks import check_partition_row_mutation

    finding = check_partition_row_mutation(
        "sector_bars",
        "trade_date",
        current_value="2026-08",
        previous_value="2026-07",
        current_stats={"rows": 200, "symbols": None},
        previous_stats={"rows": 9935, "symbols": None},
        elapsed_fraction=8 / 31,
    )
    assert finding is not None, "proration must not blind the check to a genuine collapse"
    assert "prorated" in finding["message"]
