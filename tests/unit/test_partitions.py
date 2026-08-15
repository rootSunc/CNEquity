"""Partition period arithmetic, mixed-granularity reads, compact, repartition."""

from datetime import date

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.domain.partitions import (
    granularity_of,
    parse_partition,
    partition_value,
    previous_partition,
    uses_hive,
)
from cn_market_lake.query.parquet_scan import (
    collect_parquet_root,
    list_partitions,
    partition_files_in_range,
)
from cn_market_lake.storage.parquet import StagingWriter, compact_dataset
from cn_market_lake.storage.repartition import (
    RepartitionError,
    repartition_candidates,
    repartition_dataset,
)

# --- period arithmetic ------------------------------------------------------


@pytest.mark.parametrize(
    ("granularity", "expected"),
    [("day", "2024-06-03"), ("month", "2024-06"), ("quarter", "2024Q2"), ("year", "2024")],
)
def test_partition_value_per_granularity(granularity, expected):
    assert partition_value(date(2024, 6, 3), granularity) == expected


@pytest.mark.parametrize(
    ("value", "start", "end"),
    [
        ("2024-06-03", date(2024, 6, 3), date(2024, 6, 3)),
        ("2024-06", date(2024, 6, 1), date(2024, 6, 30)),
        ("2024-02", date(2024, 2, 1), date(2024, 2, 29)),  # leap year
        ("2024", date(2024, 1, 1), date(2024, 12, 31)),
        ("2016Q1", date(2016, 1, 1), date(2016, 3, 31)),
        ("2016Q2", date(2016, 4, 1), date(2016, 6, 30)),
        ("2016Q3", date(2016, 7, 1), date(2016, 9, 30)),
        ("2016Q4", date(2016, 10, 1), date(2016, 12, 31)),
        ("2024q1", date(2024, 1, 1), date(2024, 3, 31)),  # case-insensitive Q
    ],
)
def test_parse_partition_infers_period_from_shape(value, start, end):
    """Directories are self-describing so a granularity change needs no migration."""
    part = parse_partition(value)
    assert (part.start, part.end) == (start, end)


@pytest.mark.parametrize(
    "value", ["", "junk", "2024-13", "202", "2024-06-03-01", "bak.2024", "2016Q5", "2016Q"]
)
def test_parse_partition_rejects_non_periods(value):
    assert parse_partition(value) is None


@pytest.mark.parametrize(
    ("value", "granularity"),
    [
        ("2024-06-03", "day"),
        ("2024-06", "month"),
        ("2024", "year"),
        # Q1 starts in January and Q4 ends in December, so both brush against
        # the year test; a quarter read back as "month" would make every
        # report_period dataset disagree with the registry forever.
        ("2016Q1", "quarter"),
        ("2016Q2", "quarter"),
        ("2016Q4", "quarter"),
    ],
)
def test_granularity_of_reads_back_the_written_period(value, granularity):
    assert granularity_of(parse_partition(value)) == granularity


@pytest.mark.parametrize(
    ("value", "previous"),
    [
        ("2024-06-03", "2024-06-02"),
        ("2024-01-01", "2023-12-31"),
        ("2024-06", "2024-05"),
        ("2024-01", "2023-12"),
        ("2024Q2", "2024Q1"),
        ("2024Q1", "2023Q4"),
        ("2024", "2023"),
    ],
)
def test_previous_partition_crosses_period_boundaries(value, previous):
    assert previous_partition(parse_partition(value)) == previous


def test_only_day_values_can_be_hive_parsed():
    assert uses_hive("day") is True
    assert uses_hive("month") is False
    assert uses_hive("year") is False


# --- reads over mixed layouts -----------------------------------------------


def _write_partition(root, col, value, dates):
    part = root / f"{col}={value}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["A"] * len(dates), col: dates, "x": [1.0] * len(dates)}).write_parquet(
        part / "part-merged.parquet"
    )


def test_reads_a_lake_holding_both_day_and_year_directories(tmp_path):
    """A part-migrated dataset must read correctly, not silently return nothing."""
    root = tmp_path / "curated" / "index_bars"
    _write_partition(root, "trade_date", "2023", [date(2023, 3, 1), date(2023, 9, 1)])
    _write_partition(root, "trade_date", "2024-06-03", [date(2024, 6, 3)])

    df = collect_parquet_root(root, partition_col="trade_date")

    assert sorted(df["trade_date"].to_list()) == [
        date(2023, 3, 1),
        date(2023, 9, 1),
        date(2024, 6, 3),
    ]


def test_range_query_prunes_whole_periods_but_still_filters_edges(tmp_path):
    root = tmp_path / "curated" / "index_bars"
    _write_partition(root, "trade_date", "2023", [date(2023, 3, 1), date(2023, 9, 1)])
    _write_partition(root, "trade_date", "2024", [date(2024, 3, 1), date(2024, 9, 1)])

    files = partition_files_in_range(
        root, "trade_date", start=date(2024, 1, 1), end=date(2024, 12, 31)
    )
    assert len(files) == 1, "the 2023 directory must not be opened at all"

    # The surviving period still spans days outside the window.
    df = collect_parquet_root(
        root, partition_col="trade_date", start=date(2024, 6, 1), end=date(2024, 12, 31)
    )
    assert df["trade_date"].to_list() == [date(2024, 9, 1)]


def test_window_outside_coverage_returns_empty_not_error(tmp_path):
    root = tmp_path / "curated" / "index_bars"
    _write_partition(root, "trade_date", "2024", [date(2024, 3, 1)])

    df = collect_parquet_root(
        root, partition_col="trade_date", start=date(2030, 1, 1), end=date(2030, 12, 31)
    )
    assert df.is_empty()


def test_stray_directory_is_not_read_as_a_partition(tmp_path):
    root = tmp_path / "curated" / "index_bars"
    _write_partition(root, "trade_date", "2024", [date(2024, 3, 1)])
    _write_partition(root, "trade_date", "backup-copy", [date(2024, 3, 1)])

    assert [p.value for p in list_partitions(root, "trade_date")] == ["2024"]


# --- compact writes at the configured granularity ---------------------------


def _bar_frame(dates):
    n = len(dates)
    return pl.DataFrame(
        {
            "symbol": ["600519.SH"] * n,
            "trade_date": dates,
            "open": [1.0] * n,
            "high": [1.0] * n,
            "low": [1.0] * n,
            "close": [1.0] * n,
            "volume": [1] * n,
            "amount": [1.0] * n,
            "frequency": ["d"] * n,
            "source": ["tdx"] * n,
            "data_version": ["v1"] * n,
            "fetched_at": ["2026-07-21T00:00:00+00:00"] * n,
        }
    ).with_columns(pl.col("fetched_at").str.to_datetime(time_unit="us", time_zone="UTC"))


def _stage_bars(cfg, dataset, run_id, dates):
    StagingWriter(cfg.staging_root).write_batch(dataset, run_id, "batch-0", _bar_frame(dates))


def test_compact_groups_a_year_granularity_dataset_into_one_directory(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    dates = [date(2024, 1, 4), date(2024, 6, 3), date(2024, 12, 31), date(2025, 1, 6)]
    _stage_bars(cfg, "index_bars", "run-1", dates)

    compact_dataset(cfg.staging_root, cfg.curated_root, "index_bars", "run-1")

    root = cfg.curated_root / "index_bars"
    assert sorted(p.value for p in list_partitions(root, "trade_date")) == ["2024", "2025"]
    assert collect_parquet_root(root, partition_col="trade_date").height == 4


def test_compact_merges_into_an_existing_period_partition(tmp_path):
    """A second run in the same year must not drop the first run's rows."""
    cfg = Config(data_root=tmp_path / "data")
    _stage_bars(cfg, "index_bars", "run-1", [date(2024, 1, 4)])
    compact_dataset(cfg.staging_root, cfg.curated_root, "index_bars", "run-1")
    _stage_bars(cfg, "index_bars", "run-2", [date(2024, 6, 3)])
    compact_dataset(cfg.staging_root, cfg.curated_root, "index_bars", "run-2")

    df = collect_parquet_root(cfg.curated_root / "index_bars", partition_col="trade_date")
    assert sorted(df["trade_date"].to_list()) == [date(2024, 1, 4), date(2024, 6, 3)]


# --- repartition ------------------------------------------------------------


def _daily_layout(cfg, dataset="index_bars", dates=None):
    """Schema-complete rows in one directory per day — the pre-migration layout."""
    dates = dates or [date(2024, 1, 4), date(2024, 6, 3), date(2025, 1, 6)]
    root = cfg.curated_root / dataset
    for d in dates:
        part = root / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        _bar_frame([d]).write_parquet(part / "part-merged.parquet")
    return root


def test_repartition_collapses_day_dirs_into_the_configured_period(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = _daily_layout(cfg)

    result = repartition_dataset(cfg, "index_bars")

    assert result.changed is True
    assert result.rows == 3
    assert sorted(p.value for p in list_partitions(root, "trade_date")) == ["2024", "2025"]
    df = collect_parquet_root(root, partition_col="trade_date")
    assert sorted(df["trade_date"].to_list()) == [
        date(2024, 1, 4),
        date(2024, 6, 3),
        date(2025, 1, 6),
    ]


def test_repartition_is_idempotent(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _daily_layout(cfg)
    repartition_dataset(cfg, "index_bars")

    assert repartition_dataset(cfg, "index_bars").changed is False


def test_dry_run_leaves_the_dataset_untouched(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = _daily_layout(cfg)

    result = repartition_dataset(cfg, "index_bars", dry_run=True)

    assert result.changed is False
    assert result.files_after == 2, "reports the effect it would have"
    assert len(list_partitions(root, "trade_date")) == 3, "but changes nothing"
    assert not list(root.parent.glob("*repartition-tmp*"))


def test_candidates_lists_only_datasets_whose_layout_is_stale(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _daily_layout(cfg)  # index_bars is configured year → stale
    _write_partition(
        cfg.curated_root / "daily_bars", "trade_date", "2024-06-03", [date(2024, 6, 3)]
    )

    candidates = repartition_candidates(cfg)

    assert "index_bars" in candidates
    assert "daily_bars" not in candidates, "already matches its configured day granularity"


def test_repartition_refuses_a_merge_style_dataset(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    with pytest.raises(RepartitionError, match="merge-style"):
        repartition_dataset(cfg, "instruments")


def test_repartition_refuses_an_unknown_dataset(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    with pytest.raises(RepartitionError, match="unknown dataset"):
        repartition_dataset(cfg, "not_a_dataset")


def _calendar_row(d: date, *, fetched_at: str, is_trading: bool = True) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [d],
            "is_trading": [is_trading],
            "source": ["seed"],
            "data_version": ["v1"],
            "fetched_at": [fetched_at],
        }
    ).with_columns(pl.col("fetched_at").str.to_datetime(time_unit="us", time_zone="UTC"))


def test_repartition_dedupes_pk_when_day_and_year_dirs_overlap(tmp_path):
    """A granularity flip leaves day dirs beside year dirs; rewrite must not bake the overlap in."""
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.curated_root / "trading_calendar"
    day = date(2024, 6, 3)
    day_dir = root / f"trade_date={day.isoformat()}"
    year_dir = root / "trade_date=2024"
    day_dir.mkdir(parents=True)
    year_dir.mkdir(parents=True)
    _calendar_row(day, fetched_at="2026-01-01T00:00:00+00:00", is_trading=False).write_parquet(
        day_dir / "part-merged.parquet"
    )
    _calendar_row(day, fetched_at="2026-07-01T00:00:00+00:00", is_trading=True).write_parquet(
        year_dir / "part-merged.parquet"
    )

    result = repartition_dataset(cfg, "trading_calendar")

    assert result.changed is True
    assert result.rows == 1
    assert [p.value for p in list_partitions(root, "trade_date")] == ["2024"]
    df = collect_parquet_root(root, partition_col="trade_date")
    assert df.height == 1
    assert df["is_trading"].to_list() == [True], "freshest fetched_at wins"
