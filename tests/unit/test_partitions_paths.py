"""Partition directory names come from the dataset's own period, never by hand."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from cnequity.domain.datasets import DATASETS
from cnequity.domain.partitions import partition_value, partition_value_expr


@pytest.mark.parametrize("granularity", ["day", "month", "quarter", "year"])
def test_the_column_form_matches_partition_value(granularity):
    days = [date(2023, 12, 25) + timedelta(days=n) for n in range(400)]
    column = pl.DataFrame({"d": days}).select(partition_value_expr(pl.col("d"), granularity))
    assert column.to_series().to_list() == [partition_value(d, granularity) for d in days]


def test_partition_dir_name_follows_each_dataset_period():
    day = date(2024, 6, 3)
    assert DATASETS["daily_bars"].partition_dir_name(day) == "trade_date=2024-06"
    assert DATASETS["option_bars"].partition_dir_name(day) == "trade_date=2024-06-03"
    assert DATASETS["industry_index"].partition_dir_name(day) == "trade_date=2024"
    with pytest.raises(ValueError):
        DATASETS["instruments"].partition_dir_name(day)


def test_the_writer_names_directories_the_same_way():
    from cnequity.storage.parquet import _partition_values

    days = [date(2024, 1, 31), date(2024, 2, 1), date(2024, 12, 31)]
    frame = pl.DataFrame({"trade_date": days})
    for granularity in ("day", "month", "quarter", "year"):
        values = _partition_values(frame, "trade_date", granularity).to_list()
        assert values == [partition_value(d, granularity) for d in days], granularity
