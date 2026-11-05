"""Fill a snapshot dataset's history from Tushare without touching what it holds.

``fund_flow`` and ``sector_fund_flow`` are daily EastMoney snapshots, and
EastMoney no longer serves past sessions. Tushare republishes the same measure
by date, so with a token it is their history route. It is a peer, not the
authority: a session already in the lake keeps every row it has, and Tushare
adds only the keys missing from it.

A session counts as swept once it holds a Tushare row, so an interrupted walk
resumes where it stopped; ``cne backfill --refetch`` asks every session again,
which still adds only what is missing.
"""

from __future__ import annotations

from collections.abc import Callable
from copy import copy
from datetime import date

import polars as pl

from cnequity.adapters.tushare.client import SOURCE
from cnequity.config import Config
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.run_scope import backfill_of, set_backfill
from cnequity.steps.common import walk_day_backfill


def backfill_missing_keys(
    config: Config,
    trade_date: date,
    run_id: str,
    dataset: str,
    fetch_day: Callable[[date], pl.DataFrame],
    *,
    key: str,
    floor: date,
) -> dict:
    """Walk ``[start, end]`` from *floor*, staging Tushare rows whose *key* the day lacks."""
    start = backfill_of(config).start
    if start is not None and start < floor:
        # The endpoint answers nothing earlier; asking for every older session
        # would only record a run of empty days.
        config = copy(config)
        set_backfill(config, start=floor)
    root = config.curated_root / dataset
    refetch = bool(backfill_of(config).refetch)

    def lake_rows(first: date, last: date) -> pl.LazyFrame | None:
        if not dataset_has_parquet(root):
            return None
        return scan_parquet_root(root, partition_col="trade_date", start=first, end=last)

    def swept(days: list[date]) -> set[date]:
        if not days or refetch:
            return set()
        rows = lake_rows(min(days), max(days))
        if rows is None:
            return set()
        return set(
            rows.filter(pl.col("source") == SOURCE)
            .select(pl.col("trade_date").cast(pl.Date))
            .unique()
            .collect()["trade_date"]
            .to_list()
        )

    def fetch_one(day: date) -> pl.DataFrame:
        frame = fetch_day(day)
        rows = lake_rows(day, day)
        if rows is not None and not frame.is_empty():
            held = rows.filter(pl.col("source") != SOURCE).select(key).unique().collect()
            frame = frame.join(held, on=key, how="anti")
        return frame

    return walk_day_backfill(
        config,
        trade_date,
        run_id,
        dataset,
        fetch_one,
        source=SOURCE,
        floor=floor,
        existing_dates_fn=swept,
        allow_empty_days=True,
    )
