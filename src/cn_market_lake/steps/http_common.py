"""Shared step helper for EastMoney / CNINFO HTTP datasets."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.domain.schemas import data_version_for, with_provenance
from cn_market_lake.steps.common import fetch_incremental_daily, write_simple


def write_fetched(
    config: Config,
    run_id: str,
    dataset: str,
    df: pl.DataFrame,
    *,
    source: str,
    batch_id: str = "batch-0",
) -> dict:
    df = with_provenance(df, source=source, data_version=data_version_for(dataset))
    return write_simple(config, run_id, dataset, df, batch_id=batch_id)


def run_incremental_fetched(
    config: Config,
    trade_date: date,
    run_id: str,
    dataset: str,
    fetch_fn: Callable[[date], pl.DataFrame],
    *,
    source: str,
    allow_empty: bool = False,
    universe: set[str] | None = None,
) -> dict:
    df, findings = fetch_incremental_daily(
        config,
        dataset,
        trade_date,
        fetch_fn,
        allow_empty=allow_empty,
    )
    if universe and not df.is_empty():
        # Constrain a live snapshot (e.g. EastMoney valuation clist) to the
        # tradable universe: the source returns delisted / never-traded names the
        # lake must not carry. An empty universe means "cannot reconcile" — skip
        # filtering rather than dropping every row.
        df = df.filter(pl.col("symbol").is_in(list(universe)))
    if df.is_empty():
        out: dict = {"rows_read": 0, "rows_written": 0}
        if findings:
            out["context_updates"] = {"audit_findings": findings}
        return out
    result = write_fetched(config, run_id, dataset, df, source=source)
    if findings:
        result["context_updates"] = {"audit_findings": findings}
    return result


def empty_ok(df: pl.DataFrame, dataset: str, trade_date: date) -> None:
    if df.is_empty():
        raise RuntimeError(f"{dataset}: no rows returned for {trade_date.isoformat()}")
