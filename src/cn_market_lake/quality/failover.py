"""Failover helpers — backup snapshots + tip routing support (ADR-0003 / 0005)."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.bars import fetch_daily_bars as fetch_em_daily_bars
from cn_market_lake.adapters.eastmoney.bars import fetch_daily_bars_clist
from cn_market_lake.adapters.eastmoney.corporate_actions import fetch_corporate_actions_eastmoney
from cn_market_lake.config import Config, FailoverDatasetSpec
from cn_market_lake.domain.schemas import data_version_for, with_provenance
from cn_market_lake.storage.source_snapshots import SnapshotStore

logger = logging.getLogger(__name__)


def failover_spec(config: Config, dataset: str) -> FailoverDatasetSpec | None:
    if not config.failover_enabled:
        return None
    for spec in config.failover_datasets:
        if spec.name == dataset:
            return spec
    return None


def write_backup_snapshot(
    config: Config,
    dataset: str,
    df: pl.DataFrame,
    *,
    run_id: str,
    batch_id: str,
    source: str,
    trade_date: date | None = None,
) -> None:
    if df.is_empty():
        return
    path = SnapshotStore(config.meta_root).write(
        dataset,
        df,
        source=source,
        data_version=data_version_for(dataset),
        run_id=run_id,
        batch_id=batch_id,
        trade_date=trade_date,
    )
    if path:
        logger.info(
            "Wrote backup snapshot %s source=%s rows=%s → %s",
            dataset,
            source,
            df.height,
            path,
        )


def snapshot_daily_bars_clist(
    config: Config,
    *,
    trade_date: date,
    run_id: str,
    batch_id: str = "em-clist-snapshot",
    symbols: set[str] | list[str] | None = None,
    df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Write tip clist bars to source_snapshots for audit (not curated)."""
    spec = failover_spec(config, "daily_bars")
    if spec is None or not config.sources.get(spec.backup, True):
        return pl.DataFrame() if df is None else df
    if df is None:
        config.rate_limit(spec.backup)
        df = fetch_daily_bars_clist(trade_date, symbols=symbols, config=config)
    if df.is_empty():
        return df
    stamped = with_provenance(df, source=spec.backup, data_version=data_version_for("daily_bars"))
    write_backup_snapshot(
        config,
        "daily_bars",
        stamped,
        run_id=run_id,
        batch_id=batch_id,
        source=spec.backup,
        trade_date=trade_date,
    )
    return stamped


def snapshot_daily_bars_backup(
    config: Config,
    *,
    symbols: list[str],
    start: date,
    end: date,
    run_id: str,
    batch_id: str,
) -> None:
    """Multi-day / history failover via per-symbol kline (slow — not for tip)."""
    spec = failover_spec(config, "daily_bars")
    if spec is None or not config.sources.get(spec.backup, True):
        return
    # Tip windows use clist once at the step level; avoid N×kline here.
    if start == end:
        return
    config.rate_limit(spec.backup)
    df = fetch_em_daily_bars(symbols, start, end)
    if df.is_empty():
        return
    df = with_provenance(df, source=spec.backup, data_version=data_version_for("daily_bars"))
    write_backup_snapshot(
        config,
        "daily_bars",
        df,
        run_id=run_id,
        batch_id=batch_id,
        source=spec.backup,
        trade_date=end,
    )


def snapshot_corporate_actions_backup(
    config: Config,
    *,
    trade_date: date,
    run_id: str,
    backfill: bool,
) -> None:
    """Write EastMoney rows to snapshot (used when TDX is backfill canonical)."""
    if not backfill:
        return
    spec = failover_spec(config, "corporate_actions")
    if spec is None or not config.sources.get(spec.backup, True):
        return
    config.rate_limit(spec.backup)
    df = fetch_corporate_actions_eastmoney(trade_date, backfill=backfill, config=config)
    if df.is_empty():
        return
    df = with_provenance(df, source=spec.backup, data_version="v1")
    write_backup_snapshot(
        config,
        "corporate_actions",
        df,
        run_id=run_id,
        batch_id="backup",
        source=spec.backup,
        trade_date=trade_date,
    )


def snapshot_corporate_actions_tdx_backup(
    config: Config,
    *,
    trade_date: date,
    symbols: list[str],
    run_id: str,
    rate_limit,
) -> None:
    """Snapshot TDX xdxr for ex-date symbols when EastMoney is daily canonical."""
    spec = failover_spec(config, "corporate_actions")
    if spec is None or not symbols or not config.tdx_enabled:
        return
    from cn_market_lake.adapters.tdx_protocol.client import quotes_client_factory
    from cn_market_lake.adapters.tdx_protocol.corporate_actions import (
        fetch_corporate_actions_tdx,
    )

    tdx_df = fetch_corporate_actions_tdx(
        symbols,
        trade_date=trade_date,
        backfill=False,
        client_factory=quotes_client_factory(config),
        rate_limit=rate_limit,
    )
    if tdx_df.is_empty():
        return
    tdx_df = with_provenance(tdx_df, source=spec.backup, data_version="v1")
    write_backup_snapshot(
        config,
        "corporate_actions",
        tdx_df,
        run_id=run_id,
        batch_id="tdx-backup",
        source=spec.backup,
        trade_date=trade_date,
    )
