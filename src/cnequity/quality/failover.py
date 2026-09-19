"""Failover helpers — backup snapshots + tip routing support (ADR-0003 / 0005)."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.bars import fetch_daily_bars as fetch_em_daily_bars
from cnequity.adapters.eastmoney.bars import fetch_daily_bars_clist
from cnequity.adapters.eastmoney.corporate_actions import fetch_corporate_actions_eastmoney
from cnequity.config import Config, FailoverDatasetSpec
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.storage.source_snapshots import SnapshotStore

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
    df = fetch_em_daily_bars(symbols, start, end, config=config)
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
    df = fetch_corporate_actions_eastmoney(
        trade_date,
        backfill=backfill,
        config=config,
        run_id=run_id,
    )
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


def snapshot_trading_status_exchange(
    config: Config,
    *,
    trade_date: date,
    symbols: list[str],
    run_id: str,
) -> int:
    """Record what the SH/SZ boards said about halts and ST this session.

    The exchange reader already exists and is already trusted enough to serve
    as the failover path — measured against EastMoney for 2026-09-15 over 5,219
    symbols, ST agreed on 100.000% and halts on 99.923%, with the exchange
    right in all four disagreements, for two requests and 2.6 seconds. But it
    only runs when EastMoney fails, so on a normal day the lake holds no
    exchange-grade reading of SH/SZ status at all, and the ST evidence receipt
    — which admits `bse` precisely because a board is not an aggregator —
    has nothing to admit for Shanghai and Shenzhen.

    This writes it to the snapshot store, never to curated: authority over
    `trading_status` is unchanged, and the point of the daily capture is to
    build the availability record that a decision about authority needs.
    Returns the row count so a caller can report it.
    """
    sh_sz = [symbol for symbol in symbols if not symbol.endswith(".BJ")]
    if not sh_sz or not config.sources.get("exchange", True):
        return 0
    from cnequity.adapters.exchange.trading_status import fetch_trading_status_exchange

    result = fetch_trading_status_exchange(sh_sz, trade_date, config=config)
    if result.is_empty:
        return 0
    frame = with_provenance(result.rows, source="exchange", data_version="v1")
    write_backup_snapshot(
        config,
        "trading_status",
        frame,
        run_id=run_id,
        batch_id="exchange",
        source="exchange",
        trade_date=trade_date,
    )
    return frame.height


def snapshot_corporate_actions_tdx_backup(
    config: Config,
    *,
    trade_date: date,
    symbols: list[str],
    run_id: str,
    rate_limit,
) -> bool:
    """Snapshot TDX xdxr for ex-date symbols when EastMoney is daily canonical."""
    spec = failover_spec(config, "corporate_actions")
    if spec is None or not symbols or not config.tdx_enabled:
        return False
    from cnequity.adapters.tdx_protocol.client import quotes_client_factory
    from cnequity.adapters.tdx_protocol.corporate_actions import (
        fetch_corporate_actions_tdx,
    )

    tdx_df = fetch_corporate_actions_tdx(
        symbols,
        trade_date=trade_date,
        backfill=False,
        client_factory=quotes_client_factory(config),
        rate_limit=rate_limit,
        config=config,
        run_id=run_id,
    )
    if tdx_df.is_empty():
        return False
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
    return True
