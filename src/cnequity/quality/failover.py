"""Failover helpers — backup snapshots + tip routing support (ADR-0003 / 0005)."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.adapters.baostock._session import _login, import_baostock, to_baostock_symbol
from cnequity.adapters.baostock.trading_status import fetch_trading_status_baostock
from cnequity.adapters.eastmoney.bars import fetch_daily_bars as fetch_em_daily_bars
from cnequity.adapters.eastmoney.bars import fetch_daily_bars_clist
from cnequity.adapters.eastmoney.corporate_actions import fetch_corporate_actions_eastmoney
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.adapters.eastmoney.trading_status import _fetch_suspended_symbols
from cnequity.config import Config, FailoverDatasetSpec
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.domain.symbols import is_all_a_symbol, is_cdr_symbol, parse_symbol
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.source_snapshots import SnapshotStore

logger = logging.getLogger(__name__)

# Universe filter vocabulary — a symbol whose last curated status is one of
# these must never be silently filled as "normal" when the backup snapshot
# happens to miss it (that would wash a still-ST/suspended name into tradeable).
_NON_TRADABLE_STATUSES = frozenset({"st", "*st", "suspended"})

# Baostock reference symbol for the freshness probe: essentially never
# suspended, so its k-data row is a trustworthy "baostock has processed day D".
_REFERENCE_SYMBOL = "600519.SH"

# trading_status columns minus provenance (added by the step).
_TS_OUTPUT_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "is_trading": pl.Boolean,
    "status": pl.Utf8,
}


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


def snapshot_trading_status_backup(
    config: Config,
    *,
    df: pl.DataFrame,
    run_id: str,
    trade_date: date,
) -> None:
    """Write degraded trading_status rows to source_snapshots for audit."""
    spec = failover_spec(config, "trading_status")
    if spec is None or not config.sources.get(spec.backup, True) or df.is_empty():
        return
    stamped = with_provenance(
        df.drop("source", strict=False),
        source=spec.backup,
        data_version=data_version_for("trading_status"),
    )
    write_backup_snapshot(
        config,
        "trading_status",
        stamped,
        run_id=run_id,
        batch_id="backup",
        source=spec.backup,
        trade_date=trade_date,
    )


def _baostock_has_day(config: Config, trade_date: date) -> bool:
    """Probe whether baostock has processed *trade_date* (freshness gate).

    ``query_all_stock(day=D)`` may silently serve D-1 data when D has not been
    settled yet; stamping that onto ``trade_date=D`` would be a lie, so the
    coordinator refuses the backup instead. The reference symbol is a large-cap
    name that is never suspended, so a returned row for D means the batch is
    current.
    """
    if config is not None:
        config.rate_limit("baostock")
    bs = import_baostock()
    _login(bs)
    try:
        rs = bs.query_history_k_data_plus(
            to_baostock_symbol(_REFERENCE_SYMBOL),
            "date,tradestatus",
            start_date=trade_date.isoformat(),
            end_date=trade_date.isoformat(),
            frequency="d",
            adjustflag="3",
        )
        if getattr(rs, "error_code", "0") != "0":
            return False
        target = trade_date.isoformat()
        while rs.next():
            row = rs.get_row_data()
            if row and str(row[0]) == target:
                return True
        return False
    finally:
        try:
            bs.logout()
        except Exception:  # noqa: BLE001 — logout on a dead socket may raise
            pass


def _previous_statuses(config: Config, trade_date: date) -> dict[str, str]:
    """Last curated non-degraded status per symbol strictly before *trade_date*."""
    status_root = config.curated_root / "trading_status"
    if not dataset_has_parquet(status_root):
        return {}
    df = (
        scan_parquet_root(status_root, partition_col="trade_date", end=trade_date)
        .filter(pl.col("trade_date") < pl.lit(trade_date))
        .select("symbol", "trade_date", "status")
        .collect()
    )
    if df.is_empty():
        return {}
    latest = df.sort("trade_date").group_by("symbol").agg(pl.col("status").last())
    return dict(zip(latest["symbol"].to_list(), latest["status"].to_list(), strict=False))


def _bj_rows(config: Config, bj_symbols: list[str], trade_date: date) -> tuple[list[dict], int]:
    """BJ rows from the EastMoney suspension leg when it is alive; else defaults.

    Returns ``(rows, n_defaulted)``. When EastMoney answers, BJ rows carry its
    real suspension/normal signal (n_defaulted=0); when the leg is down, every
    BJ symbol becomes ``normal`` and is counted so the degradation is auditable.
    """
    if not bj_symbols:
        return [], 0
    try:
        client = EastMoneyClient(min_interval=2.0, config=config)
        try:
            suspended = _fetch_suspended_symbols(client, trade_date)
        finally:
            client.close()
    except Exception as exc:  # noqa: BLE001 — EastMoney down: default + count
        logger.warning("trading_status backup: BJ suspension leg unavailable (%s); defaulting", exc)
        rows = [
            {
                "symbol": sym,
                "trade_date": trade_date,
                "is_trading": True,
                "status": "normal",
            }
            for sym in bj_symbols
        ]
        return rows, len(bj_symbols)
    rows = []
    for sym in bj_symbols:
        if sym in suspended:
            rows.append(
                {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "is_trading": False,
                    "status": "suspended",
                }
            )
        else:
            rows.append(
                {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "is_trading": True,
                    "status": "normal",
                }
            )
    return rows, 0


def _fill_missing(
    missing: list[str],
    previous: dict[str, str],
    trade_date: date,
    threshold: int,
) -> tuple[list[dict], list[str], int]:
    """Classify snapshot-missing SH/SZ symbols.

    Returns ``(fill_rows, fill_failures, filled_vocab_size)``.

    - A missing symbol whose previous curated status is ``st/*st/suspended`` is a
      fill-failure (never washed to ``normal``).
    - Anything else (previous ``normal`` or no record: e.g. a new listing) is
      filled as ``normal`` and counted.
    - The caller refuses the whole backup when ``fill_failures`` is non-empty or
      the filled count exceeds *threshold*.
    """
    fill_rows: list[dict] = []
    fill_failures: list[str] = []
    for sym in missing:
        prev_status = previous.get(sym)
        if prev_status in _NON_TRADABLE_STATUSES:
            fill_failures.append(sym)
            continue
        fill_rows.append(
            {
                "symbol": sym,
                "trade_date": trade_date,
                "is_trading": True,
                "status": "normal",
            }
        )
    return fill_rows, fill_failures, len(fill_rows)


def fetch_trading_status_backup(
    config: Config,
    symbols: list[str],
    trade_date: date,
) -> tuple[pl.DataFrame | None, dict]:
    """Baostock fallback for trading_status, split by universe.

    Returns ``(frame, meta)`` where *frame* is ``None`` when the backup is
    refused (not configured, stale, fill-failure, or over threshold). Meta
    carries ``failover_used / source / freshness / n_filled / n_bj_defaulted /
    n_fill_failed / reason`` for the step's audit findings.

    Exceptions (login failure, unexpected vocabulary, ''query_all_stock''
    transport) propagate — both sources failing must surface as a hard error.
    """
    spec = failover_spec(config, "trading_status")
    if spec is None or not config.sources.get(spec.backup, True):
        return None, {"failover_used": False, "reason": "trading_status failover not configured"}
    if not _baostock_has_day(config, trade_date):
        return None, {
            "failover_used": False,
            "freshness": "stale",
            "reason": f"baostock has no data for {trade_date.isoformat()} yet",
        }

    sh_sz = [s for s in symbols if parse_symbol(s).exchange in ("SH", "SZ")]
    bj = [s for s in symbols if parse_symbol(s).exchange == "BJ"]

    bs_df = fetch_trading_status_baostock(sh_sz, trade_date, config=config)
    if sh_sz and bs_df.is_empty():
        # Unusable snapshot: an empty response alongside a fresh day is
        # ambiguity, not evidence of "nothing suspended / nothing ST".
        return None, {
            "failover_used": False,
            "freshness": "fresh",
            "reason": "baostock snapshot returned no SH/SZ rows",
        }

    missing = [s for s in sh_sz if s not in set(bs_df.get_column("symbol").to_list())]
    # baostock's query_all_stock snapshot only serves SH/SZ A-shares. The
    # requested universe also carries funds / B-shares / CDRs (the primary
    # EastMoney path labels them normal too), so split the missing set: only
    # A-share gaps are eligible for the wash-guard and the critical threshold;
    # everything else is a benign out-of-scope default.
    a_share_missing, scope_defaults = [], []
    for sym in missing:
        info = parse_symbol(sym)
        if is_all_a_symbol(info.code, info.exchange) and not is_cdr_symbol(info.code, info.exchange):
            a_share_missing.append(sym)
        else:
            scope_defaults.append(sym)

    previous = _previous_statuses(config, trade_date)
    threshold = max(50, len(symbols) // 100)
    fill_rows, fill_failures, n_filled = _fill_missing(
        a_share_missing, previous, trade_date, threshold
    )
    n_scope_defaults = len(scope_defaults)
    fill_rows.extend(
        {
            "symbol": sym,
            "trade_date": trade_date,
            "is_trading": True,
            "status": "normal",
        }
        for sym in scope_defaults
    )
    if fill_failures:
        return None, {
            "failover_used": False,
            "freshness": "fresh",
            "reason": f"backup fill-failure(s) in non-tradable missing symbols: "
            f"{sorted(fill_failures)[:5]}",
            "n_fill_failed": len(fill_failures),
        }
    if n_filled > threshold:
        return None, {
            "failover_used": False,
            "freshness": "fresh",
            "reason": f"backup filled {n_filled} missing rows (> threshold {threshold})",
            "n_filled": n_filled,
        }

    bj_rows, n_bj_defaulted = _bj_rows(config, bj, trade_date)
    rows = [dict(r) for r in bs_df.iter_rows(named=True)]
    rows.extend(fill_rows)
    rows.extend(bj_rows)

    df = (
        pl.DataFrame(rows, schema=_TS_OUTPUT_SCHEMA)
        if rows
        else pl.DataFrame(schema=_TS_OUTPUT_SCHEMA)
    )
    meta = {
        "failover_used": True,
        "source": spec.backup,
        "freshness": "fresh",
        "n_filled": n_filled,
        "n_scope_defaults": n_scope_defaults,
        "n_bj_defaulted": n_bj_defaulted,
        "n_fill_failed": 0,
    }
    return df, meta
