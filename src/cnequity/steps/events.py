"""L2 corporate-event steps: corporate_actions, announcement_index,
earnings_disclosure_schedule."""

from __future__ import annotations

import inspect
import json
import logging
from datetime import date

import polars as pl

from cnequity.adapters.baostock.corporate_actions import fetch_corporate_actions_baostock
from cnequity.adapters.cninfo.announcements import (
    CNINFO_SOURCE_REVISION,
    fetch_announcement_index,
    fetch_announcement_index_range,
)
from cnequity.adapters.eastmoney.corporate_actions import fetch_corporate_actions_eastmoney
from cnequity.adapters.eastmoney.corporate_actions_migration import (
    fetch_corporate_actions_eastmoney_migrated_bj,
    migrated_bj_request_scope,
)
from cnequity.adapters.eastmoney.earnings_disclosure import (
    _backfill_report_dates,
    fetch_earnings_disclosure_schedule,
)
from cnequity.adapters.tdx_protocol.client import (
    CORPORATE_ACTIONS_BACKFILL_START,
    fetch_corporate_actions,
)
from cnequity.adapters.ths.corporate_actions import fetch_corporate_actions_ths
from cnequity.config import Config
from cnequity.domain.schemas import with_provenance
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.registry import register_step
from cnequity.quality.failover import (
    failover_spec,
    snapshot_corporate_actions_backup,
    snapshot_corporate_actions_tdx_backup,
)
from cnequity.steps.common import (
    fetch_incremental_daily,
    instrument_metadata,
    load_symbols,
)
from cnequity.steps.http_common import run_incremental_fetched, verify_raw_archive, write_fetched

# TDX xdxr is per-symbol (backfill); EastMoney datacenter supports ex-date filter (daily).
_CANONICAL_BACKFILL = "tdx_protocol"
_CANONICAL_DAILY = "eastmoney"
_CORPORATE_ACTIONS_CHUNK_TASK = "corporate_actions_chunk"
_MIN_EARNINGS_SCHEDULE_SYMBOLS_PER_PERIOD = 100
_CNINFO_CHECKPOINT_TTL_DAYS = 7


logger = logging.getLogger(__name__)


def _cninfo_checkpoint(config: Config, dataset: str):
    """Stable checkpoint path for a resumable CNINFO backfill."""
    return config.meta_root / "checkpoints" / f"{dataset}.json"


def _record_cninfo_metrics(config: Config, run_id: str, dataset: str, metrics: dict) -> None:
    """Persist source/page metrics in the run manifest without making them a gate."""
    try:
        manifest = Manifest(config.manifest_path)
        manifest.record_performance_metrics(run_id, dataset, metrics)
    except Exception as exc:  # noqa: BLE001 — telemetry must not lose data
        logger.warning("%s: unable to persist CNINFO metrics: %s", dataset, exc)


def _cninfo_checkpoint_options(config: Config, dataset: str) -> dict:
    """Resolve the production checkpoint policy for one CNINFO feed.

    Running checkpoints are useful only when they are tied to the provider
    contract and bounded in age.  Keep the defaults here (rather than relying
    on adapter call defaults) so production step calls cannot accidentally
    omit the source revision/TTL when a new adapter is introduced.
    """
    ttl = getattr(config, "cninfo_checkpoint_ttl_days", _CNINFO_CHECKPOINT_TTL_DAYS)
    if ttl is not None:
        ttl = int(ttl)
        if ttl < 0:
            raise ValueError("cninfo_checkpoint_ttl_days must be >= 0")
    configured_revision = getattr(config, "cninfo_source_revision", None)
    options = {
        "checkpoint_ttl_days": ttl,
        "source_revision": str(configured_revision or CNINFO_SOURCE_REVISION),
        # A normal run refreshes completed slices so same-date corrections are
        # observed; an explicit config override can opt into a resume cache.
        "refresh": bool(getattr(config, "cninfo_checkpoint_refresh", True)),
    }
    if getattr(config, "meta_root", None) is not None:
        options["checkpoint_path"] = _cninfo_checkpoint(config, dataset)
    return options


def _fetch_cninfo_single(
    fetcher,
    day: date,
    config: Config,
    metrics: dict,
    *,
    dataset: str = "announcement_index",
    findings: list[dict] | None = None,
) -> pl.DataFrame:
    """Call a CNINFO adapter with metrics while keeping lightweight test doubles compatible."""
    try:
        parameters = inspect.signature(fetcher).parameters
    except (TypeError, ValueError):
        parameters = {}
    accepts_var_kwargs = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
    )
    options = {
        **_cninfo_checkpoint_options(config, dataset),
        "config": config,
        "metrics": metrics,
        "findings": findings,
    }
    label = "announcement" if dataset == "announcement_index" else "regulatory"
    options.update(
        {
            "run_id": metrics.get("run_id"),
            "request_scope": f"range:{label}:{day.isoformat()}:{day.isoformat()}",
        }
    )
    kwargs = {
        key: value for key, value in options.items() if accepts_var_kwargs or key in parameters
    }
    return fetcher(day, **kwargs)


def _cninfo_range_backfill(
    config: Config,
    trade_date: date,
    run_id: str,
    dataset: str,
    fetch_range,
    *,
    date_col: str,
    floor: date,
    findings: list[dict] | None = None,
) -> dict:
    """Use one range-aware CNINFO walk behind the normal day-stage helper.

    ``walk_day_backfill`` still skips already-curated sessions and flushes
    staged rows in bounded chunks. The adapter call is made once per run so a
    busy disclosure day can recursively split its date range instead of
    repeating a fragile deep page walk for every session.
    """
    from cnequity.steps.common import walk_day_backfill

    start = getattr(config, "_backfill_start", None) or floor
    end = min(getattr(config, "_backfill_end", None) or trade_date, trade_date)
    metrics: dict = {"run_id": run_id}
    cached: dict[str, pl.DataFrame] = {}
    label = "announcement" if dataset == "announcement_index" else "regulatory"
    request_scope = f"range:{label}:{start.isoformat()}:{end.isoformat()}"

    def fetch_one(day: date) -> pl.DataFrame:
        if "frame" not in cached:
            # Keep the source observation tied to this run even when a
            # lightweight range adapter accepts an explicit run_id instead of
            # reading it from the metrics object.  Signature filtering below
            # preserves narrow offline doubles for archive-disabled tests.
            options = {
                "config": config,
                "metrics": metrics,
                "run_id": run_id,
                "request_scope": request_scope,
                "findings": findings,
            }
            options.update(_cninfo_checkpoint_options(config, dataset))
            try:
                parameters = inspect.signature(fetch_range).parameters
            except (TypeError, ValueError):
                parameters = {}
            accepts_var_kwargs = any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
            )
            if not accepts_var_kwargs:
                options = {key: value for key, value in options.items() if key in parameters}
            cached["frame"] = fetch_range(start, end, **options)
            _record_cninfo_metrics(config, run_id, dataset, metrics)
        frame = cached["frame"]
        if frame.is_empty() or date_col not in frame.columns:
            return pl.DataFrame()
        return frame.filter(pl.col(date_col) == day)

    def publish(part: pl.DataFrame, batch_id: str) -> object:
        # CNINFO range adapters archive each POST response before returning the
        # normalized frame.  Verification happens immediately before the
        # writer boundary; a missing/captureless receipt therefore leaves no
        # staging artifact to be mistaken for a resumable success.
        evidence = (
            verify_raw_archive(
                config,
                dataset,
                run_id,
                source="cninfo",
                request_scope=request_scope,
            )
            if config.should_archive_raw(dataset)
            else None
        )
        return write_fetched(
            config,
            run_id,
            dataset,
            part,
            source="cninfo",
            batch_id=batch_id,
            raw_archive_evidence=evidence,
        )

    result = walk_day_backfill(
        config,
        trade_date,
        run_id,
        dataset,
        fetch_one,
        source="cninfo",
        date_col=date_col,
        floor=floor,
        calendar_days=True,
        publish_fn=publish,
    )
    if metrics:
        result["metrics"] = metrics
    if findings:
        updates = result.setdefault("context_updates", {})
        updates["audit_findings"] = [*(updates.get("audit_findings") or []), *findings]
        result["status"] = "warning"
    return result


def _delisted_windows(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
    exchanges: tuple[str, ...],
) -> dict[str, tuple[date, date]]:
    """Scope an explicit repair source to delisted symbols and listing windows."""
    metadata = instrument_metadata(config)
    if metadata.is_empty() or "delist_date" not in metadata.columns:
        return {}
    symbol_set = set(symbols)
    suffixes = tuple(f".{exchange}" for exchange in exchanges)
    windows: dict[str, tuple[date, date]] = {}
    for row in metadata.iter_rows(named=True):
        symbol = str(row.get("symbol") or "")
        if (
            symbol not in symbol_set
            or not symbol.endswith(suffixes)
            or row.get("delist_date") is None
        ):
            continue
        symbol_start = max(start, row["list_date"]) if row.get("list_date") else start
        symbol_end = min(end, row["delist_date"]) if row.get("delist_date") else end
        if symbol_start <= symbol_end:
            windows[symbol] = (symbol_start, symbol_end)
    return windows


def _delisted_sh_sz_windows(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, tuple[date, date]]:
    return _delisted_windows(config, symbols, start, end, ("SH", "SZ"))


def _delisted_bj_windows(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, tuple[date, date]]:
    return _delisted_windows(config, symbols, start, end, ("BJ",))


def _validate_earnings_schedule_snapshot(df: pl.DataFrame) -> pl.DataFrame:
    """Reject a non-empty but obviously truncated report-period snapshot."""
    if df.is_empty():
        return df
    required = {"symbol", "report_period"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(
            "earnings_disclosure_schedule: response is missing required column(s): "
            + ", ".join(missing)
        )
    counts = (
        df.unique(subset=["symbol", "report_period"])
        .group_by("report_period")
        .agg(pl.len().alias("_symbol_count"))
        .filter(pl.col("_symbol_count") < _MIN_EARNINGS_SCHEDULE_SYMBOLS_PER_PERIOD)
    )
    if not counts.is_empty():
        details = ", ".join(
            f"{row['report_period']}={row['_symbol_count']}" for row in counts.iter_rows(named=True)
        )
        raise RuntimeError(
            "earnings_disclosure_schedule: incomplete report-period snapshot; each "
            f"observed period needs at least {_MIN_EARNINGS_SCHEDULE_SYMBOLS_PER_PERIOD} "
            f"unique symbol(s) ({details})"
        )
    return df


@register_step("corporate_actions", group="core", depends_on=["instruments"])
def step_corporate_actions(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    rl = config.tdx_rate_limit_spec()
    backfill = getattr(config, "_backfill", False)
    findings: list[dict] = []
    failed_symbols: list[str] = []

    if backfill:
        symbols = list(
            context.get("_retry_symbols")
            or getattr(config, "_backfill_symbols", None)
            or load_symbols(config)
        )
        batch_id = context.get("_batch_id")
        manifest = Manifest(config.manifest_path) if batch_id else None

        if manifest is not None:
            manifest.set_batch_symbols(run_id, batch_id, symbols)

        completed_symbols: set[str] = set()
        if manifest is not None and context.get("_retry_symbols"):
            for row in manifest.get_batches_for_run(run_id):
                if row["task_id"] != _CORPORATE_ACTIONS_CHUNK_TASK or row["status"] != "success":
                    continue
                completed_symbols.update(json.loads(row["symbols_json"] or "[]"))
        remaining_symbols = [symbol for symbol in symbols if symbol not in completed_symbols]

        def on_progress(done: int, total: int) -> None:
            if manifest is not None and (done % 50 == 0 or done == total):
                try:
                    manifest.touch_batch_heartbeat(run_id, batch_id)
                except Exception as exc:  # noqa: BLE001 — heartbeat is auxiliary
                    logger.warning(
                        "corporate_actions: heartbeat update failed at %d/%d: %s",
                        done,
                        total,
                        exc,
                    )

        if config.failover_enabled and config.failover_backfill_snapshots:
            # Best-effort: this writes an EastMoney snapshot for cross-source
            # audit, not the canonical rows. It must never decide whether the
            # backfill runs — when EastMoney changed its filter grammar the
            # raise from here aborted the whole step before TDX, the actual
            # primary, was contacted at all.
            try:
                snapshot_corporate_actions_backup(
                    config, trade_date=trade_date, run_id=run_id, backfill=True
                )
            except Exception as exc:  # noqa: BLE001 — audit artifact, not the data
                logger.warning(
                    "corporate_actions: backup snapshot failed (%s: %s); "
                    "continuing with the canonical TDX fetch",
                    type(exc).__name__,
                    exc,
                )
        frames: list[pl.DataFrame] = []
        failed_symbols: list[str] = []
        batch_size = max(1, config.batch_size)
        for chunk_index in range(0, len(remaining_symbols), batch_size):
            chunk = remaining_symbols[chunk_index : chunk_index + batch_size]
            chunk_number = chunk_index // batch_size
            chunk_batch_id = f"{batch_id or 'batch-0'}-chunk-{chunk_number:04d}"
            chunk_scope = f"chunk:{chunk_batch_id}"
            try:
                df_chunk = fetch_corporate_actions(
                    trade_date,
                    symbols=chunk,
                    backfill=True,
                    rate_limit=rl,
                    allow_mock=config.tdx_allow_mock,
                    primary_only=True,
                    config=config,
                    run_id=run_id,
                    on_progress=lambda done, total, offset=chunk_index: on_progress(
                        offset + done, len(remaining_symbols)
                    ),
                    fail_loud=True,
                    allow_empty=True,
                    request_scope=chunk_scope,
                )
            except Exception as exc:  # noqa: BLE001 — preserve completed chunks for retry
                failed_symbols.extend(chunk)
                logger.warning(
                    "corporate_actions chunk %d failed for %d symbols: %s",
                    chunk_number,
                    len(chunk),
                    exc,
                )
                continue

            if not df_chunk.is_empty():
                frames.append(df_chunk)
            if manifest is not None:
                # The fetch and staging happen before this success receipt. If
                # the process dies earlier, the parent batch remains retryable;
                # an unreceipted chunk is safely fetched again.
                staged_chunk = with_provenance(
                    df_chunk,
                    source=_CANONICAL_BACKFILL,
                    data_version="v1",
                )
                write_fetched(
                    config,
                    run_id,
                    "corporate_actions",
                    staged_chunk,
                    source=_CANONICAL_BACKFILL,
                    batch_id=chunk_batch_id,
                    raw_archive_evidence=(
                        verify_raw_archive(
                            config,
                            "corporate_actions",
                            run_id,
                            source=_CANONICAL_BACKFILL,
                            request_scope=chunk_scope,
                        )
                        if config.should_archive_raw("corporate_actions")
                        else None
                    ),
                )
                manifest.start_batch(
                    run_id,
                    chunk_batch_id,
                    task_id=_CORPORATE_ACTIONS_CHUNK_TASK,
                    dataset="corporate_actions",
                    symbols=chunk,
                    window_start=getattr(config, "_backfill_start", None).isoformat()
                    if getattr(config, "_backfill_start", None)
                    else None,
                    window_end=getattr(config, "_backfill_end", None).isoformat()
                    if getattr(config, "_backfill_end", None)
                    else trade_date.isoformat(),
                    blocks_compaction=False,
                )
                manifest.finish_batch(
                    run_id,
                    chunk_batch_id,
                    "success",
                    rows_read=df_chunk.height,
                    rows_written=df_chunk.height,
                )

        if frames:
            df = pl.concat(frames, how="diagonal_relaxed")
        else:
            df = pl.DataFrame()
        # Stamp the canonical TDX rows before optionally appending Baostock.
        # ``with_provenance`` preserves an adapter-provided source column, so
        # the two repair sources remain auditable after concatenation.
        if not df.is_empty():
            df = with_provenance(df, source=_CANONICAL_BACKFILL, data_version="v1")

        if getattr(config, "_corporate_actions_baostock_repair", False):
            if not config.sources.get("baostock", False):
                raise RuntimeError(
                    "corporate_actions: --baostock-repair requires the baostock source"
                )
            repair_start = (
                getattr(config, "_backfill_start", None) or CORPORATE_ACTIONS_BACKFILL_START
            )
            repair_end = getattr(config, "_backfill_end", None) or trade_date
            repair_windows = _delisted_sh_sz_windows(
                config,
                symbols,
                repair_start,
                repair_end,
            )
            repair_symbols = sorted(repair_windows)
            repair_scope = f"repair:baostock:{repair_start.isoformat()}:{repair_end.isoformat()}"
            logger.info(
                "corporate_actions: Baostock repair scoped to %d delisted SH/SZ symbol(s)",
                len(repair_symbols),
            )
            if repair_symbols:
                repair_df, repair_failed = fetch_corporate_actions_baostock(
                    repair_symbols,
                    repair_start,
                    repair_end,
                    config=config,
                    run_id=run_id,
                    symbol_windows=repair_windows,
                    request_scope=repair_scope,
                )
            else:
                repair_df, repair_failed = pl.DataFrame(), []
            failed_symbols.extend(repair_failed)
            if not repair_df.is_empty():
                repair_df = with_provenance(repair_df, source="baostock", data_version="v1")
                if manifest is not None:
                    repair_batch_id = f"{batch_id or 'batch-0'}-baostock-repair"
                    write_fetched(
                        config,
                        run_id,
                        "corporate_actions",
                        repair_df,
                        source="baostock",
                        batch_id=repair_batch_id,
                        raw_archive_evidence=(
                            verify_raw_archive(
                                config,
                                "corporate_actions",
                                run_id,
                                source="baostock",
                                request_scope=repair_scope,
                            )
                            if config.should_archive_raw("corporate_actions")
                            else None
                        ),
                    )
                    manifest.start_batch(
                        run_id,
                        repair_batch_id,
                        task_id="corporate_actions_baostock_repair",
                        dataset="corporate_actions",
                        symbols=repair_symbols,
                        window_start=repair_start.isoformat(),
                        window_end=repair_end.isoformat(),
                        blocks_compaction=False,
                    )
                    manifest.finish_batch(
                        run_id,
                        repair_batch_id,
                        "success",
                        rows_read=repair_df.height,
                        rows_written=repair_df.height,
                    )
                df = (
                    repair_df
                    if df.is_empty()
                    else pl.concat([df, repair_df], how="diagonal_relaxed")
                )
        if getattr(config, "_corporate_actions_ths_repair", False):
            if not config.sources.get("ths_bonus", False):
                raise RuntimeError("corporate_actions: --ths-repair requires the ths_bonus source")
            repair_start = (
                getattr(config, "_backfill_start", None) or CORPORATE_ACTIONS_BACKFILL_START
            )
            repair_end = getattr(config, "_backfill_end", None) or trade_date
            repair_windows = _delisted_bj_windows(config, symbols, repair_start, repair_end)
            repair_symbols = sorted(repair_windows)
            repair_scope = f"repair:ths:{repair_start.isoformat()}:{repair_end.isoformat()}"
            logger.info(
                "corporate_actions: THS repair scoped to %d delisted BJ symbol(s)",
                len(repair_symbols),
            )
            if repair_symbols:
                repair_df, repair_failed = fetch_corporate_actions_ths(
                    repair_symbols,
                    repair_start,
                    repair_end,
                    config=config,
                    run_id=run_id,
                    symbol_windows=repair_windows,
                    request_scope=repair_scope,
                )
            else:
                repair_df, repair_failed = pl.DataFrame(), []
            failed_symbols.extend(repair_failed)
            if not repair_df.is_empty():
                repair_df = with_provenance(repair_df, source="ths", data_version="v1")
                if manifest is not None:
                    repair_batch_id = f"{batch_id or 'batch-0'}-ths-repair"
                    write_fetched(
                        config,
                        run_id,
                        "corporate_actions",
                        repair_df,
                        source="ths",
                        batch_id=repair_batch_id,
                        raw_archive_evidence=(
                            verify_raw_archive(
                                config,
                                "corporate_actions",
                                run_id,
                                source="ths",
                                request_scope=repair_scope,
                            )
                            if config.should_archive_raw("corporate_actions")
                            else None
                        ),
                    )
                    manifest.start_batch(
                        run_id,
                        repair_batch_id,
                        task_id="corporate_actions_ths_repair",
                        dataset="corporate_actions",
                        symbols=repair_symbols,
                        window_start=repair_start.isoformat(),
                        window_end=repair_end.isoformat(),
                        blocks_compaction=False,
                    )
                    manifest.finish_batch(
                        run_id,
                        repair_batch_id,
                        "success",
                        rows_read=repair_df.height,
                        rows_written=repair_df.height,
                    )
                df = (
                    repair_df
                    if df.is_empty()
                    else pl.concat([df, repair_df], how="diagonal_relaxed")
                )
        if getattr(config, "_corporate_actions_eastmoney_bj_repair", False):
            if not config.sources.get("eastmoney", False):
                raise RuntimeError(
                    "corporate_actions: --eastmoney-bj-repair requires the eastmoney source"
                )
            repair_start = (
                getattr(config, "_backfill_start", None) or CORPORATE_ACTIONS_BACKFILL_START
            )
            repair_end = getattr(config, "_backfill_end", None) or trade_date
            repair_windows = _delisted_bj_windows(config, symbols, repair_start, repair_end)
            repair_symbols = sorted(repair_windows)
            repair_scope = migrated_bj_request_scope(repair_symbols, repair_start, repair_end)
            logger.info(
                "corporate_actions: EastMoney migrated-code repair scoped to %d delisted BJ symbol(s)",
                len(repair_symbols),
            )
            if repair_symbols:
                repair_df, repair_failed = fetch_corporate_actions_eastmoney_migrated_bj(
                    repair_symbols,
                    repair_start,
                    repair_end,
                    config=config,
                    run_id=run_id,
                    symbol_windows=repair_windows,
                    request_scope=repair_scope,
                )
            else:
                repair_df, repair_failed = pl.DataFrame(), []
            failed_symbols.extend(repair_failed)
            if not repair_df.is_empty():
                repair_df = with_provenance(
                    repair_df, source="eastmoney_migrated_bj", data_version="v1"
                )
                if manifest is not None:
                    repair_batch_id = f"{batch_id or 'batch-0'}-eastmoney-bj-repair"
                    write_fetched(
                        config,
                        run_id,
                        "corporate_actions",
                        repair_df,
                        source="eastmoney_migrated_bj",
                        batch_id=repair_batch_id,
                        raw_archive_evidence=(
                            verify_raw_archive(
                                config,
                                "corporate_actions",
                                run_id,
                                source="eastmoney_migrated_bj",
                                request_scope=repair_scope,
                            )
                            if config.should_archive_raw("corporate_actions")
                            else None
                        ),
                    )
                    manifest.start_batch(
                        run_id,
                        repair_batch_id,
                        task_id="corporate_actions_eastmoney_bj_repair",
                        dataset="corporate_actions",
                        symbols=repair_symbols,
                        window_start=repair_start.isoformat(),
                        window_end=repair_end.isoformat(),
                        blocks_compaction=False,
                    )
                    manifest.finish_batch(
                        run_id,
                        repair_batch_id,
                        "success",
                        rows_read=repair_df.height,
                        rows_written=repair_df.height,
                    )
                df = (
                    repair_df
                    if df.is_empty()
                    else pl.concat([df, repair_df], how="diagonal_relaxed")
                )
        if failed_symbols:
            failed_symbols = list(dict.fromkeys(failed_symbols))
        if failed_symbols:
            logger.warning(
                "corporate_actions backfill incomplete: %d/%d symbols failed; "
                "successful chunks are staged and will be skipped on retry",
                len(failed_symbols),
                len(symbols),
            )
        canonical_source = _CANONICAL_BACKFILL
    else:
        if not config.sources.get("eastmoney", True):
            raise RuntimeError("corporate_actions daily: eastmoney source disabled in config")
        df, findings = fetch_incremental_daily(
            config,
            "corporate_actions",
            trade_date,
            lambda d: fetch_corporate_actions_eastmoney(
                d,
                backfill=False,
                config=config,
                run_id=run_id,
                request_scope=f"daily:{d.isoformat()}",
            ),
            allow_empty=True,
            date_col="ex_date",
        )
        canonical_source = _CANONICAL_DAILY
        if config.failover_enabled:
            failover = failover_spec(config, "corporate_actions")
            if failover is not None and failover.snapshot_cadence == "daily":
                ex_today = (
                    df.filter(pl.col("ex_date") == trade_date) if df.height else pl.DataFrame()
                )
                # A clean primary day is still an observation that must be
                # checked by the independent TDX peer.  The caller may supply
                # a constrained universe (tests/repair runs); otherwise the
                # normal instrument universe is the only way to distinguish
                # "no actions" from an unqueried peer.
                backup_symbols = list(context.get("symbols") or [])
                if not backup_symbols:
                    backup_symbols = (
                        ex_today["symbol"].unique().to_list()
                        if ex_today.height
                        else load_symbols(config)
                    )
                try:
                    backup_captured = snapshot_corporate_actions_tdx_backup(
                        config,
                        trade_date=trade_date,
                        symbols=backup_symbols,
                        run_id=run_id,
                        rate_limit=rl,
                    )
                    if not backup_captured:
                        findings.append(
                            {
                                "dataset": "corporate_actions",
                                "severity": "warning",
                                "check": "backup_snapshot_unavailable",
                                "message": (
                                    "corporate_actions independent TDX backup snapshot returned "
                                    "no rows; primary data was not marked erroneous"
                                ),
                                "peer_unavailable": True,
                                "retryable": True,
                            }
                        )
                except Exception as exc:  # noqa: BLE001 — peer is best-effort
                    logger.warning(
                        "corporate_actions: independent TDX backup snapshot unavailable "
                        "(%s: %s); primary result remains valid and will be retried",
                        type(exc).__name__,
                        exc,
                    )
                    findings.append(
                        {
                            "dataset": "corporate_actions",
                            "severity": "warning",
                            "check": "backup_snapshot_unavailable",
                            "message": (
                                "corporate_actions independent backup snapshot was unavailable; "
                                "primary data was not marked erroneous"
                            ),
                            "peer_unavailable": True,
                            "retryable": True,
                        }
                    )

    if backfill and not df.is_empty():
        start = getattr(config, "_backfill_start", None) or CORPORATE_ACTIONS_BACKFILL_START
        end = getattr(config, "_backfill_end", None) or trade_date
        if "ex_date" not in df.columns:
            raise RuntimeError("corporate_actions: backfill response has no ex_date column")
        parsed_dates = df.get_column("ex_date").cast(pl.Date, strict=False)
        invalid = (
            parsed_dates.is_null()
            | (parsed_dates < start).fill_null(False)
            | (parsed_dates > end).fill_null(False)
        )
        if int(invalid.sum()):
            raise RuntimeError(
                "corporate_actions: backfill response returned row(s) outside "
                f"requested window {start.isoformat()}..{end.isoformat()}"
            )
        df = df.with_columns(parsed_dates.alias("ex_date"))

    context_updates: dict = {"symbols_to_rebackfill": []}
    if findings:
        context_updates["audit_findings"] = findings
    if df.is_empty():
        # Even an empty critical observation must have a source receipt.  It
        # is not staged, but rejecting a captureless adapter here keeps an
        # enabled archive policy from silently treating "no rows" as proof of
        # a complete source response.
        if not backfill and config.should_archive_raw("corporate_actions"):
            verify_raw_archive(
                config,
                "corporate_actions",
                run_id,
                source=_CANONICAL_DAILY,
                request_scope=f"daily:{trade_date.isoformat()}",
            )
        result = {"rows_read": 0, "rows_written": 0, "context_updates": context_updates}
        if backfill and failed_symbols:
            result["failed_symbols"] = failed_symbols
            result["status"] = "failed"
        return result

    df = with_provenance(df, source=canonical_source, data_version="v1")

    rebackfill: list[str] = []
    if df.height and "symbol" in df.columns and "ex_date" in df.columns:
        today = df.filter(pl.col("ex_date") == trade_date)
        if today.height:
            rebackfill = today["symbol"].unique().to_list()

    context_updates["symbols_to_rebackfill"] = rebackfill
    if backfill and manifest is not None:
        result = {
            "rows_read": df.height,
            "rows_written": df.height,
        }
    else:
        result = write_fetched(
            config,
            run_id,
            "corporate_actions",
            df,
            source=canonical_source,
            raw_archive_evidence=(
                verify_raw_archive(
                    config,
                    "corporate_actions",
                    run_id,
                    source=canonical_source,
                    request_scope=f"daily:{trade_date.isoformat()}",
                )
                if config.should_archive_raw("corporate_actions")
                else None
            ),
        )
    if backfill and failed_symbols:
        result["failed_symbols"] = failed_symbols
        result["status"] = "failed"
    result["context_updates"] = context_updates
    return result


@register_step("earnings_disclosure_schedule", group="fundamentals", depends_on=["instruments"])
def step_earnings_disclosure_schedule(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("earnings_disclosure_schedule: eastmoney source disabled in config")
    # Period-keyed like financial_statement_items (watermark=False): daily runs
    # refresh the open disclosure windows; backfill walks every period 2016+.
    backfill = getattr(config, "_backfill", False)
    df = _validate_earnings_schedule_snapshot(
        fetch_earnings_disclosure_schedule(trade_date, backfill=backfill, config=config)
    )
    missing_periods: set[str] = set()
    if backfill:
        expected = {
            f"{period[:4]}Q{(int(period[5:7]) - 1) // 3 + 1}"
            for period in _backfill_report_dates(
                trade_date,
                start=getattr(config, "_backfill_start", None),
                end=getattr(config, "_backfill_end", None),
            )
        }
        observed = (
            set(df.get_column("report_period").drop_nulls().to_list())
            if not df.is_empty() and "report_period" in df.columns
            else set()
        )
        missing_periods = expected - observed
    if backfill and missing_periods:
        result: dict
        if df.is_empty():
            result = {"rows_read": 0, "rows_written": 0}
        else:
            result = write_fetched(
                config, run_id, "earnings_disclosure_schedule", df, source="eastmoney"
            )
        result["status"] = "warning"
        result["missing_periods"] = len(missing_periods)
        result["context_updates"] = {
            "audit_findings": [
                {
                    "dataset": "earnings_disclosure_schedule",
                    "severity": "warning",
                    "check": "backfill_missing_report_periods",
                    "message": (
                        f"earnings disclosure schedule missing {len(missing_periods)} "
                        f"requested report period(s): {', '.join(sorted(missing_periods)[:8])}"
                    ),
                    "missing_periods": sorted(missing_periods),
                }
            ]
        }
        return result
    if df.is_empty():
        return {"rows_read": 0, "rows_written": 0}
    return write_fetched(config, run_id, "earnings_disclosure_schedule", df, source="eastmoney")


@register_step("announcement_index", group="capital", depends_on=["instruments"])
def step_announcement_index(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("cninfo", True):
        raise RuntimeError("announcement_index: cninfo source disabled in config")
    findings: list[dict] = []
    if getattr(config, "_backfill", False):
        return _cninfo_range_backfill(
            config,
            trade_date,
            run_id,
            "announcement_index",
            fetch_announcement_index_range,
            date_col="announce_date",
            floor=date(2010, 1, 1),
            findings=findings,
        )
    metrics: dict = {"run_id": run_id}
    result = run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "announcement_index",
        lambda d: _fetch_cninfo_single(
            fetch_announcement_index,
            d,
            config,
            metrics,
            dataset="announcement_index",
            findings=findings,
        ),
        source="cninfo",
        date_col="announce_date",
        raw_archive_evidence_factory=lambda: verify_raw_archive(
            config,
            "announcement_index",
            run_id,
            source="cninfo",
            request_scope=f"range:announcement:{trade_date.isoformat()}:{trade_date.isoformat()}",
        ),
    )
    if len(metrics) > 1:
        _record_cninfo_metrics(config, run_id, "announcement_index", metrics)
    result["metrics"] = metrics
    if findings:
        updates = result.setdefault("context_updates", {})
        updates["audit_findings"] = [*(updates.get("audit_findings") or []), *findings]
        result["status"] = "warning"
    return result
