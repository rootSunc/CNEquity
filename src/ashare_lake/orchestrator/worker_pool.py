from __future__ import annotations

import logging
from concurrent.futures import ProcessPoolExecutor, TimeoutError, as_completed
from concurrent.futures.process import BrokenProcessPool
from datetime import date
from pathlib import Path
from typing import Any

from ashare_lake.adapters.tdx_protocol.client import fetch_daily_bars, normalize_with_source
from ashare_lake.config import Config, load_config
from ashare_lake.domain.rate_limit import RateLimitSpec
from ashare_lake.orchestrator.manifest import Manifest
from ashare_lake.quality.failover import snapshot_daily_bars_backup
from ashare_lake.steps.common import BACKFILL_START
from ashare_lake.storage import StagingWriter

logger = logging.getLogger(__name__)

# (batch_id, symbols, window_start, window_end)
BatchSpec = tuple[str, list[str], date, date]


def _symbol_batch_id(start: date, end: date, index: int) -> str:
    """Unique batch id per symbol chunk and fetch window within a run."""
    return f"{start.isoformat()}_{end.isoformat()}-batch-{index}"


def _worker_tdx_config(
    config_path: str,
    staging_root: str,
    *,
    allow_mock: bool,
    backfill: bool,
) -> Config:
    if config_path:
        cfg = load_config(Path(config_path))
    else:
        cfg = Config(data_root=Path(staging_root).parent)
    cfg.tdx_allow_mock = allow_mock
    cfg._backfill = backfill
    return cfg


def _window_backfill(config: Config, start: date) -> bool:
    """Whether this batch is fetching history, which decides how strict it is.

    Strict means a page failure mid-pagination raises instead of keeping the
    pages that did arrive — the difference between a batch that fails loudly
    and a symbol quietly missing its older years.

    The window start alone used to decide this, and 2016-01-01 was the only
    start a backfill ever had. It is not any more: `asl init --since` picks its
    own, and inferring "not a backfill" from that would have made a shallower
    init silently lenient. The orchestrator already knows — it sets
    ``_backfill`` on the config for exactly these phases — so ask it, and keep
    the date test for callers that reach here without the flag set.
    """
    return bool(getattr(config, "_backfill", False)) or start == BACKFILL_START


def _empty_pool_result() -> dict[str, Any]:
    return {
        "rows_read": 0,
        "rows_written": 0,
        "had_error": False,
        "failed_symbols": [],
    }


def _worker_fetch_batch(args: tuple) -> dict[str, Any]:
    (
        symbols,
        start_iso,
        end_iso,
        staging_root,
        dataset,
        run_id,
        batch_id,
        rate_limit,
        allow_mock,
        manifest_path,
        failover_enabled,
        backfill,
        config_path,
    ) = args
    start = date.fromisoformat(start_iso)
    end = date.fromisoformat(end_iso)
    staging_root = Path(staging_root)
    rl = RateLimitSpec(*rate_limit) if rate_limit else None
    manifest = Manifest(manifest_path) if manifest_path else None

    if manifest:
        manifest.start_batch(
            run_id,
            batch_id,
            task_id=dataset,
            dataset=dataset,
            symbols=symbols,
            window_start=start_iso,
            window_end=end_iso,
        )

    tdx_cfg = _worker_tdx_config(
        config_path, staging_root, allow_mock=allow_mock, backfill=backfill
    )

    def _heartbeat() -> None:
        if manifest:
            manifest.touch_batch_heartbeat(run_id, batch_id)

    try:
        _heartbeat()
        df = fetch_daily_bars(
            symbols,
            start,
            end,
            rate_limit=rl,
            allow_mock=allow_mock,
            backfill=backfill,
            config=tdx_cfg,
            on_heartbeat=_heartbeat,
        )
        df = normalize_with_source(df, dataset=dataset)
        writer = StagingWriter(staging_root)
        writer.write_batch(dataset, run_id, batch_id, df)
        if manifest:
            manifest.finish_batch(
                run_id,
                batch_id,
                "success",
                rows_read=df.height,
                rows_written=df.height,
            )
        return {
            "rows_read": df.height,
            "rows_written": df.height,
            "batch_id": batch_id,
            "failed_symbols": [],
        }
    except Exception as exc:
        if manifest:
            manifest.finish_batch(run_id, batch_id, "failed", error_message=str(exc))
        # Tip windows: step-level clist gap-fill. Multi-day: kline snapshot only
        # (staging gap-fill also happens at the step for failed_symbols).
        if failover_enabled and dataset == "daily_bars" and start < end:
            from ashare_lake.adapters.eastmoney.bars import fetch_daily_bars as fetch_em_bars
            from ashare_lake.domain.schemas import data_version_for, with_provenance
            from ashare_lake.storage.source_snapshots import SnapshotStore

            backup_df = fetch_em_bars(symbols, start, end)
            if backup_df.height:
                version = data_version_for(dataset)
                backup_df = with_provenance(backup_df, source="eastmoney", data_version=version)
                SnapshotStore(Path(staging_root).parent / "meta").write(
                    dataset,
                    backup_df,
                    source="eastmoney",
                    data_version=version,
                    run_id=run_id,
                    batch_id=f"{batch_id}-backup",
                    trade_date=end,
                )
        raise


def fetch_daily_bars_parallel(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
    run_id: str,
    dataset: str = "daily_bars",
    *,
    batch_specs: list[BatchSpec] | None = None,
) -> dict[str, Any]:
    """Fetch daily bars in symbol batches; each batch is recorded in manifest.

    Returns ``had_error`` / ``failed_symbols`` instead of raising so callers can
    route tip gaps through EastMoney clist (ADR-0005) before failing the step.
    """
    if batch_specs:
        batches = batch_specs
    elif not symbols:
        return _empty_pool_result()
    else:
        batch_size = config.batch_size
        batches = [
            (_symbol_batch_id(start, end, i), symbols[i : i + batch_size], start, end)
            for i in range(0, len(symbols), batch_size)
        ]

    if not batches:
        return _empty_pool_result()

    staging_root = config.staging_root
    manifest_path = str(config.manifest_path)
    manifest = Manifest(config.manifest_path)
    total_read = 0
    total_written = 0
    failed_symbols: list[str] = []
    rl = config.tdx_rate_limit_spec()
    rate_limit_tuple = (rl.state_dir, rl.source, rl.min_interval) if rl else None
    stale_seconds = config.batch_stale_seconds

    def _run_batch(
        batch_id: str,
        batch_symbols: list[str],
        batch_start: date,
        batch_end: date,
    ) -> dict[str, Any]:
        backfill = _window_backfill(config, batch_start)
        manifest.start_batch(
            run_id,
            batch_id,
            task_id=dataset,
            dataset=dataset,
            symbols=batch_symbols,
            window_start=batch_start.isoformat(),
            window_end=batch_end.isoformat(),
        )
        try:

            def _heartbeat() -> None:
                manifest.touch_batch_heartbeat(run_id, batch_id)

            _heartbeat()
            df = fetch_daily_bars(
                batch_symbols,
                batch_start,
                batch_end,
                rate_limit=rl,
                allow_mock=config.tdx_allow_mock,
                backfill=backfill,
                config=config,
                on_heartbeat=_heartbeat,
            )
            df = normalize_with_source(df, dataset=dataset)
            writer = StagingWriter(staging_root)
            writer.write_batch(dataset, run_id, batch_id, df)
            manifest.finish_batch(
                run_id,
                batch_id,
                "success",
                rows_read=df.height,
                rows_written=df.height,
            )
            return {
                "rows_read": df.height,
                "rows_written": df.height,
                "batch_id": batch_id,
                "failed_symbols": [],
            }
        except Exception as exc:
            manifest.finish_batch(run_id, batch_id, "failed", error_message=str(exc))
            if config.failover_enabled and dataset == "daily_bars":
                snapshot_daily_bars_backup(
                    config,
                    symbols=batch_symbols,
                    start=batch_start,
                    end=batch_end,
                    run_id=run_id,
                    batch_id=f"{batch_id}-backup",
                )
            raise

    def _outcome(had_error: bool) -> dict[str, Any]:
        return {
            "rows_read": total_read,
            "rows_written": total_written,
            "had_error": had_error,
            "failed_symbols": list(dict.fromkeys(failed_symbols)),
        }

    if config.workers <= 1 or len(batches) == 1:
        had_error = False
        for batch_id, batch_symbols, batch_start, batch_end in batches:
            try:
                result = _run_batch(batch_id, batch_symbols, batch_start, batch_end)
                total_read += result["rows_read"]
                total_written += result["rows_written"]
            except Exception:
                had_error = True
                failed_symbols.extend(batch_symbols)
        return _outcome(had_error)

    def _task_for(batch: tuple) -> tuple:
        batch_id, batch_symbols, batch_start, batch_end = batch
        return (
            batch_symbols,
            batch_start.isoformat(),
            batch_end.isoformat(),
            str(staging_root),
            dataset,
            run_id,
            batch_id,
            rate_limit_tuple,
            config.tdx_allow_mock,
            manifest_path,
            config.failover_enabled,
            _window_backfill(config, batch_start),
            str(config.config_path) if config.config_path else "",
        )

    had_error = False
    # A worker killed by the OS (memory pressure under load) raises
    # BrokenProcessPool, which poisons the *whole* pool: every not-yet-collected
    # future then fails too, turning one dead batch into a wiped run. Track which
    # batches actually produced a result so the survivors of a broken pool can be
    # retried serially instead of lost with it.
    pending = {batch[0]: batch for batch in batches}
    try:
        futures: dict = {}
        with ProcessPoolExecutor(max_workers=min(config.workers, len(batches))) as pool:
            for batch in batches:
                futures[pool.submit(_worker_fetch_batch, _task_for(batch))] = batch[0]
            for fut in as_completed(futures):
                batch_id = futures[fut]
                try:
                    result = fut.result(timeout=stale_seconds)
                    total_read += result["rows_read"]
                    total_written += result["rows_written"]
                    pending.pop(batch_id, None)
                except TimeoutError:
                    had_error = True
                    batch = pending.pop(batch_id, None)
                    if batch is not None:
                        failed_symbols.extend(batch[1])
                    manifest.mark_batch_stale(
                        run_id, batch_id, f"worker result timeout after {stale_seconds}s"
                    )
                    logger.warning(
                        "%s batch %s timed out after %ss; marked stale",
                        dataset,
                        batch_id,
                        stale_seconds,
                    )
                except BrokenProcessPool:
                    # This one poisoned the pool. Leave it (and everything still
                    # pending) for the serial retry below rather than recording it
                    # as a genuine batch failure — BrokenProcessPool is an
                    # Exception subclass, so it must be caught before the generic
                    # handler or the fallback never runs.
                    raise
                except Exception as exc:
                    had_error = True
                    batch = pending.pop(batch_id, None)
                    if batch is not None:
                        failed_symbols.extend(batch[1])
                    logger.warning("%s batch %s failed: %s", dataset, batch_id, exc)
    except BrokenProcessPool:
        # The pool died mid-run. Whatever is still pending never got a parent
        # verdict — retry in-process. A child may already have finish_batch(success)
        # before the OS killed the pool; re-running start_batch would demote that
        # success via INSERT OR REPLACE. Trust the manifest when it already says
        # success and skip the re-fetch.
        logger.warning(
            "%s: worker pool broke (likely OOM under load); retrying %d batch(es) serially",
            dataset,
            len(pending),
        )
        for batch in list(pending.values()):
            batch_id = batch[0]
            existing = manifest.get_batch(run_id, batch_id)
            if existing is not None and existing["status"] == "success":
                total_read += int(existing["rows_read"] or 0)
                total_written += int(existing["rows_written"] or 0)
                pending.pop(batch_id, None)
                logger.info(
                    "%s batch %s already success in manifest; skipping serial re-fetch",
                    dataset,
                    batch_id,
                )
                continue
            try:
                result = _run_batch(batch_id, batch[1], batch[2], batch[3])
                total_read += result["rows_read"]
                total_written += result["rows_written"]
                pending.pop(batch_id, None)
            except Exception as exc:
                had_error = True
                failed_symbols.extend(batch[1])
                logger.warning("%s batch %s failed on serial retry: %s", dataset, batch_id, exc)

    return _outcome(had_error)
