from __future__ import annotations

import logging
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from cnequity.adapters.tdx_protocol.client import fetch_daily_bars, normalize_with_source
from cnequity.config import Config, load_config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.rate_limit import RateLimitSpec
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps.common import BACKFILL_START
from cnequity.storage import StagingWriter

logger = logging.getLogger(__name__)

# (batch_id, symbols, window_start, window_end)
BatchSpec = tuple[str, list[str], date, date]


def _hms(seconds: float) -> str:
    """Compact duration. A backfill runs for hours; `7245.3s` is not readable."""
    total = int(seconds)
    if total >= 3600:
        return f"{total // 3600}h{(total % 3600) // 60:02d}m"
    if total >= 60:
        return f"{total // 60}m{total % 60:02d}s"
    return f"{total}s"


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
    start a backfill ever had. It is not any more: `cne init --since` picks its
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
        "metrics": {
            "requests": 0,
            "pages": 0,
            "cache_hits": 0,
            "fallback_requests": 0,
            "retries": 0,
            "failed_requests": 0,
            "rows_read": 0,
            "rows_written": 0,
            "bytes_read": 0,
            "bytes_written": 0,
            "changed_partitions": 0,
            "request_seconds": 0.0,
            "concurrency_wait_seconds": 0.0,
            "concurrency_peak": 0,
        },
    }


def _require_daily_bar_symbol_coverage(df, symbols: list[str]) -> None:
    """Reject a partial TDX symbol batch before staging any rows.

    ``daily_bars`` is session-dense.  A TDX call can still return a non-empty
    frame when one symbol silently has no rows, which used to mark the whole
    batch successful and advance the run without giving the existing failover
    path a chance to recover that symbol.
    """
    if not symbols:
        return
    observed = set(df.get_column("symbol").unique().to_list()) if df.height else set()
    missing = sorted(set(symbols) - observed)
    if missing:
        preview = ", ".join(missing[:8])
        suffix = "..." if len(missing) > 8 else ""
        raise DailyBarCoverageError(
            f"daily_bars: TDX returned no rows for {len(missing)} requested symbol(s): "
            f"{preview}{suffix}",
            missing_symbols=missing,
        )


class DailyBarCoverageError(RuntimeError):
    """TDX returned a partial symbol batch with an explicit missing scope."""

    def __init__(self, message: str, *, missing_symbols: list[str]):
        super().__init__(message)
        self.missing_symbols = tuple(missing_symbols)


def _failed_symbols_for_error(exc: BaseException, symbols: list[str]) -> list[str]:
    """Return the narrow retry scope when a validator identified one."""
    missing = getattr(exc, "missing_symbols", None)
    if isinstance(missing, (list, tuple, set)):
        scoped = [str(symbol) for symbol in missing if str(symbol) in set(symbols)]
        if scoped:
            return list(dict.fromkeys(scoped))
    return list(symbols)


def _require_daily_bar_date_coverage(df, start: date, end: date) -> None:
    """Reject rows outside the requested window before staging a batch."""
    if df.is_empty():
        return
    if "trade_date" not in df.columns:
        raise RuntimeError("daily_bars: TDX response is missing the trade_date column")
    dates = df.get_column("trade_date").cast(pl.Date, strict=False)
    invalid = dates.is_null() | (dates < start).fill_null(False) | (dates > end).fill_null(False)
    count = int(invalid.sum())
    if count:
        raise RuntimeError(
            f"daily_bars: TDX returned {count} row(s) outside requested window "
            f"{start.isoformat()}..{end.isoformat()}"
        )


def _stage_daily_bar_rows(
    staging_root: Path,
    run_id: str,
    batch_id: str,
    df: pl.DataFrame,
) -> None:
    """Stage valid rows, merging a prior partial attempt for the same batch.

    A coverage failure narrows the retry scope to missing symbols, but the
    rows already returned by TDX are still useful. Keep them in the same
    staging object and merge them with a later retry so recovering one symbol
    cannot overwrite the other symbols from the original partial response.
    """
    if df.is_empty():
        return
    writer = StagingWriter(staging_root)
    path = staging_root / "daily_bars" / f"run_id={run_id}" / f"part-{batch_id}.parquet"
    if path.exists():
        previous = pl.read_parquet(path)
        df = pl.concat([previous, df], how="diagonal_relaxed")
        df = dedupe_by_primary_key(df, "daily_bars")
    writer.write_batch("daily_bars", run_id, batch_id, df)


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
        backfill,
        config_path,
    ) = args
    start = date.fromisoformat(start_iso)
    end = date.fromisoformat(end_iso)
    staging_root = Path(staging_root)
    rl = RateLimitSpec(*rate_limit) if rate_limit else None
    manifest = Manifest(manifest_path) if manifest_path else None

    batch_metrics: dict[str, Any] = {}
    prior_request_retries = 0
    if manifest:
        previous = manifest.get_batch(run_id, batch_id)
        # A worker may start after another process has already committed this
        # batch (for example, a retry racing a pool teardown).  Success is
        # terminal: do not demote it to running, fetch it again, or count its
        # rows twice in the parent aggregate.
        if previous is not None and previous["status"] == "success":
            rows_read = int(previous["rows_read"] or 0)
            rows_written = int(previous["rows_written"] or 0)
            return {
                "rows_read": rows_read,
                "rows_written": rows_written,
                "batch_id": batch_id,
                "failed_symbols": [],
                "already_success": True,
                "metrics": {
                    "rows_read": rows_read,
                    "rows_written": rows_written,
                },
            }
        if previous is not None:
            # Keep the durable worker requeue budget separate from retries
            # performed inside the adapter.  ``fetch_daily_bars`` has always
            # used ``retries`` for request attempts; seeding that counter with
            # the batch retry count made the two kinds impossible to audit.
            batch_metrics["orchestrator_retries"] = int(previous["retry_count"] or 0)
            prior_request_retries = int(previous["request_retry_count"] or 0)
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
            metrics=batch_metrics,
        )
        df = normalize_with_source(df, dataset=dataset)
        _require_daily_bar_date_coverage(df, start, end)
        try:
            _require_daily_bar_symbol_coverage(df, symbols)
        except DailyBarCoverageError:
            _stage_daily_bar_rows(staging_root, run_id, batch_id, df)
            raise
        _stage_daily_bar_rows(staging_root, run_id, batch_id, df)
        batch_metrics["rows_read"] = max(int(batch_metrics.get("rows_read", 0)), df.height)
        batch_metrics["rows_written"] = df.height
        batch_metrics["bytes_written"] = df.estimated_size()
        batch_metrics["changed_partitions"] = 1
        if manifest:
            request_retries = prior_request_retries + max(
                0,
                int(batch_metrics.get("request_retries", batch_metrics.get("retries", 0)) or 0),
            )
            manifest.finish_batch(
                run_id,
                batch_id,
                "success",
                rows_read=df.height,
                rows_written=df.height,
                request_retry_count=request_retries,
            )
        return {
            "rows_read": df.height,
            "rows_written": df.height,
            "batch_id": batch_id,
            "failed_symbols": [],
            "metrics": batch_metrics,
        }
    except Exception as exc:
        failed_scope = _failed_symbols_for_error(exc, symbols)
        if manifest:
            manifest.set_batch_symbols(run_id, batch_id, failed_scope)
            request_retries = prior_request_retries + max(
                0,
                int(batch_metrics.get("request_retries", batch_metrics.get("retries", 0)) or 0),
            )
            manifest.finish_batch(
                run_id,
                batch_id,
                "failed",
                error_message=str(exc),
                request_retry_count=request_retries,
            )
        # Tip windows: step-level clist gap-fill. Multi-day: kline snapshot only
        # (staging gap-fill also happens at the step for failed_symbols).
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
    metrics: dict[str, Any] = {
        "requests": 0,
        "pages": 0,
        "cache_hits": 0,
        "fallback_requests": 0,
        "retries": 0,
        "failed_requests": 0,
        "rows_read": 0,
        "rows_written": 0,
        "bytes_read": 0,
        "bytes_written": 0,
        "changed_partitions": 0,
        "request_seconds": 0.0,
        "concurrency_wait_seconds": 0.0,
        "concurrency_peak": 0,
    }
    rl = config.tdx_rate_limit_spec()
    rate_limit_tuple = (
        (
            rl.state_dir,
            rl.source,
            rl.min_interval,
            rl.lock_timeout,
            rl.concurrency_limit,
            rl.concurrency_state_dir,
            rl.concurrency_lock_timeout,
        )
        if rl
        else None
    )

    def _run_batch(
        batch_id: str,
        batch_symbols: list[str],
        batch_start: date,
        batch_end: date,
    ) -> dict[str, Any]:
        backfill = _window_backfill(config, batch_start)
        previous = manifest.get_batch(run_id, batch_id)
        prior_request_retries = (
            int(previous["request_retry_count"] or 0) if previous is not None else 0
        )
        batch_metrics: dict[str, Any] = {
            "orchestrator_retries": (
                int(previous["retry_count"] or 0) if previous is not None else 0
            )
        }
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
                metrics=batch_metrics,
            )
            df = normalize_with_source(df, dataset=dataset)
            _require_daily_bar_date_coverage(df, batch_start, batch_end)
            try:
                _require_daily_bar_symbol_coverage(df, batch_symbols)
            except DailyBarCoverageError:
                _stage_daily_bar_rows(staging_root, run_id, batch_id, df)
                raise
            _stage_daily_bar_rows(staging_root, run_id, batch_id, df)
            batch_metrics["rows_read"] = max(int(batch_metrics.get("rows_read", 0)), df.height)
            batch_metrics["rows_written"] = df.height
            batch_metrics["bytes_written"] = df.estimated_size()
            batch_metrics["changed_partitions"] = 1
            manifest.finish_batch(
                run_id,
                batch_id,
                "success",
                rows_read=df.height,
                rows_written=df.height,
                request_retry_count=prior_request_retries
                + max(
                    0,
                    int(batch_metrics.get("request_retries", batch_metrics.get("retries", 0)) or 0),
                ),
            )
            return {
                "rows_read": df.height,
                "rows_written": df.height,
                "batch_id": batch_id,
                "failed_symbols": [],
                "metrics": batch_metrics,
            }
        except Exception as exc:
            failed_scope = _failed_symbols_for_error(exc, batch_symbols)
            manifest.set_batch_symbols(run_id, batch_id, failed_scope)
            manifest.finish_batch(
                run_id,
                batch_id,
                "failed",
                error_message=str(exc),
                request_retry_count=prior_request_retries
                + max(
                    0,
                    int(batch_metrics.get("request_retries", batch_metrics.get("retries", 0)) or 0),
                ),
            )
            raise

    def _outcome(had_error: bool) -> dict[str, Any]:
        return {
            "rows_read": total_read,
            "rows_written": total_written,
            "had_error": had_error,
            "failed_symbols": list(dict.fromkeys(failed_symbols)),
            "metrics": dict(metrics),
        }

    def _merge_metrics(result: dict[str, Any]) -> None:
        result_metrics = result.get("metrics") or {}
        if not isinstance(result_metrics, dict):
            result_metrics = {}
        else:
            result_metrics = dict(result_metrics)
        # Lightweight process-pool doubles and legacy workers may return only
        # the common row totals.  Include those once without changing the
        # richer metrics emitted by current workers.
        for key in ("rows_read", "rows_written"):
            result_metrics.setdefault(key, result.get(key, 0))
        for key in metrics:
            if key == "concurrency_peak":
                try:
                    metrics[key] = max(
                        int(metrics.get(key, 0) or 0),
                        int(result_metrics.get(key, 0) or 0),
                    )
                except (TypeError, ValueError):
                    pass
                continue
            try:
                if key in {"request_seconds", "concurrency_wait_seconds"}:
                    metrics[key] += float(result_metrics.get(key, 0.0) or 0.0)
                else:
                    metrics[key] += int(result_metrics.get(key, 0) or 0)
            except (TypeError, ValueError):
                continue

    # A full-market bar sweep is ~54 batches and can run for an hour. Without a
    # line per batch the whole thing is silent until it ends, which is
    # indistinguishable from hung — and the first thing anyone does about a
    # process that looks hung is kill it. Progress is logged from the parent so
    # it is one ordered stream whether the batches ran serially or in a pool.
    done = 0
    started_at = time.monotonic()

    def _progress(batch_symbols: list[str], failed: bool = False) -> None:
        nonlocal done
        done += 1
        elapsed = time.monotonic() - started_at
        remaining = (elapsed / done) * (len(batches) - done) if done else 0.0
        logger.info(
            "%s %d/%d batches%s · %s rows · %s elapsed · ~%s left",
            dataset,
            done,
            len(batches),
            f" ({len(batch_symbols)} symbols FAILED)" if failed else "",
            f"{total_written:,}",
            _hms(elapsed),
            _hms(remaining),
        )

    daily_workers = config.tdx_daily_worker_count()
    executor_kind = config.tdx_daily_executor()
    # A programmatic Config may contain proxy/timeout/source limits that have
    # never been serialized to a TOML file.  A spawned process reconstructed
    # only from ``data_root`` would silently lose those effective settings.
    # Keep such calls in-process; this is still parallel and preserves the
    # exact Config object (including test/offline source doubles).
    if not config.config_path:
        executor_kind = "thread"
    if daily_workers <= 1 or len(batches) == 1:
        had_error = False
        for batch_id, batch_symbols, batch_start, batch_end in batches:
            existing = manifest.get_batch(run_id, batch_id)
            if existing is not None and existing["status"] == "success":
                total_read += int(existing["rows_read"] or 0)
                total_written += int(existing["rows_written"] or 0)
                metrics["rows_read"] += int(existing["rows_read"] or 0)
                metrics["rows_written"] += int(existing["rows_written"] or 0)
                _progress(batch_symbols)
                continue
            try:
                result = _run_batch(batch_id, batch_symbols, batch_start, batch_end)
                total_read += result["rows_read"]
                total_written += result["rows_written"]
                _merge_metrics(result)
                _progress(batch_symbols)
            except Exception as exc:
                had_error = True
                failed_scope = _failed_symbols_for_error(exc, batch_symbols)
                failed_symbols.extend(failed_scope)
                _progress(failed_scope, failed=True)
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
            _window_backfill(config, batch_start),
            str(config.config_path) if config.config_path else "",
        )

    # A caller may resume or replay a run after a worker has already committed
    # a batch.  Never demote that success by submitting the same batch again.
    # Keep the check outside the executor-specific branches so process and
    # thread backends have identical idempotency semantics.
    pending = {batch[0]: batch for batch in batches}
    for batch_id, batch in list(pending.items()):
        existing = manifest.get_batch(run_id, batch_id)
        if existing is None or existing["status"] != "success":
            continue
        total_read += int(existing["rows_read"] or 0)
        total_written += int(existing["rows_written"] or 0)
        metrics["rows_read"] += int(existing["rows_read"] or 0)
        metrics["rows_written"] += int(existing["rows_written"] or 0)
        pending.pop(batch_id, None)
        _progress(batch[1])
    if not pending:
        return _outcome(False)

    if executor_kind == "thread":
        # The vendored TDX client owns a heartbeat thread and a live socket.
        # Forking a process after either has been imported is unsafe on macOS;
        # use one invocation (and therefore one client) per executor thread.
        # Calling the in-process runner also preserves a Config object created
        # by a caller without a config file, which the process boundary cannot
        # serialize faithfully.
        had_error = False
        try:
            with ThreadPoolExecutor(max_workers=min(daily_workers, len(pending))) as pool:
                futures = {
                    pool.submit(_run_batch, batch[0], batch[1], batch[2], batch[3]): batch[0]
                    for batch in pending.values()
                }
                for fut in as_completed(futures):
                    batch_id = futures[fut]
                    batch = pending.pop(batch_id, None)
                    try:
                        result = fut.result()
                        total_read += result["rows_read"]
                        total_written += result["rows_written"]
                        _merge_metrics(result)
                        _progress(batch[1] if batch else [])
                    except Exception as exc:
                        had_error = True
                        if batch is not None:
                            failed_symbols.extend(_failed_symbols_for_error(exc, batch[1]))
                        _progress(batch[1] if batch else [], failed=True)
                        logger.warning("%s batch %s failed: %s", dataset, batch_id, exc)
        except Exception as exc:
            # A thread-pool construction failure is unusual (the normal batch
            # exceptions are handled above), but serially draining the pending
            # work keeps a transient executor failure from losing a run.
            had_error = True
            logger.warning(
                "%s: thread pool failed (%s); retrying %d batch(es) serially",
                dataset,
                exc,
                len(pending),
            )
            for batch in list(pending.values()):
                try:
                    result = _run_batch(batch[0], batch[1], batch[2], batch[3])
                    total_read += result["rows_read"]
                    total_written += result["rows_written"]
                    _merge_metrics(result)
                    pending.pop(batch[0], None)
                    _progress(batch[1])
                except Exception as retry_exc:
                    failed_symbols.extend(_failed_symbols_for_error(retry_exc, batch[1]))
                    pending.pop(batch[0], None)
                    _progress(batch[1], failed=True)
                    logger.warning(
                        "%s batch %s failed on serial retry: %s",
                        dataset,
                        batch[0],
                        retry_exc,
                    )
        return _outcome(had_error)

    had_error = False
    # A worker killed by the OS (memory pressure under load) raises
    # BrokenProcessPool, which poisons the *whole* pool: every not-yet-collected
    # future then fails too, turning one dead batch into a wiped run. Track which
    # batches actually produced a result so the survivors of a broken pool can be
    # retried serially instead of lost with it.
    try:
        futures: dict = {}
        with ProcessPoolExecutor(max_workers=min(daily_workers, len(pending))) as pool:
            for batch in pending.values():
                futures[pool.submit(_worker_fetch_batch, _task_for(batch))] = batch[0]
            for fut in as_completed(futures):
                batch_id = futures[fut]
                try:
                    # ``as_completed`` only yields futures that are already
                    # done, so passing a timeout to ``result`` can never
                    # enforce a wall-clock limit. Liveness is tracked by the
                    # manifest heartbeat and reconciled by the engine/retry
                    # path; keeping a fake timeout here made the failure mode
                    # look protected when a worker was actually hung.
                    result = fut.result()
                    total_read += result["rows_read"]
                    total_written += result["rows_written"]
                    _merge_metrics(result)
                    batch = pending.pop(batch_id, None)
                    _progress(batch[1] if batch else [])
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
                        failed_symbols.extend(_failed_symbols_for_error(exc, batch[1]))
                    _progress(batch[1] if batch else [], failed=True)
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
                metrics["rows_read"] += int(existing["rows_read"] or 0)
                metrics["rows_written"] += int(existing["rows_written"] or 0)
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
                _merge_metrics(result)
                pending.pop(batch_id, None)
            except Exception as exc:
                had_error = True
                failed_symbols.extend(_failed_symbols_for_error(exc, batch[1]))
                logger.warning("%s batch %s failed on serial retry: %s", dataset, batch_id, exc)

    return _outcome(had_error)
