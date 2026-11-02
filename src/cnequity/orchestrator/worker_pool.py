from __future__ import annotations

import contextlib
import json
import logging
import multiprocessing
import multiprocessing.context
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures.process import BrokenProcessPool
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from cnequity.adapters.tdx_protocol.client import fetch_daily_bars, normalize_with_source
from cnequity.config import Config, load_config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.rate_limit import RateLimitSpec
from cnequity.i18n import N_, Message
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.outcomes import SourcePayloadError, error_kind, step_outcome
from cnequity.progress import detail_scope, enable_stack_dumps
from cnequity.progress import hms as _hms
from cnequity.steps.common import BACKFILL_START
from cnequity.storage import StagingWriter

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
    start a backfill ever had. It is not any more: `cne init --since` picks its
    own, and inferring "not a backfill" from that would have made a shallower
    init silently lenient. The orchestrator already knows — it sets
    ``_backfill`` on the config for exactly these phases — so ask it, and keep
    the date test for callers that reach here without the flag set.
    """
    return bool(getattr(config, "_backfill", False)) or start == BACKFILL_START


def _process_pool_context() -> multiprocessing.context.BaseContext:
    """A start method that never forks the orchestrator itself.

    By the time a sweep starts, the parent has already run polars (the tip
    snapshot, earlier steps), so its rayon thread pool exists. ``fork`` copies
    that pool's state but none of its threads, and the child's first parallel
    polars call then waits forever: the workers stay alive, no
    ``BrokenProcessPool`` fires, and the run sits at "still working" with no
    batch ever landing (#56). ``fork`` is the Linux default through Python
    3.13. Workers already rebuild their config from ``config_path``, so a fresh
    interpreter loses nothing.
    """
    # Ask for the context rather than trusting ``get_all_start_methods()``: that
    # list follows ``sys.platform`` at call time, while the contexts a Windows
    # interpreter can build were fixed at import — and has no forkserver.
    try:
        return multiprocessing.get_context("forkserver")
    except ValueError:
        return multiprocessing.get_context("spawn")


class _PoolStalled(BrokenProcessPool):
    """The pool's workers stopped making progress and were killed."""


def _kill_pool_workers(pool) -> None:
    """Stop every worker now; the executor then reports itself broken."""
    kill = getattr(pool, "kill_workers", None)  # Python 3.14+
    if callable(kill):
        kill()
        return
    for proc in list((getattr(pool, "_processes", None) or {}).values()):
        with contextlib.suppress(Exception):
            proc.kill()


def _completed_unless_stalled(
    pool,
    futures: dict,
    manifest: Manifest,
    run_id: str,
    dataset: str,
    stall_seconds: float,
):
    """Yield futures as they finish, and break the pool once it stops moving.

    A worker that hangs without dying never raises ``BrokenProcessPool``, so a
    plain ``as_completed`` waits for it forever — #56 sat at "still working"
    for 23 minutes with no batch landing. Progress is a finished future or a
    batch heartbeat that moved; workers heartbeat before every symbol, and a
    symbol request is bounded at 30 seconds, so a quiet spell of
    ``stall_seconds`` means no worker is getting anywhere. The pool is then
    killed and ``_PoolStalled`` raised, which the caller treats like any broken
    pool: what has not finished is retried in-process.
    """
    if stall_seconds <= 0:
        yield from as_completed(futures)
        return
    poll = min(30.0, max(0.05, stall_seconds / 4))
    remaining = dict.fromkeys(futures)  # ordered: submission order breaks ties
    last_progress = time.monotonic()
    last_beat = None
    while remaining:
        try:
            for fut in as_completed(list(remaining), timeout=poll):
                remaining.pop(fut, None)
                last_progress = time.monotonic()
                yield fut
        except FuturesTimeoutError:
            pass
        if not remaining:
            return
        in_flight = {futures[fut] for fut in remaining}
        rows = [row for row in manifest.get_batches_for_run(run_id) if row["batch_id"] in in_flight]
        beat = max((str(row["heartbeat_at"] or "") for row in rows), default="")
        if beat != last_beat:
            last_beat = beat
            last_progress = time.monotonic()
            continue
        quiet = time.monotonic() - last_progress
        if quiet < stall_seconds:
            continue
        running = sorted(row["batch_id"] for row in rows if row["status"] == "running")
        logger.warning(
            N_(
                '%s: no batch finished and no worker reported progress for %s; killing the worker pool and retrying %d unfinished batch(es) in-process (stuck: %s). If this recurs, set [orchestrator] tdx_daily_backend = "thread".'
            ),
            dataset,
            _hms(quiet),
            len(remaining),
            ", ".join(running[:5]) + (" …" if len(running) > 5 else "") if running else "-",
        )
        _kill_pool_workers(pool)
        raise _PoolStalled(f"{dataset} worker pool stalled for {quiet:.0f}s")


def _announce_drain(dataset: str, futures) -> None:
    """Say why Ctrl-C has not stopped the run yet.

    The pool waits for batches already talking to the source, which takes as
    long as one batch does — tens of seconds for a hundred symbols. Silence
    for that long reads as a hang, and the second Ctrl-C it provokes changes
    nothing but the user's confidence in the lake.
    """
    running = sum(1 for future in futures if future.running())
    if running:
        logger.warning(
            N_(
                "Interrupted: letting %d %s batch(es) already in flight finish so nothing is half-written; this can take up to one batch's time."
            ),
            running,
            dataset,
        )


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

    reason_code = "source_payload_invalid"

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
        raise SourcePayloadError("daily_bars: TDX response is missing the trade_date column")
    dates = df.get_column("trade_date").cast(pl.Date, strict=False)
    invalid = dates.is_null() | (dates < start).fill_null(False) | (dates > end).fill_null(False)
    count = int(invalid.sum())
    if count:
        raise SourcePayloadError(
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
        df = normalize_with_source(df, "tdx_protocol", dataset=dataset)
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
                reason_code=error_kind(exc),
                execution_status=step_outcome("failed", error=exc).execution_status,
                request_retry_count=request_retries,
            )
        # Tip windows: step-level clist gap-fill. Multi-day: kline snapshot only
        # (staging gap-fill also happens at the step for failed_symbols).
        raise


def _describe_in_flight(manifest: Manifest, run_id: str, dataset: str):
    """Name the batch that has gone longest without a heartbeat, for the log.

    Runs on the heartbeat thread; ``Manifest`` opens a connection per call.
    """

    def describe() -> Message | None:
        rows = [
            row
            for row in manifest.get_batches_for_run(run_id)
            if row["dataset"] == dataset and row["status"] == "running"
        ]
        if not rows:
            return None
        now = datetime.now(timezone.utc)

        def quiet_for(row) -> float:
            try:
                stamp = datetime.fromisoformat(str(row["heartbeat_at"] or row["started_at"]))
                return max(0.0, (now - stamp).total_seconds())
            except (TypeError, ValueError):  # missing or naive stamp
                return 0.0

        oldest = max(rows, key=quiet_for)
        try:
            symbols = [str(symbol) for symbol in json.loads(oldest["symbols_json"] or "[]")]
        except ValueError:
            symbols = []
        return Message(
            N_("%s: %d batch(es) in flight; %s last reported progress %s ago (%s)"),
            dataset,
            len(rows),
            oldest["batch_id"],
            _hms(quiet_for(oldest)),
            ", ".join(symbols[:3]) + (" …" if len(symbols) > 3 else "") if symbols else "-",
        )

    return describe


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
    While it runs, each "still working" heartbeat also names the batch that has
    been quiet longest, so a stuck sweep says where it is stuck.
    """
    describe = _describe_in_flight(Manifest(config.manifest_path), run_id, dataset)
    with detail_scope(describe):
        return _fetch_daily_bars_parallel(
            config, symbols, start, end, run_id, dataset, batch_specs=batch_specs
        )


def _fetch_daily_bars_parallel(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
    run_id: str,
    dataset: str,
    *,
    batch_specs: list[BatchSpec] | None,
) -> dict[str, Any]:
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
            df = normalize_with_source(df, "tdx_protocol", dataset=dataset)
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
                reason_code=error_kind(exc),
                execution_status=step_outcome("failed", error=exc).execution_status,
                request_retry_count=prior_request_retries
                + max(
                    0,
                    int(batch_metrics.get("request_retries", batch_metrics.get("retries", 0)) or 0),
                ),
            )
            raise

    def _outcome(had_error: bool) -> dict[str, Any]:
        errors = [
            b
            for b in manifest.get_failed_batches(run_id)
            if b["dataset"] == dataset and b["execution_status"] == "failed"
        ]
        if errors:
            from cnequity.orchestrator.outcomes import WorkerExecutionError

            raise WorkerExecutionError(
                "worker execution failed: " + str(errors[0]["error_message"]),
                errors[0]["reason_code"] or "execution_error",
            )
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

    daily_workers = config.tdx_daily_worker_count()
    executor_kind = config.tdx_daily_executor()
    # A programmatic Config may contain proxy/timeout/source limits that have
    # never been serialized to a TOML file.  A spawned process reconstructed
    # only from ``data_root`` would silently lose those effective settings.
    # Keep such calls in-process; this is still parallel and preserves the
    # exact Config object (including test/offline source doubles).
    if not config.config_path:
        executor_kind = "thread"
    lanes = 1 if daily_workers <= 1 or len(batches) == 1 else min(daily_workers, len(batches))

    # A full-market bar sweep is ~54 batches and can run for an hour. Without a
    # line per batch the whole thing is silent until it ends, which is
    # indistinguishable from hung — and the first thing anyone does about a
    # process that looks hung is kill it. Progress is logged from the parent so
    # it is one ordered stream whether the batches ran serially or in a pool.
    done = 0
    started_at = time.monotonic()
    # Wall clock since the start over batches done is a bad estimator while the
    # lanes are still filling: the first `lanes` batches land almost together,
    # one batch-latency in, and the denominator also carries whatever one-off
    # setup ran before the sweep. On the measured 53-batch sweep that printed
    # ~34m, then ~17m, ~11m, ~8m for a run that took 10m. Time the lanes
    # instead — from the moment the first full round drained — and offer no
    # number until there is a round to measure.
    first_round_at: float | None = None
    announced_preview = False

    def _expose_batch(batch_id: str) -> None:
        """Make a sealed daily-bar batch readable by symbol before compact."""
        nonlocal announced_preview
        if dataset != "daily_bars":
            return
        try:
            from cnequity.research.preview import publish_daily_bar_preview

            symbols_ready = publish_daily_bar_preview(config, run_id, batch_id)
        except Exception as exc:  # noqa: BLE001 — a preview miss must not fail a sealed batch
            logger.warning("daily_bars preview skipped for %s: %s", batch_id, exc)
            return
        if not symbols_ready:
            return
        if not announced_preview:
            announced_preview = True
            logger.info(
                "daily_bars: finished symbols can be loaded before this sweep ends, "
                'for example load("daily_bars", symbols=[%r]). '
                "Adjusted prices follow once adj_factors covers these sessions.",
                symbols_ready[0],
            )

    def _progress(batch_symbols: list[str], failed_symbols: list[str] | None = None) -> None:
        nonlocal done, first_round_at
        done += 1
        now = time.monotonic()
        if done == lanes:
            first_round_at = now
        eta = ""
        if first_round_at is not None and done > lanes:
            per_batch = (now - first_round_at) / (done - lanes)
            if per_batch > 0:
                eta = Message(N_(" · ~%s left"), _hms(per_batch * (len(batches) - done)))
        if failed_symbols is None:
            note = ""
        elif batch_symbols:
            # The whole batch used to be reported as FAILED whatever the actual
            # scope was, so one symbol TDX had no rows for read as a hundred
            # lost names — on a sweep that then recovers most of them.
            note = Message(N_(" (%d/%d symbols failed)"), len(failed_symbols), len(batch_symbols))
        else:
            note = Message(N_(" (batch failed)"))
        logger.info(
            N_("%s %d/%d batches%s · %s rows · %s elapsed%s"),
            dataset,
            done,
            len(batches),
            note,
            f"{total_written:,}",
            _hms(now - started_at),
            eta,
        )

    # What the sweep is about to cost, before it starts costing it. TDX bills
    # per symbol and not per session, so `--start D --end D` is the same
    # whole-universe sweep as a multi-year window — the surprise behind every
    # "I only asked for one day and it ran for half an hour". Saying the batch
    # count up front also makes the first progress line, which cannot arrive
    # until a whole batch lands, stop being the first sign of life.
    window_start = min(batch[2] for batch in batches)
    window_end = max(batch[3] for batch in batches)
    logger.info(
        N_("%s: %s symbol(s) over %s..%s → %d batch(es) of up to %d on %d lane(s)"),
        dataset,
        f"{sum(len(batch[1]) for batch in batches):,}",
        window_start.isoformat(),
        window_end.isoformat(),
        len(batches),
        max(len(batch[1]) for batch in batches),
        lanes,
    )
    if window_start == window_end and getattr(config, "_backfill", False):
        # One request returns up to 800 bars, so this sweep costs the same
        # whether it asks for one session or several years. Someone filling a
        # month a day at a time pays for thirty sweeps and receives what one
        # would have returned. The daily job is excluded: its one-session
        # window is the point, not an oversight.
        logger.info(
            "%s: this is a one-session window, and the sweep is priced per symbol — "
            "the same %d request(s) would also return everything back to %s. "
            "Backfill a range in one go rather than a day at a time.",
            dataset,
            sum(len(batch[1]) for batch in batches),
            BACKFILL_START.isoformat(),
        )

    # Write the plan down before the first worker starts. Until this existed,
    # a batch that was cancelled or never reached left no row at all, and every
    # gate downstream reads the ledger: `compact` saw "nothing unfinished" and
    # published two batches of fifty-two, `step_succeeded` called the step done
    # and `cne status` called the dataset fresh. A queued row is what turns
    # those answers from attendance into coverage.
    manifest.queue_batches(
        run_id,
        dataset,
        dataset,
        [(batch[0], batch[1], batch[2].isoformat(), batch[3].isoformat()) for batch in batches],
    )

    if daily_workers <= 1 or len(batches) == 1:
        had_error = False
        for batch_id, batch_symbols, batch_start, batch_end in batches:
            existing = manifest.get_batch(run_id, batch_id)
            if existing is not None and existing["status"] == "success":
                total_read += int(existing["rows_read"] or 0)
                total_written += int(existing["rows_written"] or 0)
                metrics["rows_read"] += int(existing["rows_read"] or 0)
                metrics["rows_written"] += int(existing["rows_written"] or 0)
                _expose_batch(batch_id)
                _progress(batch_symbols)
                continue
            try:
                result = _run_batch(batch_id, batch_symbols, batch_start, batch_end)
                total_read += result["rows_read"]
                total_written += result["rows_written"]
                _merge_metrics(result)
                _expose_batch(batch_id)
                _progress(batch_symbols)
            except Exception as exc:
                had_error = True
                failed_scope = _failed_symbols_for_error(exc, batch_symbols)
                failed_symbols.extend(failed_scope)
                _progress(batch_symbols, failed_scope)
                if error_kind(exc) not in {
                    "source_transient",
                    "source_unavailable",
                    "source_payload_invalid",
                    "capability_limit",
                }:
                    raise
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
        _expose_batch(batch_id)
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
                futures: dict = {}
                try:
                    for batch in pending.values():
                        futures[pool.submit(_run_batch, batch[0], batch[1], batch[2], batch[3])] = (
                            batch[0]
                        )
                    for fut in as_completed(futures):
                        batch_id = futures[fut]
                        batch = pending.pop(batch_id, None)
                        try:
                            result = fut.result()
                            total_read += result["rows_read"]
                            total_written += result["rows_written"]
                            _merge_metrics(result)
                            _expose_batch(batch_id)
                            _progress(batch[1] if batch else [])
                        except Exception as exc:
                            had_error = True
                            failed_scope = (
                                _failed_symbols_for_error(exc, batch[1])
                                if batch is not None
                                else []
                            )
                            failed_symbols.extend(failed_scope)
                            _progress(batch[1] if batch else [], failed_scope)
                            logger.warning(N_("%s batch %s failed: %s"), dataset, batch_id, exc)
                except (KeyboardInterrupt, SystemExit):
                    for future in futures:
                        future.cancel()
                    # Do not return control while in-flight workers can still
                    # write staging. Cancel queued work, then wait only for the
                    # lanes that had already entered their source call.
                    _announce_drain(dataset, futures)
                    pool.shutdown(wait=True, cancel_futures=True)
                    raise
        except Exception as exc:
            # A thread-pool construction failure is unusual (the normal batch
            # exceptions are handled above), but serially draining the pending
            # work keeps a transient executor failure from losing a run.
            had_error = True
            logger.warning(
                N_("%s: thread pool failed (%s); retrying %d batch(es) serially"),
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
                    _expose_batch(batch[0])
                    _progress(batch[1])
                except Exception as retry_exc:
                    failed_scope = _failed_symbols_for_error(retry_exc, batch[1])
                    failed_symbols.extend(failed_scope)
                    pending.pop(batch[0], None)
                    _progress(batch[1], failed_scope)
                    logger.warning(
                        N_("%s batch %s failed on serial retry: %s"),
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
        with ProcessPoolExecutor(
            max_workers=min(daily_workers, len(pending)),
            mp_context=_process_pool_context(),
            initializer=enable_stack_dumps,
        ) as pool:
            try:
                for batch in pending.values():
                    futures[pool.submit(_worker_fetch_batch, _task_for(batch))] = batch[0]
                for fut in _completed_unless_stalled(
                    pool,
                    futures,
                    manifest,
                    run_id,
                    dataset,
                    float(getattr(config, "batch_stall_seconds", 0) or 0),
                ):
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
                        _expose_batch(str(result.get("batch_id") or batch_id))
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
                        failed_scope = (
                            _failed_symbols_for_error(exc, batch[1]) if batch is not None else []
                        )
                        failed_symbols.extend(failed_scope)
                        _progress(batch[1] if batch else [], failed_scope)
                        logger.warning(N_("%s batch %s failed: %s"), dataset, batch_id, exc)
            except (KeyboardInterrupt, SystemExit):
                for future in futures:
                    future.cancel()
                _announce_drain(dataset, futures)
                pool.shutdown(wait=True, cancel_futures=True)
                raise
    except BrokenProcessPool as exc:
        # The pool died mid-run, or was killed for stalling. Whatever is still
        # pending never got a parent verdict — retry in-process. A child may
        # already have finish_batch(success) before the pool went down;
        # re-running start_batch would demote that success via INSERT OR
        # REPLACE. Trust the manifest when it already says success and skip
        # the re-fetch.
        if not isinstance(exc, _PoolStalled):
            logger.warning(
                N_("%s: worker pool broke (likely OOM under load); retrying %d batch(es) serially"),
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
                logger.warning(N_("%s batch %s failed on serial retry: %s"), dataset, batch_id, exc)

    return _outcome(had_error)
