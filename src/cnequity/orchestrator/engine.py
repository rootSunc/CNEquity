from __future__ import annotations

import contextlib
import json
import logging
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any

from cnequity.config import Config, WaveConfig
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.deps import step_execution_levels, validate_steps_registered
from cnequity.orchestrator.init_phases import (
    DEFAULT_INIT_PHASES,
    INIT_PHASE_STEPS,
    current_phase_statuses,
    init_run_complete,
    missing_steps,
    needs_finalize,
    phase_backfill,
    phases_never_started,
    step_backfill,
    step_succeeded,
)
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.registry import get_step
from cnequity.orchestrator.run_lock import DAILY_INGESTION_LOCK, run_lock
from cnequity.steps.common import is_trading_day

logger = logging.getLogger(__name__)

_TRANSIENT_RETRY_MARKERS = (
    "connection",
    "network",
    "server",
    "socket",
    "temporarily unavailable",
    "tdx fetch failed",
    "timed out",
    "timeout",
)


def _is_transient_retry_error(error_message: str | None) -> bool:
    message = (error_message or "").lower()
    return any(marker in message for marker in _TRANSIENT_RETRY_MARKERS)


def _has_partial_failures(result: dict[str, Any]) -> bool:
    """Return whether a step reported an incomplete or failed scope.

    A few source steps report ``failed_symbols`` while intraday/tick steps use
    ``failed_symbol_days``.  Treating either as a warning keeps a step from
    being recorded as successful merely because it staged some rows.  Some
    resumable sweeps instead expose an explicit ``complete`` flag, so a false
    completion flag must use the same warning path.
    """
    for key, value in result.items():
        if key in {"complete", "coverage_complete"}:
            if value is False:
                return True
            continue
        if not (key.startswith("failed") or key in {"aborted", "empty_days", "days_empty"}):
            continue
        if isinstance(value, bool):
            if value:
                return True
        elif isinstance(value, (int, float)):
            if value > 0:
                return True
        elif value:
            return True
    return False


class JobEngine:
    """Wave-based ingestion orchestrator."""

    def __init__(self, config: Config):
        self.config = config
        self.manifest = Manifest(config.manifest_path)

    def run_job(
        self,
        job_name: str,
        trade_date: date | None = None,
        *,
        steps: list[str] | None = None,
        waves: list[WaveConfig] | None = None,
        backfill: bool = False,
        run_id: str | None = None,
        retry_failed_only: bool = False,
        finalize_run: bool = True,
    ) -> dict[str, Any]:
        trade_date = trade_date or shanghai_today()
        self.config._backfill = backfill

        if run_id and retry_failed_only:
            return self._retry_run(run_id, trade_date)

        # Close crashed peers before we start — otherwise cne status / compact
        # keep seeing ghosts, and concurrent baostock jobs pile onto a blacklist.
        self._reconcile_orphans()

        if not backfill and job_name != "init" and not is_trading_day(self.config, trade_date):
            logger.info(
                "Skipping job %s: %s is not a trading day",
                job_name,
                trade_date.isoformat(),
            )
            skip_run_id = run_id or self.manifest.start_run(
                job_name,
                {"trade_date": trade_date.isoformat(), "backfill": backfill},
            )
            self.manifest.finish_run(skip_run_id, "skipped_non_trading_day")
            return {
                "run_id": skip_run_id,
                "status": "skipped_non_trading_day",
                "trade_date": trade_date.isoformat(),
            }

        metadata = {"trade_date": trade_date.isoformat(), "backfill": backfill}
        if backfill:
            metadata["backfill_scope"] = {
                "start": (
                    self.config._backfill_start.isoformat()
                    if getattr(self.config, "_backfill_start", None)
                    else None
                ),
                "end": (
                    self.config._backfill_end.isoformat()
                    if getattr(self.config, "_backfill_end", None)
                    else None
                ),
                "symbols": getattr(self.config, "_backfill_symbols", None),
                "baostock_repair": bool(
                    getattr(self.config, "_corporate_actions_baostock_repair", False)
                ),
                "ths_repair": bool(getattr(self.config, "_corporate_actions_ths_repair", False)),
                "eastmoney_bj_repair": bool(
                    getattr(self.config, "_corporate_actions_eastmoney_bj_repair", False)
                ),
                "bse_tip_repair": bool(getattr(self.config, "_bse_tip_repair", False)),
            }
        lock_name = DAILY_INGESTION_LOCK if job_name.startswith("daily") else None
        with self._optional_job_lock(lock_name):
            if not run_id:
                run_id = self.manifest.start_run(job_name, metadata)
            else:
                merged = self.manifest.get_run_metadata(run_id)
                merged.update(metadata)
                self.manifest.update_run_metadata(run_id, merged)

            wave_list = waves or self.config.daily_waves
            if steps and waves is None:
                wave_list = [WaveConfig(name="targeted", parallel=True, steps=steps)]

            all_steps = [name for wave in wave_list for name in wave.steps]
            validate_steps_registered(all_steps)

            context: dict[str, Any] = {"run_id": run_id, "trade_date": trade_date}
            results: list[dict[str, Any]] = []
            total_read = 0
            total_written = 0
            had_error = False
            had_warning = False
            finalized = False

            try:
                for wave in wave_list:
                    logger.info("Wave %s: %s (parallel=%s)", wave.name, wave.steps, wave.parallel)
                    wave_results, wave_read, wave_written, wave_error, wave_warning = (
                        self._run_wave(wave, wave.steps, trade_date, run_id, context)
                    )
                    results.extend(wave_results)
                    total_read += wave_read
                    total_written += wave_written
                    had_error = had_error or wave_error
                    had_warning = had_warning or wave_warning

                    if "daily_bars" in wave.steps:
                        promoted = self.manifest.promote_running_to_stale(
                            run_id, stale_after_seconds=self.config.batch_stale_seconds
                        )
                        if promoted:
                            logger.warning(
                                "Promoted %s running batch(es) to stale after wave %s",
                                promoted,
                                wave.name,
                            )

                if had_error:
                    status = "failed"
                elif had_warning:
                    status = "warning"
                else:
                    status = "success"
                if finalize_run:
                    self.manifest.finish_run(
                        run_id,
                        status,
                        rows_read=total_read,
                        rows_written=total_written,
                        error_message="one or more steps failed" if had_error else None,
                    )
                    finalized = True
                return {
                    "run_id": run_id,
                    "status": status,
                    "results": results,
                    "rows_read": total_read,
                    "rows_written": total_written,
                }
            finally:
                if finalize_run and not finalized:
                    self._finish_if_still_running(
                        run_id, error_message="interrupted: worker exited without finish_run"
                    )

    def run_step(
        self,
        name: str,
        trade_date: date,
        run_id: str,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run one registered step against *run_id*, recording its batch.

        For CLI paths that execute a single step outside a job (``cne backfill``
        finalizing with compact, ``cne compact``). Calling the step function
        directly skips the manifest bookkeeping, which silently breaks anything
        that reads the batch log — notably staging cleanup, whose readiness test
        is "this run recorded a successful compact".
        """
        return self._run_step(name, trade_date, run_id, context or {})

    def _reconcile_orphans(self) -> dict[str, int]:
        out = self.manifest.reconcile_orphaned_runs(
            stale_after_seconds=self.config.batch_stale_seconds,
            locks_root=self.config.meta_root,
        )
        if out.get("runs_closed"):
            logger.warning(
                "Reconciled %d orphaned running run(s) (%d batch(es)); skipped %d locked",
                out["runs_closed"],
                out["batches_closed"],
                out.get("skipped_locked", 0),
            )
        return out

    def _finish_if_still_running(self, run_id: str, *, error_message: str) -> None:
        run = self.manifest.get_run(run_id)
        if run is not None and run["status"] == "running":
            self.manifest.finish_run(run_id, "failed", error_message=error_message)

    @contextlib.contextmanager
    def _optional_job_lock(self, lock_name: str | None):
        if lock_name is None:
            yield
            return
        with run_lock(self.config.meta_root, lock_name, blocking=False):
            yield

    def _run_wave(
        self,
        wave: WaveConfig,
        step_names: list[str],
        trade_date: date,
        run_id: str,
        context: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], int, int, bool, bool]:
        levels = step_execution_levels(step_names)
        results: list[dict[str, Any]] = []
        total_read = 0
        total_written = 0
        had_error = False
        had_warning = False
        context_lock = threading.Lock()

        def merge_result(result: dict[str, Any]) -> None:
            nonlocal total_read, total_written, had_error, had_warning
            results.append(result)
            total_read += result.get("rows_read", 0)
            total_written += result.get("rows_written", 0)
            step_status = result.get("status", "success")
            had_error = had_error or step_status == "failed"
            had_warning = had_warning or step_status == "warning"
            updates = result.get("context_updates")
            if updates:
                with context_lock:
                    findings = updates.pop("audit_findings", None)
                    if findings:
                        context.setdefault("audit_findings", []).extend(findings)
                    context.update(updates)

        if wave.parallel:
            for level in levels:
                if len(level) == 1:
                    merge_result(self._run_step(level[0], trade_date, run_id, context))
                    continue

                with ThreadPoolExecutor(max_workers=len(level)) as pool:
                    futures = {
                        pool.submit(self._run_step, name, trade_date, run_id, dict(context)): name
                        for name in level
                    }
                    for fut in as_completed(futures):
                        merge_result(fut.result())
        else:
            for level in levels:
                for name in level:
                    merge_result(self._run_step(name, trade_date, run_id, context))

        return results, total_read, total_written, had_error, had_warning

    def _run_step(
        self,
        name: str,
        trade_date: date,
        run_id: str,
        context: dict[str, Any],
        *,
        retry_of: list[str] | None = None,
    ) -> dict[str, Any]:
        entry = get_step(name)
        uses_worker_batches = entry.requires_workers
        batch_id = str(uuid.uuid4())
        if not uses_worker_batches:
            # A warning is both retryable and a data-integrity gate. The step
            # may have staged a valid subset, but compact must wait until the
            # missing scope has been retried rather than advancing its
            # dataset watermark over a hole.
            self.manifest.start_batch(
                run_id,
                batch_id,
                task_id=name,
                dataset=name,
                blocks_compaction=False,
            )

        t0 = time.perf_counter()
        try:
            # Internal step metadata is passed through a shallow copy so long-
            # running non-worker steps can refresh their own batch heartbeat
            # without exposing the batch id as user-facing context.
            step_context = dict(context)
            step_context["_batch_id"] = batch_id
            out = entry.fn(self.config, trade_date, run_id, step_context)
            elapsed = time.perf_counter() - t0
            step_status = out.pop("status", "success")
            if step_status == "success" and _has_partial_failures(out):
                step_status = "warning"
            if step_status not in ("success", "warning", "failed"):
                raise ValueError(f"step {name} returned invalid status {step_status!r}")
            if not uses_worker_batches:
                physical_dataset = out.get("dataset")
                if physical_dataset and physical_dataset != name:
                    self.manifest.set_batch_dataset(run_id, batch_id, physical_dataset)
                self.manifest.finish_batch(
                    run_id,
                    batch_id,
                    step_status,
                    rows_read=out.get("rows_read", 0),
                    rows_written=out.get("rows_written", 0),
                    error_message=(
                        None
                        if step_status == "success"
                        else f"step completed with status={step_status}"
                    ),
                )
                if step_status == "success" and retry_of:
                    self.manifest.supersede_batches(
                        run_id,
                        retry_of,
                        superseded_by=batch_id,
                    )
            logger.info(
                "Step %s %s in %.1fs (%s rows)",
                name,
                step_status,
                elapsed,
                out.get("rows_written", 0),
            )
            return {
                "step": name,
                "status": step_status,
                "elapsed": elapsed,
                **out,
            }
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            if not uses_worker_batches:
                self.manifest.finish_batch(
                    run_id,
                    batch_id,
                    "failed",
                    error_message=str(exc),
                )
            logger.exception("Step %s failed after %.1fs", name, elapsed)
            return {"step": name, "status": "failed", "error": str(exc), "elapsed": elapsed}

    def _is_init_run(self, run_id: str) -> bool:
        run = self.manifest.get_run(run_id)
        return run is not None and run["job_name"] == "init"

    def _init_phases_list(self, run_id: str | None = None) -> list[str]:
        if run_id:
            meta = self.manifest.get_run_metadata(run_id)
            phases = meta.get("phases")
            if phases:
                return list(phases)
        return list(self.config.init_phases or DEFAULT_INIT_PHASES)

    def _missing_init_steps(self, run_id: str) -> list[str]:
        phases = self._init_phases_list(run_id)
        batches = self.manifest.get_batches_for_run(run_id)
        return missing_steps(phases, batches)

    def _run_finalize_steps(
        self,
        run_id: str,
        trade_date: date,
        context: dict[str, Any],
        *,
        force: bool = False,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        batches = self.manifest.get_batches_for_run(run_id)
        for step_name in ("compact", "derive_adj_factors", "derive_industry_index", "audit"):
            if not force and step_succeeded(batches, step_name):
                continue
            result = self._run_step(step_name, trade_date, run_id, context)
            updates = result.get("context_updates")
            if updates:
                findings = updates.pop("audit_findings", None)
                if findings:
                    context.setdefault("audit_findings", []).extend(findings)
                context.update(updates)
            results.append(result)
            batches = self.manifest.get_batches_for_run(run_id)
        return results

    def _merge_retry_context(self, run_id: str, trade_date: date) -> dict[str, Any]:
        context: dict[str, Any] = {"run_id": run_id, "trade_date": trade_date}
        for key, value in self.manifest.get_run_metadata(run_id).items():
            if key not in context:
                context[key] = value
        return context

    def _retry_batch_status(self, run_id: str) -> str:
        incomplete = self.manifest.incomplete_batch_count(run_id)
        if incomplete == 0:
            return "success"
        counts = self.manifest.incomplete_batch_counts_by_status(run_id)
        if counts.get("failed"):
            return "failed"
        if counts.get("warning"):
            return "warning"
        if counts.get("running") or counts.get("stale"):
            return "pending"
        return "failed"

    def _pending_retry_payload(
        self,
        run_id: str,
        *,
        stale_marked: int,
        timeout: dict[str, int],
        retried: int = 0,
        results: list[dict[str, Any]] | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "status": "pending",
            "retried": retried,
            "incomplete_batches": self.manifest.incomplete_batch_count(run_id),
            "incomplete_by_status": self.manifest.incomplete_batch_counts_by_status(run_id),
            "stale_marked_failed": stale_marked,
            "batch_timeout": timeout,
            "results": results or [],
            **extra,
        }

    def _worker_batch_specs(
        self, batches: list, trade_date: date
    ) -> list[tuple[str, list[str], date, date]]:
        specs: list[tuple[str, list[str], date, date]] = []
        for batch in batches:
            symbols = json.loads(batch["symbols_json"] or "[]")
            window_start = batch["window_start"] or trade_date.isoformat()
            window_end = batch["window_end"] or trade_date.isoformat()
            specs.append(
                (
                    batch["batch_id"],
                    symbols,
                    date.fromisoformat(window_start),
                    date.fromisoformat(window_end),
                )
            )
        return specs

    def _retryable_batches_with_worker_budget(self, run_id: str) -> list:
        """Apply the durable cap only where retries reuse one batch identity.

        Worker retries restart the same batch id, so ``retry_count`` is a
        stable logical-attempt budget. Non-worker steps create a new UUID and
        rely on retry lineage to supersede every predecessor after success;
        filtering those predecessors by a per-row count could strand one.
        """
        return [
            batch
            for batch in self.manifest.get_retryable_batches(run_id)
            if not get_step(batch["task_id"]).requires_workers
            or batch["retry_count"] < self.config.max_retries
        ]

    def _exhausted_worker_retry_count(self, run_id: str) -> int:
        return sum(
            1
            for batch in self.manifest.get_retryable_batches(run_id)
            if get_step(batch["task_id"]).requires_workers
            and batch["retry_count"] >= self.config.max_retries
        )

    def _retry_run(
        self, run_id: str, trade_date: date, *, auto_finalize: bool = True
    ) -> dict[str, Any]:
        # Same as run_job entry: close peer zombies before we touch this run.
        # retry used to skip reconcile (early-return before start_run), so
        # crashed valuation/backfill runs sat until the next daily job.
        self._reconcile_orphans()
        with run_lock(self.config.meta_root, run_id):
            retry_passes = 0
            total_retried = 0
            all_results: list[dict[str, Any]] = []
            all_missing_steps: list[str] = []
            total_stale_marked = 0
            total_timeout = {"running_to_stale": 0, "stale_to_failed": 0}
            automatic_batch_ids: set[str] | None = None
            while True:
                result = self._retry_run_locked(
                    run_id,
                    trade_date,
                    auto_finalize=auto_finalize,
                    retry_batch_ids=automatic_batch_ids,
                )
                total_retried += int(result.get("retried", 0))
                all_results.extend(result.get("results", []))
                all_missing_steps.extend(result.get("missing_steps", []))
                total_stale_marked += int(result.get("stale_marked_failed", 0))
                for key in total_timeout:
                    total_timeout[key] += int(result.get("batch_timeout", {}).get(key, 0))
                if result.get("retried", 0):
                    retry_passes += 1
                if result["status"] not in {"failed", "warning"}:
                    break
                remaining = self._retryable_batches_with_worker_budget(run_id)
                automatic_batch_ids = {
                    batch["batch_id"]
                    for batch in remaining
                    if get_step(batch["task_id"]).requires_workers
                    and _is_transient_retry_error(batch["error_message"])
                }
                if not automatic_batch_ids:
                    break
                time.sleep(self.config.retry_backoff_seconds)
            result["retried"] = total_retried
            result["retry_passes"] = retry_passes
            result["results"] = all_results
            result["missing_steps"] = list(dict.fromkeys(all_missing_steps))
            result["stale_marked_failed"] = total_stale_marked
            result["batch_timeout"] = total_timeout
            result["retry_exhausted"] = self._exhausted_worker_retry_count(run_id)
            return result

    def _retry_run_locked(
        self,
        run_id: str,
        trade_date: date,
        *,
        auto_finalize: bool = True,
        retry_batch_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        run_meta = self.manifest.get_run_metadata(run_id)
        # The caller (cne retry) has no session date of its own — it passes
        # whatever run_job() defaulted to, which is today. A run retried after
        # its trade_date has rolled over must still fetch the session it was
        # started for, not today's, or a backfill for a past date silently
        # requests data that has not been published yet and never lands.
        stored_trade_date = run_meta.get("trade_date")
        if stored_trade_date:
            trade_date = date.fromisoformat(stored_trade_date)
        self.config._backfill = bool(run_meta.get("backfill"))
        scope = run_meta.get("backfill_scope") or {}
        for attr, key in (
            ("_backfill_start", "start"),
            ("_backfill_end", "end"),
            ("_backfill_symbols", "symbols"),
        ):
            value = scope.get(key)
            if key in ("start", "end") and value:
                value = date.fromisoformat(value)
            setattr(self.config, attr, value)
        self.config._corporate_actions_baostock_repair = bool(scope.get("baostock_repair", False))
        self.config._corporate_actions_ths_repair = bool(scope.get("ths_repair", False))
        self.config._corporate_actions_eastmoney_bj_repair = bool(
            scope.get("eastmoney_bj_repair", False)
        )
        self.config._bse_tip_repair = bool(scope.get("bse_tip_repair", False))
        timeout = self.manifest.advance_batch_timeouts(
            run_id,
            stale_after_seconds=self.config.batch_stale_seconds,
        )
        stale_marked = timeout["running_to_stale"] + timeout["stale_to_failed"]
        failed = self._retryable_batches_with_worker_budget(run_id)
        if retry_batch_ids is not None:
            failed = [batch for batch in failed if batch["batch_id"] in retry_batch_ids]
        missing_init = (
            self._missing_init_steps(run_id)
            if retry_batch_ids is None and self._is_init_run(run_id)
            else []
        )

        if not failed and not missing_init:
            incomplete = self.manifest.incomplete_batch_count(run_id)
            if incomplete > 0:
                exhausted = self._exhausted_worker_retry_count(run_id)
                status = self._retry_batch_status(run_id)
                if exhausted and status in {"failed", "warning"}:
                    if auto_finalize:
                        self.manifest.finish_run(run_id, status)
                    return {
                        "run_id": run_id,
                        "status": status,
                        "retried": 0,
                        "retry_exhausted": exhausted,
                        "incomplete_batches": incomplete,
                        "incomplete_by_status": (
                            self.manifest.incomplete_batch_counts_by_status(run_id)
                        ),
                        "stale_marked_failed": stale_marked,
                        "batch_timeout": timeout,
                        "results": [],
                    }
                return self._pending_retry_payload(
                    run_id, stale_marked=stale_marked, timeout=timeout
                )

            batches = self.manifest.get_batches_for_run(run_id)
            phases = self._init_phases_list(run_id)
            if auto_finalize and self._is_init_run(run_id) and needs_finalize(phases, batches):
                context = self._merge_retry_context(run_id, trade_date)
                fin_results = self._run_finalize_steps(run_id, trade_date, context)
                status = self._retry_batch_status(run_id)
                if auto_finalize:
                    self.manifest.finish_run(run_id, status)
                return {
                    "run_id": run_id,
                    "status": status,
                    "retried": 0,
                    "stale_marked_failed": stale_marked,
                    "batch_timeout": timeout,
                    "results": fin_results,
                }
            # All batches already success but the parent never called finish_run
            # (crash after the last step). Close the zombie so status is truthful.
            if auto_finalize:
                self.manifest.finish_run(run_id, "success")
            return {
                "run_id": run_id,
                "status": "success",
                "retried": 0,
                "stale_marked_failed": stale_marked,
                "batch_timeout": timeout,
            }

        context = self._merge_retry_context(run_id, trade_date)

        self.manifest.increment_batch_retry_counts(
            run_id,
            [batch["batch_id"] for batch in failed if get_step(batch["task_id"]).requires_workers],
        )

        worker_batches_by_step: dict[str, list] = defaultdict(list)
        step_batches_by_step: dict[str, list] = defaultdict(list)
        for batch in failed:
            step_name = batch["task_id"]
            if get_step(step_name).requires_workers:
                worker_batches_by_step[step_name].append(batch)
            else:
                step_batches_by_step[step_name].append(batch)

        results: list[dict[str, Any]] = []
        retried = len(failed)
        init_phases = self._init_phases_list(run_id) if self._is_init_run(run_id) else []

        for step_name, worker_batches in worker_batches_by_step.items():
            context["_retry_batch_specs"] = self._worker_batch_specs(worker_batches, trade_date)
            previous_backfill = self.config._backfill
            if init_phases:
                self.config._backfill = step_backfill(step_name, init_phases)
            try:
                results.append(self._run_step(step_name, trade_date, run_id, context))
            finally:
                self.config._backfill = previous_backfill
                context.pop("_retry_batch_specs", None)

        for step_name, batches in step_batches_by_step.items():
            previous_backfill = self.config._backfill
            if init_phases:
                self.config._backfill = step_backfill(step_name, init_phases)
            if step_name == "corporate_actions":
                retry_symbols: list[str] = []
                for batch in batches:
                    retry_symbols.extend(json.loads(batch["symbols_json"] or "[]"))
                if retry_symbols:
                    context["_retry_symbols"] = list(dict.fromkeys(retry_symbols))
            try:
                results.append(
                    self._run_step(
                        step_name,
                        trade_date,
                        run_id,
                        context,
                        retry_of=[batch["batch_id"] for batch in batches],
                    )
                )
            finally:
                self.config._backfill = previous_backfill
                context.pop("_retry_symbols", None)

        if missing_init:
            for step in missing_init:
                prev_backfill = self.config._backfill
                self.config._backfill = step_backfill(step, init_phases)
                try:
                    results.append(self._run_step(step, trade_date, run_id, context))
                finally:
                    self.config._backfill = prev_backfill
            retried += len(missing_init)

        if auto_finalize and self.manifest.incomplete_batch_count(run_id) == 0:
            # A retry may have staged new rows after an earlier compact/finalize
            # attempt. Re-run the finalize chain so curated data and coverage
            # receipts cannot lag behind the now-successful fetch.
            results.extend(self._run_finalize_steps(run_id, trade_date, context, force=True))

        status = self._retry_batch_status(run_id)
        if auto_finalize and status != "pending":
            self.manifest.finish_run(run_id, status)
        payload: dict[str, Any] = {
            "run_id": run_id,
            "status": status,
            "retried": retried,
            "missing_steps": missing_init,
            "stale_marked_failed": stale_marked,
            "batch_timeout": timeout,
            "results": results,
        }
        if status == "pending":
            payload["incomplete_batches"] = self.manifest.incomplete_batch_count(run_id)
            payload["incomplete_by_status"] = self.manifest.incomplete_batch_counts_by_status(
                run_id
            )
        return payload

    def _finalize_init_run(
        self,
        run_id: str,
        phase_results: list[dict[str, Any]],
        *,
        rows_read: int = 0,
        rows_written: int = 0,
    ) -> str:
        phases = self._init_phases_list(run_id)
        batches = self.manifest.get_batches_for_run(run_id)
        current = current_phase_statuses(phases, batches)
        complete = init_run_complete(phases, batches)
        incomplete = self.manifest.incomplete_batch_count(run_id) > 0
        status = "success" if complete and not incomplete else "failed"
        error_message = None if status == "success" else "one or more init steps are incomplete"
        self.manifest.finish_run(
            run_id,
            status,
            rows_read=rows_read,
            rows_written=rows_written,
            error_message=error_message,
        )
        meta = self.manifest.get_run_metadata(run_id)
        meta["phase_results"] = phase_results
        meta["current_phase_status"] = current
        self.manifest.update_run_metadata(run_id, meta)
        return status

    def _execute_init_phases(
        self,
        run_id: str,
        trade_date: date,
        phases: list[str],
        *,
        keep_going: bool = False,
    ) -> dict[str, Any]:
        phase_results: list[dict[str, Any]] = []
        total_read = 0
        total_written = 0

        for phase in phases:
            steps = INIT_PHASE_STEPS.get(phase, [])
            if not steps:
                continue
            backfill = phase_backfill(phase)
            logger.info("Init phase %s: %s", phase, steps)
            result = self.run_job(
                "init",
                trade_date,
                steps=steps,
                backfill=backfill,
                run_id=run_id,
                finalize_run=False,
            )
            phase_results.append({"phase": phase, **result})
            total_read += result.get("rows_read", 0)
            total_written += result.get("rows_written", 0)
            if result["status"] == "failed" and not keep_going:
                logger.error("Init phase %s failed; stopping remaining phases", phase)
                break

        status = self._finalize_init_run(
            run_id,
            phase_results,
            rows_read=total_read,
            rows_written=total_written,
        )
        return {"run_id": run_id, "status": status, "phases": phase_results}

    def resume_init(
        self,
        trade_date: date | None = None,
        *,
        run_id: str | None = None,
        keep_going: bool = False,
    ) -> dict[str, Any]:
        trade_date = trade_date or shanghai_today()
        if not run_id:
            latest = self.manifest.latest_incomplete_init_run()
            if latest is None:
                raise RuntimeError(
                    "No incomplete init run found. Start a new init with `cne init`."
                )
            run_id = latest["run_id"]

        run = self.manifest.get_run(run_id)
        if run is None:
            raise RuntimeError(f"Unknown run_id: {run_id}")
        if run["job_name"] != "init":
            raise RuntimeError(f"Run {run_id} is not an init job (job_name={run['job_name']})")

        phases = self._init_phases_list(run_id)
        meta = self.manifest.get_run_metadata(run_id)
        # Pick the original run's history window back up unless this invocation
        # named one, so the resumed phases fetch the same depth as the ones that
        # already ran rather than a lake with two different floors.
        recorded = meta.get("history_start")
        if recorded and not getattr(self.config, "_backfill_start", None):
            self.config._backfill_start = date.fromisoformat(str(recorded))
        meta["resumed_at"] = shanghai_today().isoformat()
        self.manifest.update_run_metadata(run_id, meta)

        logger.info("Resuming init run %s", run_id)
        retry_result = self._retry_run(run_id, trade_date, auto_finalize=False)

        batches = self.manifest.get_batches_for_run(run_id)
        to_run = phases_never_started(phases, batches)
        phase_results: list[dict[str, Any]] = list(meta.get("phase_results") or [])
        total_read = retry_result.get("rows_read", 0)
        total_written = retry_result.get("rows_written", 0)

        for phase in to_run:
            steps = INIT_PHASE_STEPS.get(phase, [])
            backfill = phase_backfill(phase)
            logger.info("Init resume phase %s: %s", phase, steps)
            result = self.run_job(
                "init",
                trade_date,
                steps=steps,
                backfill=backfill,
                run_id=run_id,
                finalize_run=False,
            )
            phase_results.append({"phase": phase, **result})
            total_read += result.get("rows_read", 0)
            total_written += result.get("rows_written", 0)
            if result["status"] == "failed" and not keep_going:
                break

        batches = self.manifest.get_batches_for_run(run_id)
        if needs_finalize(phases, batches) and self.manifest.incomplete_batch_count(run_id) == 0:
            context = self._merge_retry_context(run_id, trade_date)
            fin_results = self._run_finalize_steps(run_id, trade_date, context)
            phase_results.append(
                {
                    "phase": "phase4_finalize",
                    "status": "failed"
                    if any(r.get("status") == "failed" for r in fin_results)
                    else "success",
                    "results": fin_results,
                }
            )

        status = self._finalize_init_run(
            run_id,
            phase_results,
            rows_read=total_read,
            rows_written=total_written,
        )
        return {
            "run_id": run_id,
            "status": status,
            "resumed": True,
            "retry": retry_result,
            "phases": phase_results,
        }

    def run_init_phases(
        self,
        trade_date: date | None = None,
        *,
        resume: bool = False,
        resume_run_id: str | None = None,
        keep_going: bool = False,
    ) -> dict[str, Any]:
        trade_date = trade_date or shanghai_today()
        if resume or resume_run_id:
            return self.resume_init(
                trade_date,
                run_id=resume_run_id,
                keep_going=keep_going,
            )

        phases = self._init_phases_list()
        metadata: dict[str, Any] = {"phases": phases, "trade_date": trade_date.isoformat()}
        # Record the history window with the run, not just on this process's
        # config. A resume days later runs from a fresh CLI invocation, and
        # without this it would silently pick the default depth and spend hours
        # fetching years the operator chose not to have.
        history_start = getattr(self.config, "_backfill_start", None)
        if history_start:
            metadata["history_start"] = history_start.isoformat()
        run_id = self.manifest.start_run("init", metadata)
        return self._execute_init_phases(
            run_id,
            trade_date,
            phases,
            keep_going=keep_going,
        )
