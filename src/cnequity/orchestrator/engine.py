from __future__ import annotations

import contextlib
import json
import logging
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import copy
from datetime import date
from typing import Any

from cnequity.config import Config, WaveConfig
from cnequity.domain.datasets import DATASETS, history_mode_for
from cnequity.domain.market_time import shanghai_today
from cnequity.domain.request_meter import request_meter_scope
from cnequity.i18n import N_, NP_, Message
from cnequity.orchestrator.backfill_scope import capture_backfill_scope, restore_backfill_scope
from cnequity.orchestrator.deps import step_execution_levels, validate_steps_registered
from cnequity.orchestrator.init_phases import (
    FACTOR_PREFETCH_PHASES,
    FACTOR_PREFETCH_STOP_PHASE,
    INIT_PHASE_STEPS,
    blocks_init,
    configured_init_phases,
    current_phase_statuses,
    init_run_complete,
    missing_steps,
    missing_steps_within_phase_order,
    needs_finalize,
    only_non_blocking_owed,
    owed_steps,
    pending_phases,
    phase_backfill,
    scheduled_later,
    step_backfill,
    step_completed,
    step_succeeded,
)
from cnequity.orchestrator.init_phases import expected_steps as init_expected_steps
from cnequity.orchestrator.manifest import QUEUED_BATCH_STATUS, Manifest
from cnequity.orchestrator.outcomes import (
    SOURCE_LIMIT_REASONS,
    CapabilityLimitError,
    InputUnavailableError,
    execution_settled,
    result_is_usable,
    step_outcome,
)
from cnequity.orchestrator.registry import get_step
from cnequity.orchestrator.run_lock import (
    DAILY_INGESTION_LOCK,
    EVENTS_INGESTION_LOCK,
    init_job_lock,
    reentrant_run_lock,
    refuse_overlap_with_init,
    run_lock,
)
from cnequity.progress import step_scope
from cnequity.steps.common import is_trading_day

logger = logging.getLogger(__name__)

# Step outcomes as the terminal names them; the log file keeps these words.
STEP_STATUS_WORDS = (
    NP_("step status", "success"),
    NP_("step status", "warning"),
    NP_("step status", "failed"),
    NP_("step status", "skipped"),
)

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


def _is_transient_retry_error(error_message: str | None, reason_code: str | None = None) -> bool:
    if reason_code is not None:
        return reason_code == "source_transient"
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


def _has_execution_errors(result: dict[str, Any]) -> bool:
    """Judge a phase's attempts, allowing a source-only empty phase to close."""
    steps = result.get("results")
    if steps:
        return any(
            row.get("execution_status") in {"failed", "interrupted"}
            or (row.get("status") == "failed" and row.get("execution_status") is None)
            for row in steps
        )
    return result.get("status") == "failed"


def _failed_step_names(result: dict[str, Any]) -> set[str]:
    """The steps a phase's ``run_job`` result reports as not executed."""
    return {
        str(row["step"])
        for row in result.get("results") or []
        if row.get("step")
        and (
            row.get("execution_status") in {"failed", "interrupted"}
            or (row.get("status") == "failed" and row.get("execution_status") is None)
        )
    }


def job_family(job_name: str) -> str:
    """The scheduling family a job belongs to: ``daily:capital`` -> ``daily``.

    Two things are decided per family rather than per job: which ingestion lock
    the run takes, and whether it is bound to the exchange calendar.
    """
    return job_name.split(":", 1)[0]


#: Families that ingest on the natural calendar rather than on exchange
#: sessions. `validate_config` keeps such a job to calendar-scoped feeds, so
#: running one on a Sunday asks a source only for days it actually publishes.
CALENDAR_JOB_FAMILIES = frozenset({"events"})

_JOB_LOCKS = {
    "daily": DAILY_INGESTION_LOCK,
    "events": EVENTS_INGESTION_LOCK,
}


def _criticality_for_group(group: str) -> str:
    """Map scheduler groups to the run-level criticality contract."""
    if group in {"core", "finalize"}:
        return "core"
    if group == "research":
        return "research"
    return "advisory"


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
        # Here as well as at `cne` start-up: callers such as
        # scripts/run_catchup.py, run by another project's launchd agent at a
        # 256-descriptor limit, drive the engine without either entry point.
        from cnequity.fd_limit import raise_open_file_limit

        raise_open_file_limit()

        if run_id and retry_failed_only:
            return self._retry_run(run_id, trade_date)

        # Close crashed peers before we start — otherwise cne status / compact
        # keep seeing ghosts, and concurrent baostock jobs pile onto a blacklist.
        self._reconcile_orphans()

        family = job_family(job_name)
        if (
            not backfill
            and job_name != "init"
            and family not in CALENDAR_JOB_FAMILIES
            and not is_trading_day(self.config, trade_date)
        ):
            logger.info(
                N_("Skipping job %s: %s is not a trading day"),
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

        wave_list = waves or self.config.daily_waves
        if steps and waves is None:
            wave_list = [WaveConfig(name="targeted", parallel=True, steps=steps)]
        all_steps = [name for wave in wave_list for name in wave.steps]
        validate_steps_registered(all_steps)

        metadata = {"trade_date": trade_date.isoformat(), "backfill": backfill}
        if backfill:
            metadata["backfill_scope"] = capture_backfill_scope(self.config)
        lock_name = _JOB_LOCKS.get(family)
        with contextlib.ExitStack() as stack:
            stack.enter_context(self._optional_job_lock(lock_name))
            # Checked after taking this job's own lock, so an init starting at
            # the same instant sees this job when it checks in turn: at worst
            # both refuse, never both run.
            if family != "init":
                refuse_overlap_with_init(self.config.meta_root)
            if not run_id:
                # Pin the plan to the run. A retry days later must know what
                # this run set out to do even if the process died before
                # reaching half of it, and it must not re-derive that from a
                # config that has been edited since. An init run is resumed
                # from its recorded phases instead, and a caller that joins an
                # existing run keeps that run's original plan.
                run_id = self.manifest.start_run(
                    job_name,
                    {**metadata, "planned_steps": list(dict.fromkeys(all_steps))},
                )
            else:
                self.manifest.mutate_run_metadata(
                    run_id,
                    lambda merged: merged.update(metadata),
                )

            # Proof of life for this run, held until it finishes. Without it a
            # run killed mid-flight leaves `status=running` in the manifest and
            # nothing can tell that row from a run still working: liveness had
            # to be inferred from heartbeat age, so a crashed run stayed
            # "running" for `batch_stale_seconds` — an hour by default — and
            # every command that reads run status was wrong for that hour.
            # `cne run retry --failed-groups` reported nothing to retry; `cne
            # status` showed a ghost. The kernel releases this the moment the
            # process dies, which is exactly the signal that was missing.
            stack.enter_context(self._run_lock(run_id))

            context: dict[str, Any] = {"run_id": run_id, "trade_date": trade_date}
            results: list[dict[str, Any]] = []
            total_read = 0
            total_written = 0
            # What the run had already published before this call. An init
            # phase joins a run its predecessors have been writing into, and
            # this call's own totals start at zero — reporting those alone
            # would walk the run's counters backwards once per phase.
            base_read, base_written = self._published_run_rows(run_id)
            had_error = False
            had_warning = False
            finalized = False

            try:
                for wave in wave_list:
                    logger.info(
                        N_("Wave %s: %s (parallel=%s)"), wave.name, wave.steps, wave.parallel
                    )
                    wave_results, wave_read, wave_written, wave_error, wave_warning = (
                        self._run_wave(wave, wave.steps, trade_date, run_id, context)
                    )
                    results.extend(wave_results)
                    total_read += wave_read
                    total_written += wave_written
                    had_error = had_error or wave_error
                    had_warning = had_warning or wave_warning
                    self.manifest.record_run_progress(
                        run_id, base_read + total_read, base_written + total_written
                    )
                    if any(row.get("reason_code") == "storage_failure" for row in wave_results):
                        break

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

                self._persist_unaudited_findings(run_id, trade_date, results, context)
                if had_error:
                    fallback_status = "failed"
                elif had_warning:
                    # Step-level ``warning`` remains the retry/batch spelling;
                    # the public run contract calls this usable-but-degraded.
                    fallback_status = "warning"
                else:
                    fallback_status = "success"
                status = self._overall_status(run_id, fallback_status)
                if finalize_run:
                    self.manifest.finish_run(
                        run_id,
                        status,
                        rows_read=total_read,
                        rows_written=total_written,
                        error_message=(
                            "one or more core steps failed" if status == "failed" else None
                        ),
                    )
                    finalized = True
                return {
                    "run_id": run_id,
                    "status": status,
                    **self._public_outcome(run_id),
                    "results": results,
                    "rows_read": total_read,
                    "rows_written": total_written,
                }
            except (KeyboardInterrupt, SystemExit):
                # Independent of `finalize_run`. That flag says who closes the
                # run on success — `cne backfill` keeps it open until compact
                # has run — and it was never a statement about interrupts. Read
                # as one, it left every `cne backfill` Ctrl-C with the run and
                # its batches still `running`, unretryable until the hour-long
                # stale window expired. Closing the ledger is the exiting
                # process's job whoever finishes the run.
                self.manifest.interrupt_run(
                    run_id,
                    error_message="interrupted by operator",
                )
                raise
            finally:
                if finalize_run and not finalized:
                    self.manifest.interrupt_run(
                        run_id,
                        error_message="interrupted: worker exited without finish_run",
                    )

    def _persist_unaudited_findings(
        self,
        run_id: str,
        trade_date: date,
        results: list[dict[str, Any]],
        context: dict[str, Any],
    ) -> None:
        """Keep the steps' findings when this run did not write an audit file.

        They travel in ``context`` and only ``run_audit`` used to write them
        out. The scheduled groups run without ``audit`` and the later lake
        audit has its own context, so a fallback a step reported — a cached
        trading_status day, a stale valuation — reached no file at all.
        """
        findings = context.get("audit_findings") or []
        if not findings:
            return
        if any(row.get("step") == "audit" and "error" not in row for row in results):
            return
        from cnequity.quality.audit import persist_step_findings

        persist_step_findings(self.config, run_id, trade_date, findings)

    def run_step(
        self,
        name: str,
        trade_date: date,
        run_id: str,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run one registered step against *run_id*, recording its batch.

        For CLI paths that execute a single step outside a job (``cne backfill``
        finalizing with compact, ``cne run compact``). Calling the step function
        directly skips the manifest bookkeeping, which silently breaks anything
        that reads the batch log — notably staging cleanup, whose readiness test
        is "this run recorded a successful compact".
        The interrupt contract is the same as ``run_job``'s, and for the same
        reason: `cne run compact`, `cne backfill`'s finalize, `cne delisted`
        and `cne maintain` all reach the manifest through here. Without it a
        Ctrl-C during compact left the run `running` until the orphan
        reconciler noticed a minute later — self-healing, but a minute of
        `cne status` describing a process that had already exited.
        """
        try:
            return self._run_step(name, trade_date, run_id, context or {})
        except (KeyboardInterrupt, SystemExit):
            self.manifest.interrupt_run(run_id, error_message="interrupted by operator")
            raise

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

    def _step_criticality(self, name: str, entry: Any, run_id: str) -> str:
        """Return the durable criticality for a step's dataset receipts.

        Init is a core bootstrap contract even when a test or a legacy config
        supplied a custom scheduler group.  Compact and audit are core
        infrastructure. Otherwise a registered dataset takes its support
        level's criticality (``DatasetSpec.support``) in whichever group runs
        it, so a failed ``financial_statement_items`` fails the fundamentals
        run and a failed ``industry_index`` derive only degrades the core one.
        A source-limited attempt still degrades rather than fails at any
        level (``Manifest.aggregate_run_status``).
        """
        try:
            run = self.manifest.get_run(run_id)
        except Exception:  # pragma: no cover - defensive for custom manifests
            run = None
        if run is not None and run["job_name"] == "init":
            return "core"
        if name in {"compact", "audit"}:
            return "core"
        from cnequity.domain.datasets import dataset_criticality

        by_support = dataset_criticality(self._step_dataset(name, {}))
        if by_support is not None:
            return by_support
        return _criticality_for_group(getattr(entry, "group", "advisory"))

    @staticmethod
    def _step_dataset(name: str, out: dict[str, Any]) -> str:
        """Resolve a step's logical output dataset from its result or name."""
        explicit = out.get("dataset")
        if isinstance(explicit, str) and explicit:
            return explicit
        return {
            "derive_adj_factors": "adj_factors",
            "derive_industry_index": "industry_index",
            "derive_futures_continuous": "futures_continuous",
            "derive_option_greeks": "option_greeks",
        }.get(name, name)

    # These steps write their own dataset receipt (in ``steps/finalize.py``)
    # with finer detail than the engine has here — partial-fetch ratios and
    # audit finding counts. The engine only backfills a receipt they could not
    # write themselves. ``compact`` is not in this set: its engine marker uses
    # the ``("compact", "compact")`` key, which ``_compact_locked`` never
    # writes, so there is no double write to suppress.
    _SELF_RECORDING_STEPS = frozenset({"audit", "derive_adj_factors", "derive_industry_index"})

    def _record_step_result(
        self,
        *,
        name: str,
        entry: Any,
        run_id: str,
        status: str,
        out: dict[str, Any] | None = None,
        error: BaseException | None = None,
    ) -> None:
        """Write fetch/stage or derive/audit receipts for one step attempt."""
        out = out or {}
        dataset = self._step_dataset(name, out)
        criticality = self._step_criticality(name, entry, run_id)
        if name == "audit":
            stages = ("audit",)
        elif name == "compact":
            # ``step_compact`` records physical dataset receipts itself. Keep
            # this marker for an empty compact and for an exception before the
            # per-dataset loop starts.
            stages = ("compact",)
            dataset = "compact"
        elif name in {
            "derive_adj_factors",
            "derive_industry_index",
            "derive_futures_continuous",
            "derive_option_greeks",
        }:
            stages = ("derive",)
        else:
            # A step result is the final outcome of both source fetch and
            # writing its staging fragment. Recording both makes status useful
            # even when a worker process is not available to expose a batch.
            stages = ("fetch", "stage")

        logical = step_outcome(status, out, stage=stages[0], error=error).to_dict()
        logical.update(status="skipped" if logical["execution_status"] == "skipped" else status)
        self.manifest.mutate_run_metadata(
            run_id,
            lambda meta: meta.setdefault("step_outcomes", {}).update({name: logical}),
        )
        if logical["execution_status"] == "skipped":
            status = "skipped"

        existing_receipts = [
            self.manifest.get_dataset_result(run_id, dataset, stage) for stage in stages
        ]
        if name in self._SELF_RECORDING_STEPS and all(
            receipt is not None and receipt["status"] == status for receipt in existing_receipts
        ):
            # The step already recorded this outcome (success path, or a
            # failure it caught and recorded before re-raising). Do not
            # overwrite it with the engine's coarser message.  A successful
            # return is not evidence by itself: a compatibility implementation
            # or test double may omit the self-recording call, in which case
            # the engine must backfill the receipt below. A receipt from a
            # previous failed retry is likewise not evidence for this attempt.
            if error is None:
                return
            # Preserve the step's detailed message while upgrading the error
            # classification at the exception boundary.
            for stage, receipt in zip(stages, existing_receipts, strict=True):
                self.manifest.record_dataset_result(
                    run_id,
                    dataset,
                    stage,
                    status,
                    criticality=receipt["criticality"],
                    rows_written=receipt["rows_written"],
                    revision_id=receipt["revision_id"],
                    error_code=receipt["error_code"],
                    error_message=receipt["error_message"],
                    execution_status=logical["execution_status"],
                    coverage_status=logical["coverage_status"],
                    publication_status=receipt["publication_status"],
                    reason_code=logical["reason_code"],
                    usable_result=bool(receipt["usable_result"]),
                )
            return

        error_code = type(error).__name__ if error is not None else None
        error_message = str(error) if error is not None else None
        raw_status = status
        if status not in {"success", "warning", "failed", "skipped", "blocked", "degraded"}:
            status = "failed"
            error_code = error_code or "InvalidStepStatus"
            error_message = error_message or f"invalid step status {raw_status!r}"
        for stage in stages:
            outcome = step_outcome(status, out, stage=stage, error=error)
            self.manifest.record_dataset_result(
                run_id,
                dataset,
                stage,
                status,
                criticality=criticality,
                rows_written=int(out.get("rows_written", 0) or 0),
                error_code=error_code,
                error_message=(
                    error_message
                    or (None if status == "success" else f"step completed with status={status}")
                ),
                execution_status=outcome.execution_status,
                coverage_status=outcome.coverage_status,
                publication_status=outcome.publication_status,
                reason_code=outcome.reason_code,
                usable_result=result_is_usable(out),
            )

    def _overall_status(self, run_id: str, fallback: str) -> str:
        """Resolve legacy batch status plus dataset receipts to public status."""
        aggregate = self.manifest.aggregate_run_status(run_id)
        if aggregate["results"]:
            return aggregate["status"]
        # Runs created before ``dataset_results`` was introduced have no
        # logical receipts. Preserve their outcome while translating the old
        # warning spelling to the public degraded spelling.
        if fallback == "warning":
            return "degraded"
        if fallback in {"success", "failed", "degraded"}:
            return fallback
        return fallback

    def _finish_run_overall(
        self,
        run_id: str,
        fallback: str,
        *,
        rows_read: int = 0,
        rows_written: int = 0,
        error_message: str | None = None,
    ) -> str:
        status = self._overall_status(run_id, fallback)
        self.manifest.finish_run(
            run_id,
            status,
            rows_read=rows_read,
            rows_written=rows_written,
            error_message=error_message if status == "failed" else None,
        )
        return status

    @contextlib.contextmanager
    def _optional_job_lock(self, lock_name: str | None):
        if lock_name is None:
            yield
            return
        with run_lock(self.config.meta_root, lock_name, blocking=False):
            yield

    @contextlib.contextmanager
    def _run_lock(self, run_id: str):
        """Hold one run lock across nested init phase/retry calls."""
        with reentrant_run_lock(self.config.meta_root, run_id):
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
                if any(row.get("reason_code") == "storage_failure" for row in results):
                    break
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
                    if any(row.get("reason_code") == "storage_failure" for row in results):
                        break
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
            # Attempt status preserves retry evidence. Validated staging is
            # selected independently by the publication gate.
            self.manifest.start_batch(
                run_id,
                batch_id,
                task_id=name,
                dataset=name,
                blocks_compaction=False,
            )

        t0 = time.perf_counter()
        try:
            from cnequity.query.parquet_scan import dataset_has_parquet
            from cnequity.storage.read_context import read_root

            step_config, scope_limit = self._backfill_source_scope(name, trade_date)
            missing_inputs = [
                dataset
                for dataset in getattr(entry, "input_datasets", ())
                if not dataset_has_parquet(read_root(self.config, dataset))
            ]
            if missing_inputs:
                raise InputUnavailableError("missing committed input: " + ", ".join(missing_inputs))
            # Internal step metadata is passed through a shallow copy so long-
            # running non-worker steps can refresh their own batch heartbeat
            # without exposing the batch id as user-facing context.
            step_context = dict(context)
            step_context["_batch_id"] = batch_id
            step_context["_init"] = self._is_init_run(run_id)
            # The only line a step used to produce was the one announcing it
            # done, so a twenty-minute fetch and a hang read identically until
            # one of them ended. Name it on the way in as well, and register it
            # so the heartbeat can say which step the silence belongs to.
            logger.info(N_("Step %s starting"), name)
            from cnequity.orchestrator.source_gaps import source_gap_scope

            with (
                step_scope(name),
                source_gap_scope(step_config) as source_gaps,
                request_meter_scope() as request_meter,
            ):
                if name == "daily_bars" and step_context["_init"]:
                    self._recover_init_delisted_bars(run_id, trade_date, step_context)
                out = entry.fn(step_config, trade_date, run_id, step_context)
            if request_meter.requests and "requests" not in out:
                # Only fill what the step did not count itself.
                reported = out.setdefault("metrics", {})
                reported.setdefault("requests", request_meter.requests)
                if request_meter.request_retries:
                    reported.setdefault("request_retries", request_meter.request_retries)
            step_status = out.pop("status", "success")
            if step_status not in {
                "success",
                "warning",
                "failed",
                "skipped",
                "blocked",
                "degraded",
            }:
                raise ValueError(f"step {name} returned invalid status {step_status!r}")
            can_degrade = (
                step_status in {"success", "warning", "degraded"}
                and step_outcome(step_status, out).execution_status == "completed"
            )
            if scope_limit:
                out["coverage_status"] = "partial"
                out["requested_scope"] = scope_limit["requested_scope"]
                out["effective_scope"] = scope_limit["effective_scope"]
                if can_degrade:
                    step_status = "warning"
                    out["reason_code"] = "capability_limit"
                out.setdefault("context_updates", {}).setdefault("audit_findings", []).append(
                    scope_limit
                )
            if source_gaps:
                out["coverage_status"] = "partial"
                if can_degrade:
                    step_status = "warning"
                    out["reason_code"] = "source_scope_incomplete"
                out.setdefault("context_updates", {}).setdefault("audit_findings", []).extend(
                    source_gaps
                )
            recovery = step_context.get("init_delisted_recovery")
            if recovery and recovery.get("coverage_status") == "partial":
                out["coverage_status"] = "partial"
                if can_degrade:
                    step_status = "warning"
                    out["reason_code"] = "delisted_source_unavailable"
                out.setdefault("context_updates", {})["init_delisted_recovery"] = recovery
            elapsed = time.perf_counter() - t0
            if step_status == "success" and _has_partial_failures(out):
                step_status = "warning"
            step_metrics = self._step_metrics(out)
            outcome = step_outcome(
                step_status,
                out,
                stage="audit" if name == "audit" else "compact" if name == "compact" else "fetch",
            )
            out.update(outcome.to_dict())
            if step_status == "failed" and outcome.reason_code in SOURCE_LIMIT_REASONS:
                step_status = "warning"
            request_retry_count = self._request_retry_count(step_metrics)
            if not uses_worker_batches:
                physical_dataset = out.get("dataset")
                if physical_dataset and physical_dataset != name:
                    self.manifest.set_batch_dataset(run_id, batch_id, physical_dataset)
                # A step may warn about its *result* while its *work* is
                # finished — `daily_bars` carrying a tolerated gap is the case
                # this exists for: the rows are staged, the shortfall is in the
                # outstanding ledger, and nothing is waiting to be retried. The
                # batch status drives retry and compaction, so it has to be
                # able to say "settled" while the step still reports a warning.
                # Other source warnings keep an attempt available for an
                # explicit retry. A seal permits publishing its valid facts;
                # it never certifies the unswept scope.
                batch_status = "success" if out.get("batch_settled") else step_status
                self.manifest.finish_batch(
                    run_id,
                    batch_id,
                    batch_status,
                    rows_read=out.get("rows_read", 0),
                    rows_written=out.get("rows_written", 0),
                    error_message=(
                        None
                        if batch_status == "success"
                        else f"step completed with status={step_status}"
                    ),
                    # Worker retries reuse one batch and increment their
                    # durable budget before execution. Non-worker retries use
                    # a fresh batch id, so mark every retry-lineage attempt as
                    # one requeue; summing the ledger counts actual attempts
                    # without turning this value into a retry cap.
                    retry_count=1 if retry_of else None,
                    request_retry_count=request_retry_count,
                    execution_status=outcome.execution_status,
                    reason_code=outcome.reason_code,
                )
                if step_status == "success" and retry_of:
                    self.manifest.supersede_batches(
                        run_id,
                        retry_of,
                        superseded_by=batch_id,
                    )
            self._record_step_result(
                name=name,
                entry=entry,
                run_id=run_id,
                status=step_status,
                out=out,
            )
            self.manifest.record_stage_metrics(
                run_id,
                name,
                elapsed,
                step_metrics,
            )
            logger.info(
                N_("Step %s %s in %.1fs (%s rows)"),
                name,
                Message.word("step status", step_status),
                elapsed,
                out.get("rows_written", 0),
            )
            return {
                "step": name,
                "status": step_status,
                "elapsed": elapsed,
                **out,
            }
        except (KeyboardInterrupt, SystemExit) as exc:
            elapsed = time.perf_counter() - t0
            if not uses_worker_batches:
                self.manifest.finish_batch(
                    run_id,
                    batch_id,
                    "failed",
                    error_message="interrupted by operator",
                    retry_count=1 if retry_of else None,
                )
            self._record_step_result(
                name=name,
                entry=entry,
                run_id=run_id,
                status="failed",
                error=exc,
            )
            self.manifest.record_stage_metrics(run_id, name, elapsed)
            logger.warning(N_("Step %s interrupted after %.1fs"), name, elapsed)
            raise
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            outcome = step_outcome("failed", error=exc)
            if not uses_worker_batches:
                self.manifest.finish_batch(
                    run_id,
                    batch_id,
                    "failed",
                    error_message=str(exc),
                    retry_count=1 if retry_of else None,
                    execution_status=outcome.execution_status,
                    reason_code=outcome.reason_code,
                )
            self._record_step_result(
                name=name,
                entry=entry,
                run_id=run_id,
                status="failed",
                error=exc,
            )
            self.manifest.record_stage_metrics(run_id, name, elapsed)
            from cnequity.storage.revisions import DiskSpaceError

            if outcome.reason_code in SOURCE_LIMIT_REASONS:
                logger.warning(N_("Step %s source unavailable after %.1fs: %s"), name, elapsed, exc)
            elif outcome.execution_status == "skipped":
                logger.warning(N_("Step %s skipped: %s"), name, exc)
            elif isinstance(exc, DiskSpaceError):
                logger.error(N_("Step %s failed after %.1fs: %s"), name, elapsed, exc)
            else:
                logger.exception(N_("Step %s failed after %.1fs"), name, elapsed)
            if outcome.execution_status == "skipped":
                status = "skipped"
            elif outcome.reason_code in SOURCE_LIMIT_REASONS:
                status = "warning"
            else:
                status = "failed"
            return {
                "step": name,
                "status": status,
                "error": str(exc),
                "elapsed": elapsed,
                **outcome.to_dict(),
            }

    def _backfill_source_scope(self, name: str, trade_date: date) -> tuple[Config, dict | None]:
        """Retain the requested scope while avoiding known unservable history."""
        spec = DATASETS.get(name)
        if not getattr(self.config, "_backfill", False) or spec is None:
            return self.config, None
        if history_mode_for(spec) == "snapshot_only":
            raise CapabilityLimitError(
                f"{name}: snapshot source has no historical replay; retained observations "
                "remain readable, and daily collection can accumulate future snapshots"
            )
        start = getattr(self.config, "_backfill_start", None)
        end = getattr(self.config, "_backfill_end", None) or trade_date
        floor = spec.earliest_available(shanghai_today())
        ceiling = spec.source_retired_date
        lo = max(start, floor) if start and floor else start
        hi = min(end, ceiling) if ceiling else end
        if (floor and end < floor) or (lo and lo > hi):
            raise CapabilityLimitError(
                f"{name}: requested {start}..{end} is outside available history "
                f"{floor or 'unbounded'}..{ceiling or 'current'}; retained data remains readable"
            )
        if lo == start and hi == end:
            return self.config, None
        config = copy(self.config)
        config._backfill_start, config._backfill_end = lo, hi
        requested = {"start": start.isoformat() if start else None, "end": end.isoformat()}
        effective = {"start": lo.isoformat() if lo else None, "end": hi.isoformat()}
        finding = {
            "dataset": name,
            "severity": "warning",
            "check": "source_scope_incomplete",
            "message": f"{name}: requested history exceeds source capability; fetching {lo}..{hi}",
            "requested_scope": requested,
            "effective_scope": effective,
        }
        return config, finding

    @staticmethod
    def _step_metrics(out: dict[str, Any]) -> dict[str, Any]:
        """Normalize optional step metrics plus common result counters."""
        metrics = dict(out.get("metrics") or {})
        for key in (
            "requests",
            "pages",
            "cache_hits",
            "fallback_requests",
            "retries",
            "request_retries",
            "failed_requests",
            "rows_read",
            "rows_written",
            "bytes_read",
            "bytes_written",
            "changed_partitions",
            "request_seconds",
            "concurrency_wait_seconds",
            "concurrency_peak",
            "throughput_requests_per_second",
            "source_metrics",
            "peak_memory_bytes",
        ):
            if key in metrics:
                continue
            value = out.get(key)
            if value is not None:
                metrics[key] = value
        # Result payloads often expose fallback work as a symbol list rather
        # than a numeric counter.  Count the request scope without guessing a
        # speedup or an upstream response size.
        if "fallback_requests" not in metrics:
            fallback = out.get("fallback_symbols") or out.get("fallback_symbol_names")
            if isinstance(fallback, (list, tuple, set)):
                metrics["fallback_requests"] = len(fallback)
        # ``retries`` is the established adapter metric name. Keep it in the
        # result for compatibility, while handing the manifest an explicit
        # request-level spelling so it cannot be confused with the
        # orchestrator's batch retry budget.
        if "request_retries" not in metrics and "retries" in metrics:
            metrics["request_retries"] = metrics["retries"]
        return metrics

    @staticmethod
    def _request_retry_count(metrics: dict[str, Any]) -> int:
        """Return only adapter-observed request retries from step metrics."""
        value = metrics.get("request_retries", 0)
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    def _is_init_run(self, run_id: str) -> bool:
        run = self.manifest.get_run(run_id)
        return run is not None and run["job_name"] == "init"

    def _recover_init_delisted_bars(
        self, run_id: str, trade_date: date, context: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Publish scoped recovery before daily bars checks delegated ownership.

        A separate run avoids a circular gate on legacy init retries: the old
        daily-bar ownership warning blocks that run's compact until recovery
        has reached curated storage. Partial recovery remains durable, while
        unresolved targets remain durable coverage findings in the parent.
        """
        from cnequity.steps.bars import in_ingest_universe_symbol
        from cnequity.steps.common import BACKFILL_START
        from cnequity.steps.delisted import (
            backfill_delisted_bars,
            delisted_recovery_covers,
            delisted_recovery_targets,
        )

        specs = context.get("_retry_batch_specs") or []
        start = (
            min(s for _, _, s, _ in specs)
            if specs
            else getattr(self.config, "_backfill_start", None) or BACKFILL_START
        )
        end = (
            max(e for _, _, _, e in specs)
            if specs
            else getattr(self.config, "_backfill_end", None) or trade_date
        )
        scope = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "universe": self.config.ingest_universe,
        }
        previous = self.manifest.get_run_metadata(run_id).get("delisted_recovery_scope") or {}
        targets = (
            previous["targets"]
            if all(previous.get(key) == value for key, value in scope.items())
            and "targets" in previous
            else delisted_recovery_targets(self.config, start, end)
        )
        targets = {
            symbol: target
            for symbol, target in targets.items()
            if target["ownership"] == "dedicated_fetch"
            and in_ingest_universe_symbol(symbol, self.config)
        }
        symbols = sorted(targets)
        if not symbols or delisted_recovery_covers(self.config, start, end, symbols):
            return
        self.manifest.mutate_run_metadata(
            run_id,
            lambda meta: meta.update({"delisted_recovery_scope": {**scope, "targets": targets}}),
        )
        child_id = self.manifest.start_run(
            "delisted_backfill",
            {"parent_init_run_id": run_id, "start": start.isoformat(), "end": end.isoformat()},
        )
        logger.info(N_("Init: recovering %d delisted symbols before daily bars"), len(symbols))
        with self._run_lock(child_id):
            try:
                result = backfill_delisted_bars(
                    self.config, child_id, start, end=end, recovery_targets=targets
                )
                self._record_step_result(
                    name="daily_bars",
                    entry=get_step("daily_bars"),
                    run_id=child_id,
                    status=result.get("status", "success"),
                    out=result,
                )
                recovery_outcome = step_outcome(result.get("status", "success"), result)
                if recovery_outcome.execution_status in {"failed", "interrupted"}:
                    raise RuntimeError(
                        "init delisted recovery execution failed: "
                        + str(result.get("error") or recovery_outcome.reason_code)
                    )
                compact = self.run_step("compact", trade_date, child_id)
                no_data = set(result.get("expected_no_data_symbols", []))
                required = [symbol for symbol in symbols if symbol not in no_data]
                complete = (
                    result.get("status", "success") == "success"
                    and compact["status"] == "success"
                    and delisted_recovery_covers(self.config, start, end, required)
                )
                self.manifest.finish_run(
                    child_id,
                    "success" if complete else "warning",
                    rows_read=result.get("rows_read", 0),
                    rows_written=result.get("rows_written", 0),
                    error_message=None if complete else "init delisted recovery incomplete",
                )
            except (KeyboardInterrupt, SystemExit):
                self.manifest.interrupt_run(
                    child_id, error_message="init delisted recovery stopped"
                )
                raise
            except Exception as exc:
                self.manifest.finish_run(child_id, "failed", error_message=str(exc))
                raise
        if compact.get("execution_status") == "failed":
            raise RuntimeError(
                "init delisted recovery publication failed: " + str(compact.get("error"))
            )
        if not complete:
            recovery = {
                "run_id": child_id,
                "coverage_status": "partial",
                "reason_code": "delisted_source_unavailable",
                "requested_symbols": symbols,
            }
            context["init_delisted_recovery"] = recovery
            return recovery
        return {"run_id": child_id, "coverage_status": "complete"}

    def _init_phases_list(self, run_id: str | None = None) -> list[str]:
        if run_id:
            meta = self.manifest.get_run_metadata(run_id)
            phases = meta.get("phases")
            if phases:
                return list(phases)
        return configured_init_phases(self.config.init_phases)

    def _init_execution_batches(self, run_id: str) -> list[Any]:
        batches: list[Any] = list(self.manifest.get_batches_for_run(run_id))
        outcomes = self.manifest.get_run_metadata(run_id).get("step_outcomes", {})
        batches.extend(
            {"dataset": name, "logical_step": True, **outcome} for name, outcome in outcomes.items()
        )
        return batches

    def _public_outcome(self, run_id: str) -> dict[str, Any]:
        from cnequity.orchestrator.recovery import fallback_options

        run = self.manifest.get_run(run_id)
        aggregate = self.manifest.aggregate_run_status(run_id)
        source = run if run is not None and run["finished_at"] else aggregate
        return {
            **{
                key: source[key]
                for key in (
                    "result_schema_version",
                    "execution_status",
                    "coverage_status",
                    "publication_status",
                )
            },
            "usable_result": aggregate["usable_result"],
            "fallback": fallback_options(self.config, self.manifest, run_id),
        }

    def _published_run_rows(self, run_id: str) -> tuple[int, int]:
        """Row counts already recorded on *run_id*, or zeros for a new run."""
        run = self.manifest.get_run(run_id)
        if run is None:
            return 0, 0
        return int(run["rows_read"] or 0), int(run["rows_written"] or 0)

    def _missing_init_steps(self, run_id: str) -> list[str]:
        phases = self._init_phases_list(run_id)
        batches = self._init_execution_batches(run_id)
        return missing_steps_within_phase_order(phases, batches)

    def _all_missing_init_steps(self, run_id: str) -> list[str]:
        """Every never-started step, ignoring phase order.

        The gate that refuses to call a run successful must see the whole
        remainder; only the gate that decides what to *start* next respects
        phase order.
        """
        phases = self._init_phases_list(run_id)
        batches = self.manifest.get_batches_for_run(run_id)
        return missing_steps(phases, batches)

    def _planned_steps(self, run_id: str) -> list[str]:
        """The step list *run_id* was started with, or empty if it has none."""
        planned = self.manifest.get_run_metadata(run_id).get("planned_steps")
        if not isinstance(planned, list):
            return []
        return [step for step in planned if isinstance(step, str)]

    def _retry_finalize_steps(self, run_id: str) -> tuple[str, ...]:
        """Respect a recorded run plan when finalizing its retried staging.

        Older runs and init retain their established full finalize chain. A
        targeted run may contain only one source and compact; retrying it must
        not launch unrelated adjustment, industry or audit work.
        """
        order = ("compact", "derive_adj_factors", "derive_industry_index", "audit")
        planned = self._planned_steps(run_id)
        if self._is_init_run(run_id) or not planned:
            return order
        return tuple(step for step in order if step == "compact" or step in planned)

    def _steps_with_batches(self, run_id: str) -> set[str]:
        """Step names this run has a batch for, however that batch ended.

        A batch carries both the logical task and the physical dataset, and the
        two differ where a step writes elsewhere (``daily_bars_history`` into
        ``daily_bars``). Both spellings count as "this step ran", so a run
        resumed from an older release, whose receipts are keyed by dataset
        alone, is not told to run everything a second time.
        """
        names: set[str] = set()
        for batch in self.manifest.get_batches_for_run(run_id):
            names.add(str(batch["task_id"]))
            names.add(str(batch["dataset"]))
        return names

    def _missing_run_steps(self, run_id: str) -> list[str]:
        """Planned steps of *run_id* that never produced a batch at all.

        A process killed mid-DAG (OOM, SIGKILL) leaves nothing behind for the
        steps it never reached: no failed batch, no receipt, nothing to retry.
        The ledger alone therefore reads as a clean run, which is how a daily
        job that died after its third step used to come back from ``cne run retry``
        marked ``success`` with the rest of the day silently missing.

        Init runs describe their plan as phases and keep that path. Every other
        run carries the step list it was started with; a run started before
        that snapshot existed says nothing here rather than having a plan
        invented for it out of today's config.
        """
        if self._is_init_run(run_id):
            return self._missing_init_steps(run_id)
        planned = self._planned_steps(run_id)
        if not planned:
            return []
        present = self._steps_with_batches(run_id)
        return [step for step in planned if step not in present]

    def _split_runnable_missing(
        self,
        run_id: str,
        missing: list[str],
        *,
        in_run: set[str],
        init_phases: list[str] | None = None,
    ) -> tuple[list[str], list[str]]:
        """Split *missing* into steps whose inputs are ready and steps that are blocked.

        Running a step whose upstream in this same run is still broken would
        publish an answer built on input known to be incomplete. A blocked step
        stays missing, which keeps the run out of ``success`` until the
        upstream is repaired. Dependencies outside the run were satisfied by an
        earlier run, exactly as ``step_execution_levels`` assumes.
        """
        resolved: set[str] = set()
        unresolved: set[str] = set()
        for batch in self.manifest.get_batches_for_run(run_id):
            # A step answers to its task id and to the dataset it wrote, and the
            # two differ for the derive steps (`derive_industry_index` writes
            # `industry_index`). Track both spellings, or a dependent looks
            # blocked by an upstream that in fact succeeded.
            names = {str(batch["task_id"]), str(batch["dataset"])}
            if execution_settled(batch):
                resolved |= names
            else:
                unresolved |= names
        satisfied = resolved - unresolved

        ready: list[str] = []
        blocked: list[str] = []
        for step in missing:
            deps = [
                dep
                for dep in get_step(step).depends_on
                if dep in in_run and not (init_phases and scheduled_later(step, dep, init_phases))
            ]
            if all(dep in satisfied or dep in ready for dep in deps):
                ready.append(step)
            else:
                blocked.append(step)
        return ready, blocked

    def _gate_on_missing_steps(self, run_id: str, status: str) -> tuple[str, list[str]]:
        """Refuse a terminal success while planned steps have never run.

        Batch receipts can only speak for the steps that produced one, so a
        truncated run looks spotless: everything it managed to start finished.
        The run's own plan is the only thing that says otherwise, and callers
        (the daily pipeline, ``cne status``) read the run status to decide
        whether the day is complete.
        """
        if status == "pending":
            return status, []
        # The whole remainder, not the phase-ordered slice a retry may start.
        # "I am not allowed to run this yet" and "this never ran" are different
        # claims, and only the second one decides whether the run succeeded.
        missing = (
            self._all_missing_init_steps(run_id)
            if self._is_init_run(run_id)
            else self._missing_run_steps(run_id)
        )
        return ("failed" if missing else status), missing

    def _run_finalize_steps(
        self,
        run_id: str,
        trade_date: date,
        context: dict[str, Any],
        *,
        force: bool = False,
        steps: tuple[str, ...] = (
            "compact",
            "derive_adj_factors",
            "derive_industry_index",
            "audit",
        ),
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        batches = self.manifest.get_batches_for_run(run_id)
        for step_name in steps:
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

    def _has_ready_staging(self, run_id: str) -> bool:
        """Whether an incomplete run has a dataset compact can safely publish."""
        from cnequity.domain.datasets import PARTITION_COLS
        from cnequity.orchestrator.compact_gate import compact_allowed
        from cnequity.storage import StagingWriter

        writer = StagingWriter(self.config.staging_root)
        return any(
            writer.list_run_files(dataset, run_id)
            and compact_allowed(self.manifest, run_id, dataset)[0]
            for dataset in PARTITION_COLS
        )

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
        if counts.get("running") or counts.get("stale") or counts.get(QUEUED_BATCH_STATUS):
            return "pending"
        aggregate = self.manifest.aggregate_run_status(run_id)
        if aggregate["results"]:
            return aggregate["status"]
        if counts.get("failed"):
            return "failed"
        if counts.get("warning"):
            return "warning"
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

    @staticmethod
    def _resolve_batch_step(batch: Any) -> tuple[str, Any]:
        """Resolve a manifest batch by logical task, with a legacy fallback.

        ``dataset`` is the physical storage target and may intentionally differ
        from the step that produced it (for example, ``daily_bars_history``
        writes into ``daily_bars``). A few old auxiliary receipts also carry an
        unregistered task id; keep those retryable through their physical step
        instead of making an existing run impossible to resume.
        """
        task_id = batch["task_id"]
        try:
            return task_id, get_step(task_id)
        except KeyError as task_error:
            dataset = batch["dataset"]
            if dataset == task_id:
                raise
            try:
                step = get_step(dataset)
            except KeyError:
                raise task_error from None
            logger.debug(
                "retry batch %s uses unregistered task %s; falling back to dataset step %s",
                batch["batch_id"],
                task_id,
                dataset,
            )
            return dataset, step

    def _retryable_batches_with_worker_budget(self, run_id: str) -> list:
        """Apply the durable cap only where retries reuse one batch identity.

        Worker retries restart the same batch id, so ``retry_count`` is a
        stable logical-attempt budget. Non-worker steps create a new UUID and
        rely on retry lineage to supersede every predecessor after success;
        filtering those predecessors by a per-row count could strand one.
        """
        retryable = []
        for batch in self.manifest.get_retryable_batches(run_id):
            _, step = self._resolve_batch_step(batch)
            if not step.requires_workers or batch["retry_count"] < self.config.max_retries:
                retryable.append(batch)
        return retryable

    def _exhausted_worker_retry_count(self, run_id: str) -> int:
        exhausted = 0
        for batch in self.manifest.get_retryable_batches(run_id):
            _, step = self._resolve_batch_step(batch)
            if step.requires_workers and batch["retry_count"] >= self.config.max_retries:
                exhausted += 1
        return exhausted

    def _retry_run(
        self, run_id: str, trade_date: date, *, auto_finalize: bool = True
    ) -> dict[str, Any]:
        # Same as run_job entry: close peer zombies before we touch this run.
        # retry used to skip reconcile (early-return before start_run), so
        # crashed valuation/backfill runs sat until the next daily job.
        self._reconcile_orphans()
        with self._run_lock(run_id):
            try:
                return self._retry_run_passes(run_id, trade_date, auto_finalize=auto_finalize)
            except (KeyboardInterrupt, SystemExit):
                # `cne run retry` reaches this through an early return in
                # `run_job`, before the try/finally that closes an interrupted
                # run. Without this, Ctrl-C during a retry left exactly the
                # state the retry existed to clear.
                self.manifest.interrupt_run(
                    run_id,
                    error_message="interrupted by operator",
                )
                raise

    def _retry_run_passes(
        self, run_id: str, trade_date: date, *, auto_finalize: bool
    ) -> dict[str, Any]:
        """Drive retry passes until nothing transient is left. Lock held."""
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
            if result["status"] not in {"failed", "warning", "degraded"}:
                break

            remaining = self._retryable_batches_with_worker_budget(run_id)
            automatic_batch_ids = {
                batch["batch_id"]
                for batch in remaining
                if self._resolve_batch_step(batch)[1].requires_workers
                and _is_transient_retry_error(batch["error_message"], batch["reason_code"])
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
        if result.get("status") != "pending":
            public_status = self._overall_status(run_id, str(result.get("status")))
            # Dataset receipts describe the steps that ran. They cannot see
            # the ones that never started, so the run's own plan has the
            # last word over an aggregate that looks clean.
            public_status, unresolved = self._gate_on_missing_steps(run_id, public_status)
            # `_retry_run_locked` historically closes a terminal run with
            # batch status ``warning``. Reconcile that legacy spelling
            # after all retry/finalize receipts have been written.
            if auto_finalize and public_status != result.get("status"):
                self.manifest.finish_run(
                    run_id,
                    public_status,
                    error_message=(
                        f"planned steps never ran: {', '.join(unresolved)}" if unresolved else None
                    ),
                )
            result["status"] = public_status
            result["missing_steps_unresolved"] = unresolved
        result.update(self._public_outcome(run_id))
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
        # The caller (cne run retry) has no session date of its own — it passes
        # whatever run_job() defaulted to, which is today. A run retried after
        # its trade_date has rolled over must still fetch the session it was
        # started for, not today's, or a backfill for a past date silently
        # requests data that has not been published yet and never lands.
        stored_trade_date = run_meta.get("trade_date")
        if stored_trade_date:
            trade_date = date.fromisoformat(stored_trade_date)
        self.config._backfill = bool(run_meta.get("backfill"))
        scope = run_meta.get("backfill_scope") or {}
        restore_backfill_scope(self.config, scope)
        timeout = self.manifest.advance_batch_timeouts(
            run_id,
            stale_after_seconds=self.config.batch_stale_seconds,
        )
        stale_marked = timeout["running_to_stale"] + timeout["stale_to_failed"]
        failed = self._retryable_batches_with_worker_budget(run_id)
        if retry_batch_ids is not None:
            failed = [batch for batch in failed if batch["batch_id"] in retry_batch_ids]
        # A targeted pass retries the batch ids it was given and nothing else;
        # the broad pass is the one that owns the rest of the plan.
        missing = (
            self._missing_run_steps(run_id)
            if retry_batch_ids is None and not (self._is_init_run(run_id) and not auto_finalize)
            else []
        )

        if not failed and not missing:
            incomplete = self.manifest.incomplete_batch_count(run_id)
            if incomplete > 0:
                exhausted = self._exhausted_worker_retry_count(run_id)
                status = self._retry_batch_status(run_id)
                settled = all(
                    execution_settled(b) for b in self.manifest.get_batches_for_run(run_id)
                )
                if settled or (exhausted and status in {"failed", "warning", "degraded"}):
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
                pending = self._pending_retry_payload(
                    run_id, stale_marked=stale_marked, timeout=timeout
                )
                # An automatic pass may intentionally select only transient
                # worker failures while a non-transient failure remains in the
                # run. Preserve that overall failure instead of relabeling it
                # as pending merely because this pass had nothing to retry.
                if retry_batch_ids is not None and status in {"failed", "warning", "degraded"}:
                    pending["status"] = status
                    pending["retry_exhausted"] = exhausted
                return pending

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
            [
                batch["batch_id"]
                for batch in failed
                if self._resolve_batch_step(batch)[1].requires_workers
            ],
        )

        worker_batches_by_step: dict[str, list] = defaultdict(list)
        step_batches_by_step: dict[str, list] = defaultdict(list)
        for batch in failed:
            step_name, step = self._resolve_batch_step(batch)
            if step.requires_workers:
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

        ran_missing: list[str] = []
        blocked_missing: list[str] = []
        if missing:
            # Recompute readiness now: the batch retries above either repaired
            # the upstream a missing step needs, or failed again and left it
            # unrunnable.
            in_run = set(self._planned_steps(run_id)) | set(missing)
            if init_phases:
                in_run |= set(init_expected_steps(init_phases))
            ran_missing, blocked_missing = self._split_runnable_missing(
                run_id, missing, in_run=in_run, init_phases=init_phases
            )
            for step in ran_missing:
                prev_backfill = self.config._backfill
                if init_phases:
                    self.config._backfill = step_backfill(step, init_phases)
                try:
                    results.append(self._run_step(step, trade_date, run_id, context))
                finally:
                    self.config._backfill = prev_backfill
            retried += len(ran_missing)
            if blocked_missing:
                logger.warning(
                    "Run %s: %d planned step(s) never ran and cannot run yet — "
                    "their inputs in this run are still failing: %s",
                    run_id,
                    len(blocked_missing),
                    ", ".join(blocked_missing),
                )

        if auto_finalize and self.manifest.incomplete_batch_count(run_id) == 0:
            # A retry may have staged new rows after an earlier compact/finalize
            # attempt. Re-run the finalize chain so curated data and coverage
            # receipts cannot lag behind the now-successful fetch. A recorded
            # targeted plan must not run unrelated derived datasets or audit.
            results.extend(
                self._run_finalize_steps(
                    run_id,
                    trade_date,
                    context,
                    force=True,
                    steps=self._retry_finalize_steps(run_id),
                )
            )
        elif auto_finalize and self._has_ready_staging(run_id):
            # An unrelated failed dataset must not hold ready revisions in
            # staging. Compact itself still gates each incomplete dataset.
            results.extend(
                self._run_finalize_steps(
                    run_id, trade_date, context, force=True, steps=("compact",)
                )
            )

        self._persist_unaudited_findings(run_id, trade_date, results, context)
        status, still_missing = self._gate_on_missing_steps(
            run_id, self._retry_batch_status(run_id)
        )
        if auto_finalize and status != "pending":
            self.manifest.finish_run(
                run_id,
                status,
                error_message=(
                    f"planned steps never ran: {', '.join(still_missing)}"
                    if still_missing
                    else None
                ),
            )
        payload: dict[str, Any] = {
            "run_id": run_id,
            "status": status,
            "retried": retried,
            "missing_steps": ran_missing,
            "missing_steps_unresolved": still_missing,
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
        batches = self._init_execution_batches(run_id)
        current = current_phase_statuses(phases, batches)
        complete = init_run_complete(phases, batches)
        incomplete = not complete
        if complete and not incomplete:
            status = self._overall_status(run_id, "success")
        else:
            status = "failed"
        error_message = "one or more init steps are incomplete" if status == "failed" else None
        self.manifest.finish_run(
            run_id,
            status,
            rows_read=rows_read,
            rows_written=rows_written,
            error_message=error_message,
        )
        self.manifest.mutate_run_metadata(
            run_id,
            lambda meta: meta.update(
                {"phase_results": phase_results, "current_phase_status": current}
            ),
        )
        return status

    def _with_factor_prefetch(self, phases: list[str], trade_date: date) -> Iterator[str]:
        """Yield *phases*, warming the factor cache in the background meanwhile.

        Starts at the first bars or corporate-action phase and stops before the
        finalize phase, where the derive fetches whatever is still uncached.
        Stopped too when the loop ends early, breaks or raises.
        """
        from cnequity.derive.adj_factors import BackgroundFactorPrefetch

        prefetch = None
        try:
            for phase in phases:
                if phase == FACTOR_PREFETCH_STOP_PHASE and prefetch is not None:
                    prefetch.stop()
                    prefetch = None
                elif phase in FACTOR_PREFETCH_PHASES and prefetch is None:
                    prefetch = BackgroundFactorPrefetch(self.config, trade_date).start()
                yield phase
        finally:
            if prefetch is not None:
                prefetch.stop()

    def _init_completion(self, run_id: str, status: str) -> dict[str, Any]:
        """What a first build left owed, and whether its lake is usable anyway.

        A run can be ``failed`` because a non-blocking phase is still owed
        while its bars are published; ``lake_usable`` tells the CLI not to
        present that as a failed build.
        """
        phases = self._init_phases_list(run_id)
        batches = self._init_execution_batches(run_id)
        return {
            "owed_steps": owed_steps(phases, batches),
            "lake_usable": status != "failed" or only_non_blocking_owed(phases, batches),
        }

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

        for phase in self._with_factor_prefetch(phases, trade_date):
            steps = INIT_PHASE_STEPS.get(phase, [])
            if not steps:
                continue
            backfill = phase_backfill(phase)
            logger.info(N_("Init phase %s: %s"), phase, ", ".join(steps))
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
            if _has_execution_errors(result) and not keep_going:
                failed = _failed_step_names(result)
                if not blocks_init(phase, failed):
                    logger.warning(
                        N_(
                            "Init phase %s did not complete (%s); continuing so the lake still "
                            "publishes. Run `cne init` again to resume it."
                        ),
                        phase,
                        ", ".join(sorted(failed)) or phase,
                    )
                    continue
                logger.error(N_("Init phase %s failed; stopping remaining phases"), phase)
                break

        status = self._finalize_init_run(
            run_id,
            phase_results,
            rows_read=total_read,
            rows_written=total_written,
        )
        return {
            "run_id": run_id,
            "status": status,
            "phases": phase_results,
            **self._public_outcome(run_id),
            **self._init_completion(run_id, status),
        }

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

        with init_job_lock(self.config.meta_root), self._run_lock(run_id):
            try:
                return self._resume_init_locked(
                    trade_date,
                    run_id=run_id,
                    keep_going=keep_going,
                )
            except (KeyboardInterrupt, SystemExit):
                self.manifest.interrupt_run(
                    run_id,
                    error_message="interrupted by operator",
                )
                raise
            except Exception:
                self.manifest.interrupt_run(run_id, error_message="init resume failed")
                raise

    def _resume_init_locked(
        self,
        trade_date: date,
        *,
        run_id: str,
        keep_going: bool,
    ) -> dict[str, Any]:
        """Resume an init while its run-id lock is already held."""

        phases = self._init_phases_list(run_id)
        meta = self.manifest.get_run_metadata(run_id)
        # Pick the original run's history window back up unless this invocation
        # named one, so the resumed phases fetch the same depth as the ones that
        # already ran rather than a lake with two different floors.
        recorded = meta.get("history_start")
        requested = getattr(self.config, "_backfill_start", None)
        if recorded and not requested:
            self.config._backfill_start = date.fromisoformat(str(recorded))
            requested = self.config._backfill_start

        def _mark_resumed(current: dict[str, Any]) -> None:
            current["resumed_at"] = shanghai_today().isoformat()
            # A caller-provided history floor is an intentional scope change.
            # Persist it so another interruption resumes the same scope rather
            # than reverting to the run's original floor.
            if requested:
                current["history_start"] = requested.isoformat()

        meta = self.manifest.mutate_run_metadata(
            run_id,
            _mark_resumed,
        )

        logger.info(N_("Resuming init run %s"), run_id)
        self.manifest.resume_run(run_id)
        retry_result = self._retry_run(run_id, trade_date, auto_finalize=False)

        batches = self._init_execution_batches(run_id)
        to_run = pending_phases(phases, batches)
        phase_results: list[dict[str, Any]] = list(meta.get("phase_results") or [])
        total_read = retry_result.get("rows_read", 0)
        total_written = retry_result.get("rows_written", 0)

        for phase in self._with_factor_prefetch(to_run, trade_date):
            batches = self._init_execution_batches(run_id)
            steps = [
                step
                for step in INIT_PHASE_STEPS.get(phase, [])
                if not step_completed(batches, step)
            ]
            # `_retry_run` has just retried every failed/stale batch. If any
            # such attempt is still unresolved, this phase remains the gate;
            # only never-started siblings are safe to launch here.
            unresolved_started = {
                step
                for step in steps
                if any(
                    batch["dataset"] == step and not execution_settled(batch) for batch in batches
                )
            }
            if unresolved_started:
                if not keep_going and blocks_init(phase, unresolved_started):
                    logger.error(
                        N_(
                            "Init phase %s is still incomplete after retry (%s); "
                            "not starting later phases"
                        ),
                        phase,
                        ", ".join(sorted(unresolved_started)),
                    )
                    break
                logger.warning(
                    N_("Init phase %s is still incomplete after retry (%s); continuing"),
                    phase,
                    ", ".join(sorted(unresolved_started)),
                )
                steps = [step for step in steps if step not in unresolved_started]
            if not steps:
                continue
            backfill = phase_backfill(phase)
            logger.info(N_("Init resume phase %s: %s"), phase, ", ".join(steps))
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
            if keep_going:
                continue
            if _has_execution_errors(result) and blocks_init(phase, _failed_step_names(result)):
                break
            batches = self._init_execution_batches(run_id)
            unfinished = [
                step
                for step in INIT_PHASE_STEPS.get(phase, [])
                if not step_completed(batches, step)
            ]
            if unfinished and blocks_init(phase, unfinished):
                break

        batches = self._init_execution_batches(run_id)
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
        elif "phase4_finalize" in phases and self._has_ready_staging(run_id):
            context = self._merge_retry_context(run_id, trade_date)
            compact_results = self._run_finalize_steps(
                run_id, trade_date, context, force=True, steps=("compact",)
            )
            phase_results.append(
                {"phase": "phase4_partial_compact", "status": "warning", "results": compact_results}
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
            **self._public_outcome(run_id),
            **self._init_completion(run_id, status),
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
        # One init at a time, and visibly so. `_retry_run` locks by run id, so
        # a resume nesting inside this takes a different lock and cannot
        # deadlock against it.
        with init_job_lock(self.config.meta_root):
            return self._run_init_phases_locked(
                trade_date,
                resume=resume,
                resume_run_id=resume_run_id,
                keep_going=keep_going,
            )

    def _run_init_phases_locked(
        self,
        trade_date: date,
        *,
        resume: bool,
        resume_run_id: str | None,
        keep_going: bool,
    ) -> dict[str, Any]:
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
        with self._run_lock(run_id):
            try:
                return self._execute_init_phases(
                    run_id,
                    trade_date,
                    phases,
                    keep_going=keep_going,
                )
            except (KeyboardInterrupt, SystemExit):
                self.manifest.interrupt_run(
                    run_id,
                    error_message="interrupted by operator",
                )
                raise
