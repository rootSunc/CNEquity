"""Execution, coverage and publication are independent result contracts."""

from __future__ import annotations

import socket
import ssl
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, TypedDict

RESULT_SCHEMA_VERSION = 2


class RunStatus(StrEnum):
    """The one word a run, a batch, a step or a dataset result ends in.

    The engine, the manifest and every command's JSON use these and only
    these. Why a result is degraded or skipped is its ``reason_code``, not a
    word of its own: ``warning`` was a second spelling of ``degraded`` (a
    usable result that covers less than it was asked to), and each
    ``skipped_<why>`` is ``skipped`` with that reason.
    """

    QUEUED = "queued"
    RUNNING = "running"
    STALE = "stale"
    SUCCESS = "success"
    DEGRADED = "degraded"
    FAILED = "failed"
    SKIPPED = "skipped"
    BLOCKED = "blocked"
    INTERRUPTED = "interrupted"
    SUPERSEDED = "superseded"


#: Words the engine wrote before :class:`RunStatus`, and the reason each
#: implied. Still accepted from a step, read back from an old manifest row,
#: and rewritten by the manifest's schema upgrade.
LEGACY_STATUSES: dict[str, tuple[RunStatus, str | None]] = {
    "warning": (RunStatus.DEGRADED, None),
    "skipped_non_trading_day": (RunStatus.SKIPPED, "non_trading_day"),
    "skipped_not_scheduled": (RunStatus.SKIPPED, "not_scheduled"),
    "skipped_locked": (RunStatus.SKIPPED, "locked"),
    "skipped_disabled": (RunStatus.SKIPPED, "disabled"),
    "skipped_long_window": (RunStatus.SKIPPED, "long_window"),
}


def run_status(value: str | RunStatus, reason: str | None = None) -> tuple[RunStatus, str | None]:
    """*value* as a :class:`RunStatus`, and the reason it carries (*reason* wins).

    Raises ``ValueError`` for a word that is neither a status nor a legacy one.
    """
    if isinstance(value, RunStatus):
        return value, reason
    text = str(value)
    if text in LEGACY_STATUSES:
        status, implied = LEGACY_STATUSES[text]
        return status, reason or implied
    return RunStatus(text), reason


def status_word(value: str | RunStatus) -> str:
    """The :class:`RunStatus` value for *value*, legacy words mapped."""
    return run_status(value)[0].value


#: What a step may end in.
STEP_STATUSES = frozenset(
    {RunStatus.SUCCESS, RunStatus.DEGRADED, RunStatus.FAILED, RunStatus.SKIPPED, RunStatus.BLOCKED}
)
#: Skips that are the run doing its job, not failing at it (exit 0).
QUIET_SKIP_REASONS = frozenset({"non_trading_day", "not_scheduled", "disabled", "locked"})
EXECUTION_STATUSES = frozenset(
    {"queued", "running", "completed", "skipped", "failed", "interrupted"}
)
COVERAGE_STATUSES = frozenset({"complete", "partial", "unknown", "not_applicable"})
PUBLICATION_STATUSES = frozenset(
    {"pending", "published", "partial", "unchanged", "none", "rejected"}
)
TERMINAL_EXECUTION_STATUSES = frozenset({"completed", "skipped", "failed", "interrupted"})
SOURCE_LIMIT_REASONS = frozenset(
    {"source_transient", "source_unavailable", "capability_limit", "source_payload_invalid"}
)


#: Step result keys that report scope the step could not cover. Any truthy
#: value turns a ``success`` into a ``warning``. Listed explicitly so that a new
#: key is a decision rather than an accident of its spelling: the engine used
#: to recognise these by the ``failed`` prefix alone, which also caught any key
#: that merely happened to start with it. A ``failed*`` key that is not listed
#: is still treated as a partial failure (the safe reading) and logged, and
#: ``tests/unit/test_step_result_contract.py`` keeps this list complete.
PARTIAL_FAILURE_KEYS = frozenset(
    {
        "failed",
        "failed_days",
        "failed_indices",
        "failed_passes",
        "failed_requests",
        "failed_scopes",
        "failed_sectors",
        "failed_symbol_days",
        "failed_symbol_names",
        "failed_symbols",
        "failed_tasks",
        "aborted",
        "empty_days",
        "days_empty",
    }
)
#: Result flags that, when explicitly ``False``, mark a resumable sweep as
#: incomplete.
COMPLETION_FLAGS = frozenset({"complete", "coverage_complete"})


class StepResult(TypedDict, total=False):
    """The mapping a registered step returns to the engine.

    Every key is optional; the engine fills defaults. Steps may add their own
    diagnostic keys, which the engine passes through to the run record.
    """

    status: str  # success | warning | failed | skipped | blocked | degraded
    rows_read: int
    rows_written: int
    dataset: str  # physical dataset, when it differs from the receipt key
    reason_code: str | None
    coverage_status: str  # see COVERAGE_STATUSES
    usable_result: bool
    batch_settled: bool  # work done although the result warns (see engine)
    metrics: dict[str, Any]
    context_updates: dict[str, Any]
    # Partial-failure keys (PARTIAL_FAILURE_KEYS) and completion flags
    # (COMPLETION_FLAGS) are read by the engine as well.


class SourceUnavailableError(RuntimeError):
    """All permitted sources failed to produce a usable requested result."""

    reason_code = "source_unavailable"


class InputUnavailableError(RuntimeError):
    """A calculation has no trustworthy input for its requested scope."""


class SourcePayloadError(RuntimeError):
    """A source response violates its requested shape or scope."""

    reason_code = "source_payload_invalid"


class CapabilityLimitError(RuntimeError):
    """The provider cannot serve any observation in the requested scope."""

    reason_code = "capability_limit"


class WorkerExecutionError(RuntimeError):
    """Preserve a worker's typed failure across the process boundary."""

    def __init__(self, message: str, reason_code: str):
        super().__init__(message)
        self.reason_code = reason_code


def _is_curl_request_error(error: BaseException) -> bool:
    try:
        from curl_cffi.requests.exceptions import RequestException
    except ImportError:
        return False
    return isinstance(error, RequestException)


def error_kind(error: BaseException) -> str:
    """Classify explicit boundaries; unknown exceptions remain real failures."""
    if isinstance(error, (KeyboardInterrupt, SystemExit)):
        return "interrupted"
    if isinstance(error, SourceUnavailableError):
        return "source_unavailable"
    if isinstance(error, InputUnavailableError):
        return "input_unavailable"
    declared = getattr(error, "reason_code", None)
    if declared in {
        "source_transient",
        "source_unavailable",
        "capability_limit",
        "source_payload_invalid",
        "storage_failure",
        "configuration_error",
        "invariant_violation",
        "computation_failure",
        "execution_error",
    }:
        return declared
    import httpx

    if isinstance(error, (httpx.RequestError, httpx.HTTPStatusError)):
        return "source_transient"
    if isinstance(error, (TimeoutError, ConnectionError, socket.gaierror, socket.herror)):
        return "source_transient"
    # TLS and curl_cffi request errors subclass OSError; they are network
    # failures, not a broken disk. Certificate failures are included on
    # purpose: some exchange sites serve broken chains for a while, and the
    # cooldown and retry caps keep repeated attempts cheap.
    if isinstance(error, ssl.SSLError) or _is_curl_request_error(error):
        return "source_transient"
    if isinstance(error, OSError):
        return "storage_failure"
    return "execution_error"


@dataclass(frozen=True)
class Outcome:
    execution_status: str
    coverage_status: str
    publication_status: str
    reason_code: str | None = None
    result_schema_version: int = RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for value, permitted in (
            (self.execution_status, EXECUTION_STATUSES),
            (self.coverage_status, COVERAGE_STATUSES),
            (self.publication_status, PUBLICATION_STATUSES),
        ):
            if value not in permitted:
                raise ValueError(f"invalid result state: {value!r}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def step_outcome(
    status: str,
    result: Mapping[str, Any] | None = None,
    *,
    stage: str = "fetch",
    error: BaseException | None = None,
) -> Outcome:
    """Normalize receipts without treating row counts as coverage evidence."""
    result = result or {}
    status, implied = run_status(status)
    reason = result.get("reason_code") or implied
    if error is not None:
        reason = error_kind(error)
    execution = result.get("execution_status")
    if execution is None:
        execution = (
            "interrupted"
            if reason == "interrupted"
            else "skipped"
            if status == "skipped" or reason == "input_unavailable"
            else "failed"
            if status == "blocked" or (status == "failed" and reason not in SOURCE_LIMIT_REASONS)
            else "completed"
        )
    coverage = result.get("coverage_status")
    if coverage is None:
        coverage = (
            "partial"
            if status == RunStatus.DEGRADED
            or result.get("coverage_complete") is False
            or reason in SOURCE_LIMIT_REASONS | {"input_unavailable"}
            else "complete"
            if result.get("coverage_complete") is True
            else "not_applicable"
            if stage in {"audit", "compact", "publish_revision"}
            else "unknown"
        )
    publication = result.get("publication_status")
    if publication is None:
        publication = (
            "rejected"
            if status == "blocked"
            else "published"
            if stage == "publish_revision" and status == "success" and result.get("revision_id")
            else "unchanged"
            if stage == "publish_revision" and status == "success"
            else "none"
        )
    if reason is None and status == RunStatus.DEGRADED:
        reason = "coverage_limited"
    if reason is None and status == "failed":
        reason = "execution_error"
    return Outcome(str(execution), str(coverage), str(publication), reason)


def execution_settled(row: Mapping[str, Any]) -> bool:
    """Source-limited attempts can finish while their retry evidence survives."""
    data = dict(row)
    return data.get("execution_status") in {"completed", "skipped"} or data.get("status") in {
        "success",
        "superseded",
    }


def result_is_usable(result: Mapping[str, Any]) -> bool:
    """Accept facts or explicit coverage evidence, never a bare success flag."""
    if "usable_result" in result:
        return bool(result["usable_result"])
    return bool(
        int(result.get("rows_written", 0) or 0) > 0
        or result.get("coverage_complete") is True
        or result.get("already_covered")
        or result.get("expected_no_data")
        or result.get("reused_curated")
    )


def execution_exit_code(status: str, reason: str | None = None) -> int:
    """0 for a usable result, or a skip that is the run doing its job; 1 otherwise.

    Coverage is reported separately from execution, so ``degraded`` exits 0.
    """
    try:
        word, reason = run_status(status, reason)
    except ValueError:
        return 1
    if word in {RunStatus.SUCCESS, RunStatus.DEGRADED}:
        return 0
    return 0 if word == RunStatus.SKIPPED and reason in QUIET_SKIP_REASONS else 1
