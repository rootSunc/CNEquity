"""Machine-verifiable consecutive trading-day stability evidence.

The gate judges the core datasets (``DatasetSpec.support == "core"``) in
whichever daily schedule group fetched them, not the ``daily:core`` job: four of
the twelve (financial statements, valuation, share structure and index
constituents) run in other groups, and two datasets that do run in ``core``
(``industry_index``, the THS snapshot) are not core. For each trading day the
newest receipt per dataset and stage across every ``daily:*`` run counts, and
criticality is read from the current registry rather than the label stored
with the receipt, so a day is judged by today's definition of core.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from cnequity.orchestrator.manifest import Manifest
from cnequity.storage.atomic import write_json_atomic


@dataclass(frozen=True)
class StabilityDay:
    trade_date: str
    run_id: str | None
    status: str
    dataset_results: int
    passed: bool
    reason: str


@dataclass(frozen=True)
class StabilityReport:
    generated_at: str
    job_name: str
    required_days: int
    calendar_days_available: int
    consecutive_passed: int
    passed: bool
    days: tuple[StabilityDay, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "job_name": self.job_name,
            "required_days": self.required_days,
            "calendar_days_available": self.calendar_days_available,
            "consecutive_passed": self.consecutive_passed,
            "passed": self.passed,
            "days": [asdict(item) for item in self.days],
        }


def _run_trade_date(run: Any) -> date | None:
    try:
        metadata = json.loads(run["metadata_json"] or "{}")
        value = metadata.get("trade_date")
        return date.fromisoformat(value) if isinstance(value, str) else None
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


#: The label a core-dataset report carries; ``daily:core`` reports predate it.
GATE_NAME = "core_datasets"
_DAILY_PREFIX = "daily:"


def _runs_by_date(manifest: Manifest) -> dict[date, list[Any]]:
    """Every daily-family run per logical trade date, oldest first."""
    grouped: dict[date, list[Any]] = {}
    for run in manifest.list_runs():
        if not str(run["job_name"]).startswith(_DAILY_PREFIX):
            continue
        trade_date = _run_trade_date(run)
        if trade_date is not None:
            grouped.setdefault(trade_date, []).append(run)
    for runs in grouped.values():
        runs.sort(key=lambda run: str(run["started_at"]))
    return grouped


def _fails(row: Any) -> bool:
    """Whether a receipt fails its run, as ``Manifest.aggregate_run_status`` reads it.

    A completed attempt that the source could not serve degrades; an execution
    failure, or a failed or blocked stage, fails.
    """
    status = str(row["status"])
    v2 = int(row["result_schema_version"] or 1) >= 2
    if v2 and row["execution_status"] == "completed" and status == "failed":
        return False
    if v2 and row["execution_status"] in {"failed", "interrupted"}:
        return True
    return status in {"failed", "blocked"}


def _evaluate_day(manifest: Manifest, trade_date: date, runs: list[Any]) -> StabilityDay:
    from cnequity.domain.datasets import core_dataset_names

    if not runs:
        return _evaluate_run(manifest, trade_date, None)
    latest: dict[tuple[str, str], tuple[Any, Any]] = {}
    for run in runs:
        for row in manifest.get_dataset_results(str(run["run_id"])):
            latest[(str(row["dataset"]), str(row["stage"]))] = (run, row)
    core = core_dataset_names()
    core_rows = [(run, row) for (dataset, _), (run, row) in latest.items() if dataset in core]
    if not core_rows:
        # No receipts for any core dataset (older runs, or a manifest written
        # before receipts existed): judge the newest run by its own status.
        spine = [run for run in runs if run["job_name"] == "daily:core"] or runs
        return _evaluate_run(manifest, trade_date, spine[-1])
    failed = sorted({str(row["dataset"]) for _, row in core_rows if _fails(row)})
    newest = max((run for run, _ in core_rows), key=lambda run: str(run["started_at"]))
    if failed:
        return StabilityDay(
            trade_date=trade_date.isoformat(),
            run_id=str(newest["run_id"]),
            status="failed",
            dataset_results=len(latest),
            passed=False,
            reason="core dataset failed or was blocked: " + ", ".join(failed),
        )
    clean = all(str(row["status"]) == "success" for _, row in latest.values())
    return StabilityDay(
        trade_date=trade_date.isoformat(),
        run_id=str(newest["run_id"]),
        status="success" if clean else "degraded",
        dataset_results=len(latest),
        passed=True,
        reason="no core dataset failed" if clean else "only non-core datasets degraded or failed",
    )


def _evaluate_run(manifest: Manifest, trade_date: date, run: Any | None) -> StabilityDay:
    if run is None:
        return StabilityDay(
            trade_date=trade_date.isoformat(),
            run_id=None,
            status="missing",
            dataset_results=0,
            passed=False,
            reason="no run receipt for trading day",
        )

    run_id = str(run["run_id"])
    status = str(run["status"])
    aggregate = manifest.aggregate_run_status(run_id)
    result_count = len(aggregate["results"])
    if aggregate["core_failures"]:
        passed = False
        reason = "core dataset stage failed or was blocked"
    elif status == "success":
        passed = True
        reason = "run succeeded"
    elif status == "degraded" and result_count:
        passed = True
        reason = "only non-core dataset stages degraded"
    elif status == "warning" and result_count:
        # Transitional spelling for a run produced while the new table was
        # already available. Core failures above still fail closed.
        passed = True
        reason = "legacy warning with dataset receipts proving no core failure"
    elif status == "warning":
        passed = False
        reason = "legacy warning has no dataset receipts; core safety is unproven"
    else:
        passed = False
        reason = f"run status is {status}"
    return StabilityDay(
        trade_date=trade_date.isoformat(),
        run_id=run_id,
        status=status,
        dataset_results=result_count,
        passed=passed,
        reason=reason,
    )


def evaluate_stability(
    manifest: Manifest,
    trading_days: list[date],
    *,
    required_days: int = 20,
    as_of: date | None = None,
    now: datetime | None = None,
) -> StabilityReport:
    """Evaluate the latest *required_days* sessions without fabricating evidence.

    A degraded day counts only when dataset receipts prove that no core stage
    failed. Legacy ``warning`` rows without those receipts fail closed. Missing
    calendar sessions or missing runs also fail the gate.
    """
    if required_days < 1:
        raise ValueError("required_days must be positive")
    cutoff = as_of or date.today()
    eligible = sorted({item for item in trading_days if item <= cutoff})
    window = eligible[-required_days:]
    runs = _runs_by_date(manifest)
    days = tuple(_evaluate_day(manifest, item, runs.get(item, [])) for item in window)

    consecutive = 0
    for item in reversed(days):
        if not item.passed:
            break
        consecutive += 1
    passed = len(days) == required_days and consecutive == required_days
    generated = now or datetime.now(timezone.utc)
    return StabilityReport(
        generated_at=generated.astimezone(timezone.utc).isoformat(),
        job_name=GATE_NAME,
        required_days=required_days,
        calendar_days_available=len(eligible),
        consecutive_passed=consecutive,
        passed=passed,
        days=days,
    )


def store_stability_report(meta_root: Path, report: StabilityReport) -> tuple[Path, Path]:
    """Persist an immutable acceptance receipt plus a latest pointer."""
    root = Path(meta_root) / "stability"
    generated = datetime.fromisoformat(report.generated_at)
    stamp = generated.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    historical = root / "history" / f"{stamp}.json"
    latest = root / "latest.json"
    payload = report.to_dict()
    write_json_atomic(historical, payload, indent=2, ensure_ascii=False)
    write_json_atomic(latest, payload, indent=2, ensure_ascii=False)
    return latest, historical


__all__ = [
    "StabilityDay",
    "StabilityReport",
    "evaluate_stability",
    "store_stability_report",
]
