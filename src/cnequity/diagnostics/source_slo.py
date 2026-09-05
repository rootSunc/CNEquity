"""Aggregate point source probes into evidence-backed availability SLOs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cnequity.diagnostics.source_health import HealthReport, ProbeStatus
from cnequity.storage.atomic import write_json_atomic

CORE_DATASETS = frozenset(
    {
        "daily_bars",
        "index_bars",
        "trading_calendar",
        "instruments",
        "trading_status",
        "corporate_actions",
    }
)

# Keep the native core gate in one place so evidence consumers cannot choose a
# weaker target than the SLO evaluator itself.
CORE_TARGET = 0.99


def critical_probe_keys() -> frozenset[str]:
    """Return probe keys that exercise at least one core dataset.

    Keep this derived from the source-health registry rather than copying a
    second list of probes into the SLO code.  The registry is the contract for
    what a probe actually tests, and using it here also means a newly-added
    core probe cannot silently disappear from the SLO gate.
    """
    from cnequity.diagnostics import source_health

    return frozenset(
        probe.key for probe in source_health.PROBES if set(probe.powers) & CORE_DATASETS
    )


@dataclass(frozen=True)
class ProbeSLO:
    key: str
    vantage: str
    critical: bool
    observations: int
    successes: int
    availability: float | None
    target: float
    latest_status: str | None
    latest_generated_at: str | None
    fresh: bool
    passed: bool


@dataclass(frozen=True)
class SourceSLOReport:
    generated_at: str
    window_days: int
    minimum_observations: int
    results: tuple[ProbeSLO, ...]

    @property
    def passed(self) -> bool:
        critical = [item for item in self.results if item.critical]
        return bool(critical) and all(item.passed for item in critical)

    def to_dict(self) -> dict:
        return {
            "generated_at": self.generated_at,
            "window_days": self.window_days,
            "minimum_observations": self.minimum_observations,
            "passed": self.passed,
            "results": [asdict(item) for item in self.results],
        }


def _parse_timestamp(raw: str) -> datetime:
    value = datetime.fromisoformat(raw)
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def store_health_report(meta_root: Path, report: HealthReport) -> tuple[Path, Path]:
    """Atomically store both the latest report and an immutable history sample."""
    generated = _parse_timestamp(report.generated_at).astimezone(timezone.utc)
    root = Path(meta_root) / "source_health"
    latest = root / f"{report.vantage}.json"
    stamp = generated.strftime("%Y%m%dT%H%M%SZ")
    historical = root / "history" / report.vantage / f"{stamp}.json"
    payload = report.to_dict()
    write_json_atomic(historical, payload, indent=2, ensure_ascii=False)
    write_json_atomic(latest, payload, indent=2, ensure_ascii=False)
    return latest, historical


def load_health_history(meta_root: Path) -> list[HealthReport]:
    """Load valid immutable samples; corrupt or duplicate reports are ignored."""
    root = Path(meta_root) / "source_health"
    paths = sorted((root / "history").glob("*/*.json")) if root.exists() else []
    reports: list[HealthReport] = []
    seen: set[tuple[str, str]] = set()
    for path in paths:
        try:
            report = HealthReport.from_dict(json.loads(path.read_text(encoding="utf-8")))
            _parse_timestamp(report.generated_at)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
        identity = (report.vantage, report.generated_at)
        if identity not in seen:
            reports.append(report)
            seen.add(identity)
    return reports


def evaluate_source_slo(
    reports: list[HealthReport],
    *,
    now: datetime | None = None,
    window_days: int = 30,
    minimum_observations: int = 10,
    core_target: float = CORE_TARGET,
    other_target: float = 0.95,
    max_age: timedelta = timedelta(days=2),
) -> SourceSLOReport:
    """Evaluate availability separately per probe and vantage.

    ``skipped`` observations are excluded rather than counted as failures.  A
    critical probe cannot pass without enough observations or a recent sample;
    this prevents a stale green report from masquerading as an SLO.
    """
    if window_days < 1 or minimum_observations < 1:
        raise ValueError("window_days and minimum_observations must be positive")
    current = now or datetime.now(timezone.utc)
    cutoff = current - timedelta(days=window_days)
    critical_keys = critical_probe_keys()
    # A point probe is not a new day of availability evidence. Keep only the
    # latest observation per probe, vantage and UTC day so repeated manual
    # retries (or two schedules on Monday) cannot manufacture the minimum
    # sample count within a few minutes.
    daily: dict[tuple[str, str, date], tuple[datetime, object]] = {}
    vantages: set[str] = set()
    for report in reports:
        generated = _parse_timestamp(report.generated_at)
        if generated < cutoff or generated > current + timedelta(minutes=5):
            continue
        # The report itself is evidence that this vantage was probed. Keep it
        # even when the probe selection was empty or every row was skipped;
        # otherwise an unprobed vantage silently disappears from the gate.
        vantages.add(report.vantage)
        for result in report.results:
            if result.status == ProbeStatus.SKIPPED.value:
                continue
            identity = (result.key, report.vantage, generated.date())
            previous = daily.get(identity)
            if previous is None or generated > previous[0]:
                daily[identity] = (generated, result)

    grouped: dict[tuple[str, str], list[tuple[datetime, object]]] = {}
    for (key, vantage, _), sample in daily.items():
        grouped.setdefault((key, vantage), []).append(sample)

    results: list[ProbeSLO] = []
    for (key, vantage), samples in sorted(grouped.items()):
        samples.sort(key=lambda item: item[0])
        # Criticality belongs to the registry, not to a caller-controlled
        # ``powers`` field in an archived report.  Otherwise a forged or stale
        # payload could relabel a core probe as advisory and pass the gate.
        critical = key in critical_keys
        target = core_target if critical else other_target
        successes = sum(item.status == ProbeStatus.OK.value for _, item in samples)
        observations = len(samples)
        availability = successes / observations if observations else None
        latest_at, latest = samples[-1]
        fresh = current - latest_at <= max_age
        passed = bool(
            observations >= minimum_observations
            and availability is not None
            and availability >= target
            and fresh
        )
        results.append(
            ProbeSLO(
                key=key,
                vantage=vantage,
                critical=critical,
                observations=observations,
                successes=successes,
                availability=availability,
                target=target,
                latest_status=latest.status,
                latest_generated_at=latest_at.isoformat(),
                fresh=fresh,
                passed=passed,
            )
        )

    # A report with no rows for one core probe must fail closed.  Since the
    # result is explicitly keyed by ``probe`` *and* ``vantage``, check the
    # registry per vantage instead of letting a green mainland observation
    # cover a missing overseas observation (or vice versa).  Keep an explicit
    # row so operators can see which probe/vantage is missing.
    vantages = sorted(vantages) or ["unknown"]
    observed_by_vantage = {
        vantage: {key for key, row_vantage in grouped if row_vantage == vantage}
        for vantage in vantages
    }
    existing_rows = {(item.key, item.vantage) for item in results if item.critical}
    for vantage in vantages:
        missing = critical_keys - observed_by_vantage.get(vantage, set())
        for key in sorted(missing):
            if (key, vantage) in existing_rows:
                continue
            results.append(
                ProbeSLO(
                    key=key,
                    vantage=vantage,
                    critical=True,
                    observations=0,
                    successes=0,
                    availability=None,
                    target=core_target,
                    latest_status=None,
                    latest_generated_at=None,
                    fresh=False,
                    passed=False,
                )
            )
    return SourceSLOReport(
        generated_at=current.isoformat(),
        window_days=window_days,
        minimum_observations=minimum_observations,
        results=tuple(results),
    )


def build_source_incidents(
    reports: list[HealthReport],
    *,
    consecutive_failures: int = 3,
) -> dict[str, Any]:
    """Build stable, de-duplicated incident payloads for repeated failures.

    Incident ids exclude timestamps and details so a CI rerun updates the same
    logical incident. A later successful observation closes the streak and no
    open payload is emitted. Skipped probes neither fail nor reset a streak.
    """
    if consecutive_failures < 1:
        raise ValueError("consecutive_failures must be positive")
    grouped: dict[tuple[str, str], list[tuple[datetime, object]]] = {}
    for report in reports:
        generated = _parse_timestamp(report.generated_at)
        for result in report.results:
            if result.status == ProbeStatus.SKIPPED.value:
                continue
            grouped.setdefault((result.key, report.vantage), []).append((generated, result))

    incidents: list[dict[str, Any]] = []
    for (key, vantage), samples in sorted(grouped.items()):
        samples.sort(key=lambda item: item[0])
        streak: list[tuple[datetime, object]] = []
        for sample in samples:
            if sample[1].status == ProbeStatus.OK.value:
                streak = []
            else:
                streak.append(sample)
        if len(streak) < consecutive_failures:
            continue
        identity = hashlib.sha256(f"{vantage}\0{key}".encode()).hexdigest()[:20]
        first_at, _ = streak[0]
        last_at, last = streak[-1]
        incidents.append(
            {
                "incident_id": f"source-break-{identity}",
                "dedupe_key": f"source-break:{vantage}:{key}",
                "state": "open",
                "probe": key,
                "vantage": vantage,
                "consecutive_failures": len(streak),
                "first_failure_at": first_at.isoformat(),
                "last_failure_at": last_at.isoformat(),
                "latest_status": last.status,
                "latest_detail": last.detail,
                "powers": list(last.powers),
                "title": f"Source regression: {key} from {vantage}",
            }
        )
    return {
        "format": "cnequity.source-incidents",
        "version": 1,
        "threshold": consecutive_failures,
        "open_incidents": incidents,
        "open_count": len(incidents),
    }


def store_source_incidents(meta_root: Path, payload: dict[str, Any]) -> Path:
    path = Path(meta_root) / "source_health" / "incidents.json"
    write_json_atomic(path, payload, indent=2, ensure_ascii=False)
    return path
