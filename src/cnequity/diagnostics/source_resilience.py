"""Machine-readable source dependency and resilience reports.

The source-health probe answers *whether one endpoint answered now*.  This
module answers the operational question that follows it: *what can one source
failure take with it, and is the core spine actually recoverable?*

The report is deliberately derived from :mod:`cnequity.domain.datasets` rather
than from a second hand-maintained list.  A source name is not automatically an
independent source: EastMoney's many host names share one WAF/failure domain,
while TDX and EastMoney do not.  Composite labels (for example
``eastmoney_kline+sina_global``) are split into their constituent domains.

No network calls are made here.  The builders are suitable for CI, where a
non-zero backup gate must be a trustworthy signal rather than a side effect of
the current network vantage.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cnequity.domain.datasets import DATASETS, DatasetSpec
from cnequity.storage.atomic import write_json_atomic

DEPENDENCY_REPORT_FORMAT = "cnequity.source-dependency"
DEPENDENCY_REPORT_VERSION = 1
BACKUP_COVERAGE_FORMAT = "cnequity.source-backup-gate"

# These are operational classes, not the storage ``layer`` and not a legal
# data classification.  Tier is the stable registry metadata available to all
# releases; callers can override an individual dataset when their deployment
# has a different blast radius.
OPERATIONAL_LEVELS: tuple[str, ...] = ("core", "research", "advisory", "experimental")
LEVEL_WEIGHTS: dict[str, int] = {
    "core": 4,
    "research": 3,
    "advisory": 2,
    "experimental": 1,
}

# Keep this aligned with the critical set used by source SLO evaluation.  The
# list is intentionally explicit. ``adj_factors`` remains research-critical
# and visible as a single-source blast radius, but is not a core run gate: an
# outage degrades adjusted research while the committed raw price spine stays
# valid (the run-status contract uses the same distinction).
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

# A conservative failure-domain registry.  Unknown source names are assigned
# their own domain, but the report marks that assignment as heuristic.  This
# prevents an unreviewed source pair from being advertised as truly
# independent merely because the labels differ.
SOURCE_FAILURE_DOMAINS: dict[str, str] = {
    "tdx_protocol": "tdx",
    "tdx": "tdx",
    "eastmoney": "eastmoney",
    "eastmoney_kline": "eastmoney",
    "eastmoney_push2": "eastmoney",
    "eastmoney_push2his": "eastmoney",
    "eastmoney_datacenter": "eastmoney",
    "sina": "sina",
    "sina_global": "sina",
    "baostock": "baostock",
    "exchange": "exchange",
    "exchange_sse": "exchange",
    "exchange_szse": "exchange",
    "cninfo": "cninfo",
    "cni": "cni",
    "sw": "sw",
    "pboc": "pboc",
    "nbs": "nbs",
    "ths": "ths",
    "ths_pages": "ths_pages",
    "derived": "local_derivation",
}

_KNOWN_DOMAIN_KEYS = frozenset(SOURCE_FAILURE_DOMAINS)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def source_components(source: str | None) -> tuple[str, ...]:
    """Split a registry source label into source components.

    Composite source labels use ``+`` by convention.  Empty components are
    ignored so a malformed optional field cannot create a fake failure domain.
    """

    if source is None:
        return ()
    return tuple(part.strip() for part in str(source).split("+") if part.strip())


def source_failure_domains(
    source: str | None,
    *,
    domain_overrides: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    """Return the conservative failure domains for *source*.

    ``domain_overrides`` is useful for a deployment that has verified a
    vendor-specific network topology.  Unknown components retain a stable
    ``source:<name>`` domain, making two unknown labels independent only when a
    caller has explicitly supplied a topology override.
    """

    overrides = domain_overrides or {}
    domains: set[str] = set()
    for component in source_components(source):
        domains.add(
            str(
                overrides.get(
                    component,
                    SOURCE_FAILURE_DOMAINS.get(component, f"source:{component}"),
                )
            )
        )
    return tuple(sorted(domains))


def _domain_confidence(source: str | None, domain_overrides: Mapping[str, str] | None) -> str:
    components = source_components(source)
    if not components:
        return "unknown"
    overrides = domain_overrides or {}
    if all(component in overrides or component in _KNOWN_DOMAIN_KEYS for component in components):
        return "declared"
    return "heuristic"


def _level_for_dataset(
    spec: DatasetSpec,
    overrides: Mapping[str, str] | None,
) -> str:
    explicit = (overrides or {}).get(spec.name)
    if explicit is None:
        # A future DatasetSpec may carry this field without this release having
        # to change its constructor.  Current releases derive it from tier.
        explicit = getattr(spec, "resilience_level", None)
    if explicit is None:
        contract_level = str(getattr(spec, "contract_level", ""))
        if contract_level in OPERATIONAL_LEVELS:
            explicit = contract_level
    if explicit in OPERATIONAL_LEVELS:
        return str(explicit)
    tier = str(getattr(spec, "tier", "L8"))
    if tier in {"L0", "L1", "L2"}:
        return "core"
    if tier in {"L3", "L4", "L5"}:
        return "research"
    if tier in {"L6", "L7"}:
        return "advisory"
    return "experimental"


def _registry(datasets: Mapping[str, DatasetSpec] | Iterable[DatasetSpec] | None):
    if datasets is None:
        return DATASETS
    if isinstance(datasets, Mapping):
        return datasets
    return {spec.name: spec for spec in datasets}


def _source_role_record(
    source: str | None,
    *,
    domain_overrides: Mapping[str, str] | None,
) -> dict[str, Any]:
    components = list(source_components(source))
    domains = list(source_failure_domains(source, domain_overrides=domain_overrides))
    return {
        "label": source,
        "components": components,
        "failure_domains": domains,
        "domain_confidence": _domain_confidence(source, domain_overrides),
    }


@dataclass(frozen=True)
class BackupCoverageIssue:
    """One core dataset that cannot fail over safely."""

    dataset: str
    level: str
    primary: str | None
    backup: str | None
    primary_failure_domains: tuple[str, ...]
    backup_failure_domains: tuple[str, ...]
    reason: str
    severity: str = "error"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BackupCoverageReport:
    """Fail-closed result for critical/core source backup coverage."""

    generated_at: str
    critical_datasets: tuple[str, ...]
    covered_datasets: tuple[str, ...]
    issues: tuple[BackupCoverageIssue, ...]

    @property
    def passed(self) -> bool:
        return not self.issues and bool(self.critical_datasets)

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": BACKUP_COVERAGE_FORMAT,
            "version": DEPENDENCY_REPORT_VERSION,
            "generated_at": self.generated_at,
            "passed": self.passed,
            "critical_dataset_count": len(self.critical_datasets),
            "covered_dataset_count": len(self.covered_datasets),
            "critical_datasets": list(self.critical_datasets),
            "covered_datasets": list(self.covered_datasets),
            "issues": [item.to_dict() for item in self.issues],
        }


class BackupCoverageError(RuntimeError):
    """Raised when a fail-closed backup gate is asserted and does not pass."""

    def __init__(self, report: BackupCoverageReport):
        self.report = report
        names = ", ".join(item.dataset for item in report.issues) or "no critical datasets"
        super().__init__(f"source backup coverage gate failed: {names}")


def evaluate_backup_coverage(
    datasets: Mapping[str, DatasetSpec] | Iterable[DatasetSpec] | None = None,
    *,
    critical_datasets: Iterable[str] | None = None,
    level_overrides: Mapping[str, str] | None = None,
    domain_overrides: Mapping[str, str] | None = None,
    generated_at: str | None = None,
) -> BackupCoverageReport:
    """Evaluate whether every required critical dataset has a real backup.

    A backup is real only when its failure-domain set is disjoint from the
    primary's set.  Missing datasets in a caller-provided critical list are
    also failures, which makes the function safe for a proposed registry.
    Optional datasets are not allowed to weaken the gate, but a required
    dataset in the critical set always fails closed.
    """

    registry = _registry(datasets)
    critical = frozenset(critical_datasets or CORE_DATASETS)
    issues: list[BackupCoverageIssue] = []
    covered: list[str] = []
    for name in sorted(critical):
        spec = registry.get(name)
        if spec is None:
            issues.append(
                BackupCoverageIssue(
                    dataset=name,
                    level="unknown",
                    primary=None,
                    backup=None,
                    primary_failure_domains=(),
                    backup_failure_domains=(),
                    reason="dataset_missing_from_registry",
                )
            )
            continue
        level = _level_for_dataset(spec, level_overrides)
        primary = spec.primary_source or None
        backup = spec.backup_source or None
        p_domains = source_failure_domains(primary, domain_overrides=domain_overrides)
        b_domains = source_failure_domains(backup, domain_overrides=domain_overrides)
        if not getattr(spec, "required", True):
            # A disabled/optional core-labelled dataset is disclosed but does
            # not make a deployment fail solely because it was not enabled.
            covered.append(name)
            continue
        if not primary:
            reason = "missing_primary_source"
        elif not backup:
            reason = "missing_backup_source"
        elif set(p_domains) & set(b_domains):
            reason = "backup_shares_failure_domain"
        else:
            covered.append(name)
            continue
        issues.append(
            BackupCoverageIssue(
                dataset=name,
                level=level,
                primary=primary,
                backup=backup,
                primary_failure_domains=p_domains,
                backup_failure_domains=b_domains,
                reason=reason,
            )
        )
    return BackupCoverageReport(
        generated_at=generated_at or _utc_now(),
        critical_datasets=tuple(sorted(critical)),
        covered_datasets=tuple(sorted(covered)),
        issues=tuple(issues),
    )


def backup_coverage_gate(
    datasets: Mapping[str, DatasetSpec] | Iterable[DatasetSpec] | None = None,
    **kwargs: Any,
) -> bool:
    """Return the fail-closed backup gate result as a plain boolean."""

    return evaluate_backup_coverage(datasets, **kwargs).passed


def assert_backup_coverage(
    datasets: Mapping[str, DatasetSpec] | Iterable[DatasetSpec] | None = None,
    **kwargs: Any,
) -> BackupCoverageReport:
    report = evaluate_backup_coverage(datasets, **kwargs)
    if not report.passed:
        raise BackupCoverageError(report)
    return report


@dataclass(frozen=True)
class SourceDependencyReport:
    """Complete dependency, concentration and blast-radius report."""

    generated_at: str
    datasets: tuple[dict[str, Any], ...]
    sources: tuple[dict[str, Any], ...]
    blast_radii: tuple[dict[str, Any], ...]
    backup_gate: BackupCoverageReport

    @property
    def passed(self) -> bool:
        return self.backup_gate.passed

    def to_dict(self) -> dict[str, Any]:
        primary_count = sum(1 for item in self.datasets if item.get("primary"))
        by_level: dict[str, int] = {level: 0 for level in OPERATIONAL_LEVELS}
        for item in self.datasets:
            level = item.get("level")
            if level in by_level:
                by_level[level] += 1
        return {
            "format": DEPENDENCY_REPORT_FORMAT,
            "version": DEPENDENCY_REPORT_VERSION,
            "generated_at": self.generated_at,
            "passed": self.passed,
            "levels": list(OPERATIONAL_LEVELS),
            "summary": {
                "dataset_count": len(self.datasets),
                "primary_dependency_count": primary_count,
                "source_count": len(self.sources),
                "blast_radius_count": len(self.blast_radii),
                "datasets_by_level": by_level,
            },
            "datasets": [dict(item) for item in self.datasets],
            "sources": [dict(item) for item in self.sources],
            "blast_radii": [dict(item) for item in self.blast_radii],
            "backup_gate": self.backup_gate.to_dict(),
        }

    # Mapping-like access keeps this convenient for shell/report consumers
    # while retaining typed properties for Python callers.
    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def as_dict(self) -> dict[str, Any]:
        return self.to_dict()


def build_dependency_report(
    datasets: Mapping[str, DatasetSpec] | Iterable[DatasetSpec] | None = None,
    *,
    level_overrides: Mapping[str, str] | None = None,
    domain_overrides: Mapping[str, str] | None = None,
    critical_datasets: Iterable[str] | None = None,
    generated_at: str | None = None,
) -> SourceDependencyReport:
    """Build deterministic source dependency/concentration/blast-radius data."""

    registry = _registry(datasets)
    records: list[dict[str, Any]] = []
    source_stats: dict[str, dict[str, Any]] = {}
    radius_stats: dict[str, dict[str, Any]] = {}

    def ensure_source(label: str) -> dict[str, Any]:
        if label not in source_stats:
            source_stats[label] = {
                "source": label,
                "failure_domains": list(
                    source_failure_domains(label, domain_overrides=domain_overrides)
                ),
                "domain_confidence": _domain_confidence(label, domain_overrides),
                "dataset_names": set(),
                "primary_dataset_names": set(),
                "backup_dataset_names": set(),
                "backfill_dataset_names": set(),
                "levels": set(),
                "critical_dataset_names": set(),
            }
        return source_stats[label]

    def ensure_radius(domain: str) -> dict[str, Any]:
        if domain not in radius_stats:
            radius_stats[domain] = {
                "failure_domain": domain,
                "sources": set(),
                "dataset_names": set(),
                "critical_dataset_names": set(),
                "levels": set(),
            }
        return radius_stats[domain]

    for name in sorted(registry):
        spec = registry[name]
        level = _level_for_dataset(spec, level_overrides)
        critical = name in frozenset(critical_datasets or CORE_DATASETS)
        roles = {
            "primary": spec.primary_source or None,
            "backup": spec.backup_source or None,
            "backfill": spec.backfill_source or None,
        }
        role_records = {
            role: _source_role_record(source, domain_overrides=domain_overrides)
            for role, source in roles.items()
        }
        # The rest of the recovery chain. These are dependencies like any other
        # — a dataset whose tip is served by the exchange board files is down
        # when those are — so they belong in the blast radius even though they
        # hold none of the three roles the backup gate reasons about.
        gaps = tuple(getattr(spec, "backup_gaps", ()) or ())
        supplementary = tuple(getattr(spec, "supplementary_sources", ()) or ())
        supplementary_records = [
            _source_role_record(source, domain_overrides=domain_overrides)
            for source in supplementary
        ]
        p_domains = set(role_records["primary"]["failure_domains"])
        b_domains = set(role_records["backup"]["failure_domains"])
        if not critical:
            backup_status = "not_critical"
            independent: bool | None = None
            independence_reason = "not_critical"
        elif not getattr(spec, "required", True):
            backup_status = "optional"
            independent = None
            independence_reason = "optional_dataset"
        elif not roles["backup"]:
            backup_status = "missing"
            independent = False
            independence_reason = "missing_backup_source"
        else:
            independent = not bool(p_domains & b_domains)
            backup_status = "covered" if independent else "not_independent"
            independence_reason = (
                "disjoint_failure_domains" if independent else "backup_shares_failure_domain"
            )
            # Independent and complete are different questions. The exchanges
            # are a genuinely separate failure domain from EastMoney and still
            # publish nothing for Beijing, so a reader told only "covered"
            # would plan for a degraded day that keeps every name.
            if independent and gaps:
                backup_status = "covered_with_gaps"
        all_domains = sorted(
            set(role_records["primary"]["failure_domains"])
            | set(role_records["backup"]["failure_domains"])
            | set(role_records["backfill"]["failure_domains"])
            | {domain for rec in supplementary_records for domain in rec["failure_domains"]}
        )
        record = {
            "dataset": name,
            "backup_gaps": list(gaps),
            "tier": getattr(spec, "tier", None),
            "contract_level": getattr(spec, "contract_level", None),
            "level": level,
            "required": bool(getattr(spec, "required", True)),
            "critical": critical,
            "primary": roles["primary"],
            "backup": roles["backup"],
            "backfill": roles["backfill"],
            "sources": role_records,
            "supplementary": supplementary_records,
            "failure_domains": all_domains,
            "backup_coverage": {
                "required": critical and bool(getattr(spec, "required", True)),
                "status": backup_status,
                "independent": independent,
                "reason": independence_reason,
                "primary_domains": sorted(p_domains),
                "backup_domains": sorted(b_domains),
            },
            "impact": {
                "blast_radius": all_domains,
                "single_source_primary": bool(roles["primary"] and not roles["backup"]),
                "affected_if_primary_down": [name],
            },
        }
        records.append(record)

        for role, source in roles.items():
            if not source:
                continue
            for component in source_components(source):
                stat = ensure_source(component)
                stat["dataset_names"].add(name)
                stat[f"{role}_dataset_names"].add(name)
                stat["levels"].add(level)
                if critical:
                    stat["critical_dataset_names"].add(name)
        for domain in all_domains:
            radius = ensure_radius(domain)
            radius["dataset_names"].add(name)
            radius["levels"].add(level)
            if critical:
                radius["critical_dataset_names"].add(name)
            for source in roles.values():
                if source:
                    for component in source_components(source):
                        if domain in source_failure_domains(
                            component, domain_overrides=domain_overrides
                        ):
                            radius["sources"].add(component)

    total_primary = len(records)
    total_weight = sum(LEVEL_WEIGHTS.get(item["level"], 1) for item in records if item["primary"])
    source_records: list[dict[str, Any]] = []
    for label in sorted(source_stats):
        stat = source_stats[label]
        primary_names = sorted(stat["primary_dataset_names"])
        weighted = sum(
            LEVEL_WEIGHTS.get(next(item["level"] for item in records if item["dataset"] == name), 1)
            for name in primary_names
        )
        source_records.append(
            {
                "source": label,
                "failure_domains": stat["failure_domains"],
                "domain_confidence": stat["domain_confidence"],
                "dataset_names": sorted(stat["dataset_names"]),
                "primary_dataset_names": primary_names,
                "backup_dataset_names": sorted(stat["backup_dataset_names"]),
                "backfill_dataset_names": sorted(stat["backfill_dataset_names"]),
                "levels": sorted(stat["levels"], key=OPERATIONAL_LEVELS.index),
                "critical_dataset_names": sorted(stat["critical_dataset_names"]),
                "concentration": {
                    "primary_dataset_count": len(primary_names),
                    "primary_share": len(primary_names) / total_primary if total_primary else 0.0,
                    "weighted_primary_score": weighted,
                    "weighted_primary_share": weighted / total_weight if total_weight else 0.0,
                },
            }
        )

    radius_records = [
        {
            "failure_domain": domain,
            "sources": sorted(stat["sources"]),
            "dataset_names": sorted(stat["dataset_names"]),
            "critical_dataset_names": sorted(stat["critical_dataset_names"]),
            "levels": sorted(stat["levels"], key=OPERATIONAL_LEVELS.index),
            "dataset_count": len(stat["dataset_names"]),
            "critical_dataset_count": len(stat["critical_dataset_names"]),
        }
        for domain, stat in sorted(radius_stats.items())
    ]
    gate = evaluate_backup_coverage(
        registry,
        critical_datasets=critical_datasets,
        level_overrides=level_overrides,
        domain_overrides=domain_overrides,
        generated_at=generated_at,
    )
    return SourceDependencyReport(
        generated_at=generated_at or _utc_now(),
        datasets=tuple(records),
        sources=tuple(source_records),
        blast_radii=tuple(radius_records),
        backup_gate=gate,
    )


# Explicit aliases make the operational terminology discoverable to callers
# without forcing them to guess whether the report is called "dependency" or
# "source dependency" in a given command.
build_source_dependency_report = build_dependency_report


def export_dependency_report(
    path: str | Path,
    datasets: Mapping[str, DatasetSpec] | Iterable[DatasetSpec] | None = None,
    **kwargs: Any,
) -> SourceDependencyReport:
    """Build and atomically write a dependency report to *path*."""

    report = build_dependency_report(datasets, **kwargs)
    write_json_atomic(Path(path), report.to_dict(), indent=2, ensure_ascii=False)
    return report


export_source_dependency_report = export_dependency_report


def dependency_fingerprint(report: SourceDependencyReport | Mapping[str, Any]) -> str:
    """Return a stable digest useful for CI artifact de-duplication."""

    payload = report.to_dict() if isinstance(report, SourceDependencyReport) else dict(report)
    payload.pop("generated_at", None)
    if isinstance(payload.get("backup_gate"), dict):
        payload["backup_gate"].pop("generated_at", None)
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


__all__ = [
    "BACKUP_COVERAGE_FORMAT",
    "BackupCoverageError",
    "BackupCoverageIssue",
    "BackupCoverageReport",
    "CORE_DATASETS",
    "DEPENDENCY_REPORT_FORMAT",
    "DEPENDENCY_REPORT_VERSION",
    "LEVEL_WEIGHTS",
    "OPERATIONAL_LEVELS",
    "SOURCE_FAILURE_DOMAINS",
    "SourceDependencyReport",
    "assert_backup_coverage",
    "backup_coverage_gate",
    "build_dependency_report",
    "build_source_dependency_report",
    "dependency_fingerprint",
    "evaluate_backup_coverage",
    "export_dependency_report",
    "export_source_dependency_report",
    "source_components",
    "source_failure_domains",
]


def annotate_measured_availability(
    payload: dict[str, Any], slo: Mapping[str, Any]
) -> dict[str, Any]:
    """Join measured probe availability onto a declared-dependency report.

    ``build_dependency_report`` is deliberately a pure function of the registry:
    it says which datasets share a failure domain, and it must stay
    deterministic and testable. What it cannot say is whether a domain is
    actually *reachable from here* — and that is half of the routing decision.
    Measured from this host over 30 days, the EastMoney domain carries 29
    datasets (4 critical) at 58.8% availability while ``tdx``, ``exchange_sse``
    and ``ths_kline`` all sat at 100%. A concentration report that cannot show
    that leaves the expensive half of the choice to memory.

    The join is by failure domain, taking each domain's *worst* measured probe:
    a domain is only as reachable as its least reachable endpoint, and a feed
    that needs two of them is down when either is.

    Returns a new payload; the input is not modified. Probes with no
    observations contribute nothing rather than a false zero.
    """
    by_domain: dict[str, dict[str, Any]] = {}
    for result in slo.get("results", []) or []:
        availability = result.get("availability")
        if availability is None or not result.get("observations"):
            continue
        for domain in source_failure_domains(result.get("key")):
            worst = by_domain.get(domain)
            if worst is None or availability < worst["availability"]:
                by_domain[domain] = {
                    "availability": availability,
                    "probe": result.get("key"),
                    "vantage": result.get("vantage"),
                    "observations": result.get("observations"),
                    "target": result.get("target"),
                    "meets_target": bool(result.get("passed")),
                }

    out = json.loads(json.dumps(payload))
    out["measured_availability"] = {
        "window_days": slo.get("window_days"),
        "by_failure_domain": dict(sorted(by_domain.items())),
    }
    for radius in out.get("blast_radii", []) or []:
        measured = by_domain.get(radius.get("failure_domain"))
        if measured is not None:
            radius["measured_availability"] = measured
    return out
