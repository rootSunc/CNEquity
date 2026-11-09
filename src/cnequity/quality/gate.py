"""Which audit errors fail a gate, decided in one place.

An error finding is one of two kinds:

* a contradiction: a stored fact is wrong (a negative price, a duplicate key);
* a coverage gap (``coverage_only``): an observation is missing, and the rows
  that do exist are still right.

Publishing a candidate and a run's own status ask whether what was written is
right, so they count contradictions only (``contradictions``). The lake gates,
``cne audit`` (with or without ``--full``) and ``cne check``, ask whether the
lake is usable, so a gap fails them too (``audit_verdict``). ``cne check`` judges
only the core datasets; the errors it leaves out are returned, not dropped.

Factor findings are advisory everywhere (``factor_policy``). Every audit
boundary already downgrades them; they are excluded here as well, so a findings
file written before that downgrade gets the same verdict.

``cne check`` once dropped coverage gaps and ``cne audit`` did not, so the same
findings could pass one and fail the other.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from cnequity.domain.datasets import DATASETS, core_dataset_names
from cnequity.quality.factor_policy import factor_advisory

# These checks describe absent observations, rather than contradictions in
# the stored facts.
_COVERAGE_CHECKS = frozenset(
    {
        "exists",
        "non_empty",
        "source_scope_incomplete",
        "daily_bars_calendar_missing_day",
        "valuation_bars_no_shared_date",
        "valuation_bars_low_coverage",
        "adj_factor_source_unavailable",
        "adj_factor_coverage",
        "close_crosscheck_unavailable",
        "untraded_instruments",
        "industry_index_missing_groups",
    }
)


def coverage_only(finding: dict) -> bool:
    """An absent observation: it does not make the rows that exist wrong."""
    return finding.get("check") in _COVERAGE_CHECKS


def is_core(dataset: str) -> bool:
    """A core dataset, or a name the registry does not know (lake-wide findings)."""
    return dataset not in DATASETS or dataset in core_dataset_names()


def _gating_errors(findings: Iterable[dict]) -> list[dict]:
    return [
        item for item in findings if item.get("severity") == "error" and not factor_advisory(item)
    ]


def is_contradiction(finding: dict) -> bool:
    """Whether an error finding says a stored fact is wrong, whatever its severity."""
    return not coverage_only(finding) and not factor_advisory(finding)


def contradictions(findings: Iterable[dict]) -> list[dict]:
    """Error findings that say a stored fact is wrong (coverage gaps left out)."""
    return [item for item in findings if item.get("severity") == "error" and is_contradiction(item)]


@dataclass(frozen=True)
class AuditVerdict:
    """The errors that fail a lake gate, and the ones outside its scope."""

    errors: list[dict] = field(default_factory=list)
    outside_scope: list[dict] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.errors


def audit_verdict(findings: Iterable[dict], *, core_only: bool = False) -> AuditVerdict:
    """Judge audit findings as a lake gate; *core_only* is ``cne check``'s scope."""
    errors = _gating_errors(findings)
    if not core_only:
        return AuditVerdict(errors=errors)
    inside = [item for item in errors if is_core(str(item.get("dataset") or ""))]
    outside = [item for item in errors if not is_core(str(item.get("dataset") or ""))]
    return AuditVerdict(errors=inside, outside_scope=outside)
