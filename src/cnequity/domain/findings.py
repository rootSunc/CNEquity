"""Audit findings: one shape, one severity scale.

A finding is a JSON object written to ``meta/quality`` and read by ``cne audit``,
``cne check``, the dashboard and the gate (``quality.gate``). Every one carries
``dataset``, ``check``, ``severity`` and a ``message`` a person can read; other
keys are evidence for that check.

``severity`` is one of :data:`SEVERITIES`:

* ``error`` — a stored fact is wrong or required data is missing; gates fail on it;
* ``warning`` — worth a person's attention, but the data stands;
* ``info`` — a record of what was done or found, for the evidence trail.

Findings were untyped dicts built in some 240 places. The scale held, but
nothing enforced it, and the adjust-factor findings, most of any lake's
findings, went out with no message and printed as blank lines. New code builds
them with :func:`finding`; :func:`checked_findings` guards the audit boundary.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

logger = logging.getLogger(__name__)

SEVERITIES: tuple[str, ...] = ("error", "warning", "info")


class FindingError(ValueError):
    """A finding the audit cannot judge: no check, or a severity off the scale."""


def finding(*, dataset: str, check: str, severity: str, message: str, **evidence: object) -> dict:
    """One audit finding, checked; *evidence* keys travel with it unchanged."""
    if severity not in SEVERITIES:
        raise FindingError(f"{check}: severity {severity!r} is not one of {SEVERITIES}")
    if not check:
        raise FindingError(f"{dataset}: a finding needs a check name")
    if not message:
        raise FindingError(f"{check}: a finding needs a message")
    return {
        "dataset": dataset,
        "check": check,
        "severity": severity,
        "message": message,
        **evidence,
    }


def checked_findings(findings: Iterable[dict]) -> list[dict]:
    """*findings* as the audit writes them, refusing ones it cannot judge.

    A bad severity or a missing check name raises: the gate would misjudge the
    finding, and both are a producer's bug. A missing message is logged and
    replaced by the check name, so a daily audit never fails over wording.
    """
    out = []
    for item in findings:
        severity = item.get("severity")
        if severity not in SEVERITIES:
            raise FindingError(
                f"{item.get('check')}: severity {severity!r} is not one of {SEVERITIES}"
            )
        if not item.get("check"):
            raise FindingError(f"{item.get('dataset')}: a finding needs a check name: {item}")
        if not item.get("message"):
            logger.warning("finding %s on %s has no message", item["check"], item.get("dataset"))
            item = {**item, "message": str(item["check"])}
        out.append(item)
    return out
