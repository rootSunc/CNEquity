"""Coverage evidence derived from the exact candidate or committed bytes."""

from __future__ import annotations

GAP_SAMPLE_LIMIT = 20


def gap_summary(gaps: dict) -> dict:
    """Bound a gap description for an immutable revision receipt.

    The full gap index lives in the coverage ledger and grows with the
    lake; copying it into every receipt made each publication rewrite it
    whole. A receipt keeps the counts and the first few entries of each kind.
    """
    summary: dict = {"counts": {}, "sample": {}}
    for kind, value in gaps.items():
        items = list(value.items()) if isinstance(value, dict) else list(value)
        summary["counts"][kind] = len(items)
        sample = items[:GAP_SAMPLE_LIMIT]
        summary["sample"][kind] = dict(sample) if isinstance(value, dict) else sample
    summary["truncated"] = any(count > GAP_SAMPLE_LIMIT for count in summary["counts"].values())
    return summary
