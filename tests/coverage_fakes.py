"""Stand-ins for what a test lake's coverage ledger would say.

A dataset's watermark is computed from its published rows and what a run
fetches comes from the coverage planner, so a test that only cares about a
window says how far the lake is covered instead of publishing rows for it.
"""

from __future__ import annotations

from datetime import date


def covered(monkeypatch, dataset: str, through: date | None, *, gaps: list[date] = ()) -> None:
    """*dataset* holds every cell through *through*; the planner owes only *gaps*.

    Stacks: each call adds one dataset, the others keep their real answer.
    """
    from cnequity.quality import planned_coverage, verify

    watermarks = getattr(verify.covered_through, "_fake_watermarks", None)
    owed = getattr(planned_coverage.gap_days, "_fake_gaps", None)
    if watermarks is None:
        real_mark, real_gaps = verify.covered_through, planned_coverage.gap_days
        watermarks, owed = {}, {}

        def fake_mark(config, name):
            return watermarks[name] if name in watermarks else real_mark(config, name)

        def fake_gaps(config, name, day):
            return list(owed[name]) if name in owed else real_gaps(config, name, day)

        fake_mark._fake_watermarks = watermarks
        fake_gaps._fake_gaps = owed
        monkeypatch.setattr(verify, "covered_through", fake_mark)
        monkeypatch.setattr(planned_coverage, "gap_days", fake_gaps)
    watermarks[dataset] = through
    owed[dataset] = list(gaps)
