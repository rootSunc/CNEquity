"""Init job phase definitions and step-completion helpers."""

from __future__ import annotations

from typing import Any

INIT_PHASE_STEPS: dict[str, list[str]] = {
    "phase1_reference": ["instruments", "trading_calendar"],
    "phase2a_corporate_actions": ["corporate_actions"],
    "phase2b_daily_bars_incremental": ["daily_bars"],
    "phase2c_daily_bars_backfill": ["daily_bars"],
    "phase3_index_and_status": ["index_bars", "trading_status"],
    "phase4_finalize": ["compact", "derive_adj_factors", "derive_industry_index", "audit"],
}

INIT_BACKFILL_PHASES = frozenset(
    {
        # trading_calendar must span the full bar history (2016+) or historical
        # PIT/trading-day alignment falls back to a Mon–Fri heuristic. instruments
        # ignores the date window, so backfilling phase1 is safe.
        "phase1_reference",
        "phase2a_corporate_actions",
        "phase2c_daily_bars_backfill",
        # index_bars needs the same 2016+ history as daily_bars for market/beta
        # factors. trading_status ignores backfill (single-day snapshot fetch).
        "phase3_index_and_status",
    }
)

FINALIZE_STEPS = frozenset({"compact", "derive_adj_factors", "derive_industry_index", "audit"})
RESOLVED_BATCH_STATUSES = frozenset({"success", "superseded"})

DEFAULT_INIT_PHASES = [
    "phase1_reference",
    "phase2a_corporate_actions",
    "phase2c_daily_bars_backfill",
    "phase3_index_and_status",
    "phase4_finalize",
]


def expected_steps(phases: list[str]) -> list[str]:
    """Ordered unique step names for the given init phase list."""
    out: list[str] = []
    seen: set[str] = set()
    for phase in phases:
        for step in INIT_PHASE_STEPS.get(phase, []):
            if step not in seen:
                seen.add(step)
                out.append(step)
    return out


def phase_backfill(phase: str) -> bool:
    return phase in INIT_BACKFILL_PHASES


def step_backfill(step: str, phases: list[str]) -> bool:
    if step == "corporate_actions":
        return "phase2a_corporate_actions" in phases
    if step == "daily_bars":
        return "phase2c_daily_bars_backfill" in phases
    if step in ("instruments", "trading_calendar"):
        return "phase1_reference" in phases
    if step in ("index_bars", "trading_status"):
        return "phase3_index_and_status" in phases
    return False


def datasets_in_run(batches: list[Any]) -> set[str]:
    return {str(b["dataset"]) for b in batches}


def step_batches(batches: list[Any], step: str) -> list[Any]:
    return [b for b in batches if b["dataset"] == step]


def step_started(batches: list[Any], step: str) -> bool:
    return bool(step_batches(batches, step))


def step_succeeded(batches: list[Any], step: str) -> bool:
    rows = step_batches(batches, step)
    return bool(rows) and all(r["status"] in RESOLVED_BATCH_STATUSES for r in rows)


def step_incomplete(batches: list[Any], step: str) -> bool:
    rows = step_batches(batches, step)
    return bool(rows) and any(r["status"] not in RESOLVED_BATCH_STATUSES for r in rows)


def current_phase_statuses(phases: list[str], batches: list[Any]) -> dict[str, str]:
    """Recompute phase state from the current ledger, not old run output."""
    out: dict[str, str] = {}
    for phase in phases:
        steps = INIT_PHASE_STEPS.get(phase, [])
        if not steps:
            continue
        if all(step_succeeded(batches, step) for step in steps):
            out[phase] = "success"
        elif any(step_incomplete(batches, step) for step in steps):
            out[phase] = "incomplete"
        else:
            out[phase] = "pending"
    return out


def missing_steps(phases: list[str], batches: list[Any]) -> list[str]:
    """Steps in *phases* that have no batch records yet."""
    present = datasets_in_run(batches)
    return [s for s in expected_steps(phases) if s not in present]


def pending_phases(phases: list[str], batches: list[Any]) -> list[str]:
    """Phases that are not fully successful yet."""
    out: list[str] = []
    for phase in phases:
        steps = INIT_PHASE_STEPS.get(phase, [])
        if not steps:
            continue
        if all(step_succeeded(batches, step) for step in steps):
            continue
        out.append(phase)
    return out


def phases_never_started(phases: list[str], batches: list[Any]) -> list[str]:
    """Phases with no batch records yet (partial phases are left to retry)."""
    out: list[str] = []
    for phase in phases:
        steps = INIT_PHASE_STEPS.get(phase, [])
        if not steps:
            continue
        if all(step_succeeded(batches, step) for step in steps):
            continue
        if any(step_started(batches, step) for step in steps):
            continue
        out.append(phase)
    return out


def init_run_complete(phases: list[str], batches: list[Any]) -> bool:
    return all(step_succeeded(batches, step) for step in expected_steps(phases))


def needs_finalize(phases: list[str], batches: list[Any]) -> bool:
    if "phase4_finalize" not in phases:
        return False
    if not init_run_complete(
        [p for p in phases if p != "phase4_finalize"],
        batches,
    ):
        return False
    return not all(step_succeeded(batches, step) for step in FINALIZE_STEPS)
