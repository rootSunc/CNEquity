"""Reconcile stale BSE session debts against pinned, committed lake inputs."""

from datetime import date, datetime, timezone
from uuid import uuid4

from cnequity.config import Config
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.coverage import resolved_bj_session_dates, resolved_prelisting_keys
from cnequity.storage.lake_write import InputChanged
from cnequity.storage.read_context import ReadContext
from cnequity.storage.state import StateStore

INPUTS = ("daily_bars", "instruments", "trading_status")


def repair_daily_bar_coverage(config: Config, *, apply: bool = False) -> dict:
    """Clear the BSE session and pre-listing debts the pinned inputs prove.

    The proof is read without the lake lock. The lock is taken only to clear
    the debts, and only if every input is still the revision the proof read.
    """
    context = ReadContext.capture(config, INPUTS, {})
    state = StateStore(config.meta_root)
    payload = state.get_payload("daily_bars")
    days = resolved_bj_session_dates(config, payload, read_context=context)
    early = resolved_prelisting_keys(
        config, payload.get("outstanding_keys", []), read_context=context
    )
    report = {
        "applied": False,
        "resolved_sessions": sorted(str(day) for day in days),
        "resolved_prelisting_keys": sorted(early),
        "input_roots": {name: str(root) for name, root in context.roots.items()},
        "before": payload,
        "scope": "BSE session debts use known listing spans; unknown listing dates remain unverified in per-security coverage.",
    }
    if not apply or (not days and not early and payload.get("coverage_status") == "complete"):
        return report
    with lake_mutation_lock(config.meta_root, blocking=True):
        current = ReadContext.capture(config, INPUTS, {})
        if current.roots != context.roots:
            changed = [n for n in INPUTS if context.roots[n] != current.roots[n]]
            raise InputChanged(changed[0], "the coverage proof read it")
        return _clear(config, state, report, days, early)


def _clear(config: Config, state: StateStore, report: dict, days, early) -> dict:
    from cnequity.steps.finalize import _update_watermarks

    run_id = f"bar-coverage-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    path = config.meta_root / "repairs" / f"{run_id}.json"
    # Persist the proof before modifying the debt ledger.
    write_json_atomic(path, report, indent=2, default=str)
    state.clear_missing_dates("daily_bars", days)
    state.clear_outstanding_keys("daily_bars", early)
    _update_watermarks(config, frozenset({"daily_bars"}), date.today())
    report.update(applied=True, after=state.get_payload("daily_bars"), receipt=str(path))
    write_json_atomic(path, report, indent=2, default=str)
    return report
