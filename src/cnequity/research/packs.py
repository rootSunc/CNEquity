"""Closed sets of datasets for a research question.

``cne init`` always builds the same market spine. A pack does not change that
download. It decides which daily groups are worth their source budget afterwards,
and what is still missing before the question can be answered.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cnequity.i18n import N_, _, localize
from cnequity.storage.atomic import write_json_atomic

PACK_ORDER = ("market", "fundamentals", "universe")

#: Translate where shown: `_(SNAPSHOT_NOTE)`.
SNAPSHOT_NOTE = N_(
    "A snapshot such as trading status that misses a day cannot be filled by the next dated "
    "daily update; the closing catch-up tries again that same day. Announcements and news are "
    "not part of the research-pack daily update; run `cne run events` separately when needed."
)


@dataclass(frozen=True)
class ResearchPack:
    """A pack; `title`, `summary` and `extra` are catalog messages (`N_`)."""

    id: str
    title: str
    summary: str
    datasets: tuple[str, ...]
    groups: tuple[str, ...]
    next_command: str
    extra: str = ""


PACKS: dict[str, ResearchPack] = {
    "market": ResearchPack(
        id="market",
        title=N_("Market"),
        summary=N_(
            "Whole-market daily bars, adjustment, calendar, corporate actions and current "
            "trading status. Init always downloads this set."
        ),
        datasets=(
            "instruments",
            "trading_calendar",
            "corporate_actions",
            "daily_bars",
            "index_bars",
            "adj_factors",
            "trading_status",
        ),
        groups=("core",),
        next_command="cne init",
    ),
    "fundamentals": ResearchPack(
        id="fundamentals",
        title=N_("Fundamentals"),
        summary=N_(
            "Financial statements, share structure and the disclosure schedule. "
            "Not downloaded by init."
        ),
        datasets=(
            "financial_statement_items",
            "share_structure",
            "earnings_disclosure_schedule",
        ),
        groups=("fundamentals",),
        next_command="cne run daily --pack fundamentals",
        extra=N_(
            "This daily group also brings index constituents, industry constituents and "
            "shareholder counts. For valuation history use `cne backfill valuation_metrics`; "
            "there is no need to enable the whole money-flow group for it."
        ),
    ),
    "universe": ResearchPack(
        id="universe",
        title=N_("Universe"),
        summary=N_(
            "Historical ST evidence. Current trading status and delisted daily bars inside "
            "the window are already in the market pack."
        ),
        datasets=(),
        groups=(),
        next_command="cne backfill trading_status",
        extra=N_(
            "This pack adds no daily-update requests. Historical ST is a separate one-off backfill."
        ),
    ),
}


def normalize_packs(names: Sequence[str] | None) -> tuple[str, ...]:
    """Return known packs in display order. An empty selection is the market pack."""
    if not names:
        return ("market",)
    unknown = [name for name in names if name not in PACKS]
    if unknown:
        raise ValueError(_("Unknown research packs: {names}").format(names=_(", ").join(unknown)))
    selected = set(names)
    return tuple(name for name in PACK_ORDER if name in selected)


def next_command_for_dataset(dataset: str) -> str | None:
    """The command that first builds *dataset*, when a research pack owns it.

    Derived factors stay with ``cne derive``: init publishes them at the end,
    and a lake that already has bars should not be sent back through init.
    """
    if dataset in {"adj_factors", "industry_index"}:
        return None
    for pack in PACKS.values():
        if dataset in pack.datasets:
            return pack.next_command
    return None


def groups_for(names: Sequence[str]) -> list[str]:
    ordered: list[str] = []
    for pack_id in normalize_packs(names):
        for group in PACKS[pack_id].groups:
            if group not in ordered:
                ordered.append(group)
    return ordered


def packs_path(config) -> Path:
    from cnequity.meta_layout import CHECKPOINTS

    return Path(config.meta_root) / CHECKPOINTS / "research_packs.json"


def load_packs(config) -> tuple[str, ...] | None:
    path = packs_path(config)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        names = payload.get("packs") if isinstance(payload, dict) else None
        if not isinstance(names, list):
            return None
        return normalize_packs([str(name) for name in names])
    except (OSError, ValueError, TypeError):
        return None


def remember_packs(config, names: Sequence[str] | None, *, override: bool) -> tuple[str, ...]:
    """Persist the selection. A later init without ``--pack`` keeps the previous one."""
    existing = load_packs(config)
    chosen = normalize_packs(names) if override or not existing else existing
    path = packs_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, {"packs": list(chosen)})
    return chosen


def snapshot_steps(config, group_names: Sequence[str]) -> list[str]:
    from cnequity.domain.datasets import DATASETS, history_mode_for

    found: list[str] = []
    groups = getattr(config, "schedule_groups", {}) or {}
    for name in group_names:
        group = groups.get(name)
        if group is None:
            continue
        for step in group.steps:
            spec = DATASETS.get(step)
            if spec is not None and history_mode_for(spec) != "by_date" and step not in found:
                found.append(step)
    return found


def _catalog(config) -> dict[str, dict]:
    from cnequity.query.reader import list_datasets

    frame = list_datasets(config=config)
    return {row["dataset"]: row for row in frame.iter_rows(named=True)}


def _window(row: dict | None) -> str | None:
    if not row or not row.get("has_data"):
        return None
    start, end = row.get("coverage_start"), row.get("coverage_end")
    if start and end:
        return f"{start} .. {end}"
    return None


def assess(config, names: Sequence[str] | None = None) -> list[dict[str, Any]]:
    """One verdict per pack: window, what is missing, and the next command."""
    chosen = normalize_packs(names if names is not None else load_packs(config))
    try:
        catalog = _catalog(config)
    except (OSError, ValueError):
        catalog = {}
    from cnequity.research.preview import preview_symbols

    preview = preview_symbols(config)
    reports: list[dict[str, Any]] = []
    for pack_id in chosen:
        pack = PACKS[pack_id]
        if pack_id == "universe":
            reports.append(_assess_universe(config, pack, catalog))
            continue
        missing = [name for name in pack.datasets if not (catalog.get(name) or {}).get("has_data")]
        bars = catalog.get("daily_bars")
        window = _window(bars) if pack_id == "market" else None
        if pack_id != "market":
            present = [name for name in pack.datasets if name not in missing]
            if present:
                window = _window(catalog.get(present[0]))
        if pack_id == "market" and "daily_bars" in missing and preview:
            status = "partial"
            detail = _(
                "Unadjusted daily bars for {count} stocks can already be queried by symbol "
                '(`load("daily_bars", symbols=[...])`). Adjustment factors are published when '
                "init finishes. This is not the whole market yet."
            ).format(count=len(preview))
            nxt = 'load("daily_bars", symbols=["600519.SH"])'
        elif not missing:
            status = "ready"
            detail = _(pack.summary)
            nxt = ""
        elif len(missing) == len(pack.datasets):
            status = "missing"
            detail = _("None of this data yet: {datasets}").format(datasets=_(", ").join(missing))
            nxt = pack.next_command
        else:
            status = "partial"
            detail = _("Still missing: {datasets}").format(datasets=_(", ").join(missing))
            nxt = pack.next_command
        if window and pack_id == "market" and status == "ready":
            detail = _("Daily-bar window {window}. {detail}").format(window=window, detail=detail)
        elif window and status != "partial":
            detail = _("Window so far {window}. {detail}").format(window=window, detail=detail)
        if pack.extra:
            detail = f"{detail} {localize(pack.extra)}"
        reports.append(
            {
                "id": pack.id,
                "title": _(pack.title),
                "status": status,
                "window": window,
                "detail": detail.strip(),
                "next_command": nxt,
            }
        )
    return reports


def _assess_universe(config, pack: ResearchPack, catalog: dict[str, dict]) -> dict[str, Any]:
    if not (catalog.get("instruments") or {}).get("has_data"):
        return {
            "id": pack.id,
            "title": _(pack.title),
            "status": "missing",
            "window": None,
            "detail": _("{summary} Security master data comes first.").format(
                summary=_(pack.summary)
            ),
            "next_command": "cne init",
        }
    try:
        from cnequity.quality.st_coverage import st_evidence_coverage_report

        evidence = st_evidence_coverage_report(config)
    except (OSError, ValueError, TypeError) as exc:
        return {
            "id": pack.id,
            "title": _(pack.title),
            "status": "unknown",
            "window": None,
            "detail": _("Historical ST evidence cannot be read ({error}). {extra}").format(
                error=type(exc).__name__, extra=localize(pack.extra)
            ),
            "next_command": pack.next_command,
        }
    verified = bool(evidence.get("verified"))
    start, end = evidence.get("coverage_start"), evidence.get("coverage_end")
    window = f"{start} .. {end}" if start and end else None
    if verified:
        detail = _("Historical ST evidence covers the current universe.")
        if window:
            detail = _("Evidence window {window}. {detail}").format(window=window, detail=detail)
        return {
            "id": pack.id,
            "title": _(pack.title),
            "status": "ready",
            "window": window,
            "detail": f"{detail} {localize(pack.extra)}".strip(),
            "next_command": "",
        }
    reason = evidence.get("reason") or _("no historical ST receipt covering the whole universe")
    return {
        "id": pack.id,
        "title": _(pack.title),
        "status": "missing",
        "window": window,
        "detail": _(
            "Historical ST not covered yet: {reason}. Current trading status cannot stand in "
            "for it. {extra}"
        ).format(reason=reason, extra=localize(pack.extra)),
        "next_command": pack.next_command,
    }


def format_readiness(config, names: Sequence[str] | None = None) -> str:
    lines = [_("Research packs:")]
    for row in assess(config, names):
        lines.append(_("- {title}: {detail}").format(title=row["title"], detail=row["detail"]))
        if row["next_command"]:
            lines.append(_("  Next: {command}").format(command=row["next_command"]))
    groups = groups_for(names if names is not None else (load_packs(config) or ("market",)))
    if groups:
        snapshots = snapshot_steps(config, groups)
        lines.append(
            _(
                "If the scheduled daily update runs only these research packs, its schedule "
                "groups are {groups}, without the event stream."
            ).format(groups=_(", ").join(groups))
        )
        if snapshots:
            lines.append(
                _("Of these, datasets lost for good if a day is missed: {datasets}.").format(
                    datasets=_(", ").join(snapshots)
                )
            )
    lines.append(_(SNAPSHOT_NOTE))
    return "\n".join(lines)


def readiness_payload(config) -> dict[str, Any]:
    if config is None:
        return {
            "packs": [],
            "groups": [],
            "snapshots": [],
            "note": _("Finish the first-time setup first."),
        }
    chosen = load_packs(config) or ("market",)
    groups = groups_for(chosen)
    return {
        "packs": assess(config, chosen),
        "saved": list(chosen),
        "groups": groups,
        "snapshots": snapshot_steps(config, groups),
        "note": _(SNAPSHOT_NOTE),
    }


_STATUS_CODE = {"ready": 0, "partial": 1, "missing": 1, "unknown": 2}


def readiness_exit_code(rows: Sequence[dict[str, Any]]) -> int:
    codes = [_STATUS_CODE.get(str(row.get("status")), 2) for row in rows]
    return 1 if 1 in codes else max(codes, default=0)
