"""Move a lake's records out of ``meta/state/`` (see ``cnequity.meta_layout``).

A dataset's ``meta/state/<dataset>.json`` held its coverage ledger, which the
lake catalog's ``_attempts`` replaced, and its revision identity, which the
catalog's ``_revisions`` replaced. Both are dropped; the two files that also
kept something else have those fields moved first. Everything else under
``meta/state/`` moves to its owner's directory.

Before anything changes, the whole of ``meta/state/`` is archived to
``meta/repair_backups/``. A file the dashboard timer's installed unit writes
(its logs) stays until that unit is installed again.
"""

from __future__ import annotations

import json
import shutil
import tarfile
from datetime import UTC, datetime
from pathlib import Path

from cnequity.meta_layout import (
    LEGACY_STATE,
    MOVES,
    OBSOLETE,
    SPLIT_FIELDS,
    sector_bars_backfill_path,
)
from cnequity.storage.atomic import write_json_atomic

#: Written by the installed dashboard timer (its launchd/systemd unit names the path).
_UNIT_LOGS = ("timer.out.log", "timer.err.log")


def repair_meta_layout(config, *, apply: bool = False) -> dict:
    from cnequity.domain.datasets import DATASETS

    meta = Path(config.meta_root)
    state = meta / LEGACY_STATE
    report: dict = {"applied": False, "moved": [], "split": [], "dropped": [], "kept": []}
    if not state.is_dir():
        report["rows_changed"] = 0
        return report
    plan: list[tuple[str, Path, Path | None]] = []
    for entry in sorted(state.iterdir()):
        name = entry.name
        children = list(entry.iterdir()) if entry.is_dir() else []
        if children and all(child.name in _UNIT_LOGS for child in children):
            report["kept"].append(name)  # only what an installed unit writes is left
        elif name in MOVES:
            plan.append(("move", entry, meta / MOVES[name]))
        elif name.startswith("sector_bars_backfill_") and name.endswith(".json"):
            track = name[len("sector_bars_backfill_") : -len(".json")]
            plan.append(("move", entry, sector_bars_backfill_path(meta, track)))
        elif name in OBSOLETE or name.endswith(".lock"):
            plan.append(("drop", entry, None))
        elif name.endswith(".json") and name[: -len(".json")] in DATASETS:
            dataset = name[: -len(".json")]
            if dataset in SPLIT_FIELDS:
                plan.append(("split", entry, meta / SPLIT_FIELDS[dataset][0]))
            else:
                plan.append(("drop", entry, None))
        else:
            report["kept"].append(name)
    for kind, entry, target in plan:
        key = {"move": "moved", "split": "split", "drop": "dropped"}[kind]
        report[key].append(
            entry.name if target is None else f"{entry.name} -> {target.relative_to(meta)}"
        )
    report["rows_changed"] = len(plan)
    if not apply or not plan:
        return report
    backup = _archive(meta, state)
    for kind, entry, target in plan:
        if kind == "move":
            _move(entry, target)
        elif kind == "split":
            fields = SPLIT_FIELDS[entry.name[: -len(".json")]][1]
            _split(entry, target, fields)
        else:
            _remove(entry)
    if _empty(state):
        shutil.rmtree(state)
    report.update(applied=True, backup=str(backup.relative_to(meta)))
    return report


def _archive(meta: Path, state: Path) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    target = meta / "repair_backups" / f"meta-state-{stamp}.tar.gz"
    target.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(target, "w:gz") as archive:
        archive.add(state, arcname="state")
    return target


def _move(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        target.mkdir(parents=True, exist_ok=True)
        for child in sorted(source.iterdir()):
            if child.name in _UNIT_LOGS:
                continue  # the installed unit keeps writing here until reinstalled
            _move(child, target / child.name)
        if _empty(source):
            shutil.rmtree(source)
        return
    if target.exists():
        # A newer record already lives in the new home; the old one is in the backup.
        source.unlink()
        return
    source.replace(target)


def _split(source: Path, target: Path, fields: tuple[str, ...]) -> None:
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        payload = {}
    kept = {key: payload[key] for key in fields if isinstance(payload, dict) and key in payload}
    if kept:
        try:
            current = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            current = {}
        write_json_atomic(target, {**kept, **(current if isinstance(current, dict) else {})})
    source.unlink()


def _remove(entry: Path) -> None:
    if entry.is_dir():
        shutil.rmtree(entry)
    else:
        entry.unlink(missing_ok=True)


def _empty(directory: Path) -> bool:
    return directory.is_dir() and not any(directory.iterdir())
