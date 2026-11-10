"""Job records on disk. The serve process creates one file and then only reads it.

The job process is the only later writer. A cancel request is a sibling file,
so the two never rewrite the same bytes. A record left at ``running`` after
the slot lock is gone is reported as interrupted or lost without being rewritten:
the process that would have written the ending is already dead.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cnequity.file_lock import is_locked
from cnequity.i18n import N_, _
from cnequity.orchestrator.scheduler_lock import pid_alive
from cnequity.serve.display import en, local, zh
from cnequity.storage.atomic import write_json_atomic

# Windows can deny an open that lands on the instant the job process replaces
# the record. Retry briefly instead of failing a poll or a cancel.
_READ_ATTEMPTS = 5
_READ_BACKOFF_SEC = 0.05

_JOB_ID = re.compile(r"[0-9a-f]{32}")
ACTIVE = frozenset({"starting", "running"})
TERMINAL = frozenset(
    {
        "rejected",
        "succeeded",
        "partial",
        "failed",
        "skipped",
        "findings",
        "error",
        "interrupted",
        "complete",
        "lost",
    }
)
LABELS = {
    "starting": N_("Starting"),
    "running": N_("Running"),
    "rejected": N_("Rejected"),
    "succeeded": N_("Succeeded"),
    "partial": N_("Partly delivered"),
    "failed": N_("Failed"),
    "skipped": N_("Skipped"),
    "findings": N_("Did not pass"),
    "error": N_("Errored"),
    "interrupted": N_("Interrupted"),
    "complete": N_("Complete"),
    "lost": N_("Terminated"),
}
WRITE_CLASSES = frozenset({"ingest-daily", "ingest-events", "lake-write", "init"})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _setup_root() -> Path:
    root = Path(tempfile.gettempdir()) / "cnequity-serve" / str(os.getpid())
    root.mkdir(parents=True, exist_ok=True)
    return root


def jobs_dir(config) -> Path:
    if config is None:
        path = _setup_root() / "jobs"
    else:
        path = Path(config.meta_root) / "serve_jobs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def logs_dir(config) -> Path:
    if config is None:
        path = _setup_root() / "logs"
    else:
        path = Path(config.data_root) / "logs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def slot_path(config) -> Path:
    """The one panel-wide lock. A config-less setup keeps it beside its own jobs."""
    if config is None:
        return _setup_root() / "serve_ops.lock"
    return Path(config.meta_root) / "locks" / "serve_ops.lock"


def _check_id(job_id: str) -> str:
    if not isinstance(job_id, str) or _JOB_ID.fullmatch(job_id) is None:
        raise KeyError(job_id)
    return job_id


def record_path(config, job_id: str) -> Path:
    return jobs_dir(config) / f"{_check_id(job_id)}.json"


def cancel_path(config, job_id: str) -> Path:
    return jobs_dir(config) / f"{_check_id(job_id)}.cancel"


def atomic_write(path: Path, payload: dict) -> None:
    # The panel polls records while the job process rewrites them; Windows
    # denies a replace over a file another handle has open, so retry.
    write_json_atomic(path, payload, ensure_ascii=False, indent=2)


def read_record(path: Path) -> dict:
    for attempt in range(_READ_ATTEMPTS - 1):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except PermissionError:
            time.sleep(_READ_BACKOFF_SEC * (2**attempt))
    return json.loads(path.read_text(encoding="utf-8"))


def update_record(path: Path, **changes: Any) -> dict:
    current = read_record(path)
    current.update(changes)
    atomic_write(path, current)
    return current


def list_records(config) -> list[dict]:
    directory = jobs_dir(config)
    records = []
    for path in directory.glob("*.json"):
        if path.name.endswith(".tmp"):
            continue
        try:
            records.append(read_record(path))
        except (OSError, json.JSONDecodeError):
            continue
    records.sort(key=lambda record: record.get("created_at") or "", reverse=True)
    return records


def _age_seconds(record: dict) -> float:
    raw = record.get("created_at")
    if not raw:
        return 1e9
    try:
        started = datetime.fromisoformat(raw)
    except ValueError:
        return 1e9
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - started).total_seconds()


def live_holder(config) -> dict | None:
    """The job that currently holds the panel slot, if its record says so."""
    if not is_locked(slot_path(config)):
        return None
    active = [record for record in list_records(config) if record.get("state") in ACTIVE]
    alive = [record for record in active if record.get("pid") and pid_alive(int(record["pid"]))]
    pool = alive or [record for record in active if not record.get("pid")]
    if not pool:
        return None
    return max(pool, key=lambda record: record.get("created_at") or "")


def effective(config, record: dict) -> dict:
    """How *record* should be shown. Does not write it back."""
    state = record.get("state")
    if state not in ACTIVE:
        shown = dict(record)
        shown["label"] = zh(LABELS.get(state, state))
        return shown
    holder = live_holder(config)
    if holder and holder.get("job_id") == record.get("job_id"):
        shown = dict(record)
        shown["label"] = zh(LABELS.get(state, state))
        return shown
    # The caller may have read "running" just before the child wrote its
    # ending and released the slot. Re-read before interpreting the unlocked
    # slot as a crash, or a fast successful command can briefly look lost.
    try:
        latest = read_record(record_path(config, record["job_id"]))
    except (KeyError, OSError, ValueError):
        pass
    else:
        if latest.get("state") in TERMINAL:
            return effective(config, latest)
    if state == "starting" and not record.get("pid") and _age_seconds(record) < 15:
        shown = dict(record)
        shown["label"] = zh(LABELS["starting"])
        return shown
    shown = dict(record)
    try:
        cancelled = cancel_path(config, record["job_id"]).exists()
    except KeyError:
        cancelled = False
    shown["state"] = "interrupted" if cancelled else "lost"
    shown["label"] = zh(LABELS[shown["state"]])
    shown["finished_at"] = shown.get("finished_at") or _now()
    if not shown.get("outcome"):
        shown["outcome"] = {
            "kind": shown["state"],
            "message": (
                _("Cancelled. The process did not wind down on its own.")
                if cancelled
                else _(
                    "The process is gone. If it was force-stopped, go by the state of the linked run."
                )
            ),
        }
    return shown


def public_record(config, record: dict) -> dict:
    shown = effective(config, record)
    state = shown.get("state")
    shown["label"] = zh(LABELS.get(state, state))
    shown["label_en"] = LABELS.get(state, state)
    # Older records predate title_en and store only the Chinese catalog title,
    # which `en` maps back to its English source.
    shown["title_en"] = shown.get("title_en") or en(shown.get("title"))
    return shown


def purge_blocked_reason(config) -> str | None:
    """Why a storage purge must wait, or None when the slot is free of writers."""
    if not is_locked(slot_path(config)):
        return None
    holder = live_holder(config)
    if holder is None:
        return _(
            "A dashboard task is running; wait for it to finish before confirming the deletion."
        )
    if holder.get("lock_class") in WRITE_CLASSES:
        title = local(holder.get("title")) or holder.get("op") or _("write")
        return _(
            'The dashboard is running "{title}"; wait for it to finish before confirming the deletion.'
        ).format(title=title)
    return None
