"""What is already going on, and whether this operation may start.

The answers are advisory. The job process takes the slot and, where the
operation writes, the scheduler lock; the engine takes its own locks again.
A check that passed here can still be rejected a moment later, and that
rejection is the one that counts.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from cnequity.domain.market_time import shanghai_now, shanghai_today
from cnequity.i18n import N_, _
from cnequity.orchestrator.run_lock import (
    DAILY_INGESTION_LOCK,
    EVENTS_INGESTION_LOCK,
    INIT_JOB_LOCK,
    is_run_locked,
)
from cnequity.orchestrator.run_window import (
    due_session,
    is_done,
    mark_done,
    parse_run_at,
    pending_session,
)
from cnequity.orchestrator.scheduler_lock import lock_directory, scheduler_lock_holder
from cnequity.serve.display import en, local
from cnequity.serve.ops.catalog import OpSpec
from cnequity.serve.ops.records import live_holder, slot_path

ENGINE_TEXT = {
    DAILY_INGESTION_LOCK: N_(
        "A daily update is running (all schedule groups share one ingest lock)."
    ),
    EVENTS_INGESTION_LOCK: N_("An event stream is running."),
    INIT_JOB_LOCK: N_("An init is running."),
}
SCHEDULER_TEXT = {
    "daily": N_("A scheduled daily update or closing catch-up is running."),
    "events": N_("A scheduled event stream is running."),
}


_TOML_AT = re.compile(r"\s*\(at line (\d+), column (\d+)\)\s*$")


def config_drift(config) -> dict | None:
    """Whether the config file has moved away from the lake *config* holds.

    A running server keeps the lake it started on; changing ``data.root``
    takes a restart. A job it launches, though, reads the file afresh, so the
    file is re-read on each call. ``None`` when the two agree.
    """
    from cnequity.config.loader import configured_data_root

    path = getattr(config, "config_path", None) if config is not None else None
    if path is None:
        return None
    try:
        configured = configured_data_root(path)
    except FileNotFoundError:
        return {"kind": "missing"}
    except OSError as exc:
        return {"kind": "unreadable", "detail": exc.strerror or type(exc).__name__}
    except (ValueError, TypeError) as exc:
        # tomllib.TOMLDecodeError is a ValueError; a non-string root is a TypeError.
        text = str(exc)
        at = _TOML_AT.search(text)
        return {
            "kind": "invalid",
            "line": int(at.group(1)) if at else None,
            "column": int(at.group(2)) if at else None,
            "detail": _TOML_AT.sub("", text),
        }
    if configured == Path(config.data_root):
        return None
    return {"kind": "data_root", "configured_data_root": str(configured)}


def drift_blocker(drift: dict) -> str:
    if drift["kind"] == "data_root":
        return _(
            "The config file now points to another data directory {configured_data_root}, while the console still uses the lake it started with. Restart cne serve before starting tasks."
        ).format(configured_data_root=drift["configured_data_root"])
    return _(
        "The config file cannot be read right now, so a task would fail after starting. Fix the config file, then start it."
    )


def engine_names(lock_class: str) -> tuple[str, ...]:
    if lock_class == "ingest-daily":
        return (DAILY_INGESTION_LOCK, INIT_JOB_LOCK)
    if lock_class == "ingest-events":
        return (EVENTS_INGESTION_LOCK, INIT_JOB_LOCK)
    if lock_class in {"lake-write", "init"}:
        return (DAILY_INGESTION_LOCK, EVENTS_INGESTION_LOCK, INIT_JOB_LOCK)
    return ()


def scheduler_names(lock_class: str) -> tuple[str, ...]:
    if lock_class == "ingest-daily":
        return ("daily",)
    if lock_class == "ingest-events":
        return ("events",)
    if lock_class in {"lake-write", "init"}:
        return ("daily", "events")
    return ()


def engine_blockers(config, lock_class: str) -> list[str]:
    if config is None:
        return []
    return [
        _(ENGINE_TEXT[name])
        for name in engine_names(lock_class)
        if is_run_locked(config.meta_root, name)
    ]


def scheduler_blockers(config, lock_class: str) -> list[str]:
    if config is None:
        return []
    root = lock_directory(config)
    return [
        _(SCHEDULER_TEXT[name])
        for name in scheduler_names(lock_class)
        if scheduler_lock_holder(root, name)
    ]


def blockers(config, spec: OpSpec, *, storage_busy: bool, lock_class: str) -> list[str]:
    found: list[str] = []
    from cnequity.file_lock import is_locked

    # The job reads the config file itself; started now it would work on a
    # lake other than the one every check below and the page are looking at.
    drift = config_drift(config)
    if drift:
        found.append(drift_blocker(drift))
    if is_locked(slot_path(config)):
        holder = live_holder(config)
        if holder:
            title = local(holder.get("title")) or holder.get("op")
            found.append(_('The dashboard is running "{title}".').format(title=title))
        else:
            found.append(_("The dashboard is already running another task."))
    found.extend(scheduler_blockers(config, lock_class))
    found.extend(engine_blockers(config, lock_class))
    if storage_busy and lock_class != "read":
        found.append(_("Storage maintenance is running; wait for it to finish before starting."))
    return found


def source_status(config) -> dict:
    """push2 pause, today's breakers, request counts, keys and failures per source.

    ``readable`` is false when the breaker ledger cannot be read; the page
    still opens. ``requests`` holds the EastMoney host profiles the ledger
    counts. ``keys`` says whether each paid source has a credential, never the
    credential. ``failures`` counts today's failed batches by the primary
    source of their dataset.
    """
    if config is None:
        return {"push2_paused": False, "breakers": [], "readable": True}
    paused = bool(getattr(config, "eastmoney_push2_paused", False))
    shown = {
        "push2_paused": paused,
        "breakers": [],
        "readable": True,
        "requests": {},
        "keys": {
            "tushare": bool(getattr(config, "tushare_token", None)),
            "ths_official": bool(getattr(config, "ths_official_api_key", None)),
        },
        "failures": _failures_today(config),
    }
    try:
        from cnequity.adapters.eastmoney.host_guard import status

        ledger = status(config)
    except (OSError, ValueError):
        shown["readable"] = False
        return shown
    for name, section in ledger.items():
        if not isinstance(section, dict):
            continue
        if section.get("breaker"):
            shown["breakers"].append(name)
        if "requests" in section:
            shown["requests"][name] = {
                "requests": int(section.get("requests") or 0),
                "budget": int(section.get("budget") or 0) or None,
            }
    return shown


def _failures_today(config) -> dict[str, dict]:
    """Failed batches since Beijing midnight, grouped by their dataset's primary source."""
    path = Path(config.manifest_path)
    if not path.exists():
        return {}
    import sqlite3
    from contextlib import closing
    from datetime import time
    from zoneinfo import ZoneInfo

    from cnequity.domain.datasets import DATASETS

    since = (
        datetime.combine(shanghai_today(), time(0), tzinfo=ZoneInfo("Asia/Shanghai"))
        .astimezone(timezone.utc)
        .isoformat()
    )
    try:
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as conn:
            rows = conn.execute(
                "SELECT dataset, COUNT(*) FROM ingestion_batches WHERE status = 'failed' "
                "AND COALESCE(finished_at, started_at) >= ? GROUP BY dataset",
                (since,),
            ).fetchall()
    except sqlite3.Error:
        return {}
    grouped: dict[str, dict] = {}
    for dataset, count in rows:
        spec = DATASETS.get(dataset)
        source = getattr(spec, "primary_source", None) or "unknown"
        entry = grouped.setdefault(source, {"batches": 0, "datasets": []})
        entry["batches"] += int(count)
        entry["datasets"].append(dataset)
    for entry in grouped.values():
        entry["datasets"].sort()
    return grouped


def hints(config, spec: OpSpec, params: dict) -> list[str]:
    if config is None:
        return []
    notes: list[str] = []
    if spec.id in {"daily.full", "daily.group", "daily.stale"}:
        target = params.get("trade_date")
        today = shanghai_today().isoformat()
        if target in (None, today) and shanghai_now().time() < parse_run_at(config.daily_run_at):
            if spec.id == "daily.full":
                notes.append(
                    _(
                        "Today's daily update time {daily_run_at} (Beijing time) has not come yet, and today's data may not be published. This run will not count as the scheduled one, and the scheduled daily update still runs on time."
                    ).format(daily_run_at=config.daily_run_at)
                )
            else:
                notes.append(
                    _(
                        "Today's daily update time {daily_run_at} (Beijing time) has not come yet, and today's data may not be published."
                    ).format(daily_run_at=config.daily_run_at)
                )
    sources = source_status(config)
    if sources["push2_paused"]:
        notes.append(
            _("push2 is paused (config push2_paused); related datasets will degrade or be missing.")
        )
    if not sources["readable"]:
        notes.append(_("Could not read the source circuit-breaker state."))
    else:
        for name in sources["breakers"]:
            notes.append(
                _(
                    "The {name} circuit breaker already tripped today; related requests will fail until tomorrow."
                ).format(name=name)
            )
    if spec.id not in {"init.start", "init.resume", "diag.doctor"}:
        pending = incomplete_init(config)
        if pending and not pending["running"]:
            notes.append(
                _(
                    "An init did not finish ({run_id}). Continuing init from the dashboard resumes it; this operation can still go ahead."
                ).format(run_id=pending["run_id"])
            )
    if lock_class_waits_for_compact(spec) and is_run_locked(config.meta_root, "compact"):
        notes.append(_("The publish lock is held; the publish phase will queue and wait."))
    return notes


def lock_class_waits_for_compact(spec: OpSpec) -> bool:
    return spec.lock_class in {"ingest-daily", "ingest-events", "lake-write", "init", "run"}


def incomplete_init(config) -> dict | None:
    if config is None or not Path(config.manifest_path).exists():
        return None
    from cnequity.orchestrator.manifest import Manifest

    # Same coverage question as ``cne status``. A step this init never recorded,
    # but a later run has since completed, is not a reason to offer resume.
    # ``cne init`` itself keeps the strict ledger and can still be resumed.
    row = Manifest(config.manifest_path).latest_incomplete_init_run(discharged_by_later_runs=True)
    if row is None:
        return None
    return {
        "run_id": row["run_id"],
        "status": row["status"],
        "running": is_run_locked(config.meta_root, INIT_JOB_LOCK),
    }


def lake_empty(config) -> bool:
    if config is None:
        return True
    curated = Path(config.curated_root)
    if not curated.exists():
        return True
    return next(curated.rglob("*.parquet"), None) is None


def schedule_view(config) -> dict:
    if config is None:
        return {}
    from cnequity.domain.datasets import _is_exchange_session

    now = datetime.now(timezone.utc)
    local = shanghai_now(now)
    today = shanghai_today(now)
    run_at = parse_run_at(config.daily_run_at)
    due = due_session(now, run_at)
    pending = pending_session(config, "daily", now)
    return {
        "daily_run_at": config.daily_run_at,
        "stale_run_at": config.stale_run_at,
        "daily_due": due.isoformat() if due else None,
        "daily_pending": pending.isoformat() if pending else None,
        "daily_done": bool(due) and is_done(config.meta_root, "daily", due, config),
        "today": today.isoformat(),
        "today_is_session": _is_exchange_session(today),
        "before_daily_run_at": local.time() < run_at,
    }


def occupancy(config, *, storage_busy: bool) -> dict:
    holder = live_holder(config)
    engines = []
    schedulers = []
    if config is not None:
        engines = [
            name
            for name in (DAILY_INGESTION_LOCK, EVENTS_INGESTION_LOCK, INIT_JOB_LOCK)
            if is_run_locked(config.meta_root, name)
        ]
        root = lock_directory(config)
        schedulers = [name for name in ("daily", "events") if scheduler_lock_holder(root, name)]
    # push2 pausing is reported once, through ``sources``; the status bar shows it.
    notes: list[str] = []
    return {
        "slot": (
            {
                "job_id": holder.get("job_id"),
                "op": holder.get("op"),
                "title": holder.get("title"),
                "title_en": en(holder.get("title")),
                "state": holder.get("state"),
                "command": holder.get("command"),
            }
            if holder
            else None
        ),
        "engine_locks": engines,
        "scheduler_locks": schedulers,
        "storage_busy": storage_busy,
        "incomplete_init": incomplete_init(config),
        "lake_empty": lake_empty(config),
        "schedule": schedule_view(config),
        "sources": source_status(config),
        "hints": notes,
    }


def mark_due_daily_session(
    config, op_id: str, params: dict, *, now: datetime | None = None
) -> bool:
    """Write the scheduler's done marker when a full daily run *is* today's session.

    A single group, a backfill replay and a stale-only pass do not. Neither
    does a full run started before ``run_at``: the timed job should still run
    once the session is actually due. ``pending_session`` is None in that case,
    so this writes nothing.
    """
    if op_id != "daily.full" or params.get("backfill"):
        return False
    moment = now or datetime.now(timezone.utc)
    pending = pending_session(config, "daily", moment)
    if pending is None:
        return False
    target = params.get("trade_date")
    if target is None:
        if shanghai_today(moment) != pending:
            return False
    elif target != pending.isoformat():
        return False
    mark_done(config.meta_root, "daily", pending)
    return True
