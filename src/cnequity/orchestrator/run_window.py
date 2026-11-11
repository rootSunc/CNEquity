"""Which session a scheduled job should run for right now, in Beijing time.

The scheduler (launchd, cron) wakes the pipelines hourly; this decides whether
that wake-up is *the* run. A job is due for session S once Beijing time has
passed S's ``run_at`` ([job.daily] / [job.stale] ``run_at``), and it stays due
until the next session opens at 09:15 Beijing — after that, the live snapshot
feeds describe the next session, so a missed S is left to the next day's run
(history datasets catch it up by date; a snapshot day is lost).

Keeping the target in Beijing time and gating in code is what makes the
schedule host-independent: no timezone arithmetic at install time, nothing to
reinstall when daylight saving flips, and a laptop that slept through the
evening still runs on waking. The "already ran" marker is written by the
pipeline after it takes the scheduler lock, so each job runs once per session.
"""

from __future__ import annotations

from contextlib import closing
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

CST = ZoneInfo("Asia/Shanghai")
OPEN = time(9, 15)
JOBS = ("daily", "stale", "maintain")
#: Jobs that follow the day's run: due at its run_at, never before it ran.
_AFTER_DAILY = {"stale": "stale_run_at", "maintain": "daily_run_at"}


def parse_run_at(value: str) -> time:
    hour, minute = (int(part) for part in str(value).strip().split(":"))
    return time(hour, minute)


def _is_session(day: date) -> bool:
    from cnequity.domain.datasets import _is_exchange_session

    return _is_exchange_session(day)


def _next_session(day: date) -> date:
    day += timedelta(days=1)
    while not _is_session(day):
        day += timedelta(days=1)
    return day


def due_session(now: datetime, run_at: time) -> date | None:
    """The session a job with Beijing ``run_at`` is due for at *now*, or None."""
    local = now.astimezone(CST)
    day = local.date()
    for _ in range(40):
        if _is_session(day) and datetime.combine(day, run_at, CST) <= local:
            break
        day -= timedelta(days=1)
    else:
        return None
    next_open = datetime.combine(_next_session(day), OPEN, CST)
    return day if local < next_open else None


def marker_path(meta_root: Path, job: str, session: date) -> Path:
    return Path(meta_root) / "state" / "scheduler" / f"{job}-{session.isoformat()}.done"


def _ran_in_manifest(config, job: str, session: date) -> bool:
    """Whether the manifest already holds a run of *job* for *session*.

    Covers sessions from before the markers existed and runs started by hand,
    so a manual run never triggers a second full pipeline for the same session.

    A run tagged ``metadata.launch`` was started from the dashboard. One group
    from that page must not cancel the day's scheduled pipeline; the full daily
    operation writes the session marker itself when it is the due session.
    """
    path = Path(getattr(config, "manifest_path", None) or Path(config.meta_root) / "manifest.db")
    if not path.exists():
        return False
    import sqlite3

    pattern = "daily:stale" if job == "stale" else "daily:%"
    query = (
        "SELECT 1 FROM ingestion_runs WHERE job_name LIKE ? "
        "AND json_extract(metadata_json, '$.trade_date') = ? "
        + ("" if job == "stale" else "AND job_name != 'daily:stale' ")
        + "AND json_extract(metadata_json, '$.launch.id') IS NULL "
        + "LIMIT 1"
    )
    try:
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as conn:
            return conn.execute(query, (pattern, session.isoformat())).fetchone() is not None
    except sqlite3.Error:
        return False


def is_done(meta_root: Path, job: str, session: date, config=None) -> bool:
    if marker_path(meta_root, job, session).exists():
        return True
    # Maintenance leaves no ingestion run behind; only its marker counts.
    return config is not None and job != "maintain" and _ran_in_manifest(config, job, session)


def mark_done(meta_root: Path, job: str, session: date) -> Path:
    path = marker_path(meta_root, job, session)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    return path


def pending_session(config, job: str, now: datetime | None = None) -> date | None:
    """The session *job* should run for now: due, and not already run."""
    if job not in JOBS:
        raise ValueError(f"unknown scheduled job {job!r}; expected one of {JOBS}")
    run_at = parse_run_at(getattr(config, _AFTER_DAILY.get(job, f"{job}_run_at")))
    session = due_session(now or datetime.now(timezone.utc), run_at)
    if session is None or is_done(config.meta_root, job, session, config):
        return None
    if job in _AFTER_DAILY and not is_done(config.meta_root, "daily", session, config):
        # The catch-up retries what the day's run missed, and maintenance
        # checks and backs up what it wrote; before that run neither has
        # anything to do.
        return None
    return session


def current_anchor(config, now: datetime | None = None) -> date:
    """The trading day freshness is judged against at *now* (default: now).

    Today counts once its session is final and the scheduled daily update has
    come due or run. Between the close and ``daily_run_at`` many sources have
    not published today yet, and judging against today listed every daily
    dataset as stale. ``cne status --datasets`` and the dashboard both judge
    against this day.
    """
    from cnequity.domain.market_time import is_session_final, shanghai_now
    from cnequity.query.calendar import last_trading_day

    now = shanghai_now(now)
    today = now.date()
    if is_session_final(today, now) and not _before_daily_update(config, today, now):
        return last_trading_day(config, today, now=now)
    return last_trading_day(config, today - timedelta(days=1), now=now)


def _before_daily_update(config, today: date, now: datetime) -> bool:
    """Whether today's scheduled daily update has neither come due nor run."""
    run_at = getattr(config, "daily_run_at", None)
    if not run_at:
        return False
    try:
        due = parse_run_at(run_at)
    except ValueError:
        return False
    if now.time() >= due:
        return False
    return not is_done(config.meta_root, "daily", today, config)
