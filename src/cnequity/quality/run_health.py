"""A job that keeps not working, when no single run is alarming enough to say so.

On 2026-09-19 the local proxy died at 00:51. The news wire runs every fifteen
minutes, so twenty consecutive runs came back `degraded` — each one had lost
both EastMoney steps to `[Errno 61] Connection refused`, and each one filed its
own per-step finding and moved on. Five hours later nothing in the lake said
anything was wrong: the audit was HEALTHY, `stale_datasets` was empty (the
datasets involved are not daily-watermarked), and the outage was found by a
person noticing refusals in a log.

One degraded run is a source having a bad minute. Twenty in a row is an
outage, and only the sequence can tell them apart.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone

from cnequity.config import Config
from cnequity.config.failure_policy import (
    DEFAULT_RUN_FAILURE_STREAK as RUN_STREAK_MIN,
)
from cnequity.config.failure_policy import (
    DEFAULT_RUN_FAILURE_WINDOW_HOURS as RUN_STREAK_WINDOW_HOURS,
)
from cnequity.config.failure_policy import (
    recent_run_streak,
)
from cnequity.sqlite_readonly import connect_readonly

logger = logging.getLogger(__name__)

__all__ = ["RUN_STREAK_WINDOW_HOURS", "RUN_STREAK_MIN", "degraded_job_findings"]

_UNHEALTHY = ("failed", "degraded")


def _recent_runs(
    config: Config, since: datetime, until: datetime
) -> list[tuple[str, str, str, str]]:
    path = config.manifest_path
    if not path.exists():
        return []
    try:
        con = connect_readonly(path)
    except sqlite3.Error:  # pragma: no cover - unreadable manifest is not this check's job
        return []
    try:
        rows = con.execute(
            "select job_name, status, started_at, coalesce(error_message, '') "
            "from ingestion_runs where started_at >= ? and started_at <= ? "
            "order by started_at",
            (
                since.strftime("%Y-%m-%dT%H:%M:%S"),
                # Bounded at both ends so the trailing streak is the streak as
                # of *now* — an unbounded read makes a replay of a past outage
                # look healthy, because the recovery is already in the rows.
                until.strftime("%Y-%m-%dT%H:%M:%S"),
            ),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        con.close()
    return [(str(a), str(b), str(c), str(d)) for a, b, c, d in rows]


def degraded_job_findings(config: Config, *, now: datetime | None = None) -> list[dict]:
    """Report a job whose recent runs have all come back unhealthy."""
    moment = now or datetime.now(timezone.utc)
    rule = recent_run_streak(config)
    if not rule.minimum:
        return []
    since = moment - timedelta(hours=rule.window_hours)
    runs = _recent_runs(config, since, moment)
    if not runs:
        return []

    by_job: dict[str, list[tuple[str, str, str]]] = {}
    for job, status, started, error in runs:
        by_job.setdefault(job, []).append((status, started, error))

    findings: list[dict] = []
    for job, entries in sorted(by_job.items()):
        # The *trailing* run of unhealthy results, not the whole window: the
        # proxy outage began at 00:51 with healthy runs behind it, so a rule
        # that asked for no success all window would have stayed quiet through
        # the entire five hours it was meant to catch.
        trailing: list[tuple[str, str, str]] = []
        for entry in reversed(entries):
            if entry[0] not in _UNHEALTHY:
                break
            trailing.append(entry)
        if not rule.reached(len(trailing)):
            continue
        entries = list(reversed(trailing))
        streak = len(entries)
        last_error = next(
            (error for _status, _started, error in reversed(entries) if error),
            "",
        )
        statuses = sorted({status for status, _started, _error in entries})
        findings.append(
            {
                "dataset": "runs",
                "severity": "warning",
                "check": "job_run_streak",
                "message": (
                    f"{job}: {streak} consecutive {'/'.join(statuses)} run(s) since "
                    f"{entries[0][1][:16]} and no successful run since"
                    + (f"; last error: {last_error[:120]}" if last_error else "")
                ),
                "job_name": job,
                "streak": streak,
                "window_hours": rule.window_hours,
                "first_started_at": entries[0][1],
                "last_started_at": entries[-1][1],
                "statuses": statuses,
            }
        )
    return findings
