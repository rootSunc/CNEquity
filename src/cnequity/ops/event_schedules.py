"""When each ``[[job.events.schedules]]`` entry is due, for the packaged timer.

The timer checks once a minute; a schedule is due once its spacing has passed
since its last attempt started. Inside the entry's quiet hours the spacing is
``quiet_every_minutes``, with five minutes of slack so timer drift does not
push every other check out. Each attempt draws a random 0..``jitter_seconds``
and adds it to the next due time, spreading requests without holding the timer
in a sleep.

State lives in ``events_schedules_last.json`` beside the single-schedule
``events_last.json``, which stays for settings that predate config schedules.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cnequity.config.loader import EventSchedule
from cnequity.ops.records import atomic_write, read_record

BEIJING = timezone(timedelta(hours=8))  # no daylight saving
QUIET_SLACK = timedelta(minutes=5)
STATE_FILE = "events_schedules_last.json"


def in_quiet_hours(spec: EventSchedule, now: datetime) -> bool:
    if not spec.quiet_hours:
        return False
    start, end = (int(part) for part in spec.quiet_hours.split("-"))
    hour = now.astimezone(BEIJING).hour
    if start < end:
        return start <= hour < end
    if start > end:
        return hour >= start or hour < end
    return False


def spacing(spec: EventSchedule, now: datetime) -> timedelta:
    if in_quiet_hours(spec, now):
        return timedelta(minutes=max(spec.every_minutes, spec.quiet_every_minutes)) - QUIET_SLACK
    return timedelta(minutes=spec.every_minutes)


def _state_path(directory: Path) -> Path:
    return directory / STATE_FILE


def last_attempts(directory: Path) -> dict:
    path = _state_path(directory)
    if not path.exists():
        return {}
    value = read_record(path)
    if not isinstance(value, dict):
        raise ValueError("event schedule state is not a mapping")
    return value


def is_due(spec: EventSchedule, directory: Path, now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    record = last_attempts(directory).get(spec.id)
    if record is None:
        return True
    started = datetime.fromisoformat(record["at"])
    if started.tzinfo is None:
        raise ValueError("event schedule attempt time has no time zone")
    offset = timedelta(seconds=int(record.get("offset_seconds", 0)))
    return now >= started + spacing(spec, now) + offset


def record_attempt(spec: EventSchedule, directory: Path, now: datetime | None = None) -> None:
    state = last_attempts(directory)
    state[spec.id] = {
        "at": (now or datetime.now(timezone.utc)).isoformat(),
        "offset_seconds": random.randint(0, spec.jitter_seconds) if spec.jitter_seconds else 0,
        "every_minutes": spec.every_minutes,
    }
    atomic_write(_state_path(directory), state)


def find(config, schedule_id: str) -> EventSchedule | None:
    return next((s for s in config.event_schedules if s.id == schedule_id), None)
