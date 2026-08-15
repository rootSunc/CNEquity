"""Exchange-local clock helpers used by ingestion defaults and gates."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

# Mainland China has used UTC+08:00 without daylight-saving changes for the
# exchange dates handled by this project. A fixed offset also works on Windows
# without requiring an IANA timezone database or an extra runtime dependency.
SHANGHAI_TZ = timezone(timedelta(hours=8), name="Asia/Shanghai")


def shanghai_now(now: datetime | None = None) -> datetime:
    """Return an aware current timestamp represented in exchange time."""
    anchor = now or datetime.now(timezone.utc)
    if anchor.tzinfo is None:
        raise ValueError("market clock must be timezone-aware")
    return anchor.astimezone(SHANGHAI_TZ)


def shanghai_today(now: datetime | None = None) -> date:
    """Return the exchange-local calendar date, independent of host timezone."""
    return shanghai_now(now).date()
