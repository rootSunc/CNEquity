"""Exchange-local clock helpers used by ingestion defaults and gates."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from cnequity.domain.holidays_cn import exchange_may_open
from cnequity.domain.market_profile import BSE_FIRST_SESSION as BSE_FIRST_SESSION

SHANGHAI_ZONE = "Asia/Shanghai"
SHANGHAI_STANDARD_OFFSET = timedelta(hours=8)
SHANGHAI_STANDARD_TZ = timezone(SHANGHAI_STANDARD_OFFSET, name=SHANGHAI_ZONE)
try:
    SHANGHAI_TZ = ZoneInfo(SHANGHAI_ZONE)
except ZoneInfoNotFoundError:
    # Keep the present-day market clock usable without system tzdata.
    SHANGHAI_TZ = SHANGHAI_STANDARD_TZ

A_SHARE_OPEN_AUCTION_AT = time(9, 15)
A_SHARE_CLOSE_AT = time(15)
CONTINUOUS_SESSIONS = ((time(9, 30), time(11, 30)), (time(13), A_SHARE_CLOSE_AT))
CONTINUOUS_SESSION_MINUTES = tuple(
    (start.hour * 60 + start.minute, end.hour * 60 + end.minute)
    for start, end in CONTINUOUS_SESSIONS
)
# Leave a small settlement buffer after the 15:00 closing auction before a
# daily dataset is expected to publish the session.
A_SHARE_FINAL_AT = time(15, 5)
A_SHARE_SNAPSHOT_AT = time(15, 30)


def shanghai_now(now: datetime | None = None) -> datetime:
    """Return an aware current timestamp represented in exchange time."""
    anchor = now or datetime.now(timezone.utc)
    if anchor.tzinfo is None:
        raise ValueError("market clock must be timezone-aware")
    return anchor.astimezone(SHANGHAI_TZ)


def shanghai_today(now: datetime | None = None) -> date:
    """Return the exchange-local calendar date, independent of host timezone."""
    return shanghai_now(now).date()


def is_session_final(as_of: date, now: datetime | None = None) -> bool:
    """Whether *as_of* is a completed exchange session at the current clock.

    Historical/future dates are treated as final. Only the current Shanghai
    date is provisional before the settlement buffer, which prevents a Monday
    pre-open health check from calling Friday's complete data stale.
    """
    local_now = shanghai_now(now)
    return as_of != local_now.date() or local_now.time() >= A_SHARE_FINAL_AT


def last_closed_session(now: datetime | None = None) -> date | None:
    """The session live (undated) market data describes at *now*, or None while open.

    From a session's 15:30 Beijing close until the next session's 09:15 open,
    a live snapshot — TDX batch quotes, 同花顺's money-flow ranking — can only
    describe that session. Walks the bundled exchange calendar, so a weekend
    or holiday points at the last session before it.
    """
    local = shanghai_now(now)
    day, clock = local.date(), local.time()
    if exchange_may_open(day):
        if clock >= A_SHARE_SNAPSHOT_AT:
            return day
        if clock >= A_SHARE_OPEN_AUCTION_AT:
            return None
    day -= timedelta(days=1)
    for _ in range(40):
        if exchange_may_open(day):
            return day
        day -= timedelta(days=1)
    return None
