"""Logger and constants every ``daily_bars`` module shares."""

from __future__ import annotations

import logging
from datetime import date

from cnequity.domain.market_time import A_SHARE_FINAL_AT
from cnequity.domain.rate_limit import (
    SINA_RATE_LIMIT_CIRCUIT_SECONDS,
    SINA_RATE_LIMIT_STATUS_CODES,
)

# One logger for the package, under the name it had as a single module.
logger = logging.getLogger("cnequity.steps.bars")

# The closing auction ends at 15:00. Leave a small settlement buffer before
# trusting TDX's current daily bar; the default core schedule starts at 16:00.
_DAILY_BAR_FINAL_AT = A_SHARE_FINAL_AT
# Sina's anti-abuse policy is shared with every sweep that hits it: the retry
# loop is cnequity.domain.rate_limit.sina_retry; the circuit below is the
# gap-fill's own addition.
_SINA_RATE_LIMIT_STATUS_CODES = SINA_RATE_LIMIT_STATUS_CODES
_SINA_RATE_LIMIT_CIRCUIT_SECONDS = SINA_RATE_LIMIT_CIRCUIT_SECONDS
_EXCHANGE_BULK_GAPFILL_MAX_SESSIONS = 20


def _yearly_slices(start: date, end: date) -> list[tuple[date, date]]:
    slices: list[tuple[date, date]] = []
    year = start.year
    while year <= end.year:
        slices.append((max(start, date(year, 1, 1)), min(end, date(year, 12, 31))))
        year += 1
    return slices
