"""Suspension sessions an issuer's own announcements state, for bar coverage.

While EastMoney push2 is unavailable a Shanghai halt has no first-hand status
row until Baostock catches up, about a week later; the only same-day record is
the carried board (``eastmoney_cached``), which cannot excuse a missing bar.
The issuer's announcement index already says when a multi-session halt began,
that it continues, and when it ends.

Only bracketed spans count: a session is a halt when it falls strictly between
a suspension notice and a later continuation or resumption notice for the same
symbol, each arriving within :data:`CONFIRM_WITHIN_SESSIONS` of the last. The
notice days themselves are excluded (a notice can precede or follow the halt
by a session), and nothing is extrapolated past the last notice. Advance
notices (``提示性``, ``直至终止上市``), one-day and check halts, risk-warning
halts, convertible-conversion halts and subsidiaries' notices never open a
span. Measured on the 2015–2026 lake this yielded 3,929 sessions over 555
symbols with no positive-volume bar on any of them.
"""

from __future__ import annotations

import bisect
import re
from datetime import date

import polars as pl

#: ``trading_status.source`` for rows read from the announcement index.
ANNOUNCEMENT_SOURCE = "announcement"
#: Longest gap between notices of one halt; progress notices are weekly.
CONFIRM_WITHIN_SESSIONS = 10
#: How far before a window to read notices, so a span that opened earlier is seen.
LOOKBACK_DAYS = 60

_EXCLUDED = re.compile(
    "转股|停牌前|子公司|债|风险警示|临时停牌|停牌一天|核查|暂不复牌|提示性|直至终止上市"
)
_CONTINUE = re.compile("进展|继续停牌|停牌期间")
_OPEN = re.compile(r"停牌(的)?公告")
#: Order of a symbol's notices published on the same day.
_SAME_DAY_ORDER = {"resume": 0, "continue": 1, "open": 2}


def classify_title(title: str | None) -> str | None:
    """``open`` / ``continue`` / ``resume`` for a halt notice, else ``None``."""
    if not title or _EXCLUDED.search(title):
        return None
    if "复牌" in title:
        return "resume"
    if _CONTINUE.search(title):
        return "continue"
    if _OPEN.search(title):
        return "open"
    return None


def announced_halt_keys(announcements: pl.DataFrame, sessions: list[date]) -> set[tuple[str, date]]:
    """(symbol, session) pairs bracketed by halt notices, see the module docstring."""
    if announcements.is_empty() or not sessions:
        return set()
    sessions = sorted(sessions)
    notices = (
        announcements.select("symbol", "announce_date", "title")
        .with_columns(
            pl.col("title").map_elements(classify_title, return_dtype=pl.Utf8).alias("kind")
        )
        .drop_nulls(["symbol", "announce_date", "kind"])
        .unique(["symbol", "announce_date", "kind"])
        # Notices of one day about a halt already under way come before a new
        # one: a resumption closes the old span and a progress notice extends
        # it before an opening notice starts the next. Without a fixed order
        # within a day the spans changed from one read to the next.
        .with_columns(_rank=pl.col("kind").replace_strict(_SAME_DAY_ORDER, return_dtype=pl.Int8))
        .sort("symbol", "announce_date", "_rank")
    )
    keys: set[tuple[str, date]] = set()
    for (symbol,), group in notices.group_by("symbol", maintain_order=True):
        opened: int | None = None
        last = 0
        for day, kind in group.select("announce_date", "kind").iter_rows():
            index = bisect.bisect_left(sessions, day)
            if kind == "open":
                opened = last = index
                continue
            if opened is None:
                continue
            if index - last > CONFIRM_WITHIN_SESSIONS:
                opened = None
                continue
            keys.update((symbol, sessions[i]) for i in range(opened + 1, min(index, len(sessions))))
            if kind == "resume":
                opened = None
            else:
                last = index
    return keys
