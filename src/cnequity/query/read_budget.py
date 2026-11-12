"""How many rows one paged read would load, sized before anything is read.

``load()`` holds the whole selection before a page is cut, so memory follows
what is read, not what is returned. Measured on a real lake, reading all of
``margin_trading`` (7.0M rows) peaked at 2.5 GB and all of ``option_greeks``
(14.5M rows) at 10.8 GB to return 200 rows; one ``trade_ticks`` day (14.7M
rows) peaked at 5.5 GB, and all of ``trade_ticks`` (633M rows) would not fit
in memory at all. MCP ``query_dataset`` and the dashboard's row viewer both
refuse a selection over the budget instead of reading it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from cnequity.config import Config

#: Rows one paged read may select when nothing narrows it by symbol.
#:
#: Rows, not bytes: peak memory follows the rows a read decodes, while the
#: same rows take very different space on disk depending on compression and
#: sort order (the lake catalog stores ``trade_ticks`` in 40% of the bytes the
#: revision store did, and reading a day of it still peaks at 5.5 GB). The
#: earlier 64 MiB of compressed Parquet came to 3-7 million rows on a real
#: lake, a 1.5-3 GB peak.
SCAN_BUDGET_ROWS = 4_000_000

#: A short symbol list is pushed into the scan and keeps the read small (one
#: symbol over all of ``margin_trading`` peaked at 0.3 GB), so it is not
#: budgeted. A long list reads close to everything and is.
SYMBOL_SCAN_EXEMPT = 20


@dataclass(frozen=True)
class OversizedRead:
    """A selection over the budget, with what a caller needs to suggest a smaller one."""

    selected_rows: int
    budget_rows: int
    #: First and last day the dataset's partitions cover.
    covers: tuple[date, date] | None
    #: The newest window inside the request that fits the budget.
    fits: tuple[date, date] | None
    #: Set when even the newest period in the request is over the budget.
    newest_over: str | None


def oversized_read(
    config: Config,
    dataset: str,
    *,
    start: date | None,
    end: date | None,
    symbols: list[str] | None,
    budget: int | None = None,
) -> OversizedRead | None:
    """Size the partitions a read of *dataset* over ``start..end`` selects.

    Returns None when the read fits, names up to ``SYMBOL_SCAN_EXEMPT``
    symbols, or cannot be sized: sizing is a guard, not a reader, and anything
    it cannot size goes on to ``load()``, which reports missing data or a bad
    revision properly.
    """
    if symbols and len(symbols) <= SYMBOL_SCAN_EXEMPT:
        return None
    budget = SCAN_BUDGET_ROWS if budget is None else budget
    from cnequity.storage.published import published

    names = [dataset, "news_headlines"] if dataset == "flash_news_wire" else [dataset]
    try:
        stores = {name: published(config, name) for name in names}
    except (OSError, ValueError, RuntimeError):
        return None
    selected = sum(
        _range_rows(group, start, end)
        for store in stores.values()
        for group in store.in_range(start, end)
    )
    if selected <= budget:
        return None

    covers = fits = newest_over = None
    groups = sorted(
        (g for g in stores[dataset].groups if g.low is not None and g.high is not None),
        key=lambda g: (g.low, g.high),
    )
    in_window = [g for g in groups if g.overlaps(start, end)]
    if in_window:
        covers = (groups[0].low, max(g.high for g in groups))
        # Newest first: the recent end of a series is what a page is usually for.
        last = min(in_window[-1].high, end) if end else in_window[-1].high
        used, earliest = 0, None
        for group in reversed(in_window):
            low = max(group.low, start) if start else group.low
            high = min(group.high, end) if end else group.high
            group_rows = _range_rows(group, start, end)
            if used + group_rows <= budget:
                used += group_rows
                earliest = low
                continue
            # Only part of this group fits: take its newest days, at its
            # average size per day. One catalog file can hold a whole table.
            per_day = group.rows / ((group.high - group.low).days + 1)
            days = int((budget - used) // per_day) if per_day else 0
            if days >= 1:
                earliest = max(low, high - timedelta(days=days - 1))
            break
        if earliest is None:
            newest_over = _period_label(in_window[-1])
        else:
            fits = (earliest, last)
    return OversizedRead(selected, budget, covers, fits, newest_over)


def _range_rows(group, start: date | None, end: date | None) -> int:
    """The share of a group's rows that ``start..end`` covers, by days.

    A catalog file can span a month while a request ends mid-month; counting
    the whole file would refuse a read that selects a fraction of it.
    """
    if group.low is None or group.high is None:
        return group.rows
    low = max(group.low, start) if start else group.low
    high = min(group.high, end) if end else group.high
    if high < low:
        return 0
    days = (group.high - group.low).days + 1
    return group.rows * ((high - low).days + 1) // days


def _period_label(group) -> str:
    if group.low == group.high:
        return group.low.isoformat()
    return f"{group.low.isoformat()}..{group.high.isoformat()}"
