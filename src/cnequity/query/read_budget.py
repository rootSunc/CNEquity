"""How much Parquet one paged read would load, sized before anything is read.

``load()`` canonicalises the whole selection before a page is cut, so memory
follows what is read, not what is returned. Measured on a real lake, an
unwindowed read held 2.6 GB for ``margin_trading`` (142 MiB on disk) and 4.8 GB
for ``option_greeks`` (567 MiB) to return 200 rows; one ``trade_ticks`` day
(145 MiB) peaked at 5.2 GB, and all of ``trade_ticks`` (5.7 GiB) would not fit
in memory at all. MCP ``query_dataset`` and the dashboard's row viewer both
refuse a selection over the budget instead of reading it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS

#: Compressed Parquet one paged read may select when nothing narrows it by symbol.
SCAN_BUDGET_BYTES = 64 * 2**20

#: A short symbol list is pushed into the scan and keeps the read small (one
#: symbol over all of ``margin_trading`` peaked at 0.3 GB), so it is not
#: budgeted. A long list reads close to everything and is.
SYMBOL_SCAN_EXEMPT = 20


@dataclass(frozen=True)
class OversizedRead:
    """A selection over the budget, with what a caller needs to suggest a smaller one."""

    selected_bytes: int
    budget_bytes: int
    #: First and last day the dataset's partitions cover.
    covers: tuple[date, date] | None
    #: The newest window inside the request that fits the budget.
    fits: tuple[date, date] | None
    #: Set when even the newest period in the request is over the budget.
    newest_over: str | None


def committed_partitions(config: Config, dataset: str) -> tuple[Path, str | None]:
    """The committed generation's root and partition column for *dataset*."""
    from cnequity.storage.locator import logical_root
    from cnequity.storage.revisions import resolve_committed_root

    root = resolve_committed_root(
        logical_root(config, dataset), dataset=dataset, meta_root=config.meta_root
    )
    # The registry, not the legacy PARTITION_COLS table: that one has no
    # derived datasets, which would size a windowed option_greeks read as the
    # whole dataset and refuse it.
    return root, DATASETS[dataset].partition_col


def _files_bytes(files: list) -> int:
    return sum(path.stat().st_size for path in files)


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
    budget = SCAN_BUDGET_BYTES if budget is None else budget
    from cnequity.query.parquet_scan import list_partitions, partition_dir, partition_files_in_range

    names = [dataset, "news_headlines"] if dataset == "flash_news_wire" else [dataset]
    selected = 0
    try:
        for name in names:
            root, column = committed_partitions(config, name)
            if not root.is_dir():
                continue
            if column:
                files = partition_files_in_range(root, column, start=start, end=end, resolve=False)
            else:
                files = sorted(root.rglob("*.parquet"))
            selected += _files_bytes(files)
        if selected <= budget:
            return None
        root, column = committed_partitions(config, dataset)
        parts = list_partitions(root, column, resolve=False) if column else []
    except (OSError, ValueError, RuntimeError):
        return None

    covers = fits = newest_over = None
    in_window = [p for p in parts if p.overlaps(start, end)]
    if in_window:
        covers = (parts[0].start, parts[-1].end)
        # Newest first: the recent end of a series is what a page is usually for.
        used, first = 0, None
        for part in reversed(in_window):
            part_bytes = _files_bytes(
                sorted(partition_dir(root, column, part.value).glob("**/*.parquet"))
            )
            if first is not None and used + part_bytes > budget:
                break
            used += part_bytes
            first = part
        if first is in_window[-1] and used > budget:
            newest_over = first.value
        else:
            fits = (
                max(first.start, start) if start else first.start,
                min(in_window[-1].end, end) if end else in_window[-1].end,
            )
    return OversizedRead(selected, budget, covers, fits, newest_over)
