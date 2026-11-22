"""A few lake catalogs kept open across calls, the least recently used closed beyond a limit.

Opening a catalog attaches DuckLake to its SQLite database and loads two
extensions, which is too slow to do for every small read; keeping one open per
lake is what makes repeated reads cheap. But each open catalog holds a DuckDB
instance and its threads, so a cache keyed by lake must not grow with every
lake a process ever reads: a dashboard switching configs, a snapshot restore
reading two lakes, or a test run making a lake per test would otherwise keep
them all open until the process runs out of threads.

Not thread-safe by itself: callers hold their own lock while they look a
catalog up *and* use it, which is also what makes closing an evicted one safe.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Hashable
from pathlib import Path

from equitylake_core.catalog import Catalog


class OpenCatalogs:
    """Catalogs by lake root, each opened with some options; at most *limit* stay open."""

    def __init__(self, limit: int):
        if limit < 1:
            raise ValueError("limit must be at least 1")
        self.limit = limit
        self._open: OrderedDict[Path, tuple[Hashable, Catalog]] = OrderedDict()

    def get(self, root: Path, options: Hashable = None) -> Catalog | None:
        """The catalog open on *root* with *options*; one opened otherwise is closed."""
        found = self._open.get(root)
        if found is None:
            return None
        if found[0] != options:
            self.close(root)
            return None
        self._open.move_to_end(root)
        return found[1]

    def put(self, root: Path, catalog: Catalog, options: Hashable = None) -> Catalog:
        """Keep *catalog* open for *root*, closing the least recently used beyond the limit."""
        self.close(root)
        self._open[root] = (options, catalog)
        while len(self._open) > self.limit:
            _, (_, evicted) = self._open.popitem(last=False)
            evicted.close()
        return catalog

    def close(self, root: Path | None = None) -> None:
        """Close the catalog open on *root*, or every one."""
        roots = list(self._open) if root is None else [root]
        for key in roots:
            found = self._open.pop(key, None)
            if found is not None:
                found[1].close()

    def __contains__(self, root: Path) -> bool:
        return root in self._open

    def __len__(self) -> int:
        return len(self._open)

    def roots(self) -> list[Path]:
        return list(self._open)
