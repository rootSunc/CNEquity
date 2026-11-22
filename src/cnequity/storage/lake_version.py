"""A cheap fingerprint of everything a lake summary can depend on."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from cnequity.domain.market_time import shanghai_today
from cnequity.sqlite_readonly import connect_readonly


def lake_version(config) -> tuple:
    """What the dashboard's summaries depend on, read in milliseconds.

    Published rows and the coverage ledger both live in the DuckLake catalog,
    and every commit to it writes its WAL, so the identity of the catalog file
    and its WAL moves whenever either changes. The rest are the sidecars the
    summaries read, the newest finished run (stats freshness), and the hour,
    because the freshness anchor follows the trading day.

    The catalog is never opened here. DuckDB reads it through its own bundled
    SQLite; a second SQLite library in the same process does not see that
    library's locks, so closing its connection can checkpoint and truncate the
    WAL and shared-memory files under DuckDB, which then dies with SIGBUS.
    """
    from cnequity.storage.catalog_bridge import lake_root
    from cnequity.storage.stats import summary_path
    from equitylake_core.catalog import CATALOG_FILE

    def _scalar(path: Path, sql: str):
        if not path.is_file():
            return None
        try:
            con = connect_readonly(path, timeout=1.0)
            try:
                row = con.execute(sql).fetchone()
            finally:
                con.close()
        except Exception:  # noqa: BLE001 - an unreadable file is just "unknown"
            return None
        return row[0] if row else None

    def _mtime(path: Path):
        try:
            return path.stat().st_mtime_ns
        except OSError:
            return None

    def _identity(path: Path):
        try:
            info = path.stat()
        except OSError:
            return None
        return (info.st_ino, info.st_size, info.st_mtime_ns)

    catalog = lake_root(config) / CATALOG_FILE
    main = _identity(catalog)
    # None while the lake has no catalog yet, which callers rely on.
    snapshot = None if main is None else (main, _identity(Path(f"{catalog}-wal")))
    finished = _scalar(Path(config.manifest_path), "SELECT max(finished_at) FROM ingestion_runs")
    now = datetime.now(timezone.utc)
    return (
        snapshot,
        finished,
        _mtime(summary_path(config)),
        _mtime(Path(config.meta_root) / "quality" / "health-latest.json"),
        shanghai_today().isoformat(),
        now.hour,
    )
