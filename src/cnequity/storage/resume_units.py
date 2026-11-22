"""Where an interrupted backfill picks up: the units its earlier runs published.

A long backfill walks units (a report period and statement, a record-date
window). Each one a run validated is named in the run's findings; once compact
has published the dataset's whole staging, those units are recorded here, in
the lake catalog, and a later backfill of the same scope skips them for a
week. The age bound keeps this a resume, not a cache: an older unit is asked
again, so a long idle gap is re-observed rather than trusted.

This replaces the per-dataset resume checkpoints previously stored in ``meta/state``.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

import polars as pl

from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.catalog_bridge import lake_root
from equitylake_core.catalog import CATALOG_FILE, Catalog
from equitylake_core.write import Write

TABLE = "_resume_units"
COLUMNS = "dataset VARCHAR, unit VARCHAR, scope VARCHAR, run VARCHAR, published_at TIMESTAMPTZ"
#: The finding a backfill step names its validated units in.
CHECK = "backfill_units_staged"
#: How long a published unit stands in for asking again.
TTL_DAYS = 7


def finding(dataset: str, units: Iterable[str], scope: str) -> dict:
    """Name the units this run validated into staging, for compact to record."""
    named = sorted(set(units))
    return {
        "dataset": dataset,
        "check": CHECK,
        "severity": "info",
        "message": f"{len(named)} backfill unit(s) staged",
        "units": named,
        "scope": scope,
    }


def attach(write: Write, run_id: str, findings: Iterable[dict], datasets: Iterable[str]) -> int:
    """Add the newly published resume units to the pending publication."""
    wanted = set(datasets)
    rows = {
        (item["dataset"], unit, item["scope"])
        for item in findings
        if item.get("check") == CHECK and item.get("dataset") in wanted
        for unit in item.get("units") or ()
    }
    if not rows:
        return 0
    now = datetime.now(UTC)
    frame = pl.DataFrame(
        [
            {"dataset": d, "unit": u, "scope": s, "run": run_id, "published_at": now}
            for d, u, s in sorted(rows)
        ],
        schema={
            "dataset": pl.String,
            "unit": pl.String,
            "scope": pl.String,
            "run": pl.String,
            "published_at": pl.Datetime("us", "UTC"),
        },
    )
    catalog = write.catalog
    _prepare(catalog)
    seen = catalog.records(TABLE, "dataset, unit, scope, run")
    frame = frame.join(seen, on=["dataset", "unit", "scope", "run"], how="anti")
    write.record_frame(TABLE, frame)
    return frame.height


def record(config, run_id: str, findings: Iterable[dict], datasets: Iterable[str]) -> int:
    """Record resume units after a run that changed no published files."""
    findings = list(findings)
    if not any(item.get("check") == CHECK for item in findings):
        return 0
    with lake_mutation_lock(config.meta_root), Catalog(lake_root(config), create=True) as catalog:
        write = Write(catalog)
        count = attach(write, run_id, findings, datasets)
        if count:
            write.commit(message=f"run:{run_id}:resume-units")
    return count


def published(config, dataset: str, scope: str, *, now: datetime | None = None) -> set[str]:
    """Units of *scope* that compact published for *dataset* within ``TTL_DAYS``."""
    root = lake_root(config)
    if not (root / CATALOG_FILE).exists():
        return set()
    with Catalog(root, read_only=True) as catalog:
        if not catalog.has_table(TABLE):
            return set()
        rows = catalog.records(TABLE)
    cutoff = (now or datetime.now(UTC)) - timedelta(days=TTL_DAYS)
    return set(
        rows.filter(
            (pl.col("dataset") == dataset)
            & (pl.col("scope") == scope)
            & (pl.col("published_at") >= cutoff)
        )["unit"].to_list()
    )


def _prepare(catalog: Catalog) -> None:
    catalog.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} ({COLUMNS})")
    catalog.keep_rows_inline(TABLE)
