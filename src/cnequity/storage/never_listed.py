"""The record of Shenwan-classed securities that never listed.

``storage.repairs.sw_never_listed`` writes it when it drops their classes from
``industry_members``; ``cne verify --industry`` reads it so they stay reported.
Reading, testing and recording need no repair machinery, so they live here,
below both; the daily industry step uses them too.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.read_context import ReadContext, read_root

RECORD = "sw_never_listed.json"
#: A security never listed has no record in any of these.
EVIDENCE = ("instruments", "daily_bars", "trading_status", "announcement_index")


def _record_path(config: Config) -> Path:
    return config.meta_root / RECORD


def load_never_listed(config: Config) -> list[dict]:
    """The recorded never-listed securities, or ``[]`` before any were found."""
    path = _record_path(config)
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8")).get("securities", [])


def record_never_listed(config: Config, securities: pl.DataFrame) -> None:
    """Merge ``[symbol, first_class_date, industry_code]`` into the meta record."""
    if securities.is_empty():
        return
    known = {r["symbol"]: r for r in load_never_listed(config)}
    for row in securities.iter_rows(named=True):
        known[row["symbol"]] = {
            "symbol": row["symbol"],
            "first_class_date": str(row["first_class_date"]),
            "industry_code": row["industry_code"],
        }
    write_json_atomic(
        _record_path(config),
        {
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "rule": "no instrument, daily bar, trading-status or announcement record",
            "securities": sorted(known.values(), key=lambda r: r["symbol"]),
        },
        indent=2,
    )


def never_listed(
    config: Config, symbols: list[str], context: ReadContext | None = None
) -> set[str]:
    """Those of ``symbols`` with no record in any of ``EVIDENCE``."""
    left = set(symbols)
    for dataset in EVIDENCE:
        if not left:
            break
        try:
            root = read_root(config, dataset, context)
        except FileNotFoundError:
            continue
        if not dataset_has_parquet(root, committed=False):
            continue
        seen = (
            scan_parquet_root(root, committed=False, symbols=sorted(left))
            .filter(pl.col("symbol").is_in(sorted(left)))
            .select("symbol")
            .unique()
            .collect()["symbol"]
            .to_list()
        )
        left -= set(seen)
    return left
