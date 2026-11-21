"""Where a lake's records that are not data live under ``meta/``.

Until 0.25 they shared ``meta/state/`` with each dataset's coverage ledger.
The coverage ledger is now the lake catalog's ``_attempts``, and what was
left there went to the owner of each record:

* ``lake.json`` — which version built the lake and the migrations it has had;
* ``scheduler/`` — the scheduler's run markers, the dashboard timer and the
  soft-failure streaks;
* ``checkpoints/`` — where a long backfill or sweep resumes;
* ``source_cache/`` — what a source answered that is kept to avoid asking
  again (the delisted catalog, THS boards, source evidence for claims);
* ``decisions/`` — what the lake decided about a source, such as which
  securities' adjustment factors come from another vendor.

The ``meta-layout`` migration moves a lake's files from ``meta/state/``.
"""

from __future__ import annotations

from pathlib import Path

#: Relative to ``meta/``.
LAKE_RECORD = Path("lake.json")
SCHEDULER = Path("scheduler")
SCHEDULER_MARKERS = SCHEDULER / "markers"
WEB_SCHEDULER = SCHEDULER / "web"
SOFT_STREAK = SCHEDULER / "soft_streak"
CHECKPOINTS = Path("checkpoints")
SOURCE_CACHE = Path("source_cache")
DECISIONS = Path("decisions")

#: The old home of every record above.
LEGACY_STATE = Path("state")

#: Each entry under ``meta/state/`` and where it lives now, relative to ``meta/``.
#: A dataset's own ``<dataset>.json`` held its coverage ledger, which is gone;
#: the two that kept something else are split by field (:data:`SPLIT_FIELDS`).
MOVES: dict[str, Path] = {
    "lake.json": LAKE_RECORD,
    "scheduler": SCHEDULER_MARKERS,
    "web_scheduler": WEB_SCHEDULER,
    "soft_streak": SOFT_STREAK,
    "delisted_ingested.json": CHECKPOINTS / "delisted_ingested.json",
    "sector_bars_backfill.json": CHECKPOINTS / "sector_bars_backfill.json",
    "trading_status_st_backfill.json": CHECKPOINTS / "trading_status_st_backfill.json",
    "research_packs.json": CHECKPOINTS / "research_packs.json",
    "star-hint-shown": Path("star-hint-shown"),
    "delisted_catalog.json": SOURCE_CACHE / "delisted_catalog.json",
    "history": SOURCE_CACHE / "delisted_catalog_history",
    "ths_board_catalog.json": SOURCE_CACHE / "ths_board_catalog.json",
    "push2_snapshots": SOURCE_CACHE / "push2_snapshots",
    "historical_st_evidence": SOURCE_CACHE / "historical_st_evidence",
    "delisted_daily_bars_recovery": SOURCE_CACHE / "delisted_daily_bars_recovery",
}

#: Fields of a dataset's old state file that are not coverage, and their new file.
SPLIT_FIELDS: dict[str, tuple[Path, tuple[str, ...]]] = {
    "instruments": (
        SOURCE_CACHE / "instruments_undated.json",
        ("undated_symbols", "undated_checked_on"),
    ),
    "adj_factors": (
        DECISIONS / "adj_factors.json",
        ("source_overrides", "source_unavailable_symbols"),
    ),
}

#: A legacy file nothing reads any more: the EastMoney guard moved to the rate
#: limit directory (it imports a same-day legacy file on its own).
OBSOLETE = ("eastmoney_guard.json",)


def meta_path(meta_root: Path | str, relative: Path | str) -> Path:
    return Path(meta_root) / relative


def sector_bars_backfill_path(meta_root: Path | str, track: str | None = None) -> Path:
    name = "sector_bars_backfill.json" if track is None else f"sector_bars_backfill_{track}.json"
    return Path(meta_root) / CHECKPOINTS / name
