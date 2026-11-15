"""Explicit, offline SHFE annual import with partial-field provenance.

The annual workbooks omit fields carried by the daily exchange files. They
therefore never create daily completion receipts and never replace an existing
daily row. Importing them is an operator choice, not the normal backfill route.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from cnequity.adapters.futures_exchange.shfe_archive import iter_archive
from cnequity.query.parquet_scan import root_parquet_files, scan_parquet_files
from cnequity.steps.common import write_simple
from cnequity.storage.atomic import replace_with_retry, write_json_atomic
from cnequity.storage.read_context import read_root

_MAX_ARCHIVE_BYTES = 1_000_000_000
_MISSING_FIELDS = {
    "futures_bars": ["oi_change"],
    "option_bars": [
        "oi_change",
        "exercise_volume",
        "delta",
        "implied_vol",
        "series_implied_vol",
    ],
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _retain_archive(config, path: Path, digest: str) -> Path:
    target = config.meta_root / "derivatives" / "annual_archives" / f"{digest}.zip"
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_file() and _sha256(target) == digest:
        return target
    temporary = target.with_name(f"{target.name}.tmp-{uuid.uuid4().hex}")
    try:
        with path.open("rb") as source, temporary.open("wb") as sink:
            shutil.copyfileobj(source, sink, 1024 * 1024)
            sink.flush()
            os.fsync(sink.fileno())
        if _sha256(temporary) != digest:
            raise OSError("annual archive changed while it was being copied")
        replace_with_retry(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def _curated_keys(config, dataset: str, year: int) -> pl.DataFrame | None:
    root = read_root(config, dataset)
    files = root_parquet_files(root)
    if not files:
        return None
    return (
        scan_parquet_files(files)
        .filter(pl.col("trade_date").dt.year() == year)
        .filter(pl.col("data_version").is_null() | (pl.col("data_version") != "shfe_archive_v1"))
        .select("symbol", "trade_date")
        .unique()
        .collect()
    )


def import_shfe_annual(
    config,
    run_id: str,
    dataset: str,
    path: Path,
    *,
    year: int,
    start: date | None = None,
    end: date | None = None,
    source_url: str | None = None,
    downloaded_at: str | None = None,
) -> dict:
    """Stage verified annual rows absent from curated, preserving the ZIP.

    Each member is a durable success unit. A later broken member leaves earlier
    validated rows staged and reports degraded coverage. Existing daily keys
    are excluded before compact, so a sparse annual row cannot erase fields.
    """
    if dataset not in _MISSING_FIELDS:
        raise ValueError("SHFE annual archives only contain futures/option bars")
    path = Path(path).resolve(strict=True)
    if not path.is_file() or path.suffix.lower() != ".zip":
        raise ValueError("SHFE annual archive must be a local ZIP file")
    if not 0 < path.stat().st_size <= _MAX_ARCHIVE_BYTES:
        raise ValueError("SHFE annual archive compressed size exceeds limit")
    first, last = date(year, 1, 1), date(year, 12, 31)
    start = start or first
    end = end or last
    if not first <= start <= end <= last:
        raise ValueError("annual import range must be within the archive year")

    digest = _sha256(path)
    retained = _retain_archive(config, path, digest)
    existing = _curated_keys(config, dataset, year)
    seen: set[tuple[str, date]] = set()
    members: list[dict] = []
    rejected: list[dict] = []
    errors: list[str] = []
    rows_read = rows_written = protected_daily_rows = 0
    iterator = iter(iter_archive(retained, year=year, rejected=rejected))
    index = 0
    while True:
        try:
            name, frames = next(iterator)
        except StopIteration:
            break
        except Exception as exc:  # a bad late workbook must not erase earlier units
            errors.append(f"{type(exc).__name__}: {exc}")
            break
        frame = frames.get(dataset)
        if frame is None or frame.is_empty():
            members.append({"name": name, "rows": 0, "staged": 0})
            index += 1
            continue
        keys = list(frame.select("symbol", "trade_date").iter_rows())
        overlap = seen.intersection(keys)
        if overlap:
            errors.append(f"annual archive repeats {dataset} keys across members: {name}")
            break
        seen.update(keys)
        frame = frame.filter(pl.col("trade_date").is_between(start, end))
        rows_read += frame.height
        in_range = frame.height
        if existing is not None and not existing.is_empty() and not frame.is_empty():
            before = frame.height
            frame = frame.join(existing, on=["symbol", "trade_date"], how="anti")
            protected_daily_rows += before - frame.height
        if not frame.is_empty():
            write_simple(config, run_id, dataset, frame, batch_id=f"archive-{index:03d}")
            rows_written += frame.height
        members.append({"name": name, "rows": in_range, "staged": frame.height})
        index += 1

    receipt = {
        "version": 1,
        "run_id": run_id,
        "dataset": dataset,
        "year": year,
        "range": [start.isoformat(), end.isoformat()],
        "archive_sha256": digest,
        "retained_path": str(retained),
        "source_url": source_url,
        "downloaded_at": downloaded_at,
        "imported_at": datetime.now(timezone.utc).isoformat(),
        "data_version": "shfe_archive_v1",
        "coverage_status": "partial_fields",
        "missing_fields": _MISSING_FIELDS[dataset],
        "members": members,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "protected_daily_rows": protected_daily_rows,
        "rejected_rows": len(rejected),
        "rejected_sample": rejected[:5],
        "errors": errors,
    }
    receipt["status"] = "warning" if errors or rejected or rows_read == 0 else "success"
    write_json_atomic(
        config.meta_root / "derivatives" / "annual_archive_imports" / f"{run_id}.json",
        receipt,
        default=str,
    )
    return receipt
