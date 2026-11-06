"""Give stored ``valuation_metrics`` rows an explicit, consistent field basis.

Before 2026-09 the dataset mixed definitions under the same column names:
push2 rows stored the dynamic P/E in ``pe_ttm``; baostock ``float_mv`` was
priced at the session VWAP; baostock ``total_mv`` used the previous year-end
share count. This repair, run once over the committed generation:

- moves push2's dynamic P/E to ``pe_dynamic`` and labels vendor market caps;
- converts legacy baostock ``float_mv`` to the closing price where the stored
  bar's VWAP is consistent with its own range, and otherwise keeps the value
  labelled as VWAP-based;
- rebuilds baostock ``total_mv`` from ``share_structure`` (shares effective on
  the session) where available, and otherwise labels the year-end estimate.

It is idempotent: rows that already carry a basis are not recomputed. The
repaired generation is published as one revision and the previous one stays
retained, so every original value remains readable by revision.
"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.schemas import sanitize_dataset_rows, validate_dataframe
from cnequity.domain.valuation import (
    label_legacy_total_mv,
    reconstruct_total_mv,
    repair_legacy_float_mv,
    split_dynamic_pe,
)
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.parquet import CuratedWriter, _business_equal
from cnequity.storage.revisions import RevisionStore

_DATASET = "valuation_metrics"
_BAR_COLUMNS = ("close", "low", "high", "volume", "amount")


def _partition_day(name: str) -> date | None:
    column, _, value = name.partition("=")
    if column != "trade_date":
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _bars(root: Path | None, days: list[date]) -> pl.DataFrame:
    """Canonical daily bars of the committed generation for *days*."""
    empty = pl.DataFrame(
        schema={"symbol": pl.Utf8, "trade_date": pl.Date, **dict.fromkeys(_BAR_COLUMNS, pl.Float64)}
    )
    if root is None or not days:
        return empty
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    if not dataset_has_parquet(root, committed=False):
        return empty
    # Rows, not day directories: daily_bars is month-partitioned, and a day
    # directory lookup found nothing at all once it was, so every valuation
    # row lost the bars its basis is checked against.
    wanted = sorted(set(days))
    frame = (
        scan_parquet_root(
            root,
            partition_col="trade_date",
            start=wanted[0],
            end=wanted[-1],
            committed=False,
        )
        .filter(pl.col("trade_date").is_in(wanted))
        .collect()
    )
    if frame.is_empty():
        return empty
    bars = dedupe_by_primary_key(validate_dataframe(frame, "daily_bars"), "daily_bars")
    return bars.select("symbol", "trade_date", *[pl.col(c).cast(pl.Float64) for c in _BAR_COLUMNS])


def load_share_counts(config: Config) -> pl.DataFrame:
    """Latest known total share count per (symbol, change_date), or empty.

    Empty only when the lake holds no share history; a history that cannot
    be read is an error, not a reason to keep estimates silently.
    """
    from cnequity.query import load

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    if (
        store.current_root("share_structure") is None
        and not (config.curated_root / "share_structure").is_dir()
    ):
        return pl.DataFrame(
            schema={"symbol": pl.Utf8, "change_date": pl.Date, "total_shares": pl.Float64}
        )
    frame = load("share_structure", as_of=datetime.now(timezone.utc).date(), config=config)
    return frame.select("symbol", "change_date", "total_shares")


def repair_frame(frame: pl.DataFrame, bars: pl.DataFrame, shares: pl.DataFrame) -> pl.DataFrame:
    """Apply every basis repair to one batch of valuation rows."""
    columns = frame.columns
    out = split_dynamic_pe(frame)
    out = label_legacy_total_mv(out)
    out = out.join(bars, on=["symbol", "trade_date"], how="left")
    out = repair_legacy_float_mv(out)
    out = reconstruct_total_mv(out, shares)
    return out.select(columns)


def _rows_changed(original: pl.DataFrame, repaired: pl.DataFrame) -> int:
    """Repaired rows with no identical row in *original*, ignoring observation times."""
    columns = sorted(set(repaired.columns) & set(original.columns) - {"fetched_at", "observed_at"})
    if not columns:
        return repaired.height
    try:
        before = original.select(columns).cast(repaired.select(columns).schema)
    except pl.exceptions.PolarsError:
        return repaired.height
    # Row hashes carry no nulls, so a plain anti-join matches across polars versions.
    seen = before.hash_rows().unique().to_frame("row")
    after = repaired.select(columns).hash_rows().to_frame("row")
    return after.join(seen, on="row", how="anti").height


def repair_valuation_basis(config: Config, *, apply: bool = False) -> dict:
    """Plan (default) or publish the basis repair of ``valuation_metrics``."""
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _repair_locked(config, apply=apply)


def _repair_locked(config: Config, *, apply: bool) -> dict:
    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    base = store.repair_base(_DATASET, register=apply)
    # ``rows`` is every row the repair read; ``rows_changed`` is how many of them it rewrites.
    report: dict = {
        "dataset": _DATASET,
        "applied": False,
        "rows": 0,
        "rows_changed": 0,
        "partitions_changed": 0,
    }
    if base is None:
        report["note"] = f"no {_DATASET} data; nothing to repair"
        return report
    by_partition: dict[str, list[Path]] = {}
    for path in sorted(base.rglob("*.parquet")):
        by_partition.setdefault(path.relative_to(base).parts[0], []).append(path)
    months: dict[str, list[str]] = {}
    for name in sorted(by_partition):
        day = _partition_day(name)
        if day is None:
            raise RuntimeError(f"{_DATASET}: unexpected partition {name!r}; repair layout first")
        months.setdefault(day.strftime("%Y-%m"), []).append(name)

    shares = load_share_counts(config)
    bars_root = store.current_root("daily_bars")
    if apply:
        store.materialize_current(_DATASET)
    writer = CuratedWriter(config.curated_root)
    counts: Counter[str] = Counter()
    changed: list[Path] = []
    for names in months.values():
        days = [d for d in (_partition_day(name) for name in names) if d is not None]
        bars = _bars(bars_root, days)
        for name in names:
            original = pl.concat(
                [
                    sanitize_dataset_rows(
                        validate_dataframe(pl.read_parquet(p), _DATASET), _DATASET
                    )
                    for p in by_partition[name]
                ],
                how="diagonal_relaxed",
            )
            repaired = validate_dataframe(repair_frame(original, bars, shares), _DATASET)
            report["rows"] += repaired.height
            before = original.select(
                pl.col("pe_dynamic").is_not_null().sum().alias("pe_dynamic"),
            ).row(0, named=True)
            after = repaired.select(
                pl.col("pe_dynamic").is_not_null().sum().alias("pe_dynamic"),
            ).row(0, named=True)
            counts["pe_moved_to_dynamic"] += after["pe_dynamic"] - before["pe_dynamic"]
            for column in ("total_mv_basis", "float_mv_basis"):
                for basis, count in repaired.group_by(column).agg(pl.len()).iter_rows():
                    counts[f"{column}:{basis}"] += count
            if _business_equal(original, repaired):
                continue
            report["partitions_changed"] += 1
            report["rows_changed"] += _rows_changed(original, repaired)
            if apply:
                changed.append(
                    writer.write_partition(
                        _DATASET,
                        "trade_date",
                        name.partition("=")[2],
                        repaired,
                        "part-merged.parquet",
                    )
                )
    report["counts"] = dict(sorted(counts.items()))
    if not apply or not changed:
        return report

    from cnequity.domain.contracts import contract_fingerprint, dataset_contract

    contract = dataset_contract(_DATASET)
    run_id = f"valuation-basis-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    from cnequity.quality.publication import check_repair_publication

    publication = check_repair_publication(config, _DATASET, run_id, changed)
    revision = store.commit(
        _DATASET,
        run_id=run_id,
        changed_files=changed,
        schema_version=int(contract["schema_version"]),
        contract_fingerprint=contract_fingerprint(contract),
        metadata={
            "reason": "valuation_basis_repair",
            "counts": report["counts"],
            "publication_audit": publication["report_path"],
        },
    )
    report.update(
        applied=True,
        run_id=run_id,
        revision=None if revision is None else revision.revision,
        revision_id=None if revision is None else revision.revision_id,
        publication_audit=publication["report_path"],
    )
    return report
