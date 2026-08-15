"""Cross-source diff engine — primary curated vs backup snapshots (ADR-0003)."""

from __future__ import annotations

import json
import logging
from datetime import date

import polars as pl

from cn_market_lake.config import Config, FailoverDatasetSpec
from cn_market_lake.domain.schemas import PRIMARY_KEYS
from cn_market_lake.storage.source_snapshots import SnapshotStore

logger = logging.getLogger(__name__)

DEFAULT_PRICE_TOLERANCE_BPS = 10.0


def _relative_bps(left: float, right: float) -> float:
    if right == 0:
        return 0.0 if left == 0 else float("inf")
    return abs(left - right) / abs(right) * 10_000.0


def _compare_numeric_fields(
    joined: pl.DataFrame,
    fields: list[str],
    *,
    pk: list[str],
    tolerance_bps: float,
    dataset: str,
    primary_source: str,
    backup_source: str,
) -> list[dict]:
    diffs: list[dict] = []
    for field in fields:
        if field not in joined.columns or f"{field}_backup" not in joined.columns:
            continue
        for row in joined.iter_rows(named=True):
            left = row.get(field)
            right = row.get(f"{field}_backup")
            if left is None or right is None:
                if left is None and right is None:
                    continue
                diffs.append(
                    {
                        "dataset": dataset,
                        "check": "field_null_mismatch",
                        "severity": "warning",
                        "field": field,
                        "primary_source": primary_source,
                        "backup_source": backup_source,
                        "primary_value": left,
                        "backup_value": right,
                        **{k: row.get(k) for k in pk if k in row},
                    }
                )
                continue
            # Relative bps tolerance applies to every configured numeric field
            # (volume differs by a few lots between sources routinely; exact
            # equality checks just spam findings).
            bps = _relative_bps(float(left), float(right))
            if bps <= tolerance_bps:
                continue
            is_price = field in ("open", "high", "low", "close")
            diffs.append(
                {
                    "dataset": dataset,
                    "check": "price_drift" if is_price else "field_drift",
                    "severity": "warning" if is_price else "info",
                    "field": field,
                    "bps": round(bps, 2),
                    "tolerance_bps": tolerance_bps,
                    "primary_source": primary_source,
                    "backup_source": backup_source,
                    "primary_value": float(left),
                    "backup_value": float(right),
                    **{k: row.get(k) for k in pk if k in row},
                }
            )
    return diffs


def diff_dataset(
    config: Config,
    spec: FailoverDatasetSpec,
    *,
    trade_date: date | None = None,
    sample_limit: int = 500,
) -> list[dict]:
    curated_root = config.curated_root / spec.name
    if not curated_root.exists():
        return [
            {
                "dataset": spec.name,
                "check": "no_curated",
                "severity": "info",
                "message": f"No curated data for {spec.name}",
            }
        ]

    from cn_market_lake.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    if not dataset_has_parquet(curated_root):
        return []

    date_col = _date_column(spec.name)
    lf = scan_parquet_root(
        curated_root,
        partition_col=date_col,
        start=trade_date,
        end=trade_date,
    )
    if "source" in lf.collect_schema().names():
        lf = lf.filter(pl.col("source") == spec.primary)
    primary = lf.collect()

    backup = SnapshotStore(config.meta_root).read_latest(spec.name, source=spec.backup)
    if backup.is_empty():
        return [
            {
                "dataset": spec.name,
                "check": "no_snapshot",
                "severity": "info",
                "message": f"No backup snapshot for {spec.name} source={spec.backup}",
            }
        ]

    if trade_date is not None:
        date_col = _date_column(spec.name)
        if date_col and date_col in backup.columns:
            backup = backup.filter(pl.col(date_col) == trade_date)

    pk = PRIMARY_KEYS.get(spec.name, [])
    if not pk:
        return []

    join_keys = [k for k in pk if k in primary.columns and k in backup.columns]
    if not join_keys:
        return []

    # Deterministic spread sample: hashing the join keys picks rows across the
    # whole universe instead of always the first N symbols in file order.
    if primary.height > sample_limit:
        primary = (
            primary.with_columns(
                pl.concat_str([pl.col(k).cast(pl.Utf8) for k in join_keys])
                .hash(seed=0)
                .alias("_sample_key")
            )
            .sort("_sample_key")
            .head(sample_limit)
            .drop("_sample_key")
        )
    joined = primary.join(
        backup.select([*join_keys, *[c for c in backup.columns if c not in join_keys]]),
        on=join_keys,
        how="inner",
        suffix="_backup",
    )
    if joined.is_empty():
        return [
            {
                "dataset": spec.name,
                "check": "no_overlap",
                "severity": "info",
                "message": "Primary and backup share no PK overlap for comparison window",
            }
        ]

    return _compare_numeric_fields(
        joined,
        spec.compare_fields,
        pk=pk,
        tolerance_bps=spec.price_tolerance_bps,
        dataset=spec.name,
        primary_source=spec.primary,
        backup_source=spec.backup,
    )


def _date_column(dataset: str) -> str | None:
    from cn_market_lake.domain.datasets import DATASETS

    spec = DATASETS.get(dataset)
    return spec.query_date_col if spec else None


def run_source_diffs(
    config: Config,
    run_id: str,
    trade_date: date,
) -> list[dict]:
    if not config.failover_enabled or not config.failover_datasets:
        return []

    all_diffs: list[dict] = []
    for spec in config.failover_datasets:
        try:
            all_diffs.extend(diff_dataset(config, spec, trade_date=trade_date))
        except Exception as exc:
            logger.warning("source_diff failed for %s: %s", spec.name, exc)
            all_diffs.append(
                {
                    "dataset": spec.name,
                    "check": "diff_error",
                    "severity": "error",
                    "message": str(exc),
                }
            )

    out_dir = config.meta_root / "quality" / "source_diffs"
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "trade_date": trade_date.isoformat(),
        "diff_count": len(all_diffs),
        "diffs": all_diffs,
    }
    with open(out_dir / f"{run_id}.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    return all_diffs
