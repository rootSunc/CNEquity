"""Cross-source diff engine — primary curated vs backup snapshots (ADR-0003)."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from cnequity.config import Config, FailoverDatasetSpec
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.schemas import PRIMARY_KEYS
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.source_snapshots import SnapshotStore

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
    blocking: bool = False,
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
                        # Individual field findings are diagnostic only.  The
                        # configured fraction gate below is the single source
                        # of release-blocking truth; otherwise one row would
                        # block a candidate even when it is within tolerance.
                        "blocks_revision": False,
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
                    "blocks_revision": False,
                    **{k: row.get(k) for k in pk if k in row},
                }
            )
    return diffs


def _coverage_finding(
    *,
    dataset: str,
    primary_source: str,
    backup_source: str,
    primary: pl.DataFrame,
    backup: pl.DataFrame,
    join_keys: list[str],
) -> dict:
    """Explain whether a source diff had a comparable key universe.

    An empty inner join is not evidence of agreement. It can mean the primary
    source returned nothing, the snapshot belongs to another date, or the two
    feeds covered disjoint symbols. Keep these states explicit so an operator
    cannot read a non-comparison as a clean comparison.
    """
    primary_keys = primary.select(join_keys).unique()
    backup_keys = backup.select(join_keys).unique()
    primary_count = primary_keys.height
    backup_count = backup_keys.height
    overlap = primary_keys.join(backup_keys, on=join_keys, how="inner").height
    missing_backup = primary_keys.join(backup_keys, on=join_keys, how="anti").height
    missing_primary = backup_keys.join(primary_keys, on=join_keys, how="anti").height

    if not primary_count and not backup_count:
        check = "no_comparable_rows"
        severity = "info"
        message = "Primary and backup returned no rows for the comparison window"
    elif not primary_count:
        check = "primary_missing_for_date"
        severity = "warning"
        message = (
            f"backup returned {backup_count} unique key(s), but primary "
            "returned none for the comparison window"
        )
    elif not backup_count:
        check = "backup_missing_for_date"
        severity = "warning"
        message = (
            f"primary returned {primary_count} unique key(s), but backup "
            "returned none for the comparison window"
        )
    elif not overlap:
        check = "no_pk_overlap"
        severity = "warning"
        message = (
            f"primary ({primary_count}) and backup ({backup_count}) returned "
            "disjoint primary-key sets; no fields were comparable"
        )
    elif missing_backup:
        check = "backup_coverage_gap"
        severity = "warning"
        message = (
            f"backup is missing {missing_backup} of {primary_count} primary key(s); "
            f"{overlap} key(s) are comparable"
            + (
                f"; primary is also missing {missing_primary} backup key(s)"
                if missing_primary
                else ""
            )
        )
    elif missing_primary:
        check = "primary_coverage_gap"
        severity = "warning"
        message = (
            f"primary is missing {missing_primary} of {backup_count} backup key(s); "
            f"{overlap} key(s) are comparable"
        )
    else:
        return {}

    return {
        "dataset": dataset,
        "check": check,
        "severity": severity,
        "message": message,
        "primary_source": primary_source,
        "backup_source": backup_source,
        "primary_unique_keys": primary_count,
        "backup_unique_keys": backup_count,
        "overlap_unique_keys": overlap,
        "missing_backup_keys": missing_backup,
        "missing_primary_keys": missing_primary,
    }


def diff_dataset(
    config: Config,
    spec: FailoverDatasetSpec,
    *,
    trade_date: date | None = None,
    sample_limit: int = 500,
    candidate_root: Path | None = None,
) -> list[dict]:
    curated_root = config.curated_root / spec.name
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    read_root = Path(candidate_root) if candidate_root is not None else curated_root
    committed = candidate_root is None
    curated_has_data = dataset_has_parquet(
        read_root,
        dataset=spec.name,
        meta_root=config.meta_root,
        committed=committed,
    )

    date_col = _date_column(spec.name)
    if curated_has_data:
        lf = scan_parquet_root(
            read_root,
            partition_col=date_col,
            start=trade_date,
            end=trade_date,
            dataset=spec.name,
            meta_root=config.meta_root,
            committed=committed,
        )
        if "source" in lf.collect_schema().names():
            lf = lf.filter(pl.col("source") == spec.primary)
        primary = dedupe_by_primary_key(lf.collect(), spec.name)
    else:
        primary = pl.DataFrame()

    backup = SnapshotStore(config.meta_root).read_latest(spec.name, source=spec.backup)
    if backup.is_empty():
        if not curated_has_data:
            return [
                {
                    "dataset": spec.name,
                    "check": "no_curated",
                    "severity": "info",
                    "message": f"No curated data for {spec.name}",
                }
            ]
        return [
            {
                "dataset": spec.name,
                "check": "no_snapshot",
                "severity": "info",
                "message": (
                    f"No backup snapshot for {spec.name} source={spec.backup}; "
                    f"primary rows available for comparison: {primary.height}"
                ),
                "primary_rows": primary.height,
                "snapshot_cadence": getattr(spec, "snapshot_cadence", "on_demand"),
                "peer_unavailable": True,
                "retryable": True,
            }
        ]

    backup_rows_before_date_filter = backup.height
    if trade_date is not None:
        date_col = _date_column(spec.name)
        if date_col and date_col in backup.columns:
            backup = backup.filter(pl.col(date_col) == trade_date)

    if (
        backup.is_empty()
        and backup_rows_before_date_filter
        and getattr(spec, "snapshot_cadence", "on_demand") == "on_demand"
    ):
        return [
            {
                "dataset": spec.name,
                "check": "backup_not_captured_for_date",
                "severity": "info",
                "message": (
                    f"no {spec.backup} snapshot was captured for the comparison window; "
                    f"{spec.name} backup snapshots are on-demand"
                ),
                "primary_source": spec.primary,
                "backup_source": spec.backup,
                "primary_unique_keys": primary.height,
                "backup_unique_keys": 0,
                "backup_snapshot_rows": backup_rows_before_date_filter,
                "snapshot_cadence": "on_demand",
                "peer_unavailable": False,
                "retryable": True,
            }
        ]

    pk = PRIMARY_KEYS.get(spec.name, [])
    if not pk:
        return []

    # A backup snapshot may legitimately repeat a key (for example an
    # overlapping page or correction observation).  Compare business keys, not
    # transport duplicates, so one legal duplicate cannot inflate the drift
    # denominator or produce an accidental gate.
    backup = dedupe_by_primary_key(backup, spec.name)

    join_keys = [
        k for k in pk if k in backup.columns and (k in primary.columns or primary.is_empty())
    ]
    if not join_keys:
        return []

    # A missing curated root has no schema to carry the PK columns. Add typed
    # empty columns so the coverage report can still say "primary missing".
    if primary.is_empty():
        primary = primary.with_columns(
            [pl.Series(key, [], dtype=backup.schema[key]) for key in join_keys]
        )

    findings: list[dict] = []
    coverage = _coverage_finding(
        dataset=spec.name,
        primary_source=spec.primary,
        backup_source=spec.backup,
        primary=primary,
        backup=backup,
        join_keys=join_keys,
    )
    if coverage:
        findings.append(coverage)

    # Deterministic stratified sampling: hash within each exchange so a small
    # sample cannot accidentally contain only Shanghai or Shenzhen symbols.
    effective_sample_limit = spec.sample_limit if sample_limit == 500 else sample_limit
    if effective_sample_limit > 0 and primary.height > effective_sample_limit:
        strata = (
            pl.col("symbol").str.split(".").list.last().alias("_source_diff_stratum")
            if "symbol" in primary.columns
            else pl.lit("all").alias("_source_diff_stratum")
        )
        primary = primary.with_columns(
            strata,
            pl.concat_str([pl.col(k).cast(pl.Utf8) for k in join_keys])
            .hash(seed=0)
            .alias("_sample_key"),
        )
        groups = primary.partition_by("_source_diff_stratum", as_dict=True)
        strata_items = sorted(groups.items(), key=lambda item: str(item[0]))
        selected: list[pl.DataFrame] = []
        remaining = effective_sample_limit
        for index, (_stratum, group) in enumerate(strata_items):
            if remaining <= 0:
                break
            reserve = min(len(strata_items) - index - 1, max(0, remaining - 1))
            slots = max(1, min(group.height, remaining - reserve))
            selected.append(group.sort("_sample_key").head(slots))
            remaining -= slots
        primary = pl.concat(selected, how="diagonal_relaxed").drop(
            ["_sample_key", "_source_diff_stratum"]
        )
    joined = primary.join(
        backup.select([*join_keys, *[c for c in backup.columns if c not in join_keys]]),
        on=join_keys,
        how="inner",
        suffix="_backup",
    )
    if joined.is_empty():
        return findings

    compare_fields = list(spec.compare_fields)
    if spec.name == "daily_bars" and spec.revision_gate:
        # OHLCV is the core integrity spine. Explicit fields remain additive,
        # while missing optional columns are skipped by the comparator.
        compare_fields = list(
            dict.fromkeys(["open", "high", "low", "close", "volume", *compare_fields])
        )
    drift_findings = _compare_numeric_fields(
        joined,
        compare_fields,
        pk=pk,
        tolerance_bps=spec.price_tolerance_bps,
        dataset=spec.name,
        primary_source=spec.primary,
        backup_source=spec.backup,
        blocking=spec.revision_gate,
    )
    findings.extend(drift_findings)
    if spec.revision_gate and drift_findings:
        drift_keys = {
            tuple(item.get(key) for key in pk)
            for item in drift_findings
            if all(key in item for key in pk)
        }
        comparable_key_columns = [key for key in pk if key in joined.columns]
        comparable_keys = joined.select(comparable_key_columns).unique().height
        drift_fraction = len(drift_keys) / max(1, comparable_keys)
        if drift_fraction > spec.max_drift_fraction:
            findings.append(
                {
                    "dataset": spec.name,
                    "check": "source_diff_gate",
                    "severity": "error",
                    "message": (
                        f"{len(drift_keys)}/{comparable_keys} comparable key(s) exceed source "
                        f"tolerance ({drift_fraction:.2%} > {spec.max_drift_fraction:.2%})"
                    ),
                    "primary_source": spec.primary,
                    "backup_source": spec.backup,
                    "drift_keys": len(drift_keys),
                    "comparable_keys": comparable_keys,
                    "drift_fraction": drift_fraction,
                    "max_drift_fraction": spec.max_drift_fraction,
                    "blocks_revision": True,
                }
            )
    return findings


def _date_column(dataset: str) -> str | None:
    from cnequity.domain.datasets import DATASETS

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
    write_json_atomic(
        out_dir / f"{run_id}.json",
        payload,
        ensure_ascii=False,
        indent=2,
        default=str,
    )
    return all_diffs


def source_diff_blocks_revision(findings: list[dict]) -> bool:
    """Whether source-diff evidence explicitly rejects a candidate release."""
    # ``source_diff_gate`` is deliberately the only blocking finding.  Field
    # observations remain useful audit diagnostics, but max_drift_fraction
    # must decide whether the aggregate disagreement is release material.
    return any(
        bool(item.get("blocks_revision")) and item.get("check") == "source_diff_gate"
        for item in findings
    )
