"""Canonical row helpers shared by storage, query, and quality consumers."""

from __future__ import annotations

import polars as pl

from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import PRIMARY_KEYS
from cnequity.domain.trading_status import evidence_rank_expr

_SOURCE_RANK = "__canonical_source_rank"


def _source_rank_expr(dataset: str, columns: set[str]) -> pl.Expr | None:
    """Rank primary/backup sources for deterministic same-timestamp ties."""
    if "source" not in columns:
        return None
    spec = DATASETS.get(dataset)
    if spec is None:
        return pl.lit(0)
    rank = pl.lit(0)
    if spec.backup_source:
        rank = pl.when(pl.col("source") == spec.backup_source).then(1).otherwise(rank)
    if spec.primary_source:
        rank = pl.when(pl.col("source") == spec.primary_source).then(2).otherwise(rank)
    return rank


def _sort_for_canonical(frame, dataset: str):
    """Order rows before PK collapse so ``keep="last"`` picks the intended row.

    Most datasets prefer the freshest observation, then source priority.
    ``trading_status`` instead keys on :func:`status_evidence_rank`: rank 0
    (authority) is sorted last so it survives collision with a derived
    bar-gap suspension, and a newer ordinary current-state snapshot (rank 2)
    can never overwrite it. Within one evidence class the newest observation
    still wins.
    """
    schema = frame.collect_schema()
    columns = set(schema.names())
    sort_cols: list[str] = []
    descending: list[bool] = []

    if (
        dataset == "trading_status"
        and "source" in columns
        and "fetched_at" in columns
        and isinstance(schema.get("fetched_at"), pl.Datetime)
    ):
        fetched_dtype = schema.get("fetched_at")
        fetched_timezone = getattr(fetched_dtype, "time_zone", None)
        frame = frame.with_columns(evidence_rank_expr(fetched_timezone).alias(_SOURCE_RANK))
        sort_cols.append(_SOURCE_RANK)
        # Lower rank is more authoritative; descending puts rank 0 last so
        # ``unique(..., keep="last")`` keeps the authority over a derived row.
        descending.append(True)
        sort_cols.append("fetched_at")
        descending.append(False)
        sort_cols.append("source")
        descending.append(False)
    else:
        if "fetched_at" in columns:
            sort_cols.append("fetched_at")
            descending.append(False)
        rank = _source_rank_expr(dataset, columns)
        if rank is not None:
            frame = frame.with_columns(rank.alias(_SOURCE_RANK))
            sort_cols.extend([_SOURCE_RANK, "source"])
            # ``unique(..., keep="last")`` selects the last row after sorting.
            # Primary therefore needs the greatest rank at the end of the sort.
            descending.extend([False, False])
    if "data_version" in columns:
        sort_cols.append("data_version")
        descending.append(False)
    if not sort_cols:
        return frame
    # ``keep="last"`` below selects the final row.  Put legacy rows without a
    # fetch timestamp first so they cannot override a timestamped observation;
    # this mirrors DuckDB's ``fetched_at DESC NULLS LAST`` ordering.
    return frame.sort(sort_cols, descending=descending, nulls_last=False, maintain_order=True)


def dedupe_by_primary_key(df: pl.DataFrame, dataset: str) -> pl.DataFrame:
    """Keep one row per registered PK, preferring the freshest provenance.

    Validated lake rows always carry ``fetched_at``. The no-provenance fallback
    still collapses duplicate keys and, when available, applies source/version
    precedence so a malformed legacy fragment cannot multiply a quality join or
    let filesystem order decide between a primary and backup row; schema checks
    remain responsible for reporting that the fragment is incomplete.
    """
    primary_key = PRIMARY_KEYS.get(dataset, [])
    if df.is_empty() or not primary_key or any(k not in df.columns for k in primary_key):
        return df
    if any(column in df.columns for column in ("fetched_at", "source", "data_version")):
        df = _sort_for_canonical(df, dataset)
    out = df.unique(subset=primary_key, keep="last", maintain_order=True)
    return out.drop(_SOURCE_RANK, strict=False)


def dedupe_lazy_by_primary_key(lf: pl.LazyFrame, dataset: str) -> pl.LazyFrame:
    """Lazy equivalent of :func:`dedupe_by_primary_key`."""
    primary_key = PRIMARY_KEYS.get(dataset, [])
    columns = set(lf.collect_schema().names())
    if not primary_key or any(k not in columns for k in primary_key):
        return lf
    if any(column in columns for column in ("fetched_at", "source", "data_version")):
        lf = _sort_for_canonical(lf, dataset)
    return lf.unique(subset=primary_key, keep="last", maintain_order=True).drop(
        _SOURCE_RANK, strict=False
    )
