"""Canonical row helpers shared by storage, query, and quality consumers."""

from __future__ import annotations

import polars as pl

from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import PRIMARY_KEYS
from cnequity.domain.trading_status import evidence_rank_expr as _trading_status_evidence_rank

_SOURCE_RANK = "__canonical_source_rank"
_EVIDENCE_RANK = "__canonical_evidence_rank"
_HELPER_COLUMNS = (_SOURCE_RANK, _EVIDENCE_RANK)


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


def _evidence_rank_expr(dataset: str, schema) -> pl.Expr | None:
    """Authority ordering that outranks recency, for datasets that need one.

    Recency is the right default: a later fetch of the same key is normally a
    correction. ``trading_status`` is the exception — two of its feeds report
    current state rather than what happened in a given session, so a fresher
    row there can be a *worse* answer. See ``domain/trading_status`` for the
    classes and why the derived history depends on them.
    """
    if dataset == "trading_status":
        return _trading_status_evidence_rank(schema)
    return None


def _sort_for_canonical(frame, dataset: str):
    """Order by evidence class, then recency and source priority, before PK collapse."""
    schema = frame.collect_schema() if isinstance(frame, pl.LazyFrame) else frame.schema
    columns = set(schema.names())
    sort_cols: list[str] = []
    descending: list[bool] = []
    evidence = _evidence_rank_expr(dataset, schema)
    if evidence is not None:
        # Most significant key: a restated current-state snapshot must not win
        # a collision just because it was fetched later.
        frame = frame.with_columns(evidence.alias(_EVIDENCE_RANK))
        sort_cols.append(_EVIDENCE_RANK)
        descending.append(False)
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
    return out.drop(*_HELPER_COLUMNS, strict=False)


def dedupe_lazy_by_primary_key(lf: pl.LazyFrame, dataset: str) -> pl.LazyFrame:
    """Lazy equivalent of :func:`dedupe_by_primary_key`."""
    primary_key = PRIMARY_KEYS.get(dataset, [])
    columns = set(lf.collect_schema().names())
    if not primary_key or any(k not in columns for k in primary_key):
        return lf
    if any(column in columns for column in ("fetched_at", "source", "data_version")):
        lf = _sort_for_canonical(lf, dataset)
    return lf.unique(subset=primary_key, keep="last", maintain_order=True).drop(
        *_HELPER_COLUMNS, strict=False
    )
