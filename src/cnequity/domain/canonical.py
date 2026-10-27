"""Canonical row helpers shared by storage, query, and quality consumers."""

from __future__ import annotations

import polars as pl

from cnequity.domain.action_evidence import evidence_level_expr as _action_evidence_level
from cnequity.domain.action_evidence import valid_payment_expr as _valid_payment
from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import PRIMARY_KEYS
from cnequity.domain.trading_status import evidence_rank_expr as _trading_status_evidence_rank

_SOURCE_RANK = "__canonical_source_rank"
_EVIDENCE_RANK = "__canonical_evidence_rank"
_HELPER_COLUMNS = (_SOURCE_RANK, _EVIDENCE_RANK)


def canonical_policy(dataset: str) -> str:
    """Versioned conflict-resolution rule recorded with normal compaction.

    Sources are provenance, not permanent ownership. A configured ingestion
    may correct existing keys; these rules decide which observation wins.
    Change the version when changing precedence, not for a new observation.
    """
    return {
        "corporate_actions": "corporate_action_evidence_v1",
        "trading_status": "trading_status_evidence_v1",
        "instruments": "instrument_identity_merge_v1",
        "financial_statement_items": "issuer_original_evidence_v1",
        "index_constituents": "dated_index_weight_evidence_v1",
        "daily_bars": "positive_turnover_quantity_v1",
    }.get(dataset, "observation_recency_v1")


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
    classes and why the derived history depends on them. ``corporate_actions``
    is the other: a vendor sweep must not displace a row an issuer notice or a
    reported payment date already settled (``domain/action_evidence``).
    """
    if dataset == "trading_status":
        return _trading_status_evidence_rank(schema)
    if dataset == "corporate_actions":
        return _action_evidence_level(set(schema.names()))
    if dataset == "financial_statement_items" and "source" in schema:
        return (
            pl.when(pl.col("source") == "cninfo_original")
            .then(2)
            .when(pl.col("source").fill_null("").str.contains("backfill|reconstruct|snapshot"))
            .then(0)
            .otherwise(1)
        )
    if dataset == "index_constituents" and "weight" in schema:
        # A later membership-only observation cannot erase a real weight for
        # this exact date. No weight is ever carried between snapshot dates.
        return pl.col("weight").is_not_null().cast(pl.Int8)
    if dataset == "daily_bars" and {"volume", "amount"} <= set(schema.names()):
        # A later whole-lot history refresh must not resurrect a rounded zero
        # quantity after an independently published share count repaired it.
        # Preserve unresolved rows when no better observation exists; zero
        # amount/no-print rows and missing amounts keep normal recency rules.
        return pl.when((pl.col("volume") == 0) & (pl.col("amount") > 0)).then(0).otherwise(1)
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


def _retain_payment_evidence(frame, dataset):
    """A date-less vendor refresh cannot erase independently sourced payment evidence.

    Only propagate within identical event keys AND economic amounts. Changed
    cash amounts require new evidence; no cross-event/date/source inference.
    A date before the ex-date is never carried to another row, and a row's own
    stored value is never rewritten here: reads return what is stored.
    """
    schema = frame.collect_schema() if isinstance(frame, pl.LazyFrame) else frame.schema
    columns = set(schema.names())
    evidence = ["payment_date", "payment_source"]
    keys = PRIMARY_KEYS.get(dataset, []) + ["cash_dividend", "bonus_ratio", "transfer_ratio"]
    if dataset != "corporate_actions" or not set(keys + evidence) <= columns:
        return frame
    stamp = (
        pl.when(_valid_payment())
        .then(pl.struct(evidence))
        .otherwise(None)
        .forward_fill()
        .over(keys)
    )
    own = pl.col("payment_date").is_not_null()
    return (
        frame.with_columns(stamp.alias("__payment_evidence"))
        .with_columns(
            [
                pl.when(own)
                .then(pl.col(c))
                .otherwise(pl.col("__payment_evidence").struct.field(c))
                .alias(c)
                for c in evidence
            ]
        )
        .drop("__payment_evidence")
    )


def _retain_pre_close(frame, dataset):
    """A K-line refresh of a session cannot erase the exchange's previous close.

    ``pre_close`` comes only from tip quotes; the history sweep that later
    reconciles the same session does not carry it. Within one primary key the
    latest published value is carried to a row that has none; a row's own
    value is never rewritten.
    """
    schema = frame.collect_schema() if isinstance(frame, pl.LazyFrame) else frame.schema
    keys = PRIMARY_KEYS.get(dataset, [])
    if "pre_close" not in schema.names() or not keys:
        return frame
    return frame.with_columns(
        pl.col("pre_close").fill_null(pl.col("pre_close").forward_fill().over(keys))
    )


#: Index publishers' own books: CSI files (stored as ``exchange``) and CNI.
OFFICIAL_INDEX_SOURCES = ("exchange", "cni", "cni_weights")


def one_book_per_index_date(frame, dataset: str):
    """Keep only the official book on an index-date that has one.

    ``index_constituents`` is keyed per member, so two sources for the same
    index and day would otherwise read back as the union of both books: on
    2026-10-26 EastMoney still listed three ST stocks the SSE Composite had
    dropped, and none of its 40 B shares. A vendor row stays only where no
    official row exists for that index and day.
    """
    if dataset != "index_constituents":
        return frame
    columns = frame.collect_schema().names() if isinstance(frame, pl.LazyFrame) else frame.columns
    if not {"source", "index_symbol", "as_of_date"} <= set(columns):
        return frame
    official = pl.col("source").is_in(OFFICIAL_INDEX_SOURCES)
    return frame.filter(official | ~official.any().over(["index_symbol", "as_of_date"]))


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
    df = _retain_pre_close(_retain_payment_evidence(df, dataset), dataset)
    out = df.unique(subset=primary_key, keep="last", maintain_order=True)
    return one_book_per_index_date(out.drop(*_HELPER_COLUMNS, strict=False), dataset)


def dedupe_lazy_by_primary_key(lf: pl.LazyFrame, dataset: str) -> pl.LazyFrame:
    """Lazy equivalent of :func:`dedupe_by_primary_key`."""
    primary_key = PRIMARY_KEYS.get(dataset, [])
    columns = set(lf.collect_schema().names())
    if not primary_key or any(k not in columns for k in primary_key):
        return lf
    if any(column in columns for column in ("fetched_at", "source", "data_version")):
        lf = _sort_for_canonical(lf, dataset)
    lf = _retain_pre_close(_retain_payment_evidence(lf, dataset), dataset)
    return one_book_per_index_date(
        lf.unique(subset=primary_key, keep="last", maintain_order=True).drop(
            *_HELPER_COLUMNS, strict=False
        ),
        dataset,
    )
