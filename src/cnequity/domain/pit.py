"""Point-in-time (PIT) and bitemporal row semantics.

The lake predates bitemporal columns.  This module therefore keeps the new
columns optional on disk and provides one read-side normalisation path for old
Parquet files.  A missing ``available_at`` is *unknown*; it is never silently
treated as proof that a reconstructed value was available on its
``announce_date``.

``fetched_at`` is the legacy observation timestamp.  ``observed_at`` is its
stable name in the bitemporal contract and is filled from it when an old file
does not carry the new column.  The two source-side timestamps are nullable
because the current EastMoney backfill does not expose the publication time of
the particular vintage it returns.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date, datetime
from typing import Literal

import polars as pl

from cnequity.domain.frames import is_blank

PitMode = Literal["strict", "best_effort"]
PitQuality = Literal["strict", "reconstructed", "snapshot_only", "not_applicable"]
PIT_MODES: tuple[PitMode, ...] = ("strict", "best_effort")
# ``not_applicable`` exists because the other three had to cover a case none of
# them describes: a by-date dataset that makes no point-in-time claim at all.
# That used to be spelled ``strict``, to keep the exported contract total, and
# it read as the strongest possible claim on 29 of the 30 datasets carrying it
# -- ``daily_bars`` included. A consumer reading the contract had no way to
# tell "this source is exact as of that date" from "nobody has thought about
# it for this table". See ADR-0011.
PIT_QUALITIES: tuple[PitQuality, ...] = (
    "strict",
    "reconstructed",
    "snapshot_only",
    "not_applicable",
)

# These are intentionally optional storage columns.  They are not added to
# DATASET_SCHEMAS' required shape until every writer can provide them.  Readers
# and migration tooling use this contract for PIT datasets today.
PIT_STORAGE_COLUMNS: tuple[str, ...] = (
    "available_at",
    "source_published_at",
    "observed_at",
    "revision_id",
)
PIT_STORAGE_DTYPES: dict[str, pl.DataType] = {
    "available_at": pl.Datetime("us", "UTC"),
    "source_published_at": pl.Datetime("us", "UTC"),
    "observed_at": pl.Datetime("us", "UTC"),
    "revision_id": pl.Utf8,
}

# A truncated SHA-256.  This digest identifies a vintage; it is not a security
# boundary, and it is stored on every row of every PIT dataset, so its width is
# a storage decision.  At 96 bits the chance of any collision across the
# current 12.2M PIT rows is ~1e-15 and ~1e-13 at a hundred times that, while
# the full 64-character form costs 389 MB against 73 MB here — on datasets
# totalling 190 MB, and copied whole into each committed generation.
REVISION_ID_HEX_CHARS = 24


def _pit_dataset_names() -> frozenset[str]:
    """``domain.datasets.PIT_DATASET_NAMES``, imported late: the registry imports this module."""
    from cnequity.domain.datasets import PIT_DATASET_NAMES

    return PIT_DATASET_NAMES


def __getattr__(name: str):
    # The set used to be declared here as well as by ``pit=True`` on each spec.
    # It is derived from the specs now; the old import path still answers.
    if name == "PIT_DATASET_NAMES":
        return _pit_dataset_names()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def revision_id_for_row(row: Mapping[str, object]) -> str:
    """Return a stable identity for one observed fact/vintage.

    ``fetched_at`` and the bitemporal observation columns are deliberately
    excluded: re-fetching the same source fact must not manufacture a new
    revision.  The value and provenance are included because a source can
    publish two payloads with the same business key and disclosure date.
    """

    semantic = {
        str(key): value
        for key, value in row.items()
        if key
        not in {
            "fetched_at",
            "observed_at",
            "available_at",
            "source_published_at",
            "revision_id",
        }
    }
    payload = json.dumps(
        semantic,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:REVISION_ID_HEX_CHARS]


def _as_datetime_expr(df: pl.DataFrame, column: str) -> pl.Expr:
    """Cast one legacy/new timestamp column to nullable UTC datetimes."""

    dtype = df.schema[column]
    target = PIT_STORAGE_DTYPES.get(column, pl.Datetime("us", "UTC"))
    expr = pl.col(column)
    if dtype == pl.Date:
        return expr.cast(target, strict=False)
    if isinstance(dtype, pl.Datetime):
        # A timezone-less legacy timestamp is interpreted as UTC.  There is no
        # honest source timezone in an old parquet file, and all lake fetches
        # have historically been stamped in UTC.
        if dtype.time_zone is None:
            return expr.cast(target, strict=False)
        return expr.dt.convert_time_zone("UTC").cast(target)
    if dtype == pl.String or dtype == pl.Utf8:
        return expr.str.to_datetime(time_unit="us", time_zone="UTC", strict=False)
    return expr.cast(target, strict=False)


def normalize_pit_storage_columns(
    df: pl.DataFrame,
    dataset: str,
    *,
    derive_revision: bool = True,
) -> pl.DataFrame:
    """Return *df* with the optional PIT storage columns present and typed.

    Existing files need not contain any of the four columns.  Missing source
    timestamps remain null (unknown), while ``observed_at`` safely aliases the
    legacy ``fetched_at`` observation.  ``revision_id`` is deterministic and
    therefore safe to backfill on every read; it is not itself evidence of an
    exact vintage.
    """
    if is_blank(df):
        # A column-less frame would gain four literal columns and, with them,
        # a row that never existed. See domain/frames.py.
        return df

    if dataset not in _pit_dataset_names():
        return df

    out = df
    for column in PIT_STORAGE_COLUMNS:
        if column in out.columns:
            if column == "revision_id":
                out = out.with_columns(pl.col(column).cast(pl.Utf8, strict=False).alias(column))
            else:
                out = out.with_columns(_as_datetime_expr(out, column).alias(column))
        elif column == "observed_at" and "fetched_at" in out.columns:
            out = out.with_columns(_as_datetime_expr(out, "fetched_at").alias("observed_at"))
        else:
            out = out.with_columns(pl.lit(None, dtype=PIT_STORAGE_DTYPES[column]).alias(column))

    if derive_revision and "revision_id" in out.columns:
        # Hashing is per-row by definition, but deciding *whether* any row
        # needs it is not. The unconditional `iter_rows` below cost 3.5 us a
        # row whatever the column held: 19 s to read
        # `financial_statement_items` and 17 s for `top_holders`, spent
        # recomputing digests that were already on disk. Check first, and only
        # walk the frame when something is actually missing.
        stripped = pl.col("revision_id").cast(pl.Utf8, strict=False).str.strip_chars()
        missing = out.select((stripped.is_null() | (stripped == "")).alias("__m"))["__m"]
        if missing.any():
            values: list[str] = []
            for row, needs in zip(out.iter_rows(named=True), missing, strict=True):
                if needs:
                    values.append(revision_id_for_row(row))
                else:
                    values.append(str(row["revision_id"]).strip())
            out = out.with_columns(pl.Series("revision_id", values, dtype=pl.Utf8))
        else:
            out = out.with_columns(stripped.alias("revision_id"))
    return out


def retain_pit_vintages(df: pl.DataFrame, dataset: str) -> pl.DataFrame:
    """Retain every fact revision and its earliest complete observation.

    Re-fetching an identical payload is not a new vintage. Keep observation
    timestamps from one actual row, never a synthetic minimum of evidence
    obtained at different times.
    """
    from cnequity.domain.schemas import PRIMARY_KEYS

    out = normalize_pit_storage_columns(df, dataset)
    if dataset not in _pit_dataset_names() or out.is_empty():
        return out
    keys = [*PRIMARY_KEYS[dataset], "revision_id"]
    order = [name for name in ("observed_at", "fetched_at") if name in out.columns]
    return out.sort(order, nulls_last=True).unique(subset=keys, keep="first", maintain_order=True)


def _date_expr(df: pl.DataFrame, column: str) -> pl.Expr:
    dtype = df.schema[column]
    expr = pl.col(column)
    if dtype == pl.Date:
        return expr
    if isinstance(dtype, pl.Datetime):
        # ``as_of`` is a Shanghai business date, so an instant must be dated in
        # Shanghai too. Dating it in UTC let a row available from T+1 00:00
        # Shanghai (T 16:00 UTC) pass an ``as_of=T`` cutoff. A timezone-less
        # timestamp is UTC, as in ``_as_datetime_expr``.
        if dtype.time_zone is None:
            expr = expr.dt.replace_time_zone("UTC")
        return expr.dt.convert_time_zone("Asia/Shanghai").dt.date()
    if dtype == pl.String or dtype == pl.Utf8:
        return expr.str.to_date(strict=False)
    return expr.cast(pl.Date, strict=False)


def classify_pit_rows(
    df: pl.DataFrame,
    dataset: str,
    *,
    as_of: date,
    pit_mode: PitMode,
) -> pl.DataFrame:
    """Filter and annotate rows according to the bitemporal PIT contract.

    ``announce_date``, when present, is always a visibility lower bound.  A
    known ``available_at`` or ``source_published_at`` after ``as_of`` is also
    excluded in both modes.  Strict mode additionally requires the lake
    observation (``fetched_at``/``observed_at``) to be no later than ``as_of``
    and rejects rows from a backfill/reconstruction source.  Best-effort keeps
    such rows for exploratory use and marks them ``pit_is_exact=False``.
    """

    if pit_mode not in ("strict", "best_effort"):
        raise ValueError(f"unsupported pit_mode {pit_mode!r}")
    if df.is_empty():
        return normalize_pit_storage_columns(df, dataset).with_columns(
            pl.lit(True if pit_mode == "strict" else False).alias("pit_is_exact"),
            pl.lit("strict" if pit_mode == "strict" else "reconstructed").alias("pit_quality"),
        )

    out = normalize_pit_storage_columns(df, dataset)
    if "announce_date" not in out.columns:
        raise ValueError(f"{dataset} requires announce_date column (PIT contract)")

    out = out.with_columns(
        _date_expr(out, "announce_date").alias("__pit_announce_date"),
        _date_expr(out, "available_at").alias("__pit_available_date"),
        _date_expr(out, "source_published_at").alias("__pit_source_published_date"),
        _date_expr(out, "observed_at").alias("__pit_observed_date"),
        _date_expr(out, "fetched_at").alias("__pit_fetched_date")
        if "fetched_at" in out.columns
        else pl.lit(None, dtype=pl.Date).alias("__pit_fetched_date"),
    )
    if "source" in out.columns:
        out = out.with_columns(
            pl.col("source")
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.to_lowercase()
            .str.contains("backfill|reconstruct|snapshot")
            .alias("__pit_is_reconstructed_source")
        )
    else:
        out = out.with_columns(pl.lit(False).alias("__pit_is_reconstructed_source"))

    cutoff = pl.lit(as_of)
    visible = (
        pl.col("__pit_announce_date").is_not_null()
        & (pl.col("__pit_announce_date") <= cutoff)
        & (pl.col("__pit_available_date").is_null() | (pl.col("__pit_available_date") <= cutoff))
        & (
            pl.col("__pit_source_published_date").is_null()
            | (pl.col("__pit_source_published_date") <= cutoff)
        )
    )
    observed = pl.max_horizontal(pl.col("__pit_observed_date"), pl.col("__pit_fetched_date"))
    exact = (
        visible
        & ~pl.col("__pit_is_reconstructed_source")
        & observed.is_not_null()
        & (observed <= cutoff)
    )
    out = out.filter(visible if pit_mode == "best_effort" else exact).with_columns(
        exact.alias("pit_is_exact")
        if pit_mode == "best_effort"
        else pl.lit(True).alias("pit_is_exact"),
        pl.when(exact)
        .then(pl.lit("strict"))
        .otherwise(pl.lit("reconstructed"))
        .alias("pit_quality"),
    )
    return out.drop(
        [
            "__pit_announce_date",
            "__pit_available_date",
            "__pit_source_published_date",
            "__pit_observed_date",
            "__pit_fetched_date",
            "__pit_is_reconstructed_source",
        ],
        strict=False,
    )


__all__ = [
    "PIT_MODES",
    "PIT_QUALITIES",
    "PIT_STORAGE_COLUMNS",
    "PIT_STORAGE_DTYPES",
    "PitMode",
    "PitQuality",
    "classify_pit_rows",
    "normalize_pit_storage_columns",
    "revision_id_for_row",
]
