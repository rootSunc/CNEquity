"""Original-disclosure reconstructions, kept separate from observed vintages."""

import polars as pl

from cnequity.domain.pit import normalize_pit_storage_columns
from cnequity.domain.schemas import PRIMARY_KEYS

DATASET = "financial_statement_items"
PERIOD_KEYS = ["symbol", "report_period"]
FACT_KEYS = [*PERIOD_KEYS, "statement_type", "item_code"]


def original_disclosures(frame: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    """LICO summary items supply dates, never the publication time of a restatement."""
    return (
        frame.filter(
            (
                (pl.col("statement_type") == "indicator")
                | (
                    (pl.col("statement_type") == "income")
                    & pl.col("item_code").is_in(["revenue", "net_profit"])
                )
            )
            & pl.col("source").is_in(["eastmoney", "eastmoney_backfill"])
        )
        .group_by(PERIOD_KEYS)
        .agg(pl.col("announce_date").min().alias("original_date"))
    )


def reconstruct_disclosures(frame: pl.DataFrame, dates: pl.DataFrame) -> pl.DataFrame:
    """Fill absent original-date keys with explicitly inexact, first observed values.

    Never overwrite an existing original-date vintage, even from another source.
    Source publication and availability of the original *value* are unknown;
    observation time stays honest and strict PIT excludes these reconstructions.
    """
    if frame.is_empty() or dates.is_empty():
        return frame.clear()
    matched = frame.join(dates, on=PERIOD_KEYS)
    existing = (
        matched.filter(pl.col("announce_date") == pl.col("original_date"))
        .select(FACT_KEYS)
        .unique()
    )
    candidates = (
        matched.filter(
            pl.col("source").is_in(["eastmoney", "eastmoney_backfill"])
            & (pl.col("announce_date") > pl.col("original_date"))
        )
        .join(existing, on=FACT_KEYS, how="anti")
        .sort("fetched_at" if "fetched_at" in frame.columns else "announce_date")
        .unique(subset=FACT_KEYS, keep="first", maintain_order=True)
        .with_columns(
            pl.col("original_date").alias("announce_date"),
            pl.lit("eastmoney_backfill").alias("source"),
            pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("available_at"),
            pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("source_published_at"),
        )
        .drop("original_date", "revision_id", strict=False)
    )
    if "fetched_at" in candidates.columns:
        candidates = normalize_pit_storage_columns(candidates, DATASET)
    return candidates.unique(subset=PRIMARY_KEYS[DATASET], keep="first")


def _quarter_index(column: str) -> pl.Expr:
    return pl.col(column).str.slice(0, 4).cast(pl.Int32) * 4 + pl.col(column).str.slice(5, 1).cast(
        pl.Int32
    )


def reconstruct_comparatives(frame: pl.DataFrame, dates: pl.DataFrame) -> pl.DataFrame:
    """Original-date rows for items first seen only as a later filing's comparative.

    ``reconstruct_disclosures`` dates a period from EastMoney's results table
    (*dates*, from ``original_disclosures``), which has no row for some
    periods: 9,463 statement items across 893 symbol-periods (2013-2024) had no
    row on their own filing's date, only one a year later, when the next
    report republished them as prior-year figures. For a period *dates* does
    not cover, the earliest date anything in *frame* carries for it is the
    filing date; an item is reconstructed there only when its own earliest row
    is dated on the first disclosure of the same symbol's period one to four
    quarters later, which is what a comparative looks like. Labelled
    ``eastmoney_backfill``, so strict PIT still excludes it.
    """
    if frame.is_empty():
        return frame.clear()
    period_first = frame.group_by(PERIOD_KEYS).agg(pl.col("announce_date").min().alias("pdate"))
    undated = period_first.join(dates.select(PERIOD_KEYS), on=PERIOD_KEYS, how="anti")
    first_rows = (
        frame.filter(pl.col("source").is_in(["eastmoney", "eastmoney_backfill"]))
        .sort("announce_date", "fetched_at" if "fetched_at" in frame.columns else "announce_date")
        .unique(subset=FACT_KEYS, keep="first", maintain_order=True)
    )
    earliest = frame.group_by(FACT_KEYS).agg(pl.col("announce_date").min().alias("earliest"))
    later_periods = period_first.rename({"report_period": "later", "pdate": "earliest"})
    comparatives = (
        first_rows.join(earliest, on=FACT_KEYS)
        .filter(pl.col("announce_date") == pl.col("earliest"))
        .join(undated, on=PERIOD_KEYS)
        .filter(pl.col("earliest") > pl.col("pdate"))
        .join(later_periods, on=["symbol", "earliest"])
        .filter((_quarter_index("later") - _quarter_index("report_period")).is_between(1, 4))
        .unique(subset=FACT_KEYS, keep="first", maintain_order=True)
        .with_columns(
            pl.col("pdate").alias("announce_date"),
            pl.lit("eastmoney_backfill").alias("source"),
            pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("available_at"),
            pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("source_published_at"),
        )
        .drop("pdate", "earliest", "later", "revision_id", strict=False)
    )
    if "fetched_at" in comparatives.columns:
        comparatives = normalize_pit_storage_columns(comparatives, DATASET)
    return comparatives.unique(subset=PRIMARY_KEYS[DATASET], keep="first")


def unchanged_republications(frame: pl.DataFrame, held: pl.DataFrame) -> pl.DataFrame:
    """Rows of *frame* that only repeat, at a later date, the value *held* has.

    *held* carries the lake's rows for the same facts. A later filing repeats
    earlier periods as comparatives: of 117,381 such rows in the lake on
    2026-10-26, 188 changed the figure. The other 117,193 added a vintage that
    says nothing new and moves the fact's apparent date a year later.
    """
    if frame.is_empty() or held.is_empty():
        return frame.clear()
    latest = (
        held.sort("announce_date")
        .group_by(FACT_KEYS)
        .agg(
            pl.col("item_value").last().alias("held_value"),
            pl.col("announce_date").last().alias("held_date"),
        )
    )
    return (
        frame.join(latest, on=FACT_KEYS)
        .filter(
            (pl.col("announce_date") > pl.col("held_date"))
            & (
                (pl.col("item_value") - pl.col("held_value")).abs()
                <= 1e-6 * pl.col("held_value").abs() + 1e-9
            )
        )
        .drop("held_value", "held_date")
    )
