"""Regulatory events, read out of the announcements the lake already holds.

This dataset was fetched from CNINFO with the *same request* as
``announcement_index`` — same endpoint, same ``seDate``, no server-side
filter — and then narrowed by matching keywords against the announcement
title. Every day was therefore paid for twice: measured 2026-01-01, 46 pages
and 1,375 announcements to keep 6 events, and a dense disclosure day is ~220
pages. On a source paced at one request per second that is the difference
between minutes and tens of minutes, for bytes the lake already had.

Worse, the two fetches ran an hour apart (`announcement_index` at 17:00,
`regulatory_events` at 17:55), so they could disagree about the same day: an
announcement published in between existed in one dataset and not the other.

So the events are now derived from committed ``announcement_index`` rows.
There is one request for the day, one archived capture of it, and the two
datasets cannot disagree because one is a projection of the other.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_lazy_by_primary_key
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

logger = logging.getLogger(__name__)

#: Title keywords that make an announcement a regulatory event, and the type
#: each one implies. Order matters: the first match wins, so the specific
#: filings come before the general ones.
KEYWORD_TYPES: tuple[tuple[str, str], ...] = (
    ("行政处罚", "penalty"),
    ("处罚决定", "penalty"),
    ("立案", "investigation"),
    ("调查", "investigation"),
    ("监管函", "regulatory_letter"),
    ("警示函", "warning_letter"),
    ("处分", "disciplinary"),
)

_DEFAULT_EVENT_TYPE = "regulatory"

_EVENT_SCHEMA: dict[str, pl.DataType] = {
    "event_id": pl.Utf8,
    "symbol": pl.Utf8,
    "event_date": pl.Date,
    "event_type": pl.Utf8,
    "title": pl.Utf8,
}


@dataclass(frozen=True)
class RegulatoryDerivation:
    """The events found, and how many announcements were available to search.

    The second number is what separates "no regulatory filings that week" from
    "the announcements for that week were never indexed" — an empty result is
    only meaningful next to the size of what it was derived from.
    """

    events: pl.DataFrame
    announcements: int


def _event_type_expr() -> pl.Expr:
    """First-match-wins classification, in one pass over the titles."""
    expr = pl.lit(_DEFAULT_EVENT_TYPE)
    for keyword, event_type in reversed(KEYWORD_TYPES):
        expr = (
            pl.when(pl.col("title").str.contains(keyword, literal=True))
            .then(pl.lit(event_type))
            .otherwise(expr)
        )
    return expr


def regulatory_events_from_announcements(announcements: pl.DataFrame) -> pl.DataFrame:
    """Project the regulatory subset of ``announcement_index`` rows.

    Provenance stays with the announcement: ``event_id`` is derived from the
    announcement id, so an event and the filing it came from remain joinable,
    and re-deriving a day cannot invent a new identity for the same filing.
    """
    required = {"announcement_id", "symbol", "title", "announce_date"}
    if announcements.is_empty() or not required.issubset(announcements.columns):
        return pl.DataFrame(schema=_EVENT_SCHEMA)
    keywords = [keyword for keyword, _ in KEYWORD_TYPES]
    events = (
        announcements.filter(
            pl.col("title").is_not_null()
            & pl.col("announcement_id").is_not_null()
            & pl.col("symbol").is_not_null()
            & pl.col("title").str.contains_any(keywords)
        )
        .select(
            (pl.lit("reg-") + pl.col("announcement_id")).alias("event_id"),
            pl.col("symbol"),
            pl.col("announce_date").alias("event_date"),
            _event_type_expr().alias("event_type"),
            pl.col("title"),
        )
        # keep="last" is only defined against an order, and re-deriving a
        # day has to produce the same rows every time.
        .unique(subset=["event_id"], keep="last", maintain_order=True)
    )
    return events


def derive_regulatory_events(
    config: Config,
    *,
    start: date,
    end: date,
) -> RegulatoryDerivation:
    """Regulatory events over ``[start, end]``, from committed announcements."""
    root = config.curated_root / "announcement_index"
    if not dataset_has_parquet(root, dataset="announcement_index", meta_root=config.meta_root):
        return RegulatoryDerivation(pl.DataFrame(schema=_EVENT_SCHEMA), 0)
    frame = dedupe_lazy_by_primary_key(
        scan_parquet_root(
            root,
            partition_col="announce_date",
            start=start,
            end=end,
            dataset="announcement_index",
            meta_root=config.meta_root,
        ),
        "announcement_index",
    ).collect()
    return RegulatoryDerivation(regulatory_events_from_announcements(frame), frame.height)
