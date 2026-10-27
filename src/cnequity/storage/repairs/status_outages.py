"""Rebuild ``trading_status`` rows an EastMoney outage wrote under the old fallback.

The fallback used to reuse the latest curated snapshot — its own carried rows
included — inside a five-calendar-day limit, and to mark every live symbol it
had no row for ``suspended``. One partial day therefore propagated, a long
holiday expired the cache outright, and after National Day 2026 it published
4,863 trading names as halted.

This replays the current rule on the stored history: every ``eastmoney_cached``
row in the window is dropped, and on an exchange the primary feed did not
answer that session (see :data:`OUTAGE_SHARE`), a symbol with no other row
gets its latest first-hand status from the previous
:data:`~cnequity.domain.trading_status.CARRY_SESSIONS` sessions, if it has one.
A carried row keeps the ``fetched_at`` of the reading it repeats, so it ranks
as a restatement and any session record — Baostock history, a bar-gap
suspension — still wins over it. A symbol with no such reading gets no row.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.domain.schemas import data_version_for
from cnequity.domain.trading_status import (
    CACHED_SOURCES,
    CARRY_SESSIONS,
    cached_source,
    last_first_hand_status,
    split_before_listing,
)
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import scan_parquet_root
from cnequity.storage.repairs.orphan_symbols import _publish
from cnequity.storage.repairs.status_listing_window import listing_spans
from cnequity.storage.revisions import RevisionStore

_SEARCH_SPAN = timedelta(days=30)
# The feed whose outages this replays. EastMoney was the daily primary when
# those days were written; the exchange boards have been since 2026-10-28.
PRIMARY_SOURCE = "eastmoney"
#: An exchange counts as down for a session when the primary feed answered
#: fewer than this share of the symbols it answered there in the sessions
#: before. A board that drops a few names on a healthy day is no outage, and
#: carrying them would invent rows the feed deliberately stopped publishing.
OUTAGE_SHARE = 0.5


def repair_status_outages(config: Config, *, start: date, end: date, apply: bool = False) -> dict:
    """Plan (default) or publish the replayed fallback for ``[start, end]``."""
    if end < start:
        raise ValueError("end must not be before start")
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _repair_locked(config, start=start, end=end, apply=apply)


def _delist_dates(store: RevisionStore, config: Config) -> dict[str, date]:
    root = store.current_root("instruments") or config.curated_root / "instruments"
    files = sorted(Path(root).rglob("*.parquet")) if Path(root).is_dir() else []
    if not files:
        return {}
    frame = pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")
    if "delist_date" not in frame.columns:
        return {}
    rows = frame.select("symbol", pl.col("delist_date").cast(pl.Date, strict=False))
    return {symbol: day for symbol, day in rows.drop_nulls().unique("symbol").iter_rows()}


def plan_status_outages(
    history: pl.DataFrame,
    sessions: list[date],
    start: date,
    end: date,
    delisted: dict[str, date] | None = None,
    listings: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """``(dropped keys, carried rows)`` for the sessions in ``[start, end]``.

    ``history`` is the stored rows (all sources, undeduplicated) from at least
    ``CARRY_SESSIONS`` sessions before ``start``; ``sessions`` the exchange
    calendar over the same span. ``listings`` (``symbol / list_date /
    first_traded``) keeps a carry off the sessions before a code listed.
    """
    delisted = delisted or {}
    dropped = history.filter(
        pl.col("source").is_in(list(CACHED_SOURCES)) & pl.col("trade_date").is_between(start, end)
    ).select("symbol", "trade_date")
    exchange = pl.col("symbol").str.split(".").list.last()
    answered_by_feed = {
        (day, market): count
        for day, market, count in history.filter(pl.col("source") == PRIMARY_SOURCE)
        .group_by(pl.col("trade_date"), exchange.alias("exchange"))
        .agg(pl.col("symbol").n_unique())
        .iter_rows()
    }
    markets = sorted({market for _, market in answered_by_feed})
    carried: list[pl.DataFrame] = []
    for index, day in enumerate(sessions):
        if not start <= day <= end:
            continue
        prior = sessions[max(0, index - CARRY_SESSIONS) : index]
        if not prior:
            continue
        down = [
            market
            for market in markets
            if answered_by_feed.get((day, market), 0)
            < OUTAGE_SHARE * max(answered_by_feed.get((p, market), 0) for p in prior)
        ]
        if not down:
            continue
        answered = (
            history.filter(
                (pl.col("trade_date") == day) & ~pl.col("source").is_in(list(CACHED_SOURCES))
            )
            .get_column("symbol")
            .unique()
            .to_list()
        )
        evidence = last_first_hand_status(
            history.filter(pl.col("trade_date").is_between(prior[0], prior[-1])),
            with_source=True,
        ).filter(~pl.col("symbol").is_in(answered) & exchange.is_in(down))
        gone = [symbol for symbol, when in delisted.items() if when <= day]
        if gone:
            evidence = evidence.filter(~pl.col("symbol").is_in(gone))
        if evidence.is_empty():
            continue
        carried.append(
            evidence.drop("evidence_date").with_columns(
                pl.lit(day).alias("trade_date"),
                pl.col("source").map_elements(cached_source, return_dtype=pl.Utf8),
            )
        )
    rows = pl.concat(carried, how="vertical") if carried else pl.DataFrame()
    if listings is not None and not rows.is_empty():
        rows, _ = split_before_listing(rows, listings)
    return dropped, rows


def _repair_locked(config: Config, *, start: date, end: date, apply: bool) -> dict:
    from cnequity.query.calendar import list_trading_dates

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    base = store.repair_base("trading_status", register=apply)
    report: dict = {"applied": False, "start": start.isoformat(), "end": end.isoformat()}
    if base is None:
        report["note"] = "no trading_status data; nothing to repair"
        return report
    sessions = list_trading_dates(config, start - _SEARCH_SPAN, end)
    if not sessions:
        report["note"] = "no exchange sessions in the window"
        return report
    history = (
        scan_parquet_root(base, partition_col="trade_date", start=sessions[0], end=end)
        .filter(pl.col("trade_date").is_between(sessions[0], end))
        .collect()
    )
    dropped, carried = plan_status_outages(
        history,
        sessions,
        start,
        end,
        _delist_dates(store, config),
        listing_spans(store, config, listed_after=start),
    )
    report["by_date"] = _summary(dropped, carried)
    report["rows_dropped"] = dropped.height
    report["rows_carried"] = carried.height
    if not apply or (dropped.is_empty() and carried.is_empty()):
        return report
    template = history.head(0)
    if not carried.is_empty():
        carried = (
            pl.concat([template, carried], how="diagonal_relaxed")
            .select(template.columns)
            .with_columns(
                pl.col("data_version").fill_null(pl.lit(data_version_for("trading_status")))
            )
        )

    def rebuild(frame: pl.DataFrame) -> pl.DataFrame:
        days = frame.get_column("trade_date").unique().to_list()
        stale = pl.col("source").is_in(list(CACHED_SOURCES)) & pl.col("trade_date").is_between(
            start, end
        )
        added = carried.filter(pl.col("trade_date").is_in(days)) if carried.height else carried
        if added.is_empty() and frame.filter(stale).is_empty():
            return frame
        return pl.concat(
            [frame.filter(~stale), added.select(frame.columns)], how="diagonal_relaxed"
        )

    report.update(
        _publish(
            config,
            store,
            "trading_status",
            "trade_date",
            base,
            keep=rebuild,
            reason="status_outage_replay",
            metadata={
                "start": start.isoformat(),
                "end": end.isoformat(),
                "rows_dropped": dropped.height,
                "rows_carried": carried.height,
            },
        )
    )
    report["applied"] = True
    return report


def _summary(dropped: pl.DataFrame, carried: pl.DataFrame) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for label, frame in (("dropped", dropped), ("carried", carried)):
        if frame.is_empty():
            continue
        for day, count in frame.group_by("trade_date").len().sort("trade_date").iter_rows():
            out.setdefault(day.isoformat(), {"dropped": 0, "carried": 0})[label] = count
    return dict(sorted(out.items()))
