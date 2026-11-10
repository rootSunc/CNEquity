"""Replace push2 valuation rows stamped onto a session the snapshot never saw.

push2's clist snapshot carries no date: it describes the last closed session
at the instant it is read. The daily fallback once stamped it with whatever
``trade_date`` it was handed, so a catch-up run on 2026-07-26 wrote the same
07-24 snapshot onto each of 07-16..07-23 (5,776 rows a day).

A row is misdated when the session its ``fetched_at`` could describe
(:func:`~cnequity.domain.market_time.last_closed_session`) is not its
``trade_date``. ``--apply`` drops those rows and, in the same revision, puts
datacenter's dated report for the same keys in their place. A key datacenter
does not serve is left without a row rather than keeping the wrong one.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.market_time import last_closed_session
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.file_lock import lake_mutation_lock
from cnequity.query.parquet_scan import scan_parquet_root
from cnequity.storage.repairs.publish import publish_repair
from cnequity.storage.revisions import RevisionStore

DATASET = "valuation_metrics"
#: The undated push2 snapshot; datacenter rows carry their own TRADE_DATE.
SNAPSHOT_SOURCE = "eastmoney"

Fetch = Callable[[date, date], pl.DataFrame]


def repair_misdated_valuation(
    config: Config, *, apply: bool = False, fetch: Fetch | None = None
) -> dict:
    """Plan (default) or publish the replacement of misdated push2 rows."""
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _repair_locked(config, apply=apply, fetch=fetch)


def misdated_snapshot_keys(rows: pl.DataFrame) -> pl.DataFrame:
    """``symbol / trade_date / fetched_at`` of push2 rows dated to another session."""
    snapshot = rows.filter(pl.col("source") == SNAPSHOT_SOURCE)
    if snapshot.is_empty():
        return snapshot.select("symbol", "trade_date", "fetched_at")
    sessions = pl.DataFrame(
        [
            {"fetched_at": instant, "session": last_closed_session(instant)}
            for instant in snapshot.get_column("fetched_at").unique().drop_nulls().to_list()
        ],
        schema={"fetched_at": snapshot.schema["fetched_at"], "session": pl.Date},
    )
    return (
        snapshot.join(sessions, on="fetched_at", how="left")
        .filter(pl.col("session").is_null() | (pl.col("session") != pl.col("trade_date")))
        .select("symbol", "trade_date", "fetched_at")
    )


def _datacenter(config: Config) -> Fetch:
    from cnequity.adapters.eastmoney.valuation_datacenter import fetch_valuation_datacenter

    return lambda start, end: fetch_valuation_datacenter(start, end, config=config)


def _repair_locked(config: Config, *, apply: bool, fetch: Fetch | None) -> dict:
    from cnequity.adapters.eastmoney.valuation_datacenter import SOURCE as DATACENTER

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    base = store.repair_base(DATASET, register=apply)
    report: dict = {"applied": False, "rows": 0}
    if base is None:
        report["note"] = f"no {DATASET} data; nothing to repair"
        return report
    bad = misdated_snapshot_keys(
        scan_parquet_root(base, partition_col="trade_date")
        .filter(pl.col("source") == SNAPSHOT_SOURCE)
        .select("symbol", "trade_date", "source", "fetched_at")
        .collect()
    )
    report["rows"] = bad.height
    report["by_date"] = {
        day.isoformat(): count
        for day, count in bad.group_by("trade_date").len().sort("trade_date").iter_rows()
    }
    if not apply or bad.is_empty():
        return report

    days = sorted(set(bad.get_column("trade_date").to_list()))
    fetched = (fetch or _datacenter(config))(days[0], days[-1])
    keys = bad.select("symbol", "trade_date").unique()
    replacement = (
        with_provenance(
            fetched.join(keys, on=["symbol", "trade_date"], how="semi"),
            source=DATACENTER,
            data_version=data_version_for(DATASET),
        )
        if not fetched.is_empty()
        else pl.DataFrame()
    )
    report["rows_replaced"] = replacement.height
    report["rows_without_replacement"] = bad.height - replacement.height
    marked = bad.with_columns(pl.lit(True).alias("_misdated"))

    def rebuild(frame: pl.DataFrame) -> pl.DataFrame:
        flagged = frame.join(marked, on=["symbol", "trade_date", "fetched_at"], how="left")
        hit = (pl.col("source") == SNAPSHOT_SOURCE) & pl.col("_misdated").fill_null(False)
        if flagged.filter(hit).is_empty():
            return frame
        kept = flagged.filter(~hit).drop("_misdated")
        if replacement.is_empty():
            return kept
        added = replacement.filter(
            pl.col("trade_date").is_in(frame.get_column("trade_date").unique().to_list())
        )
        columns = [column for column in frame.columns if column in added.columns]
        return pl.concat([kept, added.select(columns)], how="diagonal_relaxed").select(
            frame.columns
        )

    report.update(
        publish_repair(
            config,
            store,
            DATASET,
            "trade_date",
            base,
            keep=rebuild,
            reason="misdated_valuation_snapshot",
            metadata={
                "rows": bad.height,
                "rows_replaced": replacement.height,
                "dates": [day.isoformat() for day in days],
            },
        )
    )
    report["applied"] = True
    return report
