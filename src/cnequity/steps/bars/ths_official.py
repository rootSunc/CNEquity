"""THS official API: deep-history repair and daily-bar snapshots."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.steps.bars import index_history


def repair_deep_history_ths_official(
    config: Config,
    run_id: str,
    *,
    start: date,
    end: date,
    symbols: list[str] | None = None,
    chunk_size: int = 100,
    workers: int = 4,
    dry_run: bool = True,
    diff_out: Path | None = None,
    adjudicator: pl.DataFrame | None = None,
) -> dict:
    """Re-source the 2005-2015 block from the licensed peer instead of the scraper.

    This is **switching**, not routing (see docs/zh/reference/cli.md for explicit source repair):
    4,403,582 rows already have a canonical owner, so nothing here happens on a
    schedule and ``dry_run`` defaults to true. A caller has to ask twice.

    The case for asking is provenance rather than accuracy. ``daily_bars`` splits
    cleanly: ``ths`` — an unauthenticated scrape of 10jqka's public pages, which
    ``sources/SOURCES.yml`` records as an unregistered client — owns
    2001-01-02..2015-12-31 alone, while ``tdx_protocol`` owns 2016 onward with a
    configured backup. Re-sourcing the part the official API reaches puts 82.3%
    of that block on a registered footing. The 949,815 rows before 2005 are
    outside the service's floor and keep their existing source.

    Accuracy barely moves either way. Measured 2026-09-12 over 100 securities and
    177,914 comparable rows, the two disagree on 117 (0.066%), clustered on a
    handful of dates — 23 of them on 2015-05-08 alone — and usually by a single
    tick. Neither side can be shown right from inside the lake: both closes sit
    within their own high/low range. What settles the choice is that both series
    are 同花顺's, and only one of them is the licensed reading.

    ``dry_run`` reports the diff without writing, so the size and shape of the
    change is known before it is made. ``diff_out`` writes the disputed rows
    themselves, which is what lets an independent third source settle whether
    the change is an improvement rather than just a different opinion.

    ``adjudicator`` is that third source: ``(symbol, trade_date, close)`` from a
    vendor sharing no lineage with either candidate — baostock, say. Both
    candidates here are 同花顺's, one scraped from its public pages and one from
    its licensed API, so they cannot arbitrate each other.

    With an adjudicator, a **disputed** row is only switched when the third
    source backs the peer. Measured 2026-09-12 over 551 adjudicated disputes the
    peer was right 373 times and the incumbent 178, with no dispute where all
    three differed — so each has a right answer, and taking the peer blindly
    would import 178 known regressions. A dispute the adjudicator has no opinion
    on keeps its existing value and source: 1,418 rows against 4,396,510 is a
    rounding error for the provenance this is for, and a guaranteed
    non-regression is worth more than those rows.

    Undisputed rows switch regardless — no value to protect, only provenance to
    improve.
    """
    from cnequity.adapters.ths_official import SOURCE as THS_SOURCE
    from cnequity.adapters.ths_official import client_from_config
    from cnequity.adapters.ths_official.bars import HISTORY_FLOOR, fetch_daily_bars
    from cnequity.domain.canonical import dedupe_lazy_by_primary_key
    from cnequity.query.parquet_scan import scan_parquet_root
    from cnequity.steps.http_common import write_fetched

    if start < HISTORY_FLOOR:
        start = HISTORY_FLOOR
    client = client_from_config(config)
    if client is None:
        return {"status": "skipped", "reason": "no api key", "rows_written": 0}

    root = config.curated_root / "daily_bars"
    existing = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col="trade_date", start=start, end=end),
            "daily_bars",
        )
        .filter(pl.col("source") == "ths")
        .select("symbol", "trade_date", "open", "high", "low", "close", "volume", "amount")
        .collect()
    )
    if existing.is_empty():
        client.close()
        return {
            "status": "skipped",
            "reason": "no scraper-sourced rows in window",
            "rows_written": 0,
        }
    if symbols is None:
        symbols = sorted(existing.get_column("symbol").unique().to_list())

    totals = {
        "rows_written": 0,
        "compared": 0,
        "changed": 0,
        "only_curated": 0,
        "only_peer": 0,
        "unanswered": 0,
    }
    counters: dict[str, int] = {}
    unanswered_symbols: set[str] = set()
    try:
        for offset in range(0, len(symbols), chunk_size):
            chunk = symbols[offset : offset + chunk_size]
            frame, chunk_counters = fetch_daily_bars(
                chunk, start, end, client=client, workers=workers
            )
            unanswered = set(chunk_counters.pop("unanswered_symbols", ()) or ())
            for key, value in chunk_counters.items():
                counters[key] = counters.get(key, 0) + value
            if unanswered:
                totals["unanswered"] += len(unanswered)
                unanswered_symbols.update(unanswered)
            if frame.is_empty() and not unanswered:
                continue
            # A symbol the vendor never answered for looks exactly like one it
            # answered "nothing" for once the frame is built, so its curated
            # rows would be counted as evidence the peer lacks them. They are
            # evidence of a failed request and nothing else.
            before = existing.filter(
                pl.col("symbol").is_in(chunk) & ~pl.col("symbol").is_in(list(unanswered))
            )
            if before.is_empty() and frame.is_empty():
                continue
            joined = before.join(frame, on=["symbol", "trade_date"], how="inner", suffix="_peer")
            changed = joined.filter(
                (pl.col("close_peer") - pl.col("close")).abs() > pl.col("close").abs() * 5e-5
            ).height
            totals["compared"] += joined.height
            totals["changed"] += changed
            totals["only_curated"] += before.join(
                frame, on=["symbol", "trade_date"], how="anti"
            ).height
            peer_only = frame.join(before, on=["symbol", "trade_date"], how="anti")
            totals["only_peer"] += peer_only.height
            if diff_out is not None:
                disputed = joined.filter(
                    (pl.col("close_peer") - pl.col("close")).abs() > pl.col("close").abs() * 5e-5
                ).select(
                    "symbol",
                    "trade_date",
                    pl.col("close").alias("curated_close"),
                    pl.col("close_peer").alias("peer_close"),
                    pl.lit("changed").alias("kind"),
                )
                added = peer_only.select(
                    "symbol",
                    "trade_date",
                    pl.lit(None, dtype=pl.Float64).alias("curated_close"),
                    pl.col("close").alias("peer_close"),
                    pl.lit("peer_only").alias("kind"),
                )
                missing = before.join(frame, on=["symbol", "trade_date"], how="anti").select(
                    "symbol",
                    "trade_date",
                    pl.col("close").alias("curated_close"),
                    pl.lit(None, dtype=pl.Float64).alias("peer_close"),
                    pl.lit("curated_only").alias("kind"),
                )
                index_history._append_diff(
                    diff_out, pl.concat([disputed, added, missing], how="vertical")
                )
            if dry_run:
                continue
            staged = frame
            if adjudicator is not None:
                staged = index_history._withhold_unbacked_disputes(
                    frame, joined, adjudicator, totals
                )
            if staged.is_empty():
                continue
            written = write_fetched(
                config,
                run_id,
                "daily_bars",
                staged,
                source=THS_SOURCE,
                batch_id=f"ths-deep-{offset // chunk_size:04d}",
            )
            totals["rows_written"] += written.get("rows_written", frame.height)
    finally:
        client.close()

    totals.update(counters)
    totals["symbols"] = len(symbols)
    # Named so a reader can re-run exactly the scope that went unanswered
    # rather than the whole window.
    totals["unanswered_symbols"] = sorted(unanswered_symbols)
    totals["status"] = "dry_run" if dry_run else "applied"
    return totals


def snapshot_daily_bars_ths_official(
    config: Config,
    run_id: str,
    *,
    start: date,
    end: date,
    symbols: list[str] | None = None,
    sample: int = 400,
    workers: int = 4,
) -> dict:
    """Capture a third opinion on recent bars, for arbitration only.

    2016 onward is already tdx against eastmoney. Two vendors can disagree but
    cannot say which is wrong, and the revision gate has to decide something —
    so a binary comparison either blocks on noise or waves through a real break.

    Verification class: this writes to ``meta/source_snapshots`` and never to
    curated, so it is safe whenever a key is present and changes nothing about
    what the lake holds. Sampled rather than exhaustive, because the point is to
    arbitrate the days the two incumbents already disagree on, not to mirror the
    market.
    """
    from cnequity.adapters.ths_official import SOURCE as THS_SOURCE
    from cnequity.adapters.ths_official import client_from_config
    from cnequity.adapters.ths_official.bars import fetch_daily_bars
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.storage.source_snapshots import SnapshotStore

    if not getattr(config, "ths_official_verify_enabled", True):
        return {"rows_written": 0, "status": "skipped", "reason": "verify off"}
    client = client_from_config(config)
    if client is None:
        return {"rows_written": 0, "status": "skipped", "reason": "no api key"}

    if symbols is None:
        root = config.curated_root / "daily_bars"
        if not dataset_has_parquet(root):
            client.close()
            return {"rows_written": 0, "status": "skipped", "reason": "no daily_bars"}
        # Stocks only. `/api/a-share/prices/historical` refuses an ETF outright
        # (`code=1002`), and ranking `daily_bars` by turnover puts ETFs at the
        # top — an unfiltered sample of 200 lost 42 to that before this guard.
        tradable = None
        inst_root = config.curated_root / "instruments"
        if dataset_has_parquet(inst_root):
            tradable = (
                scan_parquet_root(inst_root)
                .filter((pl.col("asset_type") == "stock") & pl.col("delist_date").is_null())
                .select("symbol")
                .collect()
                .get_column("symbol")
                .to_list()
            )
        # The most traded names: a thin stock's disagreement is usually an empty
        # auction rather than a data defect, and says little about either vendor.
        liquid = scan_parquet_root(root, partition_col="trade_date", start=start, end=end)
        if tradable:
            liquid = liquid.filter(pl.col("symbol").is_in(tradable))
        symbols = (
            liquid.group_by("symbol")
            .agg(pl.col("amount").median().alias("_amount"))
            .sort("_amount", descending=True)
            .limit(sample)
            .collect()
            .get_column("symbol")
            .to_list()
        )

    try:
        frame, counters = fetch_daily_bars(symbols, start, end, client=client, workers=workers)
    finally:
        client.close()
    if frame.is_empty():
        return {"rows_written": 0, "status": "warning", "reason": "peer returned nothing"}

    SnapshotStore(config.meta_root).write(
        "daily_bars",
        with_provenance(frame, source=THS_SOURCE, data_version=data_version_for("daily_bars")),
        source=THS_SOURCE,
        data_version=data_version_for("daily_bars"),
        run_id=run_id,
    )
    return {"rows_written": frame.height, "symbols": len(symbols), **counters}
