"""Reconstruct historical suspension status from daily_bars trading gaps.

For feeds that omit suspended rows, a listed symbol with no bar on a trading
day was suspended that day. Some lake sources retain an OHLC placeholder on a
suspended day, however; those rows carry ``volume=0``. A listed symbol with no
*traded* bar is therefore the actual evidence used here. It covers the whole
bar history — filling the trading_status gap that free ST feeds (EastMoney's
current-snapshot ST board) cannot reach.

Only sparse ``suspended`` rows are staged. Which row survives a primary-key
collision is decided by ``domain/trading_status``'s evidence ranking, not by
this module: an exchange record or a same-session board read still corrects a
derived row, while a current-state snapshot restated onto an older session no
longer erases one.

Optional ``start`` / ``end`` bound the calendar cross-join so a 2001→today
rebuild can run year-by-year without OOM.

A derived row's ``risk_warning`` is read, in order, from:

1. ``name_history`` (optional, Tushare): the security's name in force that
   day, whose ``ST``/``*ST`` prefix is the exchange's own label and which,
   unlike a daily feed, covers halted sessions too;
2. the symbol's own first-hand readings around the halt: when the last one
   before it and the first one after it agree, the halt carries that value;
   when they differ (a label imposed or lifted mid-halt) or one side is
   missing, it stays null.

Funds are never ST, so theirs is ``False``. Checked on the whole lake
(2026-11-04), agreeing brackets matched the name history on 99.9% of 450k
halted stock-days but leave the 27k halts with a change mid-way unknown.

Only *interior* gaps are reported. Each symbol's window is clamped to its own
first and last bar, so a session the bar feed has not delivered yet — today,
mid-run, or after a failed ingest — cannot be mistaken for a market-wide halt.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key, dedupe_lazy_by_primary_key
from cnequity.domain.schemas import with_provenance
from cnequity.domain.trading_status import (
    DERIVED_BAR_GAP_SOURCE,
    EVIDENCE_POINT_IN_TIME,
    NOT_FIRST_HAND_SOURCES,
    STATUS_SUSPENDED,
    evidence_rank_expr,
    risk_warning_value_expr,
)
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.parquet import StagingWriter

logger = logging.getLogger(__name__)


def _suspended_pairs(
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    """(symbol, trade_date) that were trading days in a symbol's active range but have no bar."""
    bars_root = config.curated_root / "daily_bars"
    cal_root = config.curated_root / "trading_calendar"
    inst_root = config.curated_root / "instruments"
    if not (
        dataset_has_parquet(bars_root)
        and dataset_has_parquet(cal_root)
        and dataset_has_parquet(inst_root)
    ):
        return pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date})

    # Lifetime bounds may include zero-volume suspension placeholders, but a
    # symbol represented only by placeholders must not get a synthetic active
    # range and be reported as suspended on every calendar day. The scanner
    # canonicalizes the daily-bar identity before filtering; legacy files
    # without volume retain row-based semantics when they coexist with current
    # files.
    bars_all_lf = scan_parquet_root(bars_root, partition_col="trade_date")
    traded_bars_lf = scan_parquet_root(bars_root, partition_col="trade_date", traded_only=True)
    traded_symbols = traded_bars_lf.select("symbol").unique()
    bars_lf = bars_all_lf.join(traded_symbols, on="symbol", how="semi")
    sym_range = (
        bars_lf.group_by("symbol")
        .agg(
            pl.col("trade_date").min().alias("bmin"),
            pl.col("trade_date").max().alias("bmax"),
        )
        .collect()
    )
    if sym_range.is_empty():
        return pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date})

    # Anti-join only needs bars inside the derive window.
    # Suspended days may survive as an OHLC placeholder with volume=0. The
    # traded-only scan already removed those rows while retaining legacy rows
    # from files without volume.
    bars_lf = traded_bars_lf.select(["symbol", "trade_date"])
    if start is not None:
        bars_lf = bars_lf.filter(pl.col("trade_date") >= start)
    if end is not None:
        bars_lf = bars_lf.filter(pl.col("trade_date") <= end)
    bars = bars_lf.unique().collect()

    cal_lf = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(cal_root, partition_col="trade_date"),
            "trading_calendar",
        )
        .filter(pl.col("is_trading"))
        .select("trade_date")
    )
    if start is not None:
        cal_lf = cal_lf.filter(pl.col("trade_date") >= start)
    if end is not None:
        cal_lf = cal_lf.filter(pl.col("trade_date") <= end)
    cal = cal_lf.collect()
    if cal.is_empty():
        return pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date})

    inst = dedupe_by_primary_key(
        scan_parquet_root(inst_root, hive=False)
        .select(["symbol", "list_date", "delist_date"])
        .collect(),
        "instruments",
    )

    active = inst.join(sym_range, on="symbol", how="inner").with_columns(
        pl.max_horizontal(pl.col("list_date").fill_null(pl.col("bmin")), pl.col("bmin")).alias(
            "astart"
        ),
        pl.min_horizontal(pl.col("delist_date").fill_null(pl.col("bmax")), pl.col("bmax")).alias(
            "aend"
        ),
    )
    if start is not None:
        active = active.with_columns(
            pl.max_horizontal(pl.col("astart"), pl.lit(start)).alias("astart")
        )
    if end is not None:
        active = active.with_columns(pl.min_horizontal(pl.col("aend"), pl.lit(end)).alias("aend"))
    active = active.filter(pl.col("astart") <= pl.col("aend"))
    if active.is_empty():
        return pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date})

    expected = (
        active.select(["symbol", "astart", "aend"])
        .join(cal, how="cross")
        .filter(
            (pl.col("trade_date") >= pl.col("astart")) & (pl.col("trade_date") <= pl.col("aend"))
        )
        .select(["symbol", "trade_date"])
    )
    gaps = expected.join(bars, on=["symbol", "trade_date"], how="anti")
    # A session the bar feed is known to have missing is a failed fetch, not
    # a market fact: inferring halts from it would label the gap "suspended"
    # and hide it from the refetch (2026-08-07, 223 Beijing names).
    unfetched = _unfetched_sessions(config)
    if unfetched:
        gaps = gaps.filter(~pl.col("trade_date").is_in(sorted(unfetched)))
    from cnequity.storage.state import StateStore

    state = StateStore(config.meta_root)
    unknown_keys = [
        {"symbol": row["symbol"], "trade_date": date.fromisoformat(row["trade_date"])}
        for row in state.get_outstanding_keys("daily_bars")
    ]
    if state.get_payload("daily_bars").get("missing_units"):
        from cnequity.orchestrator.outcomes import InputUnavailableError

        raise InputUnavailableError(
            "daily_bars has unresolved request units; cannot infer suspensions"
        )
    if unknown_keys:
        gaps = gaps.join(pl.DataFrame(unknown_keys), on=["symbol", "trade_date"], how="anti")
    return gaps


def _unfetched_sessions(config: Config) -> set[date]:
    from cnequity.storage.state import StateStore

    days: set[date] = set()
    for row in StateStore(config.meta_root).get_payload("daily_bars").get("missing_ranges", []):
        try:
            first = date.fromisoformat(str(row["start"]))
            last = date.fromisoformat(str(row.get("end") or row["start"]))
        except (KeyError, ValueError):
            continue
        day = first
        while day <= last:
            days.add(day)
            day = date.fromordinal(day.toordinal() + 1)
    return days


def _risk_warning_across_halts(config: Config, pairs: pl.DataFrame) -> pl.Series:
    """``risk_warning`` for each derived (symbol, trade_date), aligned to *pairs*.

    The name in force that day decides where ``name_history`` covers it;
    elsewhere a halt carries the label only when the readings on both sides
    agree. Before 2026-08-30 the derive wrote ``False`` here outright, and a
    suspension is exactly when a free feed stops answering, so the lake read
    every halted ST stock as clean.
    """
    unknown = pl.Series("risk_warning", [None] * pairs.height, dtype=pl.Boolean)
    keyed = pairs.with_row_index("__row").select("__row", "symbol", "trade_date")
    asset = _asset_types(config, keyed)
    status_root = config.curated_root / "trading_status"
    readings = (
        _first_hand_readings(status_root, keyed) if dataset_has_parquet(status_root) else None
    )
    if readings is None or readings.is_empty():
        resolved = keyed.with_columns(pl.lit(None, dtype=pl.Boolean).alias("risk_warning"))
    else:
        ordered = keyed.sort("trade_date")
        before = ordered.join_asof(
            readings.rename({"risk_warning": "__before"}),
            on="trade_date",
            by="symbol",
            strategy="backward",
            allow_exact_matches=False,
            check_sortedness=False,
        )
        both = before.join_asof(
            readings.rename({"risk_warning": "__after"}),
            on="trade_date",
            by="symbol",
            strategy="forward",
            allow_exact_matches=False,
            check_sortedness=False,
        )
        resolved = both.select(
            "__row",
            pl.when(pl.col("__before") == pl.col("__after"))
            .then(pl.col("__before"))
            .otherwise(None)
            .alias("risk_warning"),
        )
    resolved = keyed.select("__row", "symbol").join(
        resolved.select("__row", "risk_warning"), on="__row", how="left"
    )
    named = _named_risk_warning(config, keyed)
    if named is not None:
        resolved = resolved.join(named, on="__row", how="left").with_columns(
            pl.coalesce("__named", "risk_warning").alias("risk_warning")
        )
    if asset is not None:
        resolved = resolved.join(asset, on="symbol", how="left").with_columns(
            pl.when(pl.col("asset_type") == "etf")
            .then(False)
            .otherwise(pl.col("risk_warning"))
            .alias("risk_warning")
        )
    if resolved.height != pairs.height:
        return unknown
    return resolved.sort("__row")["risk_warning"]


def _named_risk_warning(config: Config, keyed: pl.DataFrame) -> pl.DataFrame | None:
    """``__row / __named``: the ST label of the name each symbol carried that day.

    ``None`` when the lake has no ``name_history``. A day before a symbol's
    first recorded name, or a symbol the history does not list (a code that
    was later changed, such as 000043.SZ), is left out rather than guessed.
    """
    root = config.curated_root / "name_history"
    if not dataset_has_parquet(root):
        return None
    symbols = keyed["symbol"].unique().to_list()
    names = (
        dedupe_lazy_by_primary_key(scan_parquet_root(root, hive=False), "name_history")
        .filter(pl.col("symbol").is_in(symbols) & pl.col("risk_warning").is_not_null())
        # Two names starting the same day (a same-day correction) are rare; if
        # either carries the label, the day was under a warning.
        .group_by("symbol", "start_date")
        .agg(pl.col("risk_warning").any().alias("__named"))
        .sort("start_date")
        .collect()
    )
    if names.is_empty():
        return None
    return (
        keyed.sort("trade_date")
        .join_asof(
            names,
            left_on="trade_date",
            right_on="start_date",
            by="symbol",
            strategy="backward",
            check_sortedness=False,
        )
        .select("__row", "__named")
    )


def _first_hand_readings(status_root, keyed: pl.DataFrame) -> pl.DataFrame | None:
    """Canonical first-hand ``(symbol, trade_date, risk_warning)`` for *keyed*'s symbols."""
    lf = scan_parquet_root(status_root, partition_col="trade_date")
    schema = lf.collect_schema()
    if "source" not in schema.names():
        return None
    symbols = keyed["symbol"].unique().to_list()
    lf = dedupe_lazy_by_primary_key(lf.filter(pl.col("symbol").is_in(symbols)), "trading_status")
    first_hand = ~pl.col("source").is_in(list(NOT_FIRST_HAND_SOURCES)).fill_null(False)
    rank = evidence_rank_expr(schema)
    if rank is not None:
        # A board reading restated onto an older session is not a reading of it.
        first_hand = first_hand & (rank == EVIDENCE_POINT_IN_TIME)
    return (
        lf.filter(first_hand)
        .select(
            "symbol",
            pl.col("trade_date").cast(pl.Date),
            risk_warning_value_expr(schema.names()).alias("risk_warning"),
        )
        .filter(pl.col("risk_warning").is_not_null())
        .unique(["symbol", "trade_date"])
        .sort("trade_date")
        .collect()
    )


def _asset_types(config: Config, keyed: pl.DataFrame) -> pl.DataFrame | None:
    inst_root = config.curated_root / "instruments"
    lf = scan_parquet_root(inst_root, hive=False)
    if "asset_type" not in lf.collect_schema().names():
        return None
    frame = dedupe_by_primary_key(
        lf.filter(pl.col("symbol").is_in(keyed["symbol"].unique().to_list())).collect(),
        "instruments",
    )
    return frame.select("symbol", "asset_type").unique("symbol", keep="last")


def derive_suspension_history(
    config: Config,
    run_id: str,
    *,
    start: date | None = None,
    end: date | None = None,
    batch_id: str = "derive-0",
) -> int:
    """Stage derived ``suspended`` rows for *run_id*. Returns the row count.

    The rows go through the ordinary ``staging -> compact -> commit`` channel
    rather than into the mutable curated directory. Writing curated directly
    published nothing: committed readers never saw the rows, and the next
    compact rebuilt the partition from the committed generation and dropped
    them again — so a `daily_bars` interior gap could never be excused by the
    suspension that explains it, however many times the derive was re-run.

    When *start* / *end* are set, only that calendar window is considered — use
    yearly chunks for a full-history rebuild to keep the cross-join bounded.
    """
    from cnequity.orchestrator.outcomes import InputUnavailableError

    for dataset in ("daily_bars", "trading_calendar", "instruments"):
        if not dataset_has_parquet(config.curated_root / dataset):
            raise InputUnavailableError(f"trading_status: missing {dataset} input")
    if start is not None or end is not None:
        calendar = scan_parquet_root(
            config.curated_root / "trading_calendar", partition_col="trade_date"
        )
        bars = scan_parquet_root(config.curated_root / "daily_bars", partition_col="trade_date")
        for bound, operator in ((start, "ge"), (end, "le")):
            if bound is not None:
                predicate = (
                    pl.col("trade_date") >= bound
                    if operator == "ge"
                    else pl.col("trade_date") <= bound
                )
                calendar = calendar.filter(predicate)
                bars = bars.filter(predicate)
        if calendar.select(pl.len()).collect().item() == 0:
            raise InputUnavailableError(
                "trading_status: calendar does not cover the requested window"
            )
        if (
            calendar.filter(pl.col("is_trading")).select(pl.len()).collect().item()
            and bars.select(pl.len()).collect().item() == 0
        ):
            raise InputUnavailableError("trading_status: no daily bars in the requested window")
    pairs = _suspended_pairs(config, start=start, end=end)
    if pairs.is_empty():
        return 0

    rows = pairs.with_columns(
        pl.lit(False).alias("is_trading"),
        pl.lit(STATUS_SUSPENDED).alias("status"),
        # A missing bar proves the security did not trade, not whether it was
        # ST. That comes from the readings around the halt, and stays null
        # where they disagree; `st_coverage` tracks where evidence is absent.
        _risk_warning_across_halts(config, pairs),
    )
    rows = with_provenance(rows, source=DERIVED_BAR_GAP_SOURCE, data_version="v1")
    # Merging against what is already committed is compact's job, and it uses
    # the shared evidence ranking so a restated EastMoney snapshot cannot
    # overwrite these rows while a genuine authority still can.
    StagingWriter(config.staging_root).write_batch("trading_status", run_id, batch_id, rows)
    logger.info(
        "staged %d derived suspension row(s) for trading_status run %s", rows.height, run_id
    )
    return rows.height
