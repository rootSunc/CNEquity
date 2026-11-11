"""The ``index_bars``, ``daily_bars_history`` and ``daily_bars_delisted`` steps."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl

from cnequity.adapters.tdx_protocol.client import (
    INDEX_SYMBOLS,
    fetch_index_bars,
    normalize_with_source,
)
from cnequity.config import Config
from cnequity.orchestrator.registry import register_step
from cnequity.run_scope import backfill_of
from cnequity.steps.bars import bar_scope
from cnequity.steps.bars._base import logger
from cnequity.steps.common import (
    incremental_window,
    last_session_on_or_before,
    list_trading_dates,
    load_curated_instruments,
    load_symbols,
)
from cnequity.storage.state import StateStore


def _index_bar_missing_keys(config: Config, df, start: date, end: date) -> list[tuple[str, date]]:
    """Record obligations independently of the valid rows available to publish."""
    expected_symbols = {f"{code}.{exchange}" for code, exchange in INDEX_SYMBOLS}
    sessions = list_trading_dates(config, start, end)
    if not sessions:
        sessions = [end]
    observed = set(df.select("symbol", "trade_date").iter_rows()) if not df.is_empty() else set()
    return [
        (symbol, session)
        for symbol in sorted(expected_symbols)
        for session in sessions
        if (symbol, session) not in observed
    ]


@register_step("index_bars", group="core", depends_on=["instruments"])
def step_index_bars(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if backfill_of(config).active:
        start, end = bar_scope._backfill_window(config, trade_date)
    else:
        start = incremental_window(config, "index_bars", trade_date)
        end = trade_date
    # Index daily bars have the same current-session semantics as stock daily
    # bars. Without this guard a pre-close run can stage a plausible but
    # incomplete index bar and advance the index coverage watermark.
    bar_scope._reject_unfinished_daily_bar_window(config, end)
    rl = config.tdx_rate_limit_spec()
    df = fetch_index_bars(
        start,
        end,
        rate_limit=rl,
        allow_mock=config.tdx_allow_mock,
        backfill=backfill_of(config).active,
        config=config,
        allow_partial=True,
    )
    df = normalize_with_source(df, "tdx_protocol")
    missing = _index_bar_missing_keys(config, df, start, end)
    from cnequity.steps.common import write_simple

    result = write_simple(config, run_id, "index_bars", df)
    if missing:
        StateStore(config.meta_root).record_outstanding_keys(
            "index_bars", missing, run_id=run_id, reason="source_scope_incomplete"
        )
        result.update(
            status="warning",
            coverage_status="partial",
            reason_code="source_scope_incomplete",
            missing_keys=len(missing),
        )
    return result


# The primary vendor serves 2016 onward; 同花顺 keeps per-year files back to each
# listing. Deep history is a separate step, not a wider window on the daily one:
# it uses a different source, runs for hours, and must never be on the daily path.
from cnequity.domain.market_profile import HISTORY_BACKFILL_START  # noqa: E402


def _validate_planned_stock_bars(
    rows: list[dict], symbol: str, start: date, end: date
) -> list[dict]:
    """Normalize and validate one THS response before adding it to a batch."""
    normalized: list[dict] = []
    for row in rows:
        if not isinstance(row, dict) or row.get("symbol") != symbol:
            raise RuntimeError(f"THS history returned a row for an unexpected symbol: {symbol}")
        raw_date = row.get("trade_date")
        if isinstance(raw_date, datetime):
            trade_date = raw_date.date()
        elif isinstance(raw_date, date):
            trade_date = raw_date
        elif isinstance(raw_date, str):
            try:
                trade_date = date.fromisoformat(raw_date)
            except ValueError as exc:
                raise RuntimeError(
                    f"THS history returned an invalid trade_date for {symbol}"
                ) from exc
        else:
            raise RuntimeError(f"THS history returned an invalid trade_date for {symbol}")
        if not start <= trade_date <= end:
            raise RuntimeError(
                f"THS history returned {symbol} row outside requested window "
                f"{start.isoformat()}..{end.isoformat()}: {trade_date.isoformat()}"
            )
        normalized_row = dict(row)
        normalized_row["trade_date"] = trade_date
        normalized.append(normalized_row)
    return normalized


@register_step(
    "daily_bars_history",
    group="backfill",
    depends_on=["instruments"],
    outputs=("daily_bars",),
)
def step_daily_bars_history(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Backfill pre-2016 unadjusted daily bars from 同花顺.

    Writes into ``daily_bars`` like the daily step, so `compact` and every reader
    treat the older rows identically. Only raw prices are fetched — hfq stays
    derived from the Sina factors already in use, which reach back to listing, so
    one adjustment convention spans the whole series (verified continuous across
    the 2015→2016 seam at 0.0bps).
    """
    import polars as pl

    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage import StagingWriter

    start = backfill_of(config).start or HISTORY_BACKFILL_START
    # Same normalisation as `_backfill_window`: one meaning for `_backfill_end`
    # across every reader, or a Saturday means "Friday" in one step and
    # "a session with no rows" in the next.
    end = last_session_on_or_before(config, backfill_of(config).end or date(2015, 12, 31))
    plan = _history_plan(config, start, end)
    resume = set(context.get("_history_done") or [])
    if resume:
        plan = [p for p in plan if p[0] not in resume]

    requests = sum((end.year - s.year + 1) for _, s in plan)
    logger.info(
        "daily_bars_history: %d symbols, %s..%s, ~%d year-requests "
        "(ETF/LOF included; 北交所 remains outside this SH/SZ history source)",
        len(plan),
        start,
        end,
        requests,
    )
    writer = StagingWriter(config.staging_root)
    written = 0
    batch_no = 0

    def _flush(rows: list[dict], done: list[str]) -> None:
        nonlocal written, batch_no
        if not rows:
            return
        batch_no += 1
        df = with_provenance(
            pl.DataFrame(rows), source="ths", data_version=data_version_for("daily_bars")
        )
        writer.write_batch("daily_bars", run_id, f"history-{batch_no:04d}", df)
        written += df.height
        logger.info(
            "daily_bars_history: batch %d — %d rows, %d symbols", batch_no, df.height, len(done)
        )

    failed = sweep_stock_bars_planned(plan, end, config=config, on_batch=_flush)
    return {
        "dataset": "daily_bars",
        "rows_read": written,
        "rows_written": written,
        "symbols": len(plan),
        "failed_symbols": len(failed),
        "note": f"{start}..{end} via 同花顺 (raw only; hfq derives from Sina factors)",
    }


def _history_plan(config: Config, start: date, end: date) -> list[tuple[str, date]]:
    """``[(symbol, fetch_start)]`` for the symbols worth fetching.

    Two filters and a per-symbol window, which together cut the sweep by ~78%:

    * Stocks and ETFs/LOFs. Both carry Sina hfq factors and an enriched
      ``list_date``, so deeper raw bars can be served as hfq with one
      adjustment convention. 北交所 is excluded because this THS history
      route is limited to SH/SZ.
      An ETF with no ``list_date`` is an unlisted placeholder (or an enrichment
      gap) with no verifiable history, so it is skipped rather than planned and
      failed.
    * Nothing listed after the window. A 2016 IPO has no pre-2016 history, and
      asking for it is ~2600 symbols' worth of empty year files.
    * The rest start at their listing year rather than at ``start``.
    """
    # The factor pipeline has no Beijing coverage.  Filtering only 92xxxx
    # misses legacy BSE/NEEQ codes (43/83/87xxxx), which are also represented as
    # ``.BJ`` in the instrument lake and must not enter this THS-only history
    # path.
    from cnequity.domain.market_profile import served

    symbols = [s for s in load_symbols(config) if served("ths_official_deep_history", [s.upper()])]
    inst = load_curated_instruments(config)
    if inst is None:
        # No instruments to plan against: fall back to the full window rather
        # than silently fetching nothing.
        return [(s, start) for s in symbols]
    inst = inst.select("symbol", "list_date", "asset_type")
    meta = {r["symbol"]: r for r in inst.to_dicts()}

    plan: list[tuple[str, date]] = []
    for sym in symbols:
        row = meta.get(sym)
        if row is None:
            continue
        asset_type = row.get("asset_type")
        if asset_type not in ("stock", "etf"):
            continue
        listed = row.get("list_date")
        if asset_type == "etf" and listed is None:
            continue
        if listed is not None:
            if listed > end:
                continue
            if listed > start:
                plan.append((sym, date(listed.year, 1, 1)))
                continue
        plan.append((sym, start))
    return plan


def sweep_stock_bars_planned(
    plan: list[tuple[str, date]],
    end: date,
    *,
    config: Config,
    on_batch,
    batch_size: int = 50,
) -> list[str]:
    """Sweep a per-symbol plan, batching writes. Returns failed symbols."""
    from cnequity.adapters.ths.stock_bars import fetch_stock_bars

    rows: list[dict] = []
    batch: list[str] = []
    failed: list[str] = []
    streak = 0
    for i, (symbol, sym_start) in enumerate(plan, start=1):
        try:
            got = fetch_stock_bars(symbol, sym_start, end, config=config)
            if not got:
                # A missing pre-IPO year is normal inside fetch_stock_bars, but
                # an entire planned window with no usable rows is not a success:
                # this symbol was eligible for the history sweep and would
                # otherwise disappear from the quality signal and retry scope.
                raise RuntimeError(
                    f"THS history returned no usable bars for {symbol} in "
                    f"{sym_start.isoformat()}..{end.isoformat()}"
                )
            rows.extend(_validate_planned_stock_bars(got, symbol, sym_start, end))
            batch.append(symbol)
            streak = 0
        except Exception as exc:  # noqa: BLE001 — recorded, sweep continues
            logger.warning("THS history failed for %s: %s", symbol, exc)
            failed.append(symbol)
            streak += 1
            if streak >= 10:
                logger.error("THS: %d consecutive failures at %s — aborting", streak, symbol)
                break
        if i % batch_size == 0 or i == len(plan):
            on_batch(rows, batch)
            rows, batch = [], []
    if batch:
        on_batch(rows, batch)
    return failed


# Rosters are sampled rather than walked day by day: a stock that traded at all
# appears on some quarter-end, and 40 roster queries beat 2,500.
_ROSTER_SAMPLE_MONTHS = (3, 6, 9, 12)


def _delisted_universe(config: Config, start: date, end: date) -> list[str]:
    """Symbols that traded in the window but hold no bars in the lake.

    Compares baostock's historical rosters against what daily_bars actually
    carries. Anything present then and absent now is a name the current-roster
    snapshot lost — the survivorship gap, 16.8% of the cross-section on
    2016-06-30 and still 6.0% on 2020-06-30.
    """
    from cnequity.adapters.baostock._session import _finish_session, _login, import_baostock
    from cnequity.adapters.baostock.access import hold_baostock_connection
    from cnequity.adapters.baostock.delisted_bars import roster_on
    from cnequity.domain.http_policy import SourceCoolingDown
    from cnequity.query.parquet_scan import scan_parquet_root

    bars_root = config.curated_root / "daily_bars"
    bars = scan_parquet_root(bars_root, partition_col="trade_date", hive=False, traded_only=True)
    have = set(bars.select("symbol").unique().collect()["symbol"].to_list())

    bs = import_baostock()
    missing: set[str] = set()
    blacklisted = False
    # One login covers every sample day. roster_on(login=False) reuses it, so
    # the connection lease stays with this caller rather than each sample.
    with hold_baostock_connection(config):
        _login(bs, config=config)
        try:
            for year in range(start.year, end.year + 1):
                for month in _ROSTER_SAMPLE_MONTHS:
                    day = date(year, month, 28)
                    if not (start <= day <= end):
                        continue
                    roster = roster_on(day, bs=bs, login=False, config=config)
                    if not roster:
                        continue
                    gap = roster - have
                    if gap:
                        logger.info(
                            "roster %s: %d stocks, %d absent from daily_bars",
                            day,
                            len(roster),
                            len(gap),
                        )
                    missing |= gap
        except SourceCoolingDown:
            blacklisted = True
            raise
        finally:
            _finish_session(bs, config=config, blacklisted=blacklisted)
    return sorted(missing)


@register_step(
    "daily_bars_delisted",
    group="backfill",
    depends_on=["instruments"],
    outputs=("daily_bars",),
)
def step_daily_bars_delisted(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Recover bars for stocks that delisted inside the window.

    The live vendors serve only what currently trades, so this is the one path
    that can close the survivorship gap; baostock keeps each delisted name
    through its final session. Rows land in ``daily_bars`` like any other, and
    hfq keeps deriving from the Sina factors, which still cover these symbols.
    """
    import polars as pl

    from cnequity.adapters.baostock.delisted_bars import fetch_delisted_bars
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage import StagingWriter

    start = backfill_of(config).start or date(2016, 1, 1)
    end = last_session_on_or_before(config, backfill_of(config).end or trade_date)
    symbols = context.get("_delisted_symbols") or _delisted_universe(config, start, end)
    if not symbols:
        return {"rows_read": 0, "rows_written": 0, "note": "no survivorship gap found"}

    logger.info("daily_bars_delisted: %d recovered symbols, %s..%s", len(symbols), start, end)
    rows, failed = fetch_delisted_bars(symbols, start, end, config=config)
    written = 0
    if rows:
        df = with_provenance(
            pl.DataFrame(rows), source="baostock", data_version=data_version_for("daily_bars")
        )
        StagingWriter(config.staging_root).write_batch("daily_bars", run_id, "delisted-0000", df)
        written = df.height
    return {
        "dataset": "daily_bars",
        "rows_read": written,
        "rows_written": written,
        "symbols": len(symbols),
        "failed_symbols": len(failed),
        "note": f"survivorship repair {start}..{end} via baostock",
    }


def _withhold_unbacked_disputes(
    frame: pl.DataFrame,
    joined: pl.DataFrame,
    adjudicator: pl.DataFrame,
    totals: dict,
) -> pl.DataFrame:
    """Drop the disputed rows an independent source does not back.

    A row whose close already matches is switched for its provenance alone. A
    row whose close differs is only switched when the third source agrees with
    the peer; otherwise the existing value stays, because importing a known
    regression to gain provenance on one row is a bad trade.
    """
    tolerance = 5e-5
    disputed = joined.filter(
        (pl.col("close_peer") - pl.col("close")).abs() > pl.col("close").abs() * tolerance
    ).select("symbol", "trade_date", "close", "close_peer")
    if disputed.is_empty():
        return frame

    judged = disputed.join(
        adjudicator.select("symbol", "trade_date", pl.col("close").alias("_third")),
        on=["symbol", "trade_date"],
        how="left",
    )
    backs_peer = judged.filter(
        pl.col("_third").is_not_null()
        & (
            (pl.col("_third") - pl.col("close_peer")).abs()
            <= pl.col("close_peer").abs() * tolerance
        )
    )
    withheld = judged.join(backs_peer, on=["symbol", "trade_date"], how="anti").select(
        "symbol", "trade_date"
    )
    totals["disputes_backed"] = totals.get("disputes_backed", 0) + backs_peer.height
    totals["disputes_withheld"] = totals.get("disputes_withheld", 0) + withheld.height
    if withheld.is_empty():
        return frame
    return frame.join(withheld, on=["symbol", "trade_date"], how="anti")


def _append_diff(path: Path, frame: pl.DataFrame) -> None:
    """Accumulate the diff across chunks, so a long sweep survives inspection.

    Writes through ``write_parquet_atomic``: this was the one parquet writer
    in the tree that truncated its destination inode in place, which a
    reader holding the previous footer sees as a corrupt file, and which
    would silently damage any generation that hardlinks this path.
    """
    from cnequity.storage.atomic import write_parquet_atomic

    if frame.is_empty():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        frame = pl.concat([pl.read_parquet(path), frame], how="vertical")
    write_parquet_atomic(path, frame)
