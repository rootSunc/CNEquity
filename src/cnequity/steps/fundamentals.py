"""L3 fundamentals steps: valuation metrics, financial statement items."""

from __future__ import annotations

import logging
import uuid
from datetime import date, timedelta

import polars as pl

from cnequity.adapters.eastmoney.datacenter import EastMoneyDatacenterError
from cnequity.adapters.eastmoney.fundamentals import fetch_financial_statement_items
from cnequity.adapters.eastmoney.shareholders import CHANGE_DATE, NOTICE_DATE
from cnequity.adapters.eastmoney.valuation import fetch_valuation_metrics
from cnequity.adapters.qmt_bridge import fetch_financial_statement_items_qmt
from cnequity.config import Config
from cnequity.domain.http_policy import SourceCoolingDown
from cnequity.domain.symbols import is_all_a_symbol, parse_symbol
from cnequity.orchestrator.outcomes import SourcePayloadError, SourceUnavailableError
from cnequity.orchestrator.registry import register_step
from cnequity.orchestrator.source_gaps import record_source_gap
from cnequity.progress import sweep_progress
from cnequity.query.canonical import dedupe_lazy_by_primary_key
from cnequity.steps.common import instrument_metadata, load_bar_universe, load_symbols
from cnequity.steps.http_common import run_incremental_fetched, verify_raw_archive, write_fetched
from cnequity.storage import StagingWriter
from cnequity.storage.state import StateStore

logger = logging.getLogger(__name__)

# EastMoney's valuation clist is a live snapshot only; history comes from baostock.
_VALUATION_BACKFILL_START = date(2016, 1, 1)
# Checkpoint every N symbols so a mid-sweep kill still keeps prior chunks in
# curated (resume via ``_symbols_needing_backfill`` / float_mv fill ratio).
_VALUATION_BACKFILL_CHUNK = 50
_REPORT_STATEMENT_TYPES = {
    "RPT_LICO_FN_CPD": {"income", "indicator"},
    "RPT_DMSK_FN_BALANCE": {"balance"},
    "RPT_DMSK_FN_INCOME": {"income"},
    "RPT_DMSK_FN_CASHFLOW": {"cashflow"},
}


def _valid_financial_unit(frame: pl.DataFrame, period: str, report: str) -> bool:
    try:
        day = date.fromisoformat(period)
        quarter = f"{day.year}Q{(day.month - 1) // 3 + 1}"
        return bool(
            not frame.is_empty()
            and {"report_period", "statement_type", "symbol"}.issubset(frame.columns)
            and set(frame.get_column("report_period").to_list()) == {quarter}
            and set(frame.get_column("statement_type").to_list()) <= _REPORT_STATEMENT_TYPES[report]
            and frame.get_column("symbol").null_count() == 0
        )
    except (ValueError, KeyError):
        return False


def _validate_valuation_history_batch(
    df: pl.DataFrame,
    symbols: list[str],
    start: date,
    end: date,
) -> pl.DataFrame:
    """Reject history rows outside the request before they reach staging."""
    if df.is_empty():
        return df
    missing = [column for column in ("symbol", "trade_date") if column not in df.columns]
    if missing:
        raise SourcePayloadError(
            f"valuation_metrics: baostock history response is missing {missing}"
        )

    normalized = df.with_columns(
        pl.col("trade_date").cast(pl.Date, strict=False),
        pl.col("symbol").cast(pl.Utf8, strict=False),
    )
    dates = normalized.get_column("trade_date")
    invalid_dates = (
        dates.is_null() | (dates < start).fill_null(False) | (dates > end).fill_null(False)
    )
    if normalized.filter(invalid_dates).height:
        raise SourcePayloadError(
            f"valuation_metrics: baostock history returned row(s) outside "
            f"requested window {start.isoformat()}..{end.isoformat()}"
        )
    returned_symbols = normalized.get_column("symbol")
    if returned_symbols.null_count():
        raise SourcePayloadError("valuation_metrics: baostock history returned null symbol")
    unexpected = sorted(set(returned_symbols.to_list()) - set(symbols))
    if unexpected:
        raise SourcePayloadError(
            "valuation_metrics: baostock history returned unexpected symbol(s): "
            + ", ".join(unexpected[:5])
        )
    return normalized


@register_step("valuation_metrics", group="capital", depends_on=["instruments"])
def step_valuation_metrics(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        return _backfill_valuation_metrics(config, trade_date, run_id)
    if not config.sources.get("eastmoney", True):
        raise SourceUnavailableError("valuation_metrics: eastmoney source disabled in config")
    # The EastMoney clist snapshot returns delisted / non-tradable names that
    # never have a price bar (audit: valuation_bars_orphan_symbol). Pin the daily
    # snapshot to the same universe daily_bars actually realises so PE/PB rows are
    # only written for symbols that trade.
    gap_fill = _fill_valuation_gaps(config, trade_date, run_id)
    result = run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "valuation_metrics",
        lambda d: fetch_valuation_metrics(d, config=config),
        source="eastmoney",
        allow_empty=False,
        universe=load_bar_universe(config),
    )
    if gap_fill:
        result["gap_fill"] = gap_fill
        filled = set(gap_fill.get("sessions", []))
        findings = (result.get("context_updates") or {}).get("audit_findings") or []
        kept = []
        for finding in findings:
            if finding.get("check") == "coverage_gap":
                left = [d for d in finding.get("gap_dates", []) if d not in filled]
                if not left:
                    continue
                finding = {**finding, "gap_dates": left}
            kept.append(finding)
        extra = gap_fill.get("finding")
        if extra:
            kept.append(extra)
        if kept:
            result.setdefault("context_updates", {})["audit_findings"] = kept
        elif "context_updates" in result:
            result["context_updates"].pop("audit_findings", None)
    return result


# A daily run fills at most this many missed sessions (~2 datacenter requests
# each); a longer outage is `cne backfill valuation_metrics --fill-em-outage`.
_VALUATION_GAP_FILL_SESSIONS = 30


def _fill_valuation_gaps(config: Config, trade_date: date, run_id: str) -> dict | None:
    """Stage datacenter valuation for sessions a past run missed.

    Valuation used to be a live snapshot: a session whose run failed was gone.
    datacenter's ``RPT_VALUEANALYSIS_DET`` is keyed by date, so the sessions
    between the last complete day and this one are read back automatically —
    only the (symbol, session) pairs that have a bar and no valuation row. A
    failure here never blocks the day's own fetch; it is reported instead.
    """
    from datetime import timedelta

    from cnequity.quality.cross_checks import last_dense_valuation_date
    from cnequity.steps.common import list_trading_dates

    last = last_dense_valuation_date(config)
    if last is None or last >= trade_date - timedelta(days=1):
        return None
    sessions = [
        d
        for d in list_trading_dates(config, last + timedelta(days=1), trade_date)
        if d < trade_date
    ]
    if not sessions:
        return None
    sessions = sessions[-_VALUATION_GAP_FILL_SESSIONS:]
    universe = [s for s in load_symbols(config) if _is_all_a(s)]
    bar_universe = load_bar_universe(config)
    if bar_universe:
        universe = [s for s in universe if s in bar_universe]
    try:
        filled = _fill_em_outage_from_datacenter(
            config, run_id, universe, sessions[0], sessions[-1], {}
        )
    except Exception as exc:  # noqa: BLE001 — the day's own fetch must still run
        logger.warning(
            "valuation_metrics: gap fill %s..%s failed: %s", sessions[0], sessions[-1], exc
        )
        return {
            "sessions": [],
            "finding": {
                "dataset": "valuation_metrics",
                "severity": "warning",
                "check": "valuation_gap_fill_failed",
                "message": (
                    f"valuation_metrics: could not read back {len(sessions)} missed session(s) "
                    f"{sessions[0].isoformat()}..{sessions[-1].isoformat()} from datacenter: {exc}"
                ),
            },
        }
    logger.info(
        "valuation_metrics: filled %s row(s) for missed session(s) %s..%s from datacenter",
        filled.get("rows_written"),
        sessions[0],
        sessions[-1],
    )
    return {
        "sessions": [d.isoformat() for d in sessions],
        "rows_written": filled.get("rows_written", 0),
    }


def _valuation_history_end(config: Config, trade_date: date) -> date:
    """Last date baostock history may write — never the live EastMoney tip.

    Daily snapshots belong to EastMoney. Letting history sweeps use ``end=today``
    creates sparse tip partitions (only the symbols finished so far) that look
    like coverage and push the watermark forward. Cap at the last complete EM
    day; if none exists yet, stay one day behind the run date so a first-time
    backfill still fills history without inventing today's tip.
    """
    from datetime import timedelta

    from cnequity.quality.cross_checks import last_complete_em_valuation_tip
    from cnequity.storage.state import StateStore

    em_tip = last_complete_em_valuation_tip(config)
    if em_tip is not None:
        return min(trade_date, em_tip)
    # No complete EM tip yet — stay behind the watermark (or behind trade_date)
    # so history cannot invent the live tip day that EastMoney still owns.
    watermark = StateStore(config.meta_root).get_date("valuation_metrics")
    if watermark is not None:
        return min(trade_date, watermark - timedelta(days=1))
    return min(trade_date, trade_date - timedelta(days=1))


def _em_outage_window(
    config: Config, trade_date: date, start: date, requested_end: date | None
) -> tuple[date, date]:
    """The sessions an EastMoney outage left without a snapshot, bounded.

    Opt-in (``cne backfill valuation_metrics --fill-em-outage``). The ordinary
    cap keeps baostock history behind the last complete EastMoney day, which
    is right while EastMoney publishes and leaves an outage permanently empty
    when it does not (push2 failed every host from 2026-09-22). This lets
    baostock fill only what the outage lost: sessions after that last day and
    before the run day, which EastMoney still owns.
    """
    from datetime import timedelta

    from cnequity.quality.cross_checks import last_complete_em_valuation_tip

    if requested_end is None:
        raise RuntimeError("--fill-em-outage needs an explicit --end")
    em_tip = last_complete_em_valuation_tip(config)
    if em_tip is None:
        raise RuntimeError(
            "--fill-em-outage needs a complete EastMoney day to anchor on; "
            "use the ordinary backfill for a lake without one"
        )
    return max(start, em_tip + timedelta(days=1)), min(
        requested_end, trade_date - timedelta(days=1)
    )


def _keys_in_window(
    config: Config, dataset: str, universe: list[str], start: date, end: date
) -> set[tuple[str, date]]:
    """(symbol, trade_date) keys *dataset* holds for *universe* in the window."""
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / dataset
    if not dataset_has_parquet(root):
        return set()
    keys = (
        scan_parquet_root(root, partition_col="trade_date")
        .filter(
            pl.col("trade_date").cast(pl.Date).is_between(start, end)
            & pl.col("symbol").is_in(universe)
        )
        .select("symbol", pl.col("trade_date").cast(pl.Date))
        .unique()
        .collect()
    )
    return set(keys.iter_rows())


def _fill_em_outage_from_datacenter(
    config: Config,
    run_id: str,
    universe: list[str],
    start: date,
    end: date,
    purge_summary: dict,
) -> dict:
    """Fill the outage window from datacenter's copy of EastMoney's valuation.

    ``RPT_VALUEANALYSIS_DET`` is keyed by date and served by a host push2's bans
    do not reach, so the sessions push2 missed can be read back whole: every
    A-share including Beijing (which baostock never had), two requests a day.
    The first outage fill ran baostock instead — 14 hours, and 347 Beijing
    names it could never answer.

    Publishes whole sessions or nothing: every (symbol, session) that has a
    price bar in the window must come back with a valuation row, or the step
    fails and its staged rows are never compacted — a partial window is the
    sparse tip ``_valuation_history_end`` exists to prevent. Sessions the lake
    already holds a valuation row for, from any source, are left alone.
    """
    from cnequity.adapters.eastmoney.valuation_datacenter import (
        SOURCE,
        fetch_valuation_datacenter,
    )

    base = {
        "orphan_purge": purge_summary,
        "history_start": start.isoformat(),
        "history_end": end.isoformat(),
        "source": SOURCE,
    }
    if end < start:
        return {"rows_read": 0, "rows_written": 0, "note": "empty outage window", **base}
    # Not `_symbols_needing_backfill`: that judges a symbol's whole baostock
    # history (market-cap density since 2016) and so re-fetched ~1,050 names
    # whose window was already complete. The outage question is per session.
    expected = _keys_in_window(config, "daily_bars", universe, start, end)
    held = _keys_in_window(config, "valuation_metrics", universe, start, end)
    gaps = expected - held
    todo = sorted({symbol for symbol, _ in gaps})
    if not todo:
        return {"rows_read": 0, "rows_written": 0, "note": "all sessions already filled", **base}

    df = fetch_valuation_datacenter(start, end, config=config)
    wanted = pl.DataFrame(
        sorted(gaps), schema={"symbol": pl.Utf8, "trade_date": pl.Date}, orient="row"
    )
    df = df.join(wanted, on=["symbol", "trade_date"], how="semi")
    expected = gaps
    got = set(df.select("symbol", "trade_date").iter_rows())
    missing = sorted(expected - got)
    if missing or df.is_empty():
        sample = ", ".join(f"{s}@{d.isoformat()}" for s, d in missing[:5])
        record_source_gap(
            "valuation_metrics",
            "valuation_metrics EastMoney-outage fill incomplete: datacenter lacks "
            f"{len(missing)} of {len(expected)} barred session(s) for {len(todo)} "
            f"symbol(s){f' (e.g. {sample})' if sample else ''}; retained rows require independent validation",
            dates={day for _, day in missing},
        )
    df = _validate_valuation_history_batch(df, todo, start, end)
    written = write_fetched(
        config,
        run_id,
        "valuation_metrics",
        df.with_columns(pl.lit(SOURCE).alias("source")),
        source=SOURCE,
        batch_id="datacenter-outage",
    )
    return {
        "rows_read": int(written.get("rows_read", 0)),
        "rows_written": int(written.get("rows_written", 0)),
        "symbols_todo": len(todo),
        **base,
    }


def _backfill_valuation_metrics(config: Config, trade_date: date, run_id: str) -> dict:
    """Historical PE/PB/PS + market cap from baostock over the requested window.

    Resumable: symbols that already have baostock rows *with* ``float_mv`` filled
    densely (≥80%) are skipped. Progress is written every
    ``_VALUATION_BACKFILL_CHUNK`` symbols so a mid-sweep kill still keeps prior
    chunks. Failures are surfaced as audit findings (fail-loud).

    Single-flight on ``baostock``: concurrent history jobs are what trip the
    free-tier IP blacklist. History ``end`` is capped so this path cannot invent
    a sparse tip past the last complete EastMoney day.
    """
    from cnequity.orchestrator.run_lock import RunLockError, run_lock

    try:
        with run_lock(config.meta_root, "baostock", blocking=False):
            return _backfill_valuation_metrics_locked(config, trade_date, run_id)
    except RunLockError as exc:
        return {
            "status": "warning",
            "rows_read": 0,
            "rows_written": 0,
            "note": "baostock lock held by another process; retry later",
            "context_updates": {
                "audit_findings": [
                    {
                        "dataset": "valuation_metrics",
                        "severity": "warning",
                        "check": "baostock_single_flight",
                        "message": str(exc),
                    }
                ]
            },
        }


def _backfill_valuation_metrics_locked(config: Config, trade_date: date, run_id: str) -> dict:
    from cnequity.adapters.baostock.valuation import fetch_valuation_history
    from cnequity.storage.repairs.valuation_orphans import purge_valuation_orphan_symbols

    # Drop leftover PE/PB for names that never have bars (pre-filter backfills).
    purge_summary = purge_valuation_orphan_symbols(config)

    universe = [s for s in load_symbols(config) if _is_all_a(s)]
    # Only backfill symbols that actually have price bars: a delisted name still
    # sitting in the instruments list (e.g. 退市创兴) otherwise gets years of
    # baostock PE/PB with no bar to join against (audit: orphan symbol). Skip the
    # constraint on a bars-less lake so a first-time backfill still runs.
    bar_universe = load_bar_universe(config)
    if bar_universe:
        universe = [s for s in universe if s in bar_universe]
    history_end = _valuation_history_end(config, trade_date)
    history_start = max(
        getattr(config, "_backfill_start", None) or _VALUATION_BACKFILL_START,
        _VALUATION_BACKFILL_START,
    )
    requested_end = getattr(config, "_backfill_end", None)
    if getattr(config, "_valuation_fill_em_outage", False):
        start, end = _em_outage_window(config, trade_date, history_start, requested_end)
        return _fill_em_outage_from_datacenter(config, run_id, universe, start, end, purge_summary)
    if requested_end is not None:
        history_end = min(history_end, requested_end)
    # baostock carries no Beijing names: every BJ query fails after its retries
    # (347 of them cost ~2 h of the 2026-09-26 outage fill, then failed the
    # sweep). They are not a gap this source can close, so they are not asked.
    from cnequity.domain.market_profile import served, unserved

    baostock_unserved = sorted(unserved("baostock", universe))
    universe = served("baostock", universe)
    if history_end < history_start:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "note": "history_end before backfill start; nothing to fetch",
            "history_start": history_start.isoformat(),
            "history_end": history_end.isoformat(),
            "orphan_purge": purge_summary,
        }
    todo = _symbols_needing_backfill(config, universe, start=history_start, end=history_end)
    if not todo:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "note": "all symbols already backfilled",
            "history_end": history_end.isoformat(),
            "orphan_purge": purge_summary,
        }

    from cnequity.domain.valuation import reconstruct_total_mv
    from cnequity.storage.repairs.valuation_basis import load_share_counts

    # Year-end share counts miss intra-year changes; the lake's own share
    # history gives the count effective on each session where it has one.
    shares = load_share_counts(config)
    rows_read = 0
    rows_written = 0
    all_failed: list[str] = []
    aborted_reason: str | None = None
    report = sweep_progress(logger, "valuation_metrics baostock backfill", len(todo))
    for offset in range(0, len(todo), _VALUATION_BACKFILL_CHUNK):
        batch = todo[offset : offset + _VALUATION_BACKFILL_CHUNK]
        try:
            df, failed = fetch_valuation_history(batch, history_start, history_end, config=config)
        except RuntimeError as exc:
            # Ban / login death mid-sweep: keep prior chunks, surface remainder.
            aborted_reason = str(exc)
            all_failed.extend(batch)
            all_failed.extend(todo[offset + len(batch) :])
            break
        all_failed.extend(failed)
        if not df.is_empty():
            df = _validate_valuation_history_batch(df, batch, history_start, history_end)
            df = reconstruct_total_mv(df, shares).drop("close", strict=False)
            # Unique part name per chunk — write_simple's default batch-0 would
            # overwrite prior chunks in the same run_id before compact.
            chunk = write_fetched(
                config,
                run_id,
                "valuation_metrics",
                df,
                source="baostock",
                batch_id=f"batch-{offset:05d}",
            )
            rows_read += int(chunk.get("rows_read", 0))
            rows_written += int(chunk.get("rows_written", 0))
        report(offset + len(batch))

    result: dict = {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "orphan_purge": purge_summary,
        "symbols_todo": len(todo),
        "baostock_unserved": len(baostock_unserved),
        "history_start": history_start.isoformat(),
        "history_end": history_end.isoformat(),
    }
    if aborted_reason:
        result["aborted"] = aborted_reason
    if all_failed or aborted_reason:
        result["failed_symbols"] = len(set(all_failed))
        finding = {
            "dataset": "valuation_metrics",
            "severity": "warning",
            "code": "baostock_backfill_incomplete",
            "message": (
                f"baostock backfill incomplete"
                f"{f' ({aborted_reason})' if aborted_reason else ''}; "
                f"wrote {rows_written} rows through {history_end.isoformat()}. "
                "Re-run `cne backfill valuation_metrics` to resume."
            ),
        }
        result.setdefault("context_updates", {})["audit_findings"] = [finding]
    return result


# Require dense MV coverage before skipping a symbol — a single non-null day
# must not mark a decade of null float_mv/total_mv as "done".
_MV_FILL_DONE_RATIO = 0.80


def _symbols_needing_backfill(
    config: Config,
    universe: list[str],
    *,
    start: date | None = None,
    end: date | None = None,
) -> list[str]:
    """Symbols missing baostock history, or with sparse market-cap fill.

    When ``end`` is supplied, a dense partial history is not considered done
    until its latest stored day reaches the requested window. Known listing
    spans shorten or eliminate the expected window for names that were not yet
    listed or had already delisted.
    """
    import polars as pl

    expected_end: dict[str, date | None] | None = None
    if end is not None:
        window_start = start or _VALUATION_BACKFILL_START
        expected_end = {symbol: end for symbol in universe}
        metadata = instrument_metadata(config)
        if not metadata.is_empty() and "symbol" in metadata.columns:
            for row in metadata.iter_rows(named=True):
                symbol = row.get("symbol")
                if symbol not in expected_end:
                    continue
                list_date = row.get("list_date")
                delist_date = row.get("delist_date")
                if list_date is not None and list_date > end:
                    expected_end[symbol] = None
                elif delist_date is not None and delist_date < window_start:
                    expected_end[symbol] = None
                elif delist_date is not None and delist_date < end:
                    expected_end[symbol] = delist_date

    part = config.curated_root / "valuation_metrics"
    files = list(part.glob("**/*.parquet")) if part.exists() else []
    if not files:
        return [
            symbol
            for symbol in universe
            if expected_end is None or expected_end[symbol] is not None
        ]
    stats = (
        dedupe_lazy_by_primary_key(
            pl.scan_parquet(files).filter(pl.col("source") == "baostock"),
            "valuation_metrics",
        )
        .group_by("symbol")
        .agg(
            pl.len().alias("n"),
            pl.col("float_mv").null_count().alias("float_nulls"),
            pl.col("total_mv").null_count().alias("total_nulls"),
            pl.col("trade_date").cast(pl.Date, strict=False).max().alias("latest_date"),
        )
        .collect()
    )
    # Done only when both market-cap fields are dense. K-data can succeed while
    # the separate Q4 total-share query fails; gating on float_mv alone would
    # then park total_mv nulls forever because the symbol would never resume.
    dense = stats.filter(
        (pl.col("n") > 0)
        & ((pl.col("n") - pl.col("float_nulls")) / pl.col("n") >= _MV_FILL_DONE_RATIO)
        & ((pl.col("n") - pl.col("total_nulls")) / pl.col("n") >= _MV_FILL_DONE_RATIO)
    )
    if expected_end is None:
        done = set(dense.get_column("symbol").to_list())
    else:
        done = {
            row["symbol"]
            for row in dense.iter_rows(named=True)
            if expected_end.get(row["symbol"]) is not None
            and row["latest_date"] is not None
            and row["latest_date"] >= expected_end[row["symbol"]]
        }
    return [
        symbol
        for symbol in universe
        if (expected_end is None or expected_end[symbol] is not None) and symbol not in done
    ]


def _is_all_a(symbol: str) -> bool:
    try:
        info = parse_symbol(symbol)
    except ValueError:
        return False
    return is_all_a_symbol(info.code, info.exchange)


def _expected_financial_periods(config: Config, trade_date: date) -> set[str]:
    from cnequity.adapters.eastmoney.fundamentals import _report_period_dates

    return {
        f"{period[:4]}Q{(int(period[5:7]) - 1) // 3 + 1}"
        for period in _report_period_dates(
            trade_date,
            start=getattr(config, "_backfill_start", None),
            end=getattr(config, "_backfill_end", None),
        )
    }


@register_step("financial_statement_items", group="fundamentals", depends_on=["instruments"])
def step_financial_statement_items(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    qmt_enabled = bool(getattr(config, "qmt_bridge_enabled", False))
    eastmoney_enabled = config.sources.get("eastmoney", True)
    if not qmt_enabled and not eastmoney_enabled:
        raise SourceUnavailableError(
            "financial_statement_items: eastmoney source disabled in config"
        )
    # Quarterly data: daily runs pick up same-day announcements; backfill walks
    # every report period from 2001 (CLI --start/--end clips the walk;
    # NOTICE_DATE incremental cannot reach history).
    backfill = getattr(config, "_backfill", False)
    archive_source = "eastmoney_backfill" if backfill else "eastmoney"
    archive_scope = f"{'backfill' if backfill else 'daily'}:{trade_date.isoformat()}"
    qmt_metrics: dict = {}
    if qmt_enabled and not config.tdx_allow_mock:
        symbols = getattr(config, "_backfill_symbols", None) or load_symbols(config)
        qmt_start = HISTORY_START if backfill else trade_date - timedelta(days=DAILY_LOOKBACK_DAYS)
        qmt_end = getattr(config, "_backfill_end", None) or trade_date
        try:
            df = fetch_financial_statement_items_qmt(
                symbols,
                qmt_start,
                qmt_end,
                config=config,
                metrics=qmt_metrics,
            )
            # An empty QMT answer can mean "no filings today" or "the terminal
            # has not downloaded financial data". Let EastMoney distinguish the
            # two so a local data gap cannot masquerade as a complete empty day.
            if int(qmt_metrics.get("failed_requests", 0)) == 0 and not df.is_empty():
                archive_source = "qmt_bridge"
                return write_fetched(
                    config,
                    run_id,
                    "financial_statement_items",
                    df,
                    source=archive_source,
                )
        except Exception as exc:
            logger.warning("QMT financial-statement fetch failed: %s", exc)

    state = StateStore(config.meta_root)
    unit_prefix = "fsi-"
    staged_files = StagingWriter(config.staging_root).list_run_files(
        "financial_statement_items", run_id
    )
    staged_units = set(
        state.get_payload("financial_statement_items").get("staged_units", {}).get(run_id, [])
    )
    # A state entry alone is not enough after a staging file was removed or
    # corrupted. Only the report-period units still backed by a readable file
    # can be skipped on retry.
    recoverable_units: set[str] = set()
    for unit in staged_units:
        if "|" not in unit:
            continue
        period, report = unit.split("|", 1)
        prefix = f"part-{unit_prefix}{period}-{report}-"
        for path in staged_files:
            if not path.name.startswith(prefix):
                continue
            try:
                recovered = pl.read_parquet(path)
                if _valid_financial_unit(recovered, period, report):
                    recoverable_units.add(unit)
                    break
            except (OSError, pl.exceptions.PolarsError):
                continue
    failures: list[tuple[str, str]] = []
    unit_stage_used = bool(backfill and recoverable_units)

    def _stage_unit(unit: str, scope: str, rows: list[dict]) -> None:
        nonlocal unit_stage_used
        unit_stage_used = True
        if not rows:
            return
        period, report = unit.split("|", 1)
        frame = pl.DataFrame(rows).unique(
            subset=["symbol", "report_period", "statement_type", "item_code", "announce_date"],
            keep="last",
        )
        if not _valid_financial_unit(frame, period, report):
            raise RuntimeError(f"financial_statement_items: invalid report unit {unit}")
        evidence = (
            verify_raw_archive(
                config,
                "financial_statement_items",
                run_id,
                source=archive_source,
                request_scope=scope,
            )
            if config.should_archive_raw("financial_statement_items")
            else None
        )
        write_fetched(
            config,
            run_id,
            "financial_statement_items",
            frame,
            source=archive_source,
            batch_id=f"{unit_prefix}{period}-{report}-{uuid.uuid4().hex}",
            raw_archive_evidence=evidence,
        )
        state.mark_staged_units("financial_statement_items", run_id, [unit])

    if backfill:
        df = fetch_financial_statement_items(
            trade_date,
            backfill=True,
            config=config,
            run_id=run_id,
            on_unit=_stage_unit,
            skip_units=recoverable_units,
            failures=failures,
        )
        if failures:
            state.record_missing_units("financial_statement_items", failures)
        if unit_stage_used:
            frames = [
                pl.read_parquet(path)
                for path in StagingWriter(config.staging_root).list_run_files(
                    "financial_statement_items", run_id
                )
            ]
            df = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    else:
        if not eastmoney_enabled:
            raise SourceUnavailableError(
                "financial_statement_items: QMT failed and eastmoney is disabled"
            )
        df = fetch_financial_statement_items(
            trade_date,
            backfill=False,
            config=config,
            run_id=run_id,
        )
    missing_periods: set[str] = set()
    missing_statement_types: dict[str, list[str]] = {}
    # Completeness is a whole-market claim: "every period the market reported
    # is present". Judging a scoped repair by it is a category error — five
    # delisted names do not file in all 36 periods, so a correct repair came
    # back `warning` with `missing_statement_periods: 36`, and that warning
    # then had compact skip the dataset and strand all 3,120 repaired rows.
    scoped = bool(getattr(config, "_backfill_symbols", None))
    if backfill and not scoped:
        expected = _expected_financial_periods(config, trade_date)
        observed = (
            set(df.get_column("report_period").drop_nulls().to_list())
            if not df.is_empty() and "report_period" in df.columns
            else set()
        )
        missing_periods = expected - observed
        if not df.is_empty() and {"report_period", "statement_type"}.issubset(df.columns):
            by_period = df.group_by("report_period").agg(
                pl.col("statement_type").drop_nulls().unique().alias("statement_types")
            )
            for row in by_period.iter_rows(named=True):
                period = row.get("report_period")
                if period not in observed:
                    continue
                present = set(row.get("statement_types") or [])
                # The four statement-type families fetch_financial_statement_items
                # actually issues requests for (adapters/eastmoney/fundamentals.py);
                # checking only a subset let a missing income statement pass as
                # a complete period.
                missing = sorted({"income", "indicator", "balance", "cashflow"} - present)
                if missing:
                    missing_statement_types[str(period)] = missing

    findings: list[dict] = []
    if missing_periods:
        findings.append(
            {
                "dataset": "financial_statement_items",
                "severity": "warning",
                "check": "backfill_missing_report_periods",
                "message": (
                    f"financial statement items missing {len(missing_periods)} "
                    f"requested report period(s): {', '.join(sorted(missing_periods)[:8])}"
                ),
                "missing_periods": sorted(missing_periods),
            }
        )
    if missing_statement_types:
        findings.append(
            {
                "dataset": "financial_statement_items",
                "severity": "warning",
                "check": "backfill_missing_statement_types",
                "message": (
                    f"financial statement items have incomplete report families in "
                    f"{len(missing_statement_types)} report period(s)"
                ),
                "missing_statement_types": [
                    {"report_period": period, "missing": missing}
                    for period, missing in sorted(missing_statement_types.items())
                ],
            }
        )

    if backfill and findings:
        result: dict
        if df.is_empty():
            result = {"rows_read": 0, "rows_written": 0}
        elif unit_stage_used:
            result = {"rows_read": df.height, "rows_written": df.height}
        else:
            result = write_fetched(
                config,
                run_id,
                "financial_statement_items",
                df,
                source=archive_source,
                raw_archive_evidence=(
                    verify_raw_archive(
                        config,
                        "financial_statement_items",
                        run_id,
                        source=archive_source,
                        request_scope=archive_scope,
                    )
                    if not qmt_used and config.should_archive_raw("financial_statement_items")
                    else None
                ),
            )
        result["status"] = "degraded" if unit_stage_used and not df.is_empty() else "warning"
        if result["status"] == "degraded":
            result["batch_settled"] = True
        if missing_periods:
            result["missing_periods"] = len(missing_periods)
        if missing_statement_types:
            result["missing_statement_periods"] = len(missing_statement_types)
        result["context_updates"] = {"audit_findings": findings}
        return result
    if df.is_empty():
        return {"rows_read": 0, "rows_written": 0}
    if unit_stage_used:
        result = {"rows_read": df.height, "rows_written": df.height}
        if failures:
            result["status"] = "degraded"
            result["batch_settled"] = True
        return result
    return write_fetched(
        config,
        run_id,
        "financial_statement_items",
        df,
        source=archive_source,
        raw_archive_evidence=(
            verify_raw_archive(
                config,
                "financial_statement_items",
                run_id,
                source=archive_source,
                request_scope=archive_scope,
            )
            if not qmt_used and config.should_archive_raw("financial_statement_items")
            else None
        ),
    )


# --- shareholder structure ---------------------------------------------------
# All three are swept market-wide with a date filter, never per symbol: one
# quarter of 前十大流通股东 is ~55k rows, so a per-symbol sweep would be ~5,500
# requests against ~110 pages for the filtered one.
#
# All three are keyed by DATE, not report period, and that was worth getting
# wrong once to learn. 股本结构's END_DATE is the date the share count changed.
# 股东户数 is disclosed at 旬末/月末 as well as quarter-ends. Even the holder
# lists have 10,749 rows in 2025 Q3 dated to something other than 09-30. A
# quarter-end sweep returns a plausible-looking pile of rows for each of them
# and quietly omits the rest.
HISTORY_START = date(2001, 1, 1)

# Daily lookback. Generous on purpose: the cost is one filtered sweep of a few
# pages, and the failure it prevents — a disclosure landing while the daily job
# was broken for a week — is silent.
DAILY_LOOKBACK_DAYS = 30

# top_holders windows on the record date instead (its total-scope report has no
# NOTICE_DATE), so its daily window has to be wide enough to still cover the
# last period end when that period's filings arrive months later.
TOP_HOLDERS_DAILY_LOOKBACK_DAYS = 240


def _quarter_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Stable, bounded report-date slices for large paged shareholder sweeps."""
    windows: list[tuple[date, date]] = []
    for year in range(start.year, end.year + 1):
        for month in (1, 4, 7, 10):
            lower = date(year, month, 1)
            upper = (
                date(year + 1, 1, 1) - timedelta(days=1)
                if month == 10
                else date(year, month + 3, 1) - timedelta(days=1)
            )
            if max(start, lower) <= min(end, upper):
                windows.append((max(start, lower), min(end, upper)))
    return windows


def _year_windows(start: date, end: date) -> list[tuple[date, date]]:
    return [
        (max(start, date(y, 1, 1)), min(end, date(y, 12, 31)))
        for y in range(start.year, end.year + 1)
    ]


def _run_shareholder_step(
    config: Config,
    trade_date: date,
    run_id: str,
    dataset: str,
    fetch_fn,
    *,
    daily_by: str,
    daily_lookback_days: int,
    source: str = "eastmoney",
) -> dict:
    """Persist complete date windows as they arrive and keep later failures scoped."""
    from datetime import timedelta

    if source != "qmt_bridge" and not config.sources.get("eastmoney", True):
        raise SourceUnavailableError(f"{dataset}: eastmoney source disabled in config")

    symbols = getattr(config, "_backfill_symbols", None) if dataset == "share_structure" else None
    if getattr(config, "_backfill", False):
        start = getattr(config, "_backfill_start", None) or HISTORY_START
        end = getattr(config, "_backfill_end", None) or trade_date
        windows = (
            # A few named securities hold tens of changes in their whole
            # history: one window, not one request per year.
            [(start, end)]
            if symbols
            else _quarter_windows(start, end)
            if dataset == "top_holders"
            else _year_windows(start, end)
        )
        by = CHANGE_DATE
    else:
        windows = [(trade_date - timedelta(days=daily_lookback_days), trade_date)]
        by = daily_by

    rows_read = 0
    rows_written = 0
    empty_windows: list[tuple[date, date]] = []
    failures: list[tuple[str, str]] = []
    stopped = False
    state = StateStore(config.meta_root)
    report = sweep_progress(logger, f"{dataset} windows", len(windows), every=1, unit="windows")
    staged = StagingWriter(config.staging_root).list_run_files(dataset, run_id)
    for index, (win_start, win_end) in enumerate(windows, start=1):
        unit = f"{by}:{win_start.isoformat()}:{win_end.isoformat()}"
        if symbols:
            # A targeted window is not evidence about the whole market's.
            unit += ":symbols=" + ",".join(sorted(symbols))
        if stopped:
            failures.append((unit, "earlier shareholder window failed"))
            report(index)
            continue
        # A failed paginated window has no completed batch file. Its archived
        # pages remain evidence, but cannot be spliced into a later dynamic
        # source snapshot and called complete.
        prefix = f"part-window-{win_start.isoformat()}-{win_end.isoformat()}-"
        previous = next((path for path in staged if path.name.startswith(prefix)), None)
        if previous is not None:
            try:
                recovered = pl.read_parquet(previous)
                window_column = (
                    "record_date"
                    if dataset == "top_holders"
                    else "announce_date"
                    if by == NOTICE_DATE
                    else "count_date"
                    if dataset == "shareholder_counts"
                    else "change_date"
                )
                if (
                    not recovered.is_empty()
                    and window_column in recovered.columns
                    and recovered.filter(
                        pl.col(window_column).is_null()
                        | (pl.col(window_column) < win_start)
                        | (pl.col(window_column) > win_end)
                    ).is_empty()
                ):
                    rows_read += recovered.height
                    rows_written += recovered.height
                    state.mark_staged_units(dataset, run_id, [unit])
                    report(index)
                    continue
            except (OSError, pl.exceptions.PolarsError):
                pass
        if source == "qmt_bridge":
            part = fetch_fn(win_start, win_end, by=by, config=config)
            incomplete = False
            report(index)
        else:
            source = "eastmoney_backfill" if getattr(config, "_backfill", False) else "eastmoney"
            capture = None
            if config.should_archive_raw(dataset):
                from cnequity.adapters.eastmoney.shareholders import ShareholderCapture
                from cnequity.storage.raw_archive import begin_capture

                capture = ShareholderCapture(
                    dataset=dataset,
                    run_id=run_id,
                    source=source,
                    request_scope=unit,
                    nonce=begin_capture(config, dataset, run_id, source=source, request_scope=unit),
                )
            from cnequity.adapters.eastmoney.shareholders import ShareholderProgress

            progress = ShareholderProgress()
            kwargs = {"archive_context": capture} if capture is not None else {}
            kwargs["progress"] = progress
            if symbols:
                kwargs["symbols"] = symbols
            try:
                part = fetch_fn(win_start, win_end, by=by, config=config, **kwargs)
            except (EastMoneyDatacenterError, SourceCoolingDown) as exc:
                failures.append((unit, str(exc)))
                stopped = True
                logger.warning(
                    "%s window %s failed; completed windows remain staged: %s", dataset, unit, exc
                )
                report(index)
                continue
            report(index)
            incomplete = progress.failed_report is not None
        if incomplete:
            failures.append((unit, progress.failure or f"{progress.failed_report} incomplete"))
            stopped = True
        if part.is_empty():
            if not incomplete:
                state.clear_missing_units(dataset, [unit])
            if not incomplete and getattr(config, "_backfill", False):
                empty_windows.append((win_start, win_end))
            continue
        chunk = write_fetched(
            config,
            run_id,
            dataset,
            part,
            # Historical shareholder endpoints expose the source's current
            # reconstructed snapshot for an old record/disclosure window.
            source=source,
            batch_id=(
                f"{'partial-' if incomplete else ''}window-"
                f"{win_start.isoformat()}-{win_end.isoformat()}-{uuid.uuid4().hex}"
            ),
            raw_archive_evidence=(
                verify_raw_archive(
                    config,
                    dataset,
                    run_id,
                    source=source,
                    request_scope=capture.request_scope,
                )
                if capture is not None
                else None
            ),
        )
        if not incomplete:
            state.mark_staged_units(dataset, run_id, [unit])
        rows_read += int(chunk.get("rows_read", 0))
        rows_written += int(chunk.get("rows_written", 0))

    if failures:
        state.record_missing_units(dataset, failures)
    result: dict = {"rows_read": rows_read, "rows_written": rows_written, "windows": len(windows)}
    findings: list[dict] = []
    if failures:
        result["status"] = "degraded"
        result["missing_units"] = len(failures)
        findings.append(
            {
                "dataset": dataset,
                "severity": "warning",
                "check": "shareholder_missing_windows",
                "message": (
                    f"{dataset}: {len(failures)} retryable window(s) incomplete; "
                    "validated earlier windows remain available"
                ),
                "missing_units": [unit for unit, _ in failures],
            }
        )
    if empty_windows:
        if not failures:
            result["status"] = "warning"
        result["empty_windows"] = len(empty_windows)
        findings.append(
            {
                "dataset": dataset,
                "severity": "warning",
                "check": "backfill_empty_windows",
                "message": (
                    f"{dataset}: {len(empty_windows)} requested backfill window(s) returned no rows"
                ),
                "empty_windows": [
                    {"start": start.isoformat(), "end": end.isoformat()}
                    for start, end in empty_windows
                ],
            }
        )
    if findings:
        result["context_updates"] = {"audit_findings": findings}
    if rows_written > 0 and (failures or (dataset == "top_holders" and empty_windows)):
        # Positive facts from complete windows can publish while the
        # outstanding window stays incomplete in the persistent ledger.
        result["batch_settled"] = True
    return result


@register_step("share_structure", group="fundamentals", depends_on=["instruments"])
def step_share_structure(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    from cnequity.adapters.eastmoney.shareholders import fetch_share_structure

    return _run_shareholder_step(
        config,
        trade_date,
        run_id,
        "share_structure",
        fetch_share_structure,
        daily_by=NOTICE_DATE,
        daily_lookback_days=DAILY_LOOKBACK_DAYS,
    )


@register_step("shareholder_counts", group="fundamentals", depends_on=["instruments"])
def step_shareholder_counts(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    eastmoney_enabled = config.sources.get("eastmoney", True)
    if not eastmoney_enabled and getattr(config, "qmt_bridge_enabled", False):
        from cnequity.adapters.qmt_bridge import fetch_shareholder_counts_qmt
        from cnequity.steps.common import load_symbols

        symbols = getattr(config, "_backfill_symbols", None) or load_symbols(config)

        def fetch_qmt(start: date, end: date, *, by: str, config: Config):
            return fetch_shareholder_counts_qmt(symbols, start, end, by=by, config=config)

        return _run_shareholder_step(
            config,
            trade_date,
            run_id,
            "shareholder_counts",
            fetch_qmt,
            daily_by=NOTICE_DATE,
            daily_lookback_days=DAILY_LOOKBACK_DAYS,
            source="qmt_bridge",
        )

    from cnequity.adapters.eastmoney.shareholders import fetch_shareholder_counts

    return _run_shareholder_step(
        config,
        trade_date,
        run_id,
        "shareholder_counts",
        fetch_shareholder_counts,
        daily_by=NOTICE_DATE,
        daily_lookback_days=DAILY_LOOKBACK_DAYS,
    )


@register_step("top_holders", group="fundamentals", depends_on=["instruments"])
def step_top_holders(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    from cnequity.adapters.eastmoney.shareholders import fetch_top_holders

    return _run_shareholder_step(
        config,
        trade_date,
        run_id,
        "top_holders",
        fetch_top_holders,
        # Its total-scope report has no NOTICE_DATE, so the daily path windows
        # on the record date like the backfill does — just a narrower window.
        daily_by=CHANGE_DATE,
        daily_lookback_days=TOP_HOLDERS_DAILY_LOOKBACK_DAYS,
    )


def _borrowable_announce_dates(
    config: Config, start_period: str, end_period: str
) -> dict[tuple[str, str], date]:
    """Disclosure dates the lake already knows, keyed by (symbol, report_period).

    All four statements come from one filing and share one date; measured across
    the lake, 303,769 of 315,264 multi-statement periods (96.35%) already carry a
    single date. ``income`` is preferred because it is the statement with full
    coverage over the gap years; ``indicator`` fills in behind it.
    """
    from cnequity.query.canonical import dedupe_lazy_by_primary_key
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / "financial_statement_items"
    if not dataset_has_parquet(root):
        return {}
    frame = (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col="report_period"),
            "financial_statement_items",
        )
        .filter(
            pl.col("statement_type").is_in(["income", "indicator"])
            & (pl.col("report_period") >= start_period)
            & (pl.col("report_period") <= end_period)
            & pl.col("announce_date").is_not_null()
        )
        .select("symbol", "report_period", "statement_type", "announce_date")
        .collect()
    )
    if frame.is_empty():
        return {}
    # income wins ties; sorting puts it last so `keep="last"` selects it.
    ordered = frame.with_columns(
        pl.when(pl.col("statement_type") == "income").then(1).otherwise(0).alias("_rank")
    ).sort(["symbol", "report_period", "_rank"])
    unique = ordered.unique(subset=["symbol", "report_period"], keep="last", maintain_order=True)
    return {
        (row["symbol"], row["report_period"]): row["announce_date"]
        for row in unique.iter_rows(named=True)
    }


def backfill_statement_gap_ths_official(
    config: Config,
    run_id: str,
    *,
    start: date,
    end: date,
    symbols: list[str] | None = None,
    chunk_size: int = 200,
    workers: int = 4,
    announce_dates: dict[tuple[str, str], date] | None = None,
) -> dict:
    """Fill the 2016-2024 balance-sheet and cash-flow hole from the licensed peer.

    Routing, not switching (ADR-0005): those primary keys hold no rows today, so
    nothing canonical is overwritten and no repair flag is needed. It still
    requires ``[sources.ths_official].backfill`` because it changes what the lake
    holds, and a lake with no key keeps its existing sources untouched.
    """
    from cnequity.adapters.ths_official import SOURCE as THS_SOURCE
    from cnequity.adapters.ths_official import ThsOfficialClient
    from cnequity.adapters.ths_official.financials import fetch_statements
    from cnequity.storage.raw_archive import RawPayloadArchive, begin_capture

    dataset = "financial_statement_items"
    if not getattr(config, "ths_official_backfill_enabled", False):
        return {"rows_read": 0, "rows_written": 0, "status": "skipped", "reason": "backfill off"}
    api_key = getattr(config, "ths_official_api_key", None)
    if not api_key or not config.sources.get(THS_SOURCE, False):
        return {"rows_read": 0, "rows_written": 0, "status": "skipped", "reason": "no api key"}

    # Exact wire evidence for every staged row; `write_fetched` requires the
    # receipt for any dataset the lake archives. One capture per staged chunk,
    # not one for the sweep: a receipt is consumed by the publish it backs, so
    # a single scope spanning every chunk was spent by the first one and every
    # request after it failed with "raw archive capture was already consumed" —
    # measured at 7,930 such failures on a full-market run that wrote one chunk.
    archives_raw = config.should_archive_raw(dataset)

    def _chunk_archive(offset: int) -> tuple[str, RawPayloadArchive | None]:
        scope = f"ths_gap:{start.isoformat()}:{end.isoformat()}:{offset:06d}"
        if not archives_raw:
            return scope, None
        nonce = begin_capture(config, dataset, run_id, source=THS_SOURCE, request_scope=scope)
        return scope, RawPayloadArchive(
            config.meta_root,
            enabled=True,
            datasets=[dataset],
            compression=getattr(config, "raw_archive_compression", "gzip"),
            max_payload_bytes=getattr(config, "raw_archive_max_payload_bytes", None),
            capture_owner=config,
            capture_run_id=run_id,
            capture_source=THS_SOURCE,
            capture_scope=scope,
            capture_nonce=nonce,
        )

    # The announce-date scan below needs no archive; the per-chunk clients do.
    client = ThsOfficialClient(api_key, config=config, archive=None, run_id=run_id)

    start_period = f"{start.year}Q{(start.month - 1) // 3 + 1}"
    end_period = f"{end.year}Q{(end.month - 1) // 3 + 1}"
    # Building this scans the whole four-million-row dataset, so a caller
    # sweeping the market in chunks should build it once and pass it in rather
    # than paying for the scan on every chunk.
    if announce_dates is None:
        announce_dates = _borrowable_announce_dates(config, start_period, end_period)
    if not announce_dates:
        client.close()
        return {"rows_read": 0, "rows_written": 0, "status": "skipped", "reason": "no known dates"}

    if symbols is None:
        symbols = sorted({symbol for symbol, _ in announce_dates})

    totals = {"rows_read": 0, "rows_written": 0}
    counters: dict[str, int] = {}
    try:
        for offset in range(0, len(symbols), chunk_size):
            chunk = symbols[offset : offset + chunk_size]
            archive_scope, archive = _chunk_archive(offset)
            chunk_client = ThsOfficialClient(
                api_key, config=config, archive=archive, archive_dataset=dataset, run_id=run_id
            )
            try:
                rows, chunk_counters = fetch_statements(
                    chunk,
                    start,
                    end,
                    client=chunk_client,
                    announce_dates=announce_dates,
                    workers=workers,
                )
            finally:
                chunk_client.close()
            for key, value in chunk_counters.items():
                counters[key] = counters.get(key, 0) + value
            if not rows:
                continue
            frame = pl.DataFrame(
                rows,
                schema={
                    "symbol": pl.Utf8,
                    "report_period": pl.Utf8,
                    "statement_type": pl.Utf8,
                    "item_code": pl.Utf8,
                    "item_value": pl.Float64,
                    "announce_date": pl.Date,
                },
            )
            written = write_fetched(
                config,
                run_id,
                dataset,
                frame,
                source=THS_SOURCE,
                batch_id=f"ths-gap-{offset // chunk_size:04d}",
                raw_archive_evidence=(
                    verify_raw_archive(
                        config,
                        dataset,
                        run_id,
                        source=THS_SOURCE,
                        request_scope=archive_scope,
                    )
                    if archive is not None
                    else None
                ),
            )
            totals["rows_read"] += written.get("rows_read", frame.height)
            totals["rows_written"] += written.get("rows_written", frame.height)
    finally:
        client.close()

    totals.update(counters)
    totals["symbols"] = len(symbols)
    return totals


def snapshot_corporate_actions_ths_official(config: Config, run_id: str) -> dict:
    """Capture the 同花顺 adjustment-factor dump as a corporate-action snapshot.

    Verification, not content: this writes to ``meta/source_snapshots`` and never
    to curated, so it is safe whenever a key is present and needs no backfill
    flag. One download replaces the 5,500 per-symbol requests the REST event
    stream would cost, and it is the only route that carries 配股 at all.
    """
    from cnequity.adapters.ths_official import SOURCE as THS_SOURCE
    from cnequity.adapters.ths_official import client_from_config
    from cnequity.adapters.ths_official.corporate_actions import fetch_corporate_actions_dump
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage.source_snapshots import SnapshotStore

    if not getattr(config, "ths_official_verify_enabled", True):
        return {"rows_written": 0, "status": "skipped", "reason": "verify off"}
    client = client_from_config(config)
    if client is None:
        return {"rows_written": 0, "status": "skipped", "reason": "no api key"}
    try:
        frame, cached = fetch_corporate_actions_dump(
            client, cache_dir=config.meta_root / "ths_official_dumps"
        )
    finally:
        client.close()
    if frame.is_empty():
        return {"rows_written": 0, "status": "warning", "reason": "dump parsed empty"}

    SnapshotStore(config.meta_root).write(
        "corporate_actions",
        with_provenance(
            frame, source=THS_SOURCE, data_version=data_version_for("corporate_actions")
        ),
        source=THS_SOURCE,
        data_version=data_version_for("corporate_actions"),
        run_id=run_id,
    )
    return {
        "rows_written": frame.height,
        "symbols": frame.get_column("symbol").n_unique(),
        "dump": str(cached),
    }


def snapshot_financials_ths_official(
    config: Config,
    run_id: str,
    *,
    start: date,
    end: date,
    sample: int = 300,
    workers: int = 4,
    statement_types: tuple[str, ...] = ("income",),
) -> dict:
    """Capture a peer reading of the statements the lake holds from one source.

    ``income`` is 1,624,060 rows and ``indicator`` 1,058,909, both entirely from
    EastMoney — the two largest single-sourced blocks left once balance and cash
    flow gained a peer. Nothing arbitrates them today.

    Verification class: writes to ``meta/source_snapshots``, never to curated.
    Sampled, because the point is a standing second opinion rather than a mirror
    of a four-million-row dataset.
    """
    from cnequity.adapters.ths_official import SOURCE as THS_SOURCE
    from cnequity.adapters.ths_official import client_from_config
    from cnequity.adapters.ths_official.financials import fetch_statements
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage.source_snapshots import SnapshotStore

    dataset = "financial_statement_items"
    if not getattr(config, "ths_official_verify_enabled", True):
        return {"rows_written": 0, "status": "skipped", "reason": "verify off"}
    client = client_from_config(config)
    if client is None:
        return {"rows_written": 0, "status": "skipped", "reason": "no api key"}

    start_period = f"{start.year}Q{(start.month - 1) // 3 + 1}"
    end_period = f"{end.year}Q{(end.month - 1) // 3 + 1}"
    announce_dates = _borrowable_announce_dates(config, start_period, end_period)
    if not announce_dates:
        client.close()
        return {"rows_written": 0, "status": "skipped", "reason": "no known dates"}

    # Spread the sample across the code space rather than taking a prefix, so a
    # whole exchange or listing vintage cannot sit outside the second opinion.
    symbols = sorted({symbol for symbol, _ in announce_dates})
    if sample and len(symbols) > sample:
        stride = max(1, len(symbols) // sample)
        symbols = symbols[::stride][:sample]

    try:
        rows, counters = fetch_statements(
            symbols,
            start,
            end,
            client=client,
            announce_dates=announce_dates,
            statement_types=statement_types,
            workers=workers,
        )
    finally:
        client.close()
    if not rows:
        return {"rows_written": 0, "status": "warning", "reason": "peer returned nothing"}

    frame = pl.DataFrame(
        rows,
        schema={
            "symbol": pl.Utf8,
            "report_period": pl.Utf8,
            "statement_type": pl.Utf8,
            "item_code": pl.Utf8,
            "item_value": pl.Float64,
            "announce_date": pl.Date,
        },
    )
    SnapshotStore(config.meta_root).write(
        dataset,
        with_provenance(frame, source=THS_SOURCE, data_version=data_version_for(dataset)),
        source=THS_SOURCE,
        data_version=data_version_for(dataset),
        run_id=run_id,
    )
    return {"rows_written": frame.height, "symbols": len(symbols), **counters}
