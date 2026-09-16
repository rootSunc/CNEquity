"""L3 fundamentals steps: valuation metrics, financial statement items."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.fundamentals import fetch_financial_statement_items
from cnequity.adapters.eastmoney.shareholders import CHANGE_DATE, NOTICE_DATE
from cnequity.adapters.eastmoney.valuation import fetch_valuation_metrics
from cnequity.config import Config
from cnequity.domain.symbols import is_all_a_symbol, parse_symbol
from cnequity.orchestrator.registry import register_step
from cnequity.progress import sweep_progress
from cnequity.query.canonical import dedupe_lazy_by_primary_key
from cnequity.steps.common import instrument_metadata, load_bar_universe, load_symbols
from cnequity.steps.http_common import run_incremental_fetched, verify_raw_archive, write_fetched

logger = logging.getLogger(__name__)

# EastMoney's valuation clist is a live snapshot only; history comes from baostock.
_VALUATION_BACKFILL_START = date(2016, 1, 1)
# Checkpoint every N symbols so a mid-sweep kill still keeps prior chunks in
# curated (resume via ``_symbols_needing_backfill`` / float_mv fill ratio).
_VALUATION_BACKFILL_CHUNK = 50


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
        raise RuntimeError(f"valuation_metrics: baostock history response is missing {missing}")

    normalized = df.with_columns(
        pl.col("trade_date").cast(pl.Date, strict=False),
        pl.col("symbol").cast(pl.Utf8, strict=False),
    )
    dates = normalized.get_column("trade_date")
    invalid_dates = (
        dates.is_null() | (dates < start).fill_null(False) | (dates > end).fill_null(False)
    )
    if normalized.filter(invalid_dates).height:
        raise RuntimeError(
            f"valuation_metrics: baostock history returned row(s) outside "
            f"requested window {start.isoformat()}..{end.isoformat()}"
        )
    returned_symbols = normalized.get_column("symbol")
    if returned_symbols.null_count():
        raise RuntimeError("valuation_metrics: baostock history returned null symbol")
    unexpected = sorted(set(returned_symbols.to_list()) - set(symbols))
    if unexpected:
        raise RuntimeError(
            "valuation_metrics: baostock history returned unexpected symbol(s): "
            + ", ".join(unexpected[:5])
        )
    return normalized


@register_step("valuation_metrics", group="capital", depends_on=["instruments"])
def step_valuation_metrics(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        return _backfill_valuation_metrics(config, trade_date, run_id)
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("valuation_metrics: eastmoney source disabled in config")
    # The EastMoney clist snapshot returns delisted / non-tradable names that
    # never have a price bar (audit: valuation_bars_orphan_symbol). Pin the daily
    # snapshot to the same universe daily_bars actually realises so PE/PB rows are
    # only written for symbols that trade.
    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "valuation_metrics",
        lambda d: fetch_valuation_metrics(d, config=config),
        source="eastmoney",
        allow_empty=False,
        universe=load_bar_universe(config),
    )


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
    from cnequity.storage.valuation_orphans import purge_valuation_orphan_symbols

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
    if requested_end is not None:
        history_end = min(history_end, requested_end)
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
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("financial_statement_items: eastmoney source disabled in config")
    # Quarterly data: daily runs pick up same-day announcements; backfill walks
    # every report period from 2001 (CLI --start/--end clips the walk;
    # NOTICE_DATE incremental cannot reach history).
    backfill = getattr(config, "_backfill", False)
    archive_source = "eastmoney_backfill" if backfill else "eastmoney"
    archive_scope = f"{'backfill' if backfill else 'daily'}:{trade_date.isoformat()}"
    df = fetch_financial_statement_items(
        trade_date,
        backfill=backfill,
        config=config,
        run_id=run_id,
    )
    missing_periods: set[str] = set()
    missing_statement_types: dict[str, list[str]] = {}
    if backfill:
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
                    if config.should_archive_raw("financial_statement_items")
                    else None
                ),
            )
        result["status"] = "warning"
        if missing_periods:
            result["missing_periods"] = len(missing_periods)
        if missing_statement_types:
            result["missing_statement_periods"] = len(missing_statement_types)
        result["context_updates"] = {"audit_findings": findings}
        return result
    if df.is_empty():
        return {"rows_read": 0, "rows_written": 0}
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
            if config.should_archive_raw("financial_statement_items")
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


def _year_windows(start: date, end: date) -> list[tuple[date, date]]:
    """One calendar year per window, so a killed backfill costs one year."""
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
) -> dict:
    """Walk date windows, writing each as it lands.

    Backfill windows on the record date so it writes exactly the partitions it
    names. Daily windows on *daily_by* — the announcement date where the report
    has one, because a change effective weeks ago can be disclosed today and a
    record-date window would never see it.
    """
    from datetime import timedelta

    if not config.sources.get("eastmoney", True):
        raise RuntimeError(f"{dataset}: eastmoney source disabled in config")

    if getattr(config, "_backfill", False):
        start = getattr(config, "_backfill_start", None) or HISTORY_START
        end = getattr(config, "_backfill_end", None) or trade_date
        windows = _year_windows(start, end)
        by = CHANGE_DATE
    else:
        windows = [(trade_date - timedelta(days=daily_lookback_days), trade_date)]
        by = daily_by

    rows_read = 0
    rows_written = 0
    empty_windows: list[tuple[date, date]] = []
    # A backfill here is one window per year over ~25 years, each a paginated
    # sweep of its own: silent, and long enough to look stopped.
    report = sweep_progress(logger, f"{dataset} windows", len(windows), every=1, unit="windows")
    for index, (win_start, win_end) in enumerate(windows, start=1):
        # Write per window rather than concatenating the walk: a full
        # top_holders backfill is ~110k rows a quarter across ~25 years, and
        # holding all of it costs both memory and everything fetched so far if
        # the run is killed. Unique batch id — write_simple's default batch-0
        # would overwrite the window before it.
        part = fetch_fn(win_start, win_end, by=by, config=config)
        report(index)
        if part.is_empty():
            if getattr(config, "_backfill", False):
                empty_windows.append((win_start, win_end))
            continue
        chunk = write_fetched(
            config,
            run_id,
            dataset,
            part,
            # Historical shareholder endpoints expose the source's current
            # reconstructed snapshot for an old record/disclosure window.
            # Preserve that fact at row level so strict PIT reads can reject
            # it even after the data has been copied to another lake without
            # the registry metadata beside it.
            source=("eastmoney_backfill" if getattr(config, "_backfill", False) else "eastmoney"),
            batch_id=f"batch-{win_start.isoformat()}",
        )
        rows_read += int(chunk.get("rows_read", 0))
        rows_written += int(chunk.get("rows_written", 0))
    result: dict = {"rows_read": rows_read, "rows_written": rows_written, "windows": len(windows)}
    if empty_windows:
        result["status"] = "warning"
        result["empty_windows"] = len(empty_windows)
        result["context_updates"] = {
            "audit_findings": [
                {
                    "dataset": dataset,
                    "severity": "warning",
                    "check": "backfill_empty_windows",
                    "message": (
                        f"{dataset}: {len(empty_windows)} requested backfill window(s) "
                        "returned no rows"
                    ),
                    "empty_windows": [
                        {"start": start.isoformat(), "end": end.isoformat()}
                        for start, end in empty_windows
                    ],
                }
            ]
        }
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
    # receipt for any dataset the lake archives.
    archive_scope = f"ths_gap:{start.isoformat()}:{end.isoformat()}"
    archive = None
    if config.should_archive_raw(dataset):
        nonce = begin_capture(
            config, dataset, run_id, source=THS_SOURCE, request_scope=archive_scope
        )
        archive = RawPayloadArchive(
            config.meta_root,
            enabled=True,
            datasets=[dataset],
            compression=getattr(config, "raw_archive_compression", "gzip"),
            max_payload_bytes=getattr(config, "raw_archive_max_payload_bytes", None),
            capture_owner=config,
            capture_run_id=run_id,
            capture_source=THS_SOURCE,
            capture_scope=archive_scope,
            capture_nonce=nonce,
        )
    client = ThsOfficialClient(
        api_key, config=config, archive=archive, archive_dataset=dataset, run_id=run_id
    )

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
            rows, chunk_counters = fetch_statements(
                chunk, start, end, client=client, announce_dates=announce_dates, workers=workers
            )
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
