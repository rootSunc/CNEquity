"""L3 fundamentals steps: valuation metrics, financial statement items."""

from __future__ import annotations

from datetime import date

from cn_market_lake.adapters.eastmoney.fundamentals import fetch_financial_statement_items
from cn_market_lake.adapters.eastmoney.shareholders import CHANGE_DATE, NOTICE_DATE
from cn_market_lake.adapters.eastmoney.valuation import fetch_valuation_metrics
from cn_market_lake.config import Config
from cn_market_lake.domain.symbols import is_all_a_symbol, parse_symbol
from cn_market_lake.orchestrator.registry import register_step
from cn_market_lake.steps.common import load_bar_universe, load_symbols
from cn_market_lake.steps.http_common import run_incremental_fetched, write_fetched

# EastMoney's valuation clist is a live snapshot only; history comes from baostock.
_VALUATION_BACKFILL_START = date(2016, 1, 1)
# Checkpoint every N symbols so a mid-sweep kill still keeps prior chunks in
# curated (resume via ``_symbols_needing_backfill`` / float_mv fill ratio).
_VALUATION_BACKFILL_CHUNK = 50


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
        allow_empty=True,
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

    from cn_market_lake.quality.cross_checks import last_complete_em_valuation_tip
    from cn_market_lake.storage.state import StateStore

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
    """Historical PE/PB/PS + market cap from baostock over all_a (2016 → tip).

    Resumable: symbols that already have baostock rows *with* ``float_mv`` filled
    densely (≥80%) are skipped. Progress is written every
    ``_VALUATION_BACKFILL_CHUNK`` symbols so a mid-sweep kill still keeps prior
    chunks. Failures are surfaced as audit findings (fail-loud).

    Single-flight on ``baostock``: concurrent history jobs are what trip the
    free-tier IP blacklist. History ``end`` is capped so this path cannot invent
    a sparse tip past the last complete EastMoney day.
    """
    from cn_market_lake.orchestrator.run_lock import RunLockError, run_lock

    try:
        with run_lock(config.meta_root, "baostock", blocking=False):
            return _backfill_valuation_metrics_locked(config, trade_date, run_id)
    except RunLockError as exc:
        return {
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
    from cn_market_lake.adapters.baostock.valuation import fetch_valuation_history
    from cn_market_lake.storage.valuation_orphans import purge_valuation_orphan_symbols

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
    todo = _symbols_needing_backfill(config, universe)
    history_end = _valuation_history_end(config, trade_date)
    if history_end < _VALUATION_BACKFILL_START:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "note": "history_end before backfill start; nothing to fetch",
            "history_end": history_end.isoformat(),
            "orphan_purge": purge_summary,
        }
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
    for offset in range(0, len(todo), _VALUATION_BACKFILL_CHUNK):
        batch = todo[offset : offset + _VALUATION_BACKFILL_CHUNK]
        try:
            df, failed = fetch_valuation_history(
                batch, _VALUATION_BACKFILL_START, history_end, config=config
            )
        except RuntimeError as exc:
            # Ban / login death mid-sweep: keep prior chunks, surface remainder.
            aborted_reason = str(exc)
            all_failed.extend(batch)
            all_failed.extend(todo[offset + len(batch) :])
            break
        all_failed.extend(failed)
        if not df.is_empty():
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

    result: dict = {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "orphan_purge": purge_summary,
        "symbols_todo": len(todo),
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
                "Re-run `cml backfill valuation_metrics` to resume."
            ),
        }
        result.setdefault("context_updates", {})["audit_findings"] = [finding]
    return result


# Require dense MV coverage before skipping a symbol — a single non-null day
# must not mark a decade of null float_mv/total_mv as "done".
_MV_FILL_DONE_RATIO = 0.80


def _symbols_needing_backfill(config: Config, universe: list[str]) -> list[str]:
    """Symbols missing baostock history, or with sparse market-cap fill."""
    import polars as pl

    part = config.curated_root / "valuation_metrics"
    files = list(part.glob("**/*.parquet")) if part.exists() else []
    if not files:
        return universe
    stats = (
        pl.scan_parquet(files)
        .filter(pl.col("source") == "baostock")
        .group_by("symbol")
        .agg(
            pl.len().alias("n"),
            pl.col("float_mv").null_count().alias("float_nulls"),
        )
        .collect()
    )
    # Done when ≥80% of baostock rows have float_mv (MV fill landed densely).
    done = set(
        stats.filter(
            (pl.col("n") > 0)
            & ((pl.col("n") - pl.col("float_nulls")) / pl.col("n") >= _MV_FILL_DONE_RATIO)
        )
        .get_column("symbol")
        .to_list()
    )
    return [s for s in universe if s not in done]


def _is_all_a(symbol: str) -> bool:
    try:
        info = parse_symbol(symbol)
    except ValueError:
        return False
    return is_all_a_symbol(info.code, info.exchange)


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
    df = fetch_financial_statement_items(trade_date, backfill=backfill, config=config)
    if df.is_empty():
        return {"rows_read": 0, "rows_written": 0}
    return write_fetched(config, run_id, "financial_statement_items", df, source="eastmoney")


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
    for win_start, win_end in windows:
        # Write per window rather than concatenating the walk: a full
        # top_holders backfill is ~110k rows a quarter across ~25 years, and
        # holding all of it costs both memory and everything fetched so far if
        # the run is killed. Unique batch id — write_simple's default batch-0
        # would overwrite the window before it.
        part = fetch_fn(win_start, win_end, by=by, config=config)
        if part.is_empty():
            continue
        chunk = write_fetched(
            config,
            run_id,
            dataset,
            part,
            source="eastmoney",
            batch_id=f"batch-{win_start.isoformat()}",
        )
        rows_read += int(chunk.get("rows_read", 0))
        rows_written += int(chunk.get("rows_written", 0))
    return {"rows_read": rows_read, "rows_written": rows_written, "windows": len(windows)}


@register_step("share_structure", group="fundamentals", depends_on=["instruments"])
def step_share_structure(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    from cn_market_lake.adapters.eastmoney.shareholders import fetch_share_structure

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
    from cn_market_lake.adapters.eastmoney.shareholders import fetch_shareholder_counts

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
    from cn_market_lake.adapters.eastmoney.shareholders import fetch_top_holders

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
