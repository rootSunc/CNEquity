"""L4 capital steps: fund flow, northbound, margin, dragon tiger, block trades."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.capital import (
    NORTHBOUND_HISTORY_START,
    fetch_block_trades,
    fetch_dragon_tiger,
    fetch_fund_flow,
    fetch_margin_trading,
    fetch_northbound_flows_range,
    fetch_northbound_holdings,
)
from cn_market_lake.config import Config
from cn_market_lake.orchestrator.registry import register_step
from cn_market_lake.steps.common import BACKFILL_START, incremental_trade_dates, list_trading_dates
from cn_market_lake.steps.http_common import run_incremental_fetched, write_fetched

logger = logging.getLogger(__name__)

_MARGIN_FLUSH_DAYS = 63  # stage a parquet part roughly every quarter of fetched days


def _run_capital_step(
    config: Config,
    trade_date: date,
    run_id: str,
    dataset: str,
    fetch_fn,
    *,
    allow_empty: bool = True,
) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError(f"{dataset}: eastmoney source disabled in config")

    # Bind Config so EastMoneyClient uses [sources.eastmoney] shared pacing /
    # proxy / timeout — bare clients only throttle at 1s in-process and trip EM
    # WAF on first-run multi-page clist/datacenter sweeps (fund_flow, margin).
    def _bound(d: date) -> pl.DataFrame:
        return fetch_fn(d, config=config)

    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        dataset,
        _bound,
        source="eastmoney",
        allow_empty=allow_empty,
    )


@register_step("fund_flow", group="capital", depends_on=["instruments"])
def step_fund_flow(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    return _run_capital_step(config, trade_date, run_id, "fund_flow", fetch_fund_flow)


@register_step("northbound_holdings", group="capital", depends_on=["instruments"])
def step_northbound_holdings(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("northbound_holdings: eastmoney source disabled in config")
    # Quarterly since Aug 2024: daily refreshes the latest quarter. Backfill
    # walks all quarter-ends from 2016 but the EM report only serves the most
    # recent quarter(s) — historical TRADE_DATE filters return 0 rows (verified
    # 2026-07), so history accrues forward only, one quarter per disclosure.
    from cn_market_lake.steps.http_common import write_fetched

    backfill = getattr(config, "_backfill", False)
    df = fetch_northbound_holdings(trade_date, backfill=backfill, config=config)
    if df.is_empty():
        return {"rows_read": 0, "rows_written": 0}
    return write_fetched(config, run_id, "northbound_holdings", df, source="eastmoney")


@register_step("northbound_flows", group="capital")
def step_northbound_flows(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Northbound flows over the whole outstanding window in one request.

    Deliberately not on ``_run_capital_step``: that helper fetches one day at a
    time, and this dataset's watermark is frozen at the last session the
    exchanges published (see ``NORTHBOUND_LAST_PUBLISHED``). Per-day fetching
    would therefore issue one more request every day, forever, all of them
    returning nothing.
    """
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("northbound_flows: eastmoney source disabled in config")

    if getattr(config, "_backfill", False):
        start = getattr(config, "_backfill_start", None) or NORTHBOUND_HISTORY_START
        end = getattr(config, "_backfill_end", None) or trade_date
    else:
        dates = incremental_trade_dates(config, "northbound_flows", trade_date)
        if not dates:
            return {"rows_read": 0, "rows_written": 0}
        start, end = dates[0], dates[-1]

    df = fetch_northbound_flows_range(start, end, config=config)
    if df.is_empty():
        # Expected for any window past the cutoff — not a fetch failure, and
        # not something to zero-fill. The audit reports the frozen watermark.
        logger.info(
            "northbound_flows: no published rows in %s..%s", start.isoformat(), end.isoformat()
        )
        return {"rows_read": 0, "rows_written": 0}
    return write_fetched(config, run_id, "northbound_flows", df, source="eastmoney")


def _existing_margin_dates(config: Config) -> set[date]:
    root = config.curated_root / "margin_trading"
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        return set()
    return set(
        pl.scan_parquet(files).select("trade_date").unique().collect()["trade_date"].to_list()
    )


def _backfill_margin_trading(config: Config, trade_date: date, run_id: str) -> dict:
    """Walk trading days fetching the EM margin report (history is served).

    Resumable: days already in curated are skipped, so a killed sweep can be
    rerun. ``--start/--end`` on ``cml backfill`` bound the walk; parts are
    staged in chunks so progress survives mid-run failures via compact.
    ``--workers N`` fetches days concurrently — each worker holds its own
    client throttled to 1 req/s (bypasses the shared source limiter, so the
    aggregate rate is up to N req/s; an explicit operator choice for sweeps).
    """
    from concurrent.futures import ThreadPoolExecutor

    from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
    from cn_market_lake.domain.schemas import with_provenance
    from cn_market_lake.storage import StagingWriter

    start = getattr(config, "_backfill_start", None) or BACKFILL_START
    end = getattr(config, "_backfill_end", None) or trade_date
    workers = max(1, int(getattr(config, "_backfill_workers", 1)))
    days = list_trading_dates(config, start, min(end, trade_date))
    have = _existing_margin_dates(config)
    todo = [d for d in days if d not in have]
    if not todo:
        return {"rows_read": 0, "rows_written": 0, "days_skipped": len(days)}

    writer = StagingWriter(config.staging_root)
    frames: list[pl.DataFrame] = []
    total_rows = 0
    empty_days: list[date] = []
    n_parts = 0

    def flush() -> None:
        nonlocal frames, total_rows, n_parts
        if not frames:
            return
        part = with_provenance(
            pl.concat(frames, how="diagonal_relaxed"), source="eastmoney", data_version="v1"
        )
        writer.write_batch("margin_trading", run_id, f"bf-{n_parts:04d}", part)
        n_parts += 1
        total_rows += part.height
        frames = []

    import threading

    local = threading.local()
    clients: list[EastMoneyClient] = []
    clients_lock = threading.Lock()

    def fetch_one(d: date) -> pl.DataFrame:
        client = getattr(local, "client", None)
        if client is None:
            # Prefer config so cross-process [sources.eastmoney] pacing applies
            # even with multiple workers (file lock serializes across threads).
            client = EastMoneyClient(config=config)
            local.client = client
            with clients_lock:
                clients.append(client)
        return fetch_margin_trading(d, client=client)

    done = 0
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # Submit one flush-chunk at a time: a mid-sweep failure only waits
            # out the current chunk, and staged parts land as the sweep goes.
            for lo in range(0, len(todo), _MARGIN_FLUSH_DAYS):
                chunk = todo[lo : lo + _MARGIN_FLUSH_DAYS]
                for d, df in zip(chunk, pool.map(fetch_one, chunk), strict=True):
                    if df.is_empty():
                        empty_days.append(d)
                    else:
                        frames.append(df)
                done += len(chunk)
                flush()
                logger.info(
                    "margin_trading backfill: %d/%d days (at %s, %d rows staged)",
                    done,
                    len(todo),
                    chunk[-1].isoformat(),
                    total_rows,
                )
    finally:
        for client in clients:
            client.close()

    if empty_days:
        logger.warning(
            "margin_trading backfill: %d trading day(s) returned no rows (e.g. %s) — "
            "left absent; a rerun retries them",
            len(empty_days),
            empty_days[0].isoformat(),
        )
    return {
        "rows_read": total_rows,
        "rows_written": total_rows,
        "days_fetched": len(todo) - len(empty_days),
        "days_skipped": len(days) - len(todo),
        "days_empty": len(empty_days),
    }


@register_step("margin_trading", group="capital", depends_on=["instruments"])
def step_margin_trading(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        return _backfill_margin_trading(config, trade_date, run_id)
    return _run_capital_step(config, trade_date, run_id, "margin_trading", fetch_margin_trading)


def _backfill_daily_report(
    config: Config, trade_date: date, run_id: str, dataset: str, fetch_fn, floor: date
) -> dict:
    """dragon_tiger / block_trades: each day's fetch works standalone and the
    daily step never walked a range through it — see ``walk_day_backfill``."""
    from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
    from cn_market_lake.steps.common import walk_day_backfill

    client = EastMoneyClient(config=config)
    try:
        return walk_day_backfill(
            config,
            trade_date,
            run_id,
            dataset,
            lambda d: fetch_fn(d, client=client, config=config),
            source="eastmoney",
            floor=floor,
        )
    finally:
        client.close()


@register_step("dragon_tiger", group="signals", depends_on=["instruments"])
def step_dragon_tiger(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        # Confirmed live 2007-01-04 has rows, 2006-01-04 does not.
        return _backfill_daily_report(
            config, trade_date, run_id, "dragon_tiger", fetch_dragon_tiger, date(2007, 1, 1)
        )
    return _run_capital_step(config, trade_date, run_id, "dragon_tiger", fetch_dragon_tiger)


@register_step("block_trades", group="signals", depends_on=["instruments"])
def step_block_trades(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        # Confirmed live from 2010-01-04; older single-day probes were
        # ambiguous (block trades are sparse — a quiet day and "no report yet"
        # look identical), so this floor is the conservative, confirmed one.
        return _backfill_daily_report(
            config, trade_date, run_id, "block_trades", fetch_block_trades, date(2010, 1, 1)
        )
    return _run_capital_step(config, trade_date, run_id, "block_trades", fetch_block_trades)
