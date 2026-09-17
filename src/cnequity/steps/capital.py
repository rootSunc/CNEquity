"""L4 capital steps: fund flow, northbound, margin, dragon tiger, block trades.

``margin_trading`` reads from the exchanges themselves by default; the rest
still come from EastMoney. See ``_margin_source`` for why that one moved and
what it costs.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import polars as pl

from cnequity.adapters.eastmoney.capital import (
    NORTHBOUND_HISTORY_START,
    NORTHBOUND_LAST_PUBLISHED,
    _quarter_end_dates,
    fetch_block_trades,
    fetch_dragon_tiger,
    fetch_fund_flow,
    fetch_margin_trading,
    fetch_northbound_flows_range,
    fetch_northbound_holdings,
)
from cnequity.config import Config
from cnequity.domain.market_time import shanghai_now
from cnequity.orchestrator.registry import register_step
from cnequity.steps.common import BACKFILL_START, incremental_trade_dates, list_trading_dates
from cnequity.steps.http_common import (
    call_with_run_id,
    run_incremental_fetched,
    verify_raw_archive,
    write_fetched,
)

logger = logging.getLogger(__name__)

_MARGIN_FLUSH_DAYS = 63  # stage a parquet part roughly every quarter of fetched days
_MIN_MARGIN_SYMBOLS_PER_DAY = 50
_NORTHBOUND_SZ_START = date(2016, 12, 5)
# Stock Connect is closed on HK-only holidays that this codebase has no
# calendar for (see step_northbound_flows); that mismatch alone is on the
# order of a few percent of mainland trading days a year. Set well above
# that so it doesn't mask a real fetch failure, but low enough that a
# largely-empty response still trips it.
_NORTHBOUND_GAP_TOLERANCE = 0.15
_MIN_NORTHBOUND_HOLDING_ROWS_PER_PERIOD = 100
_MIN_NORTHBOUND_HOLDING_ROWS_PER_CHANNEL = 50


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
        return call_with_run_id(
            fetch_fn,
            d,
            pipeline_config=config,
            dataset=dataset,
            run_id=run_id,
            config=config,
        )

    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        dataset,
        _bound,
        source="eastmoney",
        allow_empty=allow_empty,
        raw_archive_evidence_factory=lambda: verify_raw_archive(
            config,
            dataset,
            run_id,
            source="eastmoney",
            request_scope=f"daily:{trade_date.isoformat()}",
        ),
    )


@register_step("fund_flow", group="capital", depends_on=["instruments"])
def step_fund_flow(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    return _run_capital_step(
        config, trade_date, run_id, "fund_flow", fetch_fund_flow, allow_empty=False
    )


@register_step("northbound_holdings", group="capital", depends_on=["instruments"])
def step_northbound_holdings(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if not config.sources.get("eastmoney", True):
        raise RuntimeError("northbound_holdings: eastmoney source disabled in config")
    # Quarterly since Aug 2024: daily refreshes the latest quarter. Backfill
    # walks all quarter-ends from 2016 but the EM report only serves the most
    # recent quarter(s) — historical TRADE_DATE filters return 0 rows (verified
    # 2026-07), so history accrues forward only, one quarter per disclosure.
    from cnequity.steps.http_common import write_fetched

    backfill = getattr(config, "_backfill", False)
    df = _validate_northbound_holdings_snapshot(
        fetch_northbound_holdings(trade_date, backfill=backfill, config=config)
    )
    missing_periods: list[str] = []
    if backfill:
        expected = set(
            _quarter_end_dates(
                trade_date,
                start=getattr(config, "_backfill_start", None),
                end=getattr(config, "_backfill_end", None),
            )
        )
        observed = (
            {value.isoformat() for value in df.get_column("trade_date").drop_nulls().to_list()}
            if not df.is_empty() and "trade_date" in df.columns
            else set()
        )
        missing_periods = sorted(expected - observed)
    if df.is_empty():
        if not backfill:
            raise RuntimeError(
                f"northbound_holdings: no rows returned for {trade_date.isoformat()}"
            )
        result: dict = {"rows_read": 0, "rows_written": 0}
    else:
        result = write_fetched(config, run_id, "northbound_holdings", df, source="eastmoney")
    if missing_periods:
        result["status"] = "warning"
        result["missing_periods"] = len(missing_periods)
        result["context_updates"] = {
            "audit_findings": [
                {
                    "dataset": "northbound_holdings",
                    "severity": "warning",
                    "check": "backfill_missing_quarters",
                    "message": (
                        f"northbound holdings missing {len(missing_periods)} requested "
                        f"quarter(s): {', '.join(missing_periods[:8])}"
                    ),
                    "missing_periods": missing_periods,
                }
            ]
        }
    return result


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

    # The report has both a fixed lower bound and a retirement date. Clamp the
    # request before touching the network: otherwise a retired feed is queried
    # in full on every daily run after its watermark freezes, and an explicit
    # pre-2014 backfill spends a request proving that the channel did not exist.
    start = max(start, NORTHBOUND_HISTORY_START)
    end = min(end, NORTHBOUND_LAST_PUBLISHED)
    if start > end:
        logger.info(
            "northbound_flows: requested window %s..%s is outside the published range "
            "%s..%s; skipping source request",
            start.isoformat(),
            end.isoformat(),
            NORTHBOUND_HISTORY_START.isoformat(),
            NORTHBOUND_LAST_PUBLISHED.isoformat(),
        )
        return {
            "rows_read": 0,
            "rows_written": 0,
            "note": "source window is outside northbound flow publication range",
        }

    df = fetch_northbound_flows_range(start, end, config=config)
    if df.is_empty():
        # Expected for any window past the cutoff — not a fetch failure, and
        # not something to zero-fill. The audit reports the frozen watermark.
        logger.info(
            "northbound_flows: no published rows in %s..%s", start.isoformat(), end.isoformat()
        )
        return {"rows_read": 0, "rows_written": 0}
    expected_dates = list_trading_dates(config, start, end)
    expected = {(day, "SH") for day in expected_dates if day >= NORTHBOUND_HISTORY_START}
    expected.update((day, "SZ") for day in expected_dates if day >= _NORTHBOUND_SZ_START)
    required_columns = {"trade_date", "channel"}
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        raise RuntimeError(
            "northbound_flows: response is missing required column(s): "
            + ", ".join(missing_columns)
        )
    observed = {
        (day, channel)
        for day, channel in df.select(["trade_date", "channel"]).iter_rows()
        if day is not None and channel is not None
    }
    missing = sorted(expected - observed)
    if missing:
        # `expected` is built from the mainland trading calendar only: there
        # is no Hong Kong / Stock Connect holiday calendar in this codebase.
        # Stock Connect trades only when both mainland exchanges and HKEX are
        # open, so a mainland trading day that is an HK-only holiday (Good
        # Friday, HKSAR Establishment Day, etc.) always looks "missing" here
        # even though the source published a genuinely complete range. That
        # mismatch is small and well known (a handful of HK-only holidays a
        # year); tolerate it, but still raise once the gap is far too large
        # for that explanation to plausibly cover, which is the signature of
        # a real fetch failure rather than a calendar mismatch.
        sample = ", ".join(f"{day.isoformat()}/{channel}" for day, channel in missing[:8])
        gap_ratio = len(missing) / len(expected)
        if gap_ratio > _NORTHBOUND_GAP_TOLERANCE:
            raise RuntimeError(
                "northbound_flows: incomplete published range; missing "
                f"{len(missing)} of {len(expected)} expected day/channel row(s) "
                f"({gap_ratio:.0%}, e.g. {sample})"
            )
        logger.warning(
            "northbound_flows: %s of %s expected day/channel row(s) absent from "
            "%s..%s (e.g. %s) - within HK-holiday tolerance, not raised",
            len(missing),
            len(expected),
            start.isoformat(),
            end.isoformat(),
            sample,
        )
    return write_fetched(config, run_id, "northbound_flows", df, source="eastmoney")


# --- margin_trading source selection -----------------------------------------
#
# `margin_trading` is not a vendor measurement: the exchanges aggregate what
# member firms report and publish the per-security detail themselves. Reading
# it from them removes a redistribution hop, and measured against curated
# EastMoney for 2026-08-26 it loses nothing — every one of margin_balance,
# margin_buy, short_balance and short_sell_volume matched exactly (0 bps) over
# the 3,522 shared symbols, while the exchanges carried 4,100 securities to
# EastMoney's 3,857.
#
# Two properties of the publishers shape the wiring:
#
# * **SSE does not publish 融券余额.** `short_balance` is therefore null on SH
#   rows (see the adapter). It is the one field EastMoney has and the exchange
#   does not, which is why `[margin_trading] source` can still select the
#   vendor path rather than this being a one-way replacement.
# * **The two publish on different lags.** Measured 2026-08-30, SSE had already
#   served 2026-08-28 while SZSE's export for that session was still
#   header-only; SZSE lands on the next business day. A day is therefore
#   written only when *both* exchanges have published it — a half-market day
#   would advance the watermark and strand the other half — so the dataset
#   trails the vendor path by about one session in exchange for authority.
_MARGIN_SOURCES = ("exchange", "eastmoney")
# Calendar days an unpublished day is tolerated before it becomes an error.
# The vendor path answers same-day, so 2 is right there; the exchanges add a
# business day, which a weekend stretches to three.
_MARGIN_EMPTY_GRACE_DAYS = {"eastmoney": 2, "exchange": 5}


def _margin_source(config: Config) -> str:
    """The configured `margin_trading` vendor, refusing a disabled one.

    Falling back on its own would be exactly the automatic source switching
    ADR-0003 rules out: the choice of who owns these rows is an operator's, and
    a silent switch would change what `source` means without anyone deciding it.
    """
    source = getattr(config, "margin_trading_source", "exchange")
    if source not in _MARGIN_SOURCES:
        raise RuntimeError(
            f"margin_trading: unknown source {source!r}; expected one of {_MARGIN_SOURCES}"
        )
    # Absent means enabled, as it does for every other ingest source. The
    # fail-closed default belongs to the optional publisher *checks* in
    # `quality/authority_checks.py`, which add network calls an operator opts
    # into; this one is a dataset's primary feed and cannot default to off.
    if not config.sources.get(source, True):
        raise RuntimeError(f"margin_trading: {source} source disabled in config")
    return source


def _fetch_margin_via_exchange(day: date, *, config: Config) -> pl.DataFrame:
    """Official SSE + SZSE margin detail, or empty until both have published."""
    from cnequity.adapters.exchange.margin_trading import fetch_exchange_margin_trading

    result = fetch_exchange_margin_trading(day, config=config)
    if result.covered != frozenset({"sse", "szse"}):
        # Not an error: SZSE lands a business day behind SSE. Returning empty
        # leaves the day unwritten, the watermark where it was, and the next
        # run picks it up complete.
        logger.info(
            "margin_trading: %s published by %s only; leaving the day for a later run (%s)",
            day.isoformat(),
            ",".join(sorted(result.covered)) or "neither exchange",
            result.failures,
        )
        return pl.DataFrame()
    return result.rows


def _margin_fetcher(source: str):
    """``(day, *, config) -> frame`` for the selected publisher."""
    if source == "exchange":
        return _fetch_margin_via_exchange
    return lambda day, *, config: fetch_margin_trading(day, config=config)


def _existing_margin_dates(
    config: Config,
    *,
    min_symbols: int = _MIN_MARGIN_SYMBOLS_PER_DAY,
) -> set[date]:
    root = config.curated_root / "margin_trading"
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        return set()
    scan = pl.scan_parquet(files)
    if "symbol" not in scan.collect_schema().names():
        return set()
    return set(
        scan.group_by("trade_date")
        .agg(pl.col("symbol").n_unique().alias("_symbol_count"))
        .filter(pl.col("_symbol_count") >= min_symbols)
        .select("trade_date")
        .collect()
        .get_column("trade_date")
        .to_list()
    )


def _margin_symbol_count(df: pl.DataFrame) -> int:
    if "symbol" not in df.columns:
        return 0
    return df.get_column("symbol").drop_nulls().n_unique()


def _validate_northbound_holdings_snapshot(df: pl.DataFrame) -> pl.DataFrame:
    """Reject a non-empty but obviously truncated quarterly holdings response.

    The report contains two independent exchange legs.  A total row floor is
    not enough: a response containing only SH rows can still exceed the floor
    while silently losing the whole SZ leg.
    """
    if df.is_empty():
        return df
    required = {"symbol", "trade_date", "channel"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(
            "northbound_holdings: response is missing required column(s): " + ", ".join(missing)
        )
    unique = df.unique(subset=["symbol", "trade_date", "channel"])
    counts = unique.group_by("trade_date").agg(pl.len().alias("_holding_rows"))
    channel_counts = unique.group_by("trade_date", "channel").agg(pl.len().alias("_channel_rows"))
    missing_channels: list[str] = []
    for row in counts.iter_rows(named=True):
        observed = set(
            unique.filter(pl.col("trade_date") == row["trade_date"])
            .get_column("channel")
            .drop_nulls()
            .to_list()
        )
        expected = {"SH"}
        if row["trade_date"] >= _NORTHBOUND_SZ_START:
            expected.add("SZ")
        missing = sorted(expected - observed)
        if missing:
            missing_channels.append(f"{row['trade_date']} missing {','.join(missing)}")

    incomplete_channels = channel_counts.filter(
        pl.col("_channel_rows") < _MIN_NORTHBOUND_HOLDING_ROWS_PER_CHANNEL
    )
    incomplete_totals = counts.filter(
        pl.col("_holding_rows") < _MIN_NORTHBOUND_HOLDING_ROWS_PER_PERIOD
    )
    if not incomplete_totals.is_empty() or not incomplete_channels.is_empty() or missing_channels:
        details = [
            f"{row['trade_date']} total={row['_holding_rows']}"
            for row in incomplete_totals.iter_rows(named=True)
        ]
        details.extend(
            f"{row['trade_date']} {row['channel']}={row['_channel_rows']}"
            for row in incomplete_channels.iter_rows(named=True)
        )
        details.extend(missing_channels)
        raise RuntimeError(
            "northbound_holdings: incomplete quarterly snapshot; each observed "
            f"period needs at least {_MIN_NORTHBOUND_HOLDING_ROWS_PER_PERIOD} "
            f"unique holding row(s), and each exchange channel needs at least "
            f"{_MIN_NORTHBOUND_HOLDING_ROWS_PER_CHANNEL} row(s) ({'; '.join(details)})"
        )
    return df


def _validate_margin_snapshot(df: pl.DataFrame, trade_date: date) -> pl.DataFrame:
    """Reject a non-empty margin response that is obviously truncated."""
    if df.is_empty():
        return df
    count = _margin_symbol_count(df)
    if count < _MIN_MARGIN_SYMBOLS_PER_DAY:
        raise RuntimeError(
            "margin_trading: incomplete daily snapshot; expected at least "
            f"{_MIN_MARGIN_SYMBOLS_PER_DAY} unique symbols on {trade_date.isoformat()}, "
            f"got {count}"
        )
    return df


def _backfill_margin_trading(config: Config, trade_date: date, run_id: str) -> dict:
    """Walk trading days fetching the configured margin report (history is served).

    Both publishers serve history: SSE takes ``detailsDate`` and SZSE takes
    ``txtDate``, so the exchange path backfills the same way the vendor one does.

    Resumable: days already in curated are skipped, so a killed sweep can be
    rerun. ``--start/--end`` on ``cne backfill`` bound the walk; parts are
    staged in chunks so progress survives mid-run failures via compact.
    ``--workers N`` fetches days concurrently.  Each worker owns its client,
    while every underlying EastMoney request still goes through the shared
    cross-process QPS and in-flight source limiter, so increasing this value
    cannot bypass the configured upstream budget.
    """
    from concurrent.futures import ThreadPoolExecutor

    from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
    from cnequity.domain.schemas import with_provenance
    from cnequity.steps.common import _backfill_empty_day_finding
    from cnequity.storage import StagingWriter

    source = _margin_source(config)
    fetch_day = _margin_fetcher(source)
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
    incomplete_days: list[tuple[date, int]] = []
    n_parts = 0

    def flush() -> None:
        nonlocal frames, total_rows, n_parts
        if not frames:
            return
        part = with_provenance(
            pl.concat(frames, how="diagonal_relaxed"), source=source, data_version="v1"
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
        if source != "eastmoney":
            # The exchange adapters are stateless; only the EM path needs a
            # per-thread client to reuse its authenticated session.
            return fetch_day(d, config=config)
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
                        if "trade_date" not in df.columns:
                            raise RuntimeError(
                                f"margin_trading: fetch for {d.isoformat()} did not return "
                                "the configured trade_date column"
                            )
                        parsed_dates = df.get_column("trade_date").cast(pl.Date, strict=False)
                        invalid = parsed_dates.is_null() | (parsed_dates != d).fill_null(True)
                        invalid_count = int(invalid.sum())
                        if invalid_count:
                            raise RuntimeError(
                                f"margin_trading: fetch for {d.isoformat()} returned "
                                f"{invalid_count} row(s) with a different or invalid trade_date"
                            )
                        symbol_count = _margin_symbol_count(df)
                        if symbol_count < _MIN_MARGIN_SYMBOLS_PER_DAY:
                            incomplete_days.append((d, symbol_count))
                            continue
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
    except Exception:
        # Preserve successful rows from the current concurrent chunk when a
        # later fetch or response validation fails; the next run can then
        # resume instead of replaying the whole chunk.
        flush()
        raise
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
    result = {
        "rows_read": total_rows,
        "rows_written": total_rows,
        "days_fetched": len(todo) - len(empty_days) - len(incomplete_days),
        "days_skipped": len(days) - len(todo),
        "days_empty": len(empty_days),
    }
    if incomplete_days:
        result["status"] = "warning"
        result["failed_days"] = len(incomplete_days)
        result.setdefault("context_updates", {})["audit_findings"] = [
            {
                "dataset": "margin_trading",
                "severity": "warning",
                "check": "backfill_incomplete_days",
                "message": (
                    f"margin_trading: {len(incomplete_days)} day(s) returned fewer than "
                    f"{_MIN_MARGIN_SYMBOLS_PER_DAY} unique symbols; rows were not staged"
                ),
                "days": [
                    {"trade_date": day.isoformat(), "symbols": count}
                    for day, count in incomplete_days
                ],
            }
        ]
    if empty_days:
        result.setdefault("context_updates", {})["audit_findings"] = [
            *(result.get("context_updates", {}).get("audit_findings") or []),
            _backfill_empty_day_finding("margin_trading", empty_days),
        ]
    if empty_days or incomplete_days:
        result.setdefault("status", "warning")
    return result


@register_step("margin_trading", group="capital", depends_on=["instruments"])
def step_margin_trading(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        return _backfill_margin_trading(config, trade_date, run_id)

    source = _margin_source(config)
    fetch = _margin_fetcher(source)
    grace = _MARGIN_EMPTY_GRACE_DAYS[source]

    def _fetch(day: date, *, config: Config) -> pl.DataFrame:
        frame = fetch(day, config=config)
        if frame.is_empty() and day < shanghai_now().date() - timedelta(days=grace):
            raise RuntimeError(f"margin_trading: no rows returned for {day.isoformat()}")
        return _validate_margin_snapshot(frame, day)

    return run_incremental_fetched(
        config,
        trade_date,
        run_id,
        "margin_trading",
        lambda d: _fetch(d, config=config),
        source=source,
        allow_empty=True,
    )


def _backfill_daily_report(
    config: Config, trade_date: date, run_id: str, dataset: str, fetch_fn, floor: date
) -> dict:
    """dragon_tiger / block_trades: each day's fetch works standalone and the
    daily step never walked a range through it — see ``walk_day_backfill``."""
    from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
    from cnequity.steps.common import walk_day_backfill

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
    from cnequity.adapters.exchange.dragon_tiger import fetch_dragon_tiger_exchange

    return _run_capital_step(
        config,
        trade_date,
        run_id,
        "dragon_tiger",
        _with_exchange_fallback("dragon_tiger", fetch_dragon_tiger, fetch_dragon_tiger_exchange),
    )


def _with_exchange_fallback(dataset: str, primary_fn, exchange_fn):
    """Read the vendor; if it cannot serve the session, read the exchanges.

    The failover lives at the fetch layer on purpose: provenance, watermarking
    and compaction all sit above it and need no knowledge of which route ran.
    Each row still carries its own `source`, so a degraded day is visible in
    the data rather than only in a log line.

    An empty vendor result counts as a failure worth trying the exchanges for.
    Block trades are genuinely sparse, so this can mean a second request on a
    quiet day — cheap, and the alternative is treating an outage as a quiet
    market, which is the mistake that makes a gap permanent.
    """

    def _fetch(day: date, **kwargs) -> pl.DataFrame:
        try:
            frame = primary_fn(day, **kwargs)
        except Exception as exc:  # noqa: BLE001 — that is what the backup is for
            logger.warning(
                "%s: eastmoney failed for %s (%s); trying the exchanges", dataset, day, exc
            )
            frame = None
        if frame is not None and not frame.is_empty():
            return frame
        if frame is not None:
            logger.info("%s: eastmoney returned nothing for %s; asking the exchanges", dataset, day)
        fallback = exchange_fn(day, config=kwargs.get("config"))
        if fallback.is_empty():
            return frame if frame is not None else fallback
        logger.warning(
            "%s %s: served by the exchanges, so Beijing names are absent from this session "
            "(see DatasetSpec.backup_gaps)",
            dataset,
            day,
        )
        return fallback

    return _fetch


@register_step("block_trades", group="signals", depends_on=["instruments"])
def step_block_trades(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    if getattr(config, "_backfill", False):
        # Confirmed live from 2010-01-04; older single-day probes were
        # ambiguous (block trades are sparse — a quiet day and "no report yet"
        # look identical), so this floor is the conservative, confirmed one.
        return _backfill_daily_report(
            config, trade_date, run_id, "block_trades", fetch_block_trades, date(2010, 1, 1)
        )
    from cnequity.adapters.exchange.block_trades import fetch_block_trades_exchange

    return _run_capital_step(
        config,
        trade_date,
        run_id,
        "block_trades",
        _with_exchange_fallback("block_trades", fetch_block_trades, fetch_block_trades_exchange),
    )


def snapshot_valuations_ths_official(
    config: Config,
    run_id: str,
    *,
    as_of: date | None = None,
    symbols: list[str] | None = None,
) -> dict:
    """Keep today's licensed valuation ratios, which nothing can reconstruct later.

    ``valuation_metrics`` holds 16,457,034 rows from baostock back to 2001, but
    they are restated — a ratio computed from today's share count and earnings,
    stamped with an old date. Right for comparing across time, wrong for asking
    what a screen would have seen on the day, which is why the dataset declares
    ``pit=none``.

    The upstream publishes no valuation history at all, only today. Run this
    daily and the snapshot store accumulates the other thing: a licensed record
    of what the market actually showed, one session at a time. It cannot reach a
    single day before the first run, so the sooner it starts the longer that
    record is.

    Verification class — writes to ``meta/source_snapshots``, never to curated.
    """
    from cnequity.adapters.ths_official import SOURCE as THS_SOURCE
    from cnequity.adapters.ths_official import client_from_config
    from cnequity.adapters.ths_official.valuations import fetch_valuation_snapshot
    from cnequity.domain.market_time import shanghai_today
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.steps.common import load_symbols
    from cnequity.storage.source_snapshots import SnapshotStore

    if not getattr(config, "ths_official_verify_enabled", True):
        return {"rows_written": 0, "status": "skipped", "reason": "verify off"}
    client = client_from_config(config)
    if client is None:
        return {"rows_written": 0, "status": "skipped", "reason": "no api key"}

    session = as_of or shanghai_today()
    try:
        wanted = symbols if symbols is not None else load_symbols(config)
        # One unknown code fails a whole 100-symbol request, so intersect with
        # the upstream's own code table first. Measured without it: 91 of 500
        # securities were rejected and recovering them by halving cost 525
        # requests for what should be five.
        carried = _ths_official_code_table(client)
        if carried:
            wanted = [symbol for symbol in wanted if symbol in carried]
        frame, counters = fetch_valuation_snapshot(wanted, client=client, as_of=session)
    finally:
        client.close()
    if frame.is_empty():
        return {"rows_written": 0, "status": "warning", "reason": "peer returned nothing"}

    SnapshotStore(config.meta_root).write(
        "valuation_metrics",
        with_provenance(
            frame, source=THS_SOURCE, data_version=data_version_for("valuation_metrics")
        ),
        source=THS_SOURCE,
        data_version=data_version_for("valuation_metrics"),
        run_id=run_id,
        trade_date=session,
    )
    return {"rows_written": frame.height, "trade_date": session.isoformat(), **counters}


def _ths_official_code_table(client) -> set[str]:
    """Every A-share code the upstream carries, or an empty set if it will not say."""
    carried: set[str] = set()
    offset = 0
    while True:
        try:
            data = client.get(
                "/api/meta/tickers/list", asset_type="a-share", limit=10000, offset=offset
            )
        except Exception:  # noqa: BLE001 — the filter is an optimisation, not a gate
            return set()
        page = (data or {}).get("item") or []
        carried.update(str(row["thscode"]) for row in page if row.get("thscode"))
        if len(page) < 10000:
            return carried
        offset += 10000
