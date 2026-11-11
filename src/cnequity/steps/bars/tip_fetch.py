"""Fetch the newest session from the exchanges, TDX quotes and the BSE."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.orchestrator.worker_pool import fetch_daily_bars_parallel
from cnequity.run_scope import backfill_of
from cnequity.steps.bars import staging
from cnequity.steps.bars._base import logger
from cnequity.steps.common import (
    list_trading_dates,
)


def _fetch_tip_via_exchange(
    config: Config, symbols: list[str], trade_date: date, run_id: str
) -> dict:
    """Stage the SSE auction snapshot; leave SZSE report totals to audit.

    SZSE's historical stock report includes turnover outside the intraday
    auction series. Its OHLC can arbitrate prices, but its volume/amount must
    not replace auction-based daily bars from TDX/Sina/THS. Keeping those SZ
    symbols uncovered lets the regular quote path fetch compatible quantities.
    """
    from cnequity.adapters.exchange.daily_quotes import fetch_exchange_daily_quotes
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage import StagingWriter

    empty = {"rows_read": 0, "rows_written": 0, "covered": set(), "source_outcomes": {}}
    if not config.sources.get("exchange", True) or not symbols:
        return empty
    wanted = set(symbols)
    try:
        result = fetch_exchange_daily_quotes(trade_date, config=config)
    except Exception as exc:  # noqa: BLE001 — the per-symbol path still owns these
        logger.warning(
            "exchange tip snapshot unavailable (%s: %s); SH/SZ tip falls back to TDX",
            type(exc).__name__,
            exc,
        )
        return {**empty, "source_outcomes": {"exchange": {"status": "failed", "requests": 2}}}

    # `covered` is the point of ExchangeQuotesResult: one exchange answering is
    # not the market answering, and a caller that ignores it reports a
    # SZSE-only snapshot as if it had covered SH too.
    if result.is_empty:
        if result.failures:
            logger.warning(
                "exchange tip snapshot returned nothing (%s); SH/SZ tip falls back to TDX",
                "; ".join(f"{k}: {v}" for k, v in sorted(result.failures.items())),
            )
        return {**empty, "source_outcomes": {"exchange": {"status": "empty", "requests": 2}}}
    if result.failures:
        logger.info(
            "exchange tip snapshot: %s did not answer; those symbols stay with TDX",
            ", ".join(sorted(result.failures)),
        )
    frame = result.quotes.filter(
        pl.col("symbol").is_in(sorted(wanted)) & pl.col("symbol").str.ends_with(".SH")
    )
    if frame.is_empty():
        return {**empty, "source_outcomes": {"exchange": {"status": "empty", "requests": 2}}}

    staged = with_provenance(frame, source="exchange", data_version=data_version_for("daily_bars"))
    StagingWriter(config.staging_root).write_batch(
        "daily_bars", run_id, "exchange-tip-0000", staged
    )
    covered = set(staged["symbol"].to_list())
    logger.info(
        "exchange tip snapshot staged %d SH/SZ bar(s) for %s from %s",
        staged.height,
        trade_date,
        ", ".join(sorted(result.covered)),
    )
    return {
        "rows_read": staged.height,
        "rows_written": staged.height,
        "covered": covered,
        "source_outcomes": {"exchange": {"status": "success", "requests": 2}},
    }


def _fetch_tip_via_tdx_quotes(
    config: Config, symbols: list[str], trade_date: date, run_id: str
) -> dict:
    """Stage the tip session for *symbols* from TDX batch quotes.

    Quotes carry no date: they describe the session whose close has passed and
    whose successor has not opened (``last_closed_session``); any other time
    this returns nothing and the per-symbol sweep owns the tip. Rows are
    labelled ``tdx_protocol_quote`` — same vendor, quote route — so the weekly
    per-symbol deep sweep reconciles them like any other TDX row. A symbol that
    did not trade or did not come back stays with the sweep.
    """
    from cnequity.domain.market_time import last_closed_session
    from cnequity.domain.rate_limit import wait_spec
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage import StagingWriter

    empty = {"rows_read": 0, "rows_written": 0, "covered": set()}
    if not symbols or not getattr(config, "tdx_enabled", True):
        return empty
    if last_closed_session() != trade_date:
        return empty
    from cnequity.adapters.tdx_protocol.client import _connect_with_retry
    from cnequity.adapters.tdx_protocol.quotes import quote_bars

    spec = config.tdx_rate_limit_spec()
    try:
        client = _connect_with_retry(config)
        try:
            rows = quote_bars(client, symbols, trade_date, pace=lambda: wait_spec(spec))
        finally:
            client.close()
    except Exception as exc:  # noqa: BLE001 — the per-symbol sweep still owns these
        logger.warning(
            "TDX quote tip unavailable (%s: %s); per-symbol sweep", type(exc).__name__, exc
        )
        return empty
    if not rows:
        return empty
    frame = pl.DataFrame(
        list(rows.values()),
        schema={
            "symbol": pl.Utf8,
            "trade_date": pl.Date,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "pre_close": pl.Float64,
            "volume": pl.Int64,
            "amount": pl.Float64,
        },
    )
    staged = with_provenance(
        frame, source="tdx_protocol_quote", data_version=data_version_for("daily_bars")
    )
    StagingWriter(config.staging_root).write_batch(
        "daily_bars", run_id, "tdx-quote-tip-0000", staged
    )
    logger.info(
        "TDX quote tip staged %d of %d bar(s) for %s; the rest stay with the per-symbol sweep",
        staged.height,
        len(symbols),
        trade_date,
    )
    return {"rows_read": staged.height, "rows_written": staged.height, "covered": set(rows)}


def _bj_history_covered(
    config: Config, run_id: str, symbols: list[str], start: date, end: date
) -> set[str]:
    """Which of *symbols* this run has whole staged evidence for over the window."""
    import polars as pl

    from cnequity.storage import StagingWriter

    files = StagingWriter(config.staging_root).list_run_files("daily_bars", run_id)
    if not files:
        return set()
    staged = set(
        pl.scan_parquet([str(f) for f in files])
        .filter(
            (pl.col("trade_date") >= start)
            & (pl.col("trade_date") <= end)
            & pl.col("symbol").is_in(symbols)
        )
        .select("symbol")
        .unique()
        .collect()["symbol"]
        .to_list()
    )
    return staged - staging._staged_daily_bar_partial_symbols(
        config, run_id, list(symbols), start, end
    )


def _fetch_bj_history_via_tdx(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
    run_id: str,
    *,
    reserve_tip: bool,
) -> dict:
    """Beijing daily history from TDX, which serves it under market id 2.

    Sina answers one symbol for one session per request — the whole board over
    five sessions is ~2,900 of them, which is what earns HTTP 456 and a
    vendor-wide cooldown. TDX answers a symbol's whole range in one call, so
    the same work is one request per symbol.

    Measured against what the lake already held: open/high/low/close agree to
    the last digit — 3,086 rows over 344 securities against BSE/Sina/THS, and
    2,048 rows over 120 securities against Sina alone, with no exception in
    either. Two differences, both in TDX's favour on balance:

    - ``amount``: Sina publishes none for Beijing, so all 505,518 Sina rows in
      the lake carry a null turnover. TDX fills it, agreeing with BSE to
      0.00003%.
    - ``volume``: TDX reports it in lots. So did Sina for 2019 through 2025 —
      every one of those rows is a multiple of 100 — so the history gains
      nothing coarser than it already had. Only from 2026 is Sina finer (86%
      lots), and there TDX lands a median 50 shares away, one row in 2,048
      further than 100.

    ``reserve_tip`` keeps the current session on the BSE snapshot regardless,
    which publishes exact shares and its own turnover. TDX fills only the
    history behind it, where the alternative was Sina's cost or nothing at all.
    """
    sessions = list_trading_dates(config, start, end)
    if reserve_tip:
        sessions = [day for day in sessions if day < end]
    if not symbols or not sessions:
        return {"rows_read": 0, "rows_written": 0, "covered": set(), "requested": False}
    lo, hi = sessions[0], sessions[-1]
    result = fetch_daily_bars_parallel(config, list(symbols), lo, hi, run_id, "daily_bars")
    failed = set(result.get("failed_symbols") or [])
    covered = _bj_history_covered(config, run_id, list(symbols), lo, hi) - failed
    logger.info(
        "Beijing history via TDX: %d/%d symbol(s) answered over %s..%s (%d rows)",
        len(covered),
        len(symbols),
        lo,
        hi,
        int(result.get("rows_written", 0)),
    )
    return {
        "rows_read": int(result.get("rows_read", 0)),
        "rows_written": int(result.get("rows_written", 0)),
        "covered": covered,
        "requested": True,
    }


# Above this the scope is no longer a repair and the board-wide guard applies.
_BJ_SCOPED_WINDOW_MAX_SYMBOLS = 60


def _bj_history_start(config: Config, start: date, end: date) -> date:
    """Window start for the per-symbol Beijing backstop.

    The dataset's 5-session reconciliation lookback exists for vendors that
    revise settled rows. Running it through Sina costs one request per symbol
    per session — ~2,900 a day for the Beijing board — and that is what earns
    HTTP 456 and a vendor-wide cooldown that then strands unrelated symbols.
    The tip comes from the BSE board snapshot instead, so this only decides how
    far the per-symbol backstop reaches behind it.
    """
    sessions = [day for day in list_trading_dates(config, start, end) if day <= end]
    if not sessions:
        return end
    # A scoped repair is not the daily sweep. The lookback guards the whole
    # board — 580 symbols over five sessions — but truncating an explicit
    # `--symbols` window to its last session is what left fourteen securities
    # owing 225 sessions that the repair kept declining to fetch. The scope
    # itself bounds the cost, so honour the window the caller asked for.
    scoped = list(backfill_of(config).symbol_list() or [])
    if scoped and len(scoped) <= _BJ_SCOPED_WINDOW_MAX_SYMBOLS:
        return sessions[0]
    lookback = max(int(getattr(config, "bj_history_lookback_days", 1) or 1), 1)
    return sessions[max(0, len(sessions) - lookback)]


def _fetch_bj_tip_via_bse(
    config: Config, symbols: list[str], trade_date: date, run_id: str
) -> dict:
    """Stage the whole Beijing board's tip from the exchange's own snapshot.

    One paginated sweep (~30 requests) replaces one request per symbol (~580),
    and it comes from the exchange rather than a third party. BSE publishes a
    *current* snapshot: `fetch_daily_quotes` returns nothing when its session
    is not `trade_date`, so this can never stamp today's prices onto an older
    session. Anything it does not return stays with the Sina backstop.
    """
    from cnequity.adapters.bse.daily_quotes import fetch_daily_quotes
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.storage import StagingWriter

    if not config.sources.get("bse", True) or not symbols:
        return {"rows_read": 0, "rows_written": 0, "covered": set()}
    try:
        quotes = fetch_daily_quotes(trade_date, symbols=set(symbols), config=config)
    except Exception as exc:  # noqa: BLE001 — Sina still backs this leg
        logger.warning(
            "BSE tip snapshot unavailable (%s: %s); Beijing tip falls back to Sina",
            type(exc).__name__,
            exc,
        )
        return {
            "rows_read": 0,
            "rows_written": 0,
            "covered": set(),
            "source_outcomes": {"bse": {"status": "failed", "requests": 1}},
        }
    if quotes is None or quotes.is_empty():
        return {
            "rows_read": 0,
            "rows_written": 0,
            "covered": set(),
            "source_outcomes": {"bse": {"status": "empty", "requests": 1}},
        }
    staged = with_provenance(quotes, source="bse", data_version=data_version_for("daily_bars"))
    StagingWriter(config.staging_root).write_batch("daily_bars", run_id, "bse-tip-0000", staged)
    covered = set(staged["symbol"].to_list())
    logger.info("BSE tip snapshot staged %d Beijing bar(s) for %s", staged.height, trade_date)
    return {
        "rows_read": staged.height,
        "rows_written": staged.height,
        "covered": covered,
        "source_outcomes": {"bse": {"status": "success", "requests": 1}},
    }
