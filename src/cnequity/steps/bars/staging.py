"""What a run has already staged, by symbol and by missing key."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.steps.bars import bar_scope
from cnequity.steps.common import (
    list_trading_dates,
    load_curated_trading_status,
)
from cnequity.storage.fund_no_print import approved_no_print_keys


def _staged_daily_bar_symbols(
    config: Config,
    run_id: str,
    trade_date: date | None,
    *,
    start: date | None = None,
    end: date | None = None,
) -> set[str]:

    from cnequity.storage import StagingWriter

    files = StagingWriter(config.staging_root).list_run_files("daily_bars", run_id)
    if not files:
        return set()
    lf = pl.scan_parquet([str(f) for f in files]).select("symbol", "trade_date")
    if trade_date is not None:
        lf = lf.filter(pl.col("trade_date") == trade_date)
    if start is not None:
        lf = lf.filter(pl.col("trade_date") >= start)
    if end is not None:
        lf = lf.filter(pl.col("trade_date") <= end)
    return set(lf.select("symbol").unique().collect()["symbol"].to_list())


def _staged_daily_bar_partial_symbols(
    config: Config,
    run_id: str,
    symbols: list[str],
    start: date,
    end: date,
) -> set[str]:
    """Find symbols with a missing expected session in a staged window.

    A symbol can legitimately have no rows before listing, after delisting, or
    during a suspension. Instrument metadata narrows the expected range for
    the first two cases; when metadata is unavailable, use the requested range
    so a vendor response cannot hide a missing leading or trailing session.
    """
    if start >= end or not symbols:
        return set()
    return {
        symbol
        for symbol, _day in _staged_daily_bar_missing_keys(config, run_id, symbols, start, end)
    }


def _staged_daily_bar_missing_keys(
    config: Config,
    run_id: str,
    symbols: list[str],
    start: date,
    end: date,
) -> set[tuple[str, date]]:
    """Return missing session keys for symbols that have partial evidence."""
    if start >= end or not symbols:
        return set()

    from cnequity.storage import StagingWriter

    files = StagingWriter(config.staging_root).list_run_files("daily_bars", run_id)
    if not files:
        return set()
    staged = (
        pl.scan_parquet([str(f) for f in files])
        .filter(
            (pl.col("trade_date") >= start)
            & (pl.col("trade_date") <= end)
            & pl.col("symbol").is_in(symbols)
        )
        .select("symbol", "trade_date")
        .unique()
        .collect()
    )
    if staged.is_empty():
        return set()

    sessions = list_trading_dates(config, start, end)
    observed_symbols = set(staged["symbol"].to_list())
    missing: set[tuple[str, date]] = set()
    metadata = bar_scope._instrument_spans(config)
    from cnequity.domain.trading_status import independent_suspension_keys

    suspended: set[tuple[str, date]] = set()
    status = load_curated_trading_status(
        config,
        start=start,
        end=end,
        symbols=sorted(observed_symbols),
    )
    if status is not None:
        suspended = set(independent_suspension_keys(status).iter_rows())
    no_print = approved_no_print_keys(config, start=start, end=end)
    # Suspensions this run learned from the vendor are staged but not yet
    # curated, so they are not in `suspended` and would otherwise be
    # condemned here as interior gaps. Only this run's: a Config reused for a
    # later run must not excuse that run's gaps with an earlier run's answer.
    learned = getattr(config, "_learned_suspensions", None)
    learned_suspensions: set[tuple[str, date]] = (
        learned["keys"]
        if isinstance(learned, dict) and learned.get("run_id") == run_id and learned.get("keys")
        else set()
    )
    observed = staged.group_by("symbol").agg(pl.col("trade_date").unique().alias("dates"))
    for row in observed.iter_rows(named=True):
        span = metadata.get(row["symbol"], (None, None, None))
        list_date, delist_date = span[:2]
        expected_start = max(start, list_date) if list_date is not None else start
        expected_end = min(end, delist_date) if delist_date is not None else end
        expected = {session for session in sessions if expected_start <= session <= expected_end}
        missing.update(
            (row["symbol"], day)
            for day in expected - set(row["dates"])
            if (row["symbol"], day) not in suspended
            and (row["symbol"], day) not in learned_suspensions
            and (row["symbol"], day) not in no_print
        )
    # A symbol with no rows at all is handled by the explicit no-data/unknown
    # classifier.  This helper is specifically the interior partial-evidence
    # gate and must not turn a whole-symbol empty response into a duplicate
    # error path.
    return {key for key in missing if key[0] in observed_symbols}
