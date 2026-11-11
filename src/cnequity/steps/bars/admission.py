"""Late admissions, unresolved-key budgets and certified no-data evidence."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, timedelta

import polars as pl

from cnequity.config import Config
from cnequity.domain.symbols import (
    filter_ingest_universe,
    in_ingest_universe,
    parse_symbol,
)
from cnequity.steps.bars import bar_scope
from cnequity.steps.bars._base import logger
from cnequity.steps.common import (
    DailyBarOwnership,
    classify_daily_bar_ownership,
    instrument_metadata,
    list_trading_dates,
    load_curated_trading_status,
    load_negative_evidence,
    record_negative_evidence,
)
from cnequity.storage.fund_no_print import approved_no_print_keys
from cnequity.storage.state import StateStore


def _owed_keys_for_symbols(
    config: Config, symbols: Iterable[str], start: date, end: date
) -> set[tuple[str, date]]:
    """The (symbol, session) keys a whole-symbol gap actually owes.

    Clipped to each symbol's own listing window. Owing the full sweep window
    for a symbol listed halfway through it would record sessions that never
    existed, and a debt nothing can ever pay off is worse than no ledger: it
    would sit at the same weight as a real one and drown it.
    """
    spans = bar_scope._instrument_spans(config)
    sessions = list_trading_dates(config, start, end)
    owed: set[tuple[str, date]] = set()
    for symbol in symbols:
        listed, delisted, _asset = spans.get(symbol, (None, None, None))
        first = max(start, listed) if listed else start
        last = min(end, delisted) if delisted else end
        owed |= {(symbol, day) for day in sessions if first <= day <= last}
    return owed


def in_ingest_universe_symbol(symbol: str, config: Config) -> bool:
    """Whether *symbol* belongs to this lake's configured ingest universe."""
    return bool(filter_ingest_universe([symbol], config.ingest_universe))


def _in_bar_universe(symbol: str, universe: str) -> bool:
    try:
        info = parse_symbol(symbol)
    except Exception:  # noqa: BLE001 — an unparseable symbol is simply not ours
        return False
    return in_ingest_universe(info.code, info.exchange, universe)


_LATE_ADMISSION_LOOKBACK_DAYS = 365


def _record_late_admissions(config: Config, run_id: str, symbols: Iterable[str], end: date) -> int:
    """Sessions a security traded before the universe knew about it.

    A security listed after the last instrument refresh is in no sweep until
    that refresh picks it up, and the watermark then moves past the sessions it
    missed, so no incremental run asks for them again. The interior-gap ledger
    does not catch it either: those keys were never expected.

    Measured on 2026-09-17: thirteen BSE securities listed between 07-22 and
    09-04 all took their first bar on 09-07, short 366 sessions between them,
    every one of which TDX still served. Recording them makes `cne backfill
    daily_bars --outstanding` the one way to work this off too.

    Bounded to a year of listings so the scan stays proportional to what can
    plausibly still be owed.
    """
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(root):
        return 0
    horizon = end - timedelta(days=_LATE_ADMISSION_LOOKBACK_DAYS)
    spans = bar_scope._instrument_spans(config)
    universe = getattr(config, "ingest_universe", "all_a")
    recent = {
        symbol: listed
        for symbol in symbols
        # `daily_bars` does not carry the ETF and LOF quote codes that
        # `instruments` also lists. Owing their sessions would put debts on the
        # ledger that no repair can ever pay off, at the same weight as the
        # real ones — the caller's universe is not trusted to have filtered.
        if _in_bar_universe(symbol, universe)
        and (listed := spans.get(symbol, (None, None, None))[0]) is not None
        and horizon <= listed < end
    }
    if not recent:
        return 0
    first_bar = (
        scan_parquet_root(root, partition_col="trade_date", start=horizon, end=end)
        .filter(pl.col("symbol").is_in(list(recent)))
        .group_by("symbol")
        .agg(pl.col("trade_date").min().alias("first_bar"))
        .collect()
    )
    seen = dict(zip(first_bar["symbol"], first_bar["first_bar"], strict=True))
    owed: set[tuple[str, date]] = set()
    for symbol, listed in recent.items():
        # No bar at all still owes from the listing: the security may have been
        # admitted today and simply not swept yet.
        cutoff = seen.get(symbol, end + timedelta(days=1))
        if cutoff <= listed:
            continue
        owed |= _owed_keys_for_symbols(config, [symbol], listed, cutoff - timedelta(days=1))
    if not owed:
        return 0
    recorded = StateStore(config.meta_root).record_outstanding_keys(
        "daily_bars", owed, run_id=run_id, reason="late_admission"
    )
    logger.warning(
        "%d securit(y/ies) listed before the universe carried them owe %d session(s) "
        "(`cne backfill daily_bars --outstanding`)",
        len({symbol for symbol, _ in owed}),
        recorded,
    )
    return recorded


def _unresolved_budget(config: Config, expected: int, *, tip: bool = False) -> int:
    """Coverage warning budget, rounded down for the requested key scope.

    The budget is diagnostic. Independent validated facts can publish even
    above it; every unresolved key remains in the durable repair ledger.
    """
    knob = "daily_bars_tip_unresolved_tolerance" if tip else "daily_bars_unresolved_tolerance"
    fraction = float(getattr(config, knob, 0.0) or 0.0)
    if fraction <= 0 or expected <= 0:
        return 0
    return int(expected * fraction)


def _unresolved_key_remedy(
    config: Config,
    run_id: str,
    unknown: set[str] | list[str],
    start: date,
    end: date,
) -> str:
    """What to do about keys no source would resolve.

    The terminal error is the only thing most operators read, and these three
    gates gave a count and nothing to act on: not which keys, not which vendor
    was down, not which command resumes the run. All of it is known here — the
    findings file already holds the per-key reasons — so say it once, in the
    message that actually reaches the terminal.
    """
    findings_path = config.meta_root / "quality" / "findings" / f"{run_id}.json"
    keys = sorted(unknown)
    sample = ",".join(keys[:3]) + (",..." if len(keys) > 3 else "")
    return (
        f"\n  Full list and per-key reasons: {findings_path}"
        "\n  Which vendor is down: cne sources probe"
        f"\n  Resume this run once it is back: cne run retry --run-id {run_id}"
        f"\n  Or repair just these keys: cne backfill daily_bars --symbols {sample} "
        f"--start {start} --end {end}"
    )


def _certify_missing_daily_symbols(
    config: Config,
    symbols: set[str],
    start: date,
    end: date,
    *,
    explicit_no_data: set[str] | None = None,
    source_empty: set[str] | None = None,
) -> tuple[set[str], set[str], DailyBarOwnership]:
    """Split missing keys into evidenced no-data and strict unknown keys.

    ``explicit_no_data`` comes from the pre-fetch ownership classifier or from
    the gap-fill chain after at least two independent per-symbol sources both
    returned a successful empty response. ``source_empty`` therefore contains
    only already-arbitrated empties, never a raw single-vendor absence.
    Transport failures, rate limits, publisher-file omissions, and partial
    status snapshots stay unknown. There is no market-size based allowance.
    """
    requested = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
    if not requested:
        return set(), set(), DailyBarOwnership()
    metadata = instrument_metadata(config)
    spans = {
        row["symbol"]: (row["list_date"], row["delist_date"], row.get("asset_type"))
        for row in metadata.iter_rows(named=True)
    }
    status = load_curated_trading_status(
        config,
        start=start,
        end=end,
        symbols=sorted(requested),
    )
    sessions = list_trading_dates(config, start, end)
    evidence = load_negative_evidence(config, "daily_bars", metadata=metadata)
    ownership = classify_daily_bar_ownership(
        sorted(requested),
        spans,
        start,
        end,
        trading_status=status,
        trading_sessions=sessions,
        negative_evidence=evidence,
        no_print_keys=approved_no_print_keys(config, start=start, end=end),
    )
    certified = {
        str(symbol).strip().upper() for symbol in (explicit_no_data or ()) if str(symbol).strip()
    }
    certified.update(
        str(symbol).strip().upper() for symbol in (source_empty or ()) if str(symbol).strip()
    )
    certified.update(ownership.expected_no_data)
    certified.intersection_update(requested)
    unknown = requested - certified
    return certified, unknown, ownership


def _record_daily_negative_observations(
    config: Config,
    symbols: set[str],
    start: date,
    end: date,
    *,
    reason: str,
    source: str,
) -> None:
    """Write source-empty observations after the final missing-key split."""
    if symbols:
        record_negative_evidence(
            config,
            "daily_bars",
            symbols,
            start,
            end,
            reason=reason,
            source=source,
        )


def _record_certified_daily_no_data(
    config: Config,
    certified: set[str],
    source_empty: set[str],
    ownership: DailyBarOwnership,
    start: date,
    end: date,
) -> None:
    """Persist fresh, symbol-scoped no-data proofs without extending cache TTL.

    ``ownership.negative_cached`` contains claims merely reused from the
    persistent cache.  Re-saving those claims on every run would turn a TTL
    into a permanent suppression.  Listing/status proofs and fresh upstream
    empty responses are new observations and may refresh their own bounded
    evidence records.
    """
    fresh = set(certified) - set(ownership.negative_cached)
    empty = fresh & set(source_empty)
    if empty:
        _record_daily_negative_observations(
            config,
            empty,
            start,
            end,
            reason="source_empty",
            source="fallback",
        )
    verified = fresh - empty
    if verified:
        _record_daily_negative_observations(
            config,
            verified,
            start,
            end,
            reason="verified_no_data",
            source="instruments_or_trading_status",
        )
