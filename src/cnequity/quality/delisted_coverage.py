"""Disk-only historical-universe evidence, independent of ingestion steps."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_lazy_by_primary_key
from cnequity.domain.symbols import is_all_a_symbol, is_cdr_symbol, issued_code_space, parse_symbol
from cnequity.query.universe import coverage_end_date
from cnequity.storage.instrument_catalog import load_curated_instruments
from cnequity.storage.read_context import ReadContext, read_root

_CATALOG_FILE = "delisted_catalog.json"
_IDENTITY_CLAIM = "delisted_security_identity"
_IDENTITY_EVIDENCE_VERSION = 1
LIVE_RECENCY_DAYS = 30


def catalog_path(config: Config) -> Path:
    return config.meta_root / "state" / _CATALOG_FILE


def _read_catalog(config: Config) -> dict:
    path = catalog_path(config)
    if not path.exists():
        return {"delisted": {}, "never_issued": [], "version": 1}
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.setdefault("delisted", {})
    payload.setdefault("never_issued", [])
    return payload


def _reference_date(config: Config, read_context: ReadContext | None = None) -> date:
    """The market's latest session, as the lake sees it."""
    from cnequity.domain.market_time import shanghai_today

    return coverage_end_date(config, "daily_bars", read_context=read_context) or shanghai_today()


def classify_catalog(
    config: Config, read_context: ReadContext | None = None
) -> tuple[dict[str, date], dict[str, date]]:
    """Split the swept catalogue into (delisted, live-but-missing)."""
    raw = _read_catalog(config)["delisted"]
    reference = _reference_date(config, read_context)
    cutoff = reference - timedelta(days=LIVE_RECENCY_DAYS)
    # A probe can be stale even when it is just outside the calendar-day
    # recency window (this happened for the BJ snapshot recovered on 2026-08-21).
    # If the current security master still has no formal delist date and the
    # lake contains a positive-volume bar after the probe terminal, that is
    # direct evidence of a listed/missing instrument, not a delisting. Use the
    # bar span as a stronger read-time correction so repair and coverage never
    # write a delist_date for a currently trading symbol.
    instrument_dates: dict[str, date | None] = {}
    instrument_list_dates: dict[str, date | None] = {}
    instruments = load_curated_instruments(config, read_context)
    if instruments is not None and {"symbol", "delist_date"} <= set(instruments.columns):
        instrument_dates = {
            row["symbol"]: row["delist_date"]
            for row in instruments.select("symbol", "delist_date").iter_rows(named=True)
        }
    if instruments is not None and {"symbol", "list_date"} <= set(instruments.columns):
        instrument_list_dates = {
            row["symbol"]: row["list_date"]
            for row in instruments.select("symbol", "list_date").iter_rows(named=True)
        }
    candidate_symbols = [
        symbol
        for symbol, value in raw.items()
        if instrument_dates.get(symbol) is None and date.fromisoformat(value) < reference
    ]
    observed_spans = _bar_spans(
        config,
        candidate_symbols,
        positive_volume_only=True,
        start=reference - timedelta(days=LIVE_RECENCY_DAYS),
        end=reference,
        read_context=read_context,
    )
    delisted: dict[str, date] = {}
    live: dict[str, date] = {}
    for sym, value in raw.items():
        last = date.fromisoformat(value)
        observed = observed_spans.get(sym)
        listed_on = instrument_list_dates.get(sym)
        if instrument_dates.get(sym) is None and observed is not None and observed[1] > last:
            live[sym] = observed[1]
        elif listed_on is not None and listed_on >= last:
            # A code cannot stop trading before it starts. The Sina probe answers
            # for an issued-but-not-yet-trading code exactly as it does for one
            # that stopped, so a fresh listing swept days before its first
            # session looks delisted: 11 stored rows carried a delist_date
            # earlier than their own list_date, all stamped 2026-09-01 against
            # list dates of 2026-09-02..09-07. The security master settles it.
            live[sym] = last
        else:
            (delisted if last < cutoff else live)[sym] = last
    return delisted, live


def load_delisted_catalog(
    config: Config, read_context: ReadContext | None = None
) -> dict[str, date]:
    """Symbols that genuinely stopped trading -> their last trading date."""
    return classify_catalog(config, read_context)[0]


def load_live_missing(config: Config, read_context: ReadContext | None = None) -> dict[str, date]:
    """Symbols still trading that the lake's instrument list does not carry.

    Not a survivorship problem — a coverage hole. These need adding to the daily
    pipeline, not a historical backfill of a dead name.
    """
    return classify_catalog(config, read_context)[1]


def renamed_symbols() -> frozenset[str]:
    """Codes the exchange retired by renaming, not by delisting.

    The BSE renumbered 248 securities to 920xxx in 2025. Each one's old code
    stops returning bars on 2025-09-30, which is exactly what a probe sees when
    a security delists — so all 248 were catalogued as delistings on one day
    and went on to become 248 of the 343 rows in `delisting_events`. Anything
    studying how listings end was reading mostly renames.

    A probe cannot tell the two apart. The exchange publishes the mapping and
    the repo already carries it, so the answer is looked up rather than
    inferred.
    """
    from cnequity.adapters.eastmoney.corporate_actions_migration import _code_mapping

    return frozenset(f"{old}.BJ" for old in _code_mapping())


def _in_historical_universe(symbol: str, universe: str) -> bool:
    """Whether *symbol* belongs to a supported historical research universe."""
    if universe not in {"all_a", "all_a_sh_sz"}:
        raise ValueError(f"unsupported historical universe: {universe!r}")
    try:
        info = parse_symbol(symbol)
    except ValueError:
        return False
    if not is_all_a_symbol(info.code, info.exchange) or is_cdr_symbol(info.code, info.exchange):
        return False
    return universe == "all_a" or info.exchange in {"SH", "SZ"}


def _identity_evidence_path(config: Config) -> Path:
    return (
        config.meta_root
        / "quality"
        / "evidence"
        / _IDENTITY_CLAIM
        / f"baostock-v{_IDENTITY_EVIDENCE_VERSION}.json"
    )


def _known_listing_dates(config: Config) -> dict[str, date]:
    """IPO dates from the complete vendor identity observation, never first bars."""
    try:
        payload = json.loads(_identity_evidence_path(config).read_text())
        if (
            payload.get("claim") != _IDENTITY_CLAIM
            or payload.get("evidence_version") != _IDENTITY_EVIDENCE_VERSION
            or payload.get("status") != "complete"
            or payload.get("source") != "baostock.query_stock_basic"
        ):
            return {}
        return {k: date.fromisoformat(v) for k, v in payload.get("listing_dates", {}).items()}
    except (OSError, ValueError, TypeError):
        return {}


def _verified_suspended_windows(
    config, symbols, start, end, listing_dates, read_context: ReadContext | None = None
):
    """Require independent suspension evidence for every expected open session."""
    if not symbols:
        return {}
    from cnequity.query.parquet_scan import scan_parquet_root

    roots = [
        read_root(config, name, read_context) for name in ("trading_calendar", "trading_status")
    ]
    if any(not root.exists() or not any(root.rglob("*.parquet")) for root in roots):
        return {}
    calendar = scan_parquet_root(
        roots[0], partition_col="trade_date", start=start, end=end, committed=False
    ).collect()
    if not {"trade_date", "is_trading"} <= set(calendar.columns):
        return {}
    expected_calendar = {start + timedelta(days=i) for i in range((end - start).days + 1)}
    if (
        not expected_calendar <= set(calendar["trade_date"].to_list())
        or calendar["is_trading"].null_count()
    ):
        return {}
    days = set(calendar.filter(pl.col("is_trading"))["trade_date"].to_list())
    if not days:
        return {}
    status = dedupe_lazy_by_primary_key(
        scan_parquet_root(
            roots[1],
            partition_col="trade_date",
            start=start,
            end=end,
            symbols=symbols,
            committed=False,
        ),
        "trading_status",
    ).collect()
    if not {"source", "is_trading", "status"} <= set(status.columns):
        return {}
    status = status.filter(
        (pl.col("source") == "baostock")
        & (~pl.col("is_trading"))
        & (pl.col("status") == "suspended")
    )
    evidence = {}
    for symbol in symbols:
        expected = {d for d in days if d >= listing_dates.get(symbol, start)}
        observed = set(status.filter(pl.col("symbol") == symbol)["trade_date"].to_list())
        if expected and expected <= observed:
            evidence[symbol] = {
                "symbol": symbol,
                "sessions": len(expected),
                "source": "baostock",
                "start": str(min(expected)),
                "end": str(max(expected)),
            }
    return evidence


def known_delisted_instruments(config: Config, as_of: date) -> dict[str, date]:
    """Formal delisting identity from source evidence, independent of probes."""
    path = _identity_evidence_path(config)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if (
            payload.get("claim") != _IDENTITY_CLAIM
            or payload.get("evidence_version") != _IDENTITY_EVIDENCE_VERSION
            or payload.get("status") != "complete"
            or payload.get("source") != "baostock.query_stock_basic"
        ):
            return {}
        evidence = {
            symbol: date.fromisoformat(value)
            for symbol, value in payload.get("delisted_symbols", {}).items()
        }
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}
    return {symbol: value for symbol, value in evidence.items() if value <= as_of}


def _bar_spans(
    config: Config,
    symbols: list[str],
    *,
    positive_volume_only: bool = True,
    start: date | None = None,
    end: date | None = None,
    read_context: ReadContext | None = None,
) -> dict[str, tuple[date, date]]:
    """``symbol -> (first/last traded date)`` for existing daily bars."""
    if not symbols:
        return {}
    root = read_root(config, "daily_bars", read_context)
    if not root.exists() or not any(root.rglob("*.parquet")):
        return {}
    from cnequity.query.parquet_scan import scan_parquet_root

    bars = scan_parquet_root(
        root,
        partition_col="trade_date",
        hive=False,
        traded_only=positive_volume_only,
        start=start,
        end=end,
        symbols=symbols,
        dataset="daily_bars",
        committed=False,
    )
    frame = (
        bars.group_by("symbol")
        .agg(
            pl.col("trade_date").min().alias("first"),
            pl.col("trade_date").max().alias("last"),
        )
        .collect()
    )
    return {r["symbol"]: (r["first"], r["last"]) for r in frame.iter_rows(named=True)}


def delisted_coverage_report(
    config: Config,
    start: date,
    end: date | None = None,
    *,
    sample: int = 15,
    universe: str = "all_a",
    read_context: ReadContext | None = None,
) -> dict:
    """Verify catalogued-delisting coverage for a research window, read-only.

    This deliberately proves a narrow claim: discovery is complete and every
    catalogued name known to overlap ``[start, end]`` has traded bars and a
    consistent instruments row. It does *not* claim that every session between
    the first and last bar exists; continuous-series checks belong to the
    research gate.

    A catalogue terminal after ``end`` does not prove that the security was
    already listed during the requested window. If no bar establishes that
    overlap, the name is reported as ``unknown_overlap`` instead of being
    silently treated as out of scope.
    """
    end = end or _reference_date(config, read_context)
    if start > end:
        raise ValueError("start must be on or before end")
    if sample < 0:
        raise ValueError("sample must be non-negative")
    if universe not in {"all_a", "all_a_sh_sz"}:
        raise ValueError(f"unsupported historical universe: {universe!r}")

    catalog = {
        symbol: last
        for symbol, last in load_delisted_catalog(config, read_context).items()
        if _in_historical_universe(symbol, universe)
    }
    recent = {
        symbol: terminal
        for symbol, terminal in load_live_missing(config, read_context).items()
        if _in_historical_universe(symbol, universe)
    }
    candidates = {symbol: last for symbol, last in catalog.items() if last >= start}
    formal = {
        symbol: delist_date
        for symbol, delist_date in known_delisted_instruments(config, end).items()
        if _in_historical_universe(symbol, universe)
    }
    formal_in_window = {symbol: value for symbol, value in formal.items() if value >= start}
    recent_in_window = {
        symbol: terminal for symbol, terminal in recent.items() if start <= terminal <= end
    }
    # Recent catalogue probes are provisional live/missing names, not dead
    # symbols. Include them in the evidence scan so a name already restored by
    # the daily BJ fallback (instrument + bars present) is not quarantined as a
    # survivorship gap merely because its discovery record is recent.
    symbols = sorted(set(candidates) | set(formal_in_window) | set(recent_in_window))

    spans: dict[str, tuple[date, date]] = {}
    bars_root = read_root(config, "daily_bars", read_context)
    if symbols and bars_root.exists() and any(bars_root.rglob("*.parquet")):
        from cnequity.query.parquet_scan import scan_parquet_root

        bars = scan_parquet_root(
            bars_root,
            partition_col="trade_date",
            start=start,
            end=end,
            symbols=symbols,
            traded_only=True,
            dataset="daily_bars",
            committed=False,
        )
        # EastMoney and baostock may retain suspended/formally-delisting rows
        # with zero volume after the final actual trade. The catalogue records
        # the last *traded* session, so the traded-only scan excludes those
        # placeholders while preserving legacy files without volume.
        frame = (
            bars.group_by("symbol")
            .agg(
                pl.col("trade_date").min().alias("first"),
                pl.col("trade_date").max().alias("last"),
            )
            .collect()
        )
        spans = {r["symbol"]: (r["first"], r["last"]) for r in frame.iter_rows(named=True)}

    instrument_dates: dict[str, date | None] = {}
    instruments = load_curated_instruments(config, read_context)
    if instruments is not None:
        instruments = instruments.select(["symbol", "delist_date"])
        instrument_dates = {
            row["symbol"]: row["delist_date"] for row in instruments.iter_rows(named=True)
        }

    listing_dates = _known_listing_dates(config)
    not_yet_listed: list[dict] = []
    verified_nontrading: list[dict] = []
    uncertain = [
        symbol
        for symbol, last in candidates.items()
        if symbol not in spans and last > end and listing_dates.get(symbol, start) <= end
    ]
    suspension_evidence = _verified_suspended_windows(
        config, uncertain, start, end, listing_dates, read_context
    )

    missing_bars: list[dict] = []
    unknown_overlap: list[dict] = []
    terminal_mismatches: list[dict] = []
    terminal_nonprinting: list[dict] = []
    missing_instruments: list[dict] = []
    invalid_delist_dates: list[dict] = []
    proven_overlap = 0

    for symbol in sorted(candidates):
        catalog_last = candidates[symbol]
        span = spans.get(symbol)
        overlap_is_definite = catalog_last <= end
        if span is None:
            if listing_dates.get(symbol, start) > end:
                not_yet_listed.append(
                    {
                        "symbol": symbol,
                        "list_date": str(listing_dates[symbol]),
                        "source": "baostock.query_stock_basic",
                    }
                )
                continue
            if not overlap_is_definite and symbol in suspension_evidence:
                verified_nontrading.append(suspension_evidence[symbol])
                continue
            finding = {"symbol": symbol, "catalog_last_traded": catalog_last.isoformat()}
            if overlap_is_definite:
                missing_bars.append(finding)
            else:
                unknown_overlap.append(finding)
                continue
        else:
            proven_overlap += 1
            if overlap_is_definite and span[1] != catalog_last:
                formal_delist = formal.get(symbol)  # removal date, not delist_date
                # Baostock's formal delist date is the day after the catalogue
                # terminal for two recent names whose final catalogue quote
                # carried no positive-volume print. Both independent raw-bar
                # sources omit that date too, so this is a terminal semantics
                # difference (last quote vs last print), not a missing bar.
                if (
                    formal_delist == catalog_last + timedelta(days=1)
                    and 0 < (catalog_last - span[1]).days <= 3
                ):
                    terminal_nonprinting.append(
                        {
                            "symbol": symbol,
                            "catalog_last_traded": catalog_last.isoformat(),
                            "observed_last_bar": span[1].isoformat(),
                            "instrument_delist_date": formal_delist.isoformat(),
                        }
                    )
                else:
                    terminal_mismatches.append(
                        {
                            "symbol": symbol,
                            "catalog_last_traded": catalog_last.isoformat(),
                            "observed_last_bar": span[1].isoformat(),
                        }
                    )

        # A definite catalogue terminal proves overlap even when its bars are
        # absent, so the security master must still represent the delisting.
        if symbol not in instrument_dates:
            missing_instruments.append(
                {"symbol": symbol, "catalog_last_traded": catalog_last.isoformat()}
            )
        # instruments.delist_date is the formal delisting date, which may be
        # later than the last traded session after a suspension. Only null or a
        # date before the final trade contradicts the catalogue.
        elif (instrument_dates[symbol] is None and catalog_last <= end) or (
            instrument_dates[symbol] is not None and instrument_dates[symbol] < catalog_last
        ):
            # A proven later trade establishes that this name had not yet
            # delisted in the research window. Its still-unknown formal date
            # must remain unknown, but is not an in-window survivorship gap.
            # Contradictory dates still fail, as do missing dates once the
            # requested window reaches the catalogue terminal.
            actual = instrument_dates[symbol]
            invalid_delist_dates.append(
                {
                    "symbol": symbol,
                    "catalog_last_traded": catalog_last.isoformat(),
                    "instrument_delist_date": actual.isoformat() if actual else None,
                }
            )

    raw_catalog = {
        symbol: date.fromisoformat(value)
        for symbol, value in _read_catalog(config)["delisted"].items()
        if _in_historical_universe(symbol, universe)
    }
    recent_quarantined = [
        {"symbol": symbol, "probe_last_traded": terminal.isoformat()}
        for symbol, terminal in sorted(recent_in_window.items())
        # A live/missing symbol whose last probe is after the requested
        # window is not evidence about that historical window.  Including it
        # here made a 2020..2024 research gate fail on 2026 newly issued BJ
        # codes, even though the catalogue had no in-window gap for them. A
        # recent name is only a blocker when the current lake still lacks its
        # security-master row or its in-window bars.
        if symbol not in formal_in_window
        and (symbol not in instrument_dates or symbol not in spans)
    ]
    formal_no_overlap: list[dict] = []
    formal_unresolved: list[dict] = []
    for symbol, formal_date in sorted(formal_in_window.items()):
        if symbol in candidates:
            continue
        terminal = raw_catalog.get(symbol)
        if terminal is not None and terminal < start:
            formal_no_overlap.append(
                {
                    "symbol": symbol,
                    "formal_delist_date": formal_date.isoformat(),
                    "last_traded_date": terminal.isoformat(),
                }
            )
            continue
        span = spans.get(symbol)
        if span is None:
            formal_unresolved.append(
                {
                    "symbol": symbol,
                    "formal_delist_date": formal_date.isoformat(),
                    "reason": "no_window_overlap_evidence",
                }
            )
            continue
        proven_overlap += 1
        actual = instrument_dates.get(symbol)
        if actual is None or actual < span[1]:
            invalid_delist_dates.append(
                {
                    "symbol": symbol,
                    "formal_delist_date": formal_date.isoformat(),
                    "observed_last_bar": span[1].isoformat(),
                    "instrument_delist_date": actual.isoformat() if actual else None,
                }
            )

    pending = [
        symbol
        for symbol in pending_codes(config, read_context)
        if _in_historical_universe(symbol, universe)
    ]
    known_coverage_complete = not any(
        (
            missing_bars,
            unknown_overlap,
            terminal_mismatches,
            missing_instruments,
            invalid_delist_dates,
            recent_quarantined,
            formal_unresolved,
        )
    )
    discovery_complete = not pending

    def limited(rows: list) -> list:
        return rows[:sample]

    return {
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "claim": "catalog_terminal_survivorship_coverage",
        "universe": universe,
        "discovery_complete": discovery_complete,
        "known_coverage_complete": known_coverage_complete,
        "verified": discovery_complete and known_coverage_complete,
        "counts": {
            "catalogued_delistings": len(catalog),
            "catalogue_candidates": len(candidates),
            "proven_overlap": proven_overlap,
            "pending_probe": len(pending),
            "formal_delistings_in_window": len(formal_in_window),
            "formal_no_overlap": len(formal_no_overlap),
            "recent_quarantined": len(recent_quarantined),
            "formal_unresolved": len(formal_unresolved),
            "missing_bars": len(missing_bars),
            "unknown_overlap": len(unknown_overlap),
            "not_yet_listed": len(not_yet_listed),
            "verified_nontrading": len(verified_nontrading),
            "terminal_mismatch": len(terminal_mismatches),
            "terminal_nonprinting": len(terminal_nonprinting),
            "missing_instrument": len(missing_instruments),
            "invalid_delist_date": len(invalid_delist_dates),
        },
        "samples": {
            "pending_probe": limited(pending),
            "formal_no_overlap": limited(formal_no_overlap),
            "recent_quarantined": limited(recent_quarantined),
            "formal_unresolved": limited(formal_unresolved),
            "missing_bars": limited(missing_bars),
            "unknown_overlap": limited(unknown_overlap),
            "not_yet_listed": limited(not_yet_listed),
            "verified_nontrading": limited(verified_nontrading),
            "terminal_mismatch": limited(terminal_mismatches),
            "terminal_nonprinting": limited(terminal_nonprinting),
            "missing_instrument": limited(missing_instruments),
            "invalid_delist_date": limited(invalid_delist_dates),
        },
        "limitations": [
            "Verifies catalogue discovery, window overlap, last traded bars, and instruments identity.",
            "Does not verify every expected trading session inside each observed price series.",
        ],
    }


def pending_codes(config: Config, read_context: ReadContext | None = None) -> list[str]:
    metadata = load_curated_instruments(config, read_context)
    live = set()
    if metadata is not None and not metadata.is_empty():
        reference = _reference_date(config, read_context)
        if "delist_date" in metadata.columns:
            metadata = metadata.filter(
                pl.col("delist_date").is_null() | (pl.col("delist_date") > reference)
            )
        live = set(metadata["symbol"].to_list())
    catalog = _read_catalog(config)
    done = set(catalog["delisted"]) | set(catalog["never_issued"]) | renamed_symbols()
    return [symbol for symbol in issued_code_space() if symbol not in live and symbol not in done]
