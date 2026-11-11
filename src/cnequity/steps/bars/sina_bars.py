"""Daily bars from Sina, with its shared rate-limit circuit."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.rate_limit import (
    sina_retry,
)
from cnequity.run_scope import backfill_of
from cnequity.steps.bars import amounts
from cnequity.steps.bars._base import (
    _SINA_RATE_LIMIT_CIRCUIT_SECONDS,
    _SINA_RATE_LIMIT_STATUS_CODES,
    logger,
)
from cnequity.steps.common import (
    list_trading_dates,
)


def fetch_bars_via_sina(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
    run_id: str,
    *,
    batch_prefix: str = "sina",
    fetch=None,
) -> dict:
    """Stage daily bars for symbols the primary protocol cannot serve.

    Failures are collected rather than raised: one unreachable symbol must not
    cost the whole run its Beijing coverage. They surface as an audit finding so
    a persistent gap is visible instead of silently shrinking the universe.
    """
    from concurrent.futures import ThreadPoolExecutor

    import httpx
    import polars as pl

    from cnequity.adapters.sina.bars import fetch_daily_bars_sina
    from cnequity.steps.http_common import write_fetched

    default_fetch = fetch is None
    use_parallel = default_fetch
    requested_symbols = list(dict.fromkeys(symbols))
    fetch = fetch or (
        lambda symbol, client: fetch_daily_bars_sina(
            symbol, start=start, end=end, client=client, config=config
        )
    )
    frames: list[pl.DataFrame] = []
    failed: list[str] = []
    empty: list[str] = []
    covered_dates: dict[str, set[date]] = {}
    audit_findings: list[dict] = []
    failure_reasons: dict[str, int] = {}
    rows = 0
    # A daily run can enter this helper for BJ, TDX failures and interior gaps.
    # Share the circuit across those passes; resetting it per call repeatedly
    # re-triggers the same vendor ban and its 30/120-second cooldowns.
    circuit_state = getattr(config, "_sina_bar_circuit", None)
    if circuit_state is None or circuit_state[0] != run_id:
        # Config is passed to process workers elsewhere. Store only simple
        # values here; a threading.Event would make that config unpicklable.
        circuit_state = (run_id, False)
        config._sina_bar_circuit = circuit_state

    # The BSE quotation API is an official current-session snapshot, not a
    # history source.  Use it first whenever this window contains exactly one
    # live session.  The normal incremental Monday window is often
    # ``Friday+1 .. Monday`` because the watermark is calendar-based; without
    # this check BJ symbols unnecessarily enter the much larger Sina sweep.
    # ``fetch`` is injectable for tests, so skip this live path when a fake
    # fetcher is supplied.
    from cnequity.domain.market_profile import served

    bse_symbols = [s for s in requested_symbols if served("bse_boards", [s.upper()])]
    bse_attempted = False
    if use_parallel and bse_symbols and config.sources.get("bse", False):
        sessions = list_trading_dates(config, start, end)
        if len(sessions) == 1 and not backfill_of(config).active:
            bse_attempted = True
            try:
                from cnequity.adapters.bse.daily_quotes import fetch_daily_quotes

                bse = fetch_daily_quotes(sessions[0], symbols=bse_symbols, config=config)
            except Exception as exc:  # noqa: BLE001 — Sina remains the fallback
                logger.warning("BSE tip bars failed for %s: %s", sessions[0], exc)
                audit_findings.append(
                    {
                        "dataset": "daily_bars",
                        "severity": "info",
                        "check": "daily_bars_bse_tip_unavailable",
                        "message": f"BSE tip quote unavailable for {sessions[0]}: {exc}",
                        "source": "bse",
                        "source_limited": True,
                    }
                )
            else:
                if not bse.is_empty():
                    out = write_fetched(
                        config,
                        run_id,
                        "daily_bars",
                        bse,
                        source="bse",
                        batch_id=f"{batch_prefix}-bse-0000",
                    )
                    rows += int(out.get("rows_written", 0))
                    for symbol in bse.get_column("symbol").unique().to_list():
                        covered_dates[symbol] = {sessions[0]}
                    covered = set(bse.get_column("symbol").unique().to_list())
                    requested_symbols = [
                        symbol for symbol in requested_symbols if symbol not in covered
                    ]
                    audit_findings.append(
                        {
                            "dataset": "daily_bars",
                            "severity": "info",
                            "check": "daily_bars_bse_tip",
                            "message": (
                                f"routed {len(covered)} current BJ bar(s) through the official BSE "
                                f"snapshot for {sessions[0]}"
                            ),
                            "source": "bse",
                            "rows_written": int(out.get("rows_written", 0)),
                            "symbols": len(covered),
                        }
                    )

    class _Stop(Exception):
        def __init__(self, reason: str):
            super().__init__(reason)
            self.reason = reason

    def fetch_one(symbol: str, client: httpx.Client) -> tuple[str, pl.DataFrame | None, str | None]:
        if config._sina_bar_circuit == (run_id, True):
            return symbol, None, "circuit_open"
        rate_limits = 0

        def before_retry() -> None:
            if config._sina_bar_circuit == (run_id, True):
                raise _Stop("circuit_open")

        def on_rate_limit(status_code: int) -> None:
            # A second rate limit in one symbol stops the whole gap-fill for
            # this run, on top of the vendor-wide cooldown every caller gets.
            nonlocal rate_limits
            rate_limits += 1
            if rate_limits >= 2:
                config.defer_source("sina_bars", _SINA_RATE_LIMIT_CIRCUIT_SECONDS)
                config._sina_bar_circuit = (run_id, True)
                logger.warning(
                    "sina bars repeated HTTP %s; opening run-local circuit and "
                    "cooling all Sina lanes for %.0fs",
                    status_code,
                    _SINA_RATE_LIMIT_CIRCUIT_SECONDS,
                )
                raise _Stop("rate_limited")

        # Gapfill is a best-effort repair path. Keep one unresponsive symbol
        # from holding the whole daily run for the full timeout. The default
        # adapter owns its HTTP boundary (an outer lease would turn several
        # wire calls into one slot/QPS event); an injected fetch is leased here.
        try:
            bars = sina_retry(
                config,
                "sina_bars",
                lambda: fetch(symbol, client),
                describe=f"sina bars for {symbol}",
                request_managed=default_fetch,
                on_rate_limit=on_rate_limit,
                before_retry=before_retry,
            )
        except _Stop as stop:
            return symbol, None, stop.reason
        except Exception as exc:  # noqa: BLE001 — keep the rest of the board
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            logger.warning("sina bars failed for %s: %s", symbol, exc)
            if status_code in _SINA_RATE_LIMIT_STATUS_CODES:
                reason = "rate_limited"
            elif status_code is not None:
                reason = "http_error"
            elif isinstance(exc, httpx.TransportError):
                reason = "transport_error"
            else:
                reason = "parse_or_adapter_error"
            return symbol, None, reason
        if bars.is_empty():
            return symbol, None, "source_empty"
        return symbol, bars, None

    with httpx.Client(timeout=8.0) as client:
        if use_parallel and requested_symbols:
            workers = min(
                config.source_concurrency_for("sina_bars", default=1),
                len(requested_symbols),
            )
            with ThreadPoolExecutor(max_workers=workers) as pool:
                results = list(
                    pool.map(lambda symbol: fetch_one(symbol, client), requested_symbols)
                )
        else:
            results = [fetch_one(symbol, client) for symbol in requested_symbols]
    for symbol, bars, failure_kind in results:
        if failure_kind is not None:
            failed.append(symbol)
            failure_reasons[failure_kind] = failure_reasons.get(failure_kind, 0) + 1
            if failure_kind == "source_empty":
                empty.append(symbol)
            continue
        assert bars is not None
        covered_dates[symbol] = set(bars["trade_date"].to_list())
        frames.append(bars)

    expected_dates = set(list_trading_dates(config, start, end))
    for symbol in requested_symbols:
        if symbol in failed:
            continue
        if expected_dates - covered_dates.get(symbol, set()):
            failed.append(symbol)

    supplement_findings: list[dict] = []
    if frames:
        merged = pl.concat(frames, how="diagonal_relaxed")
        # Every fallback is append-only with respect to keys already staged by
        # TDX, exchange quotes, or an earlier route. This is the write-side
        # enforcement of source priority; compact ordering never has to guess.
        from cnequity.storage import StagingWriter

        files = StagingWriter(config.staging_root).list_run_files("daily_bars", run_id)
        if files:
            existing = (
                pl.scan_parquet([str(path) for path in files])
                .select("symbol", "trade_date")
                .unique()
                .collect()
            )
            merged = merged.join(existing, on=["symbol", "trade_date"], how="anti")
        if start == end and not bse_attempted:
            merged, supplement_findings = amounts._supplement_bse_tip_amounts(
                config, merged, trade_date=start, symbols=requested_symbols
            )
        if not merged.is_empty():
            out = write_fetched(
                config,
                run_id,
                "daily_bars",
                merged,
                source="sina",
                batch_id=f"{batch_prefix}-0000",
            )
            rows += int(out.get("rows_written", 0))

    result: dict = {"rows_read": rows, "rows_written": rows}
    audit_findings.extend(supplement_findings)
    if failed:
        result["failed_symbols"] = len(failed)
        # Keep the names as well as the count. The daily step can then route
        # failed fallback symbols through the same historical gap-fill as TDX
        # failures; a count alone cannot identify which keys need recovery.
        result["failed_symbol_names"] = list(dict.fromkeys(failed))
        result["empty_symbol_names"] = list(dict.fromkeys(empty))
        audit_findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "fallback_source_incomplete",
                "message": (
                    f"{len(failed)}/{len(requested_symbols)} symbols without a TDX route "
                    f"failed to fetch from the fallback vendor "
                    f"(e.g. {', '.join(failed[:5])})"
                ),
                "empty_symbols": len(empty),
            }
        )
    if audit_findings:
        result["context_updates"] = {"audit_findings": audit_findings}
    result["source_outcomes"] = {
        "sina": {
            "status": "success" if not failed else ("partial" if rows else "failed"),
            "requests": len(requested_symbols),
            "rows_written": rows,
            "failed_symbols": len(failed),
            "empty_symbols": len(empty),
            "failure_reasons": dict(sorted(failure_reasons.items())),
        }
    }
    return result
