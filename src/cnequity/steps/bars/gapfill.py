"""Gap-fill sources for missing daily keys: clist, exchange, THS, baostock."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.symbols import (
    in_ingest_universe,
    is_baostock_stock,
    parse_symbol,
)
from cnequity.steps.bars import bar_scope, staging
from cnequity.steps.bars._base import _EXCHANGE_BULK_GAPFILL_MAX_SESSIONS, logger
from cnequity.steps.common import (
    list_trading_dates,
    load_curated_trading_status,
)


def _gapfill_tip_via_clist(
    config: Config,
    trade_date: date,
    run_id: str,
    *,
    expected_symbols: list[str],
) -> dict:
    """Route missing tip keys through one EastMoney clist snapshot (ADR-0005)."""
    import polars as pl

    from cnequity.adapters.eastmoney.bars import fetch_daily_bars_clist
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.quality.failover import failover_spec, snapshot_daily_bars_clist
    from cnequity.storage import StagingWriter

    if not expected_symbols:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "filled": False,
            "source_outcomes": {"eastmoney": {"status": "not_needed", "requests": 0}},
        }
    staged = staging._staged_daily_bar_symbols(config, run_id, trade_date)
    missing = [s for s in expected_symbols if s not in staged]
    if not missing:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "filled": False,
            "source_outcomes": {"eastmoney": {"status": "not_needed", "requests": 0}},
        }

    spec = failover_spec(config, "daily_bars")
    if spec is None or not config.sources.get(spec.backup, True):
        return {
            "rows_read": 0,
            "rows_written": 0,
            "filled": False,
            "source_outcomes": {"eastmoney": {"status": "disabled", "requests": 0}},
            "audit_findings": [
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_clist_gapfill",
                    "message": (
                        f"{len(missing)} tip key(s) missing after TDX but eastmoney "
                        "backup is disabled; curated tip stays sparse"
                    ),
                }
            ],
        }

    # One full clist pull, then keep only missing keys so compact cannot
    # overwrite successful TDX rows for the same PK (keep=last by fetched_at).
    try:
        full = fetch_daily_bars_clist(trade_date, config=config)
    except Exception as exc:  # noqa: BLE001 — continue the independent recovery chain
        from cnequity.adapters.eastmoney.host_guard import EastMoneyHostBlockedError

        blocked = isinstance(exc, EastMoneyHostBlockedError)
        logger.warning("daily_bars clist gap-fill unavailable: %s", exc)
        return {
            "rows_read": 0,
            "rows_written": 0,
            "filled": False,
            "source_outcomes": {
                "eastmoney": {
                    "status": "blocked" if blocked else "error",
                    "requests": 0 if blocked else 1,
                }
            },
            "audit_findings": [
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_clist_gapfill",
                    "message": (
                        f"EastMoney clist unavailable ({type(exc).__name__}); "
                        f"{len(missing)} missing tip key(s) continue through historical fallbacks"
                    ),
                }
            ],
        }
    if full.is_empty():
        return {
            "rows_read": 0,
            "rows_written": 0,
            "filled": False,
            "source_outcomes": {
                "eastmoney": {"status": "empty", "requests": 1, "empty_symbols": len(missing)}
            },
            "audit_findings": [
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_clist_gapfill",
                    "message": (
                        f"{len(missing)} tip key(s) missing after TDX; "
                        "EastMoney clist returned no rows"
                    ),
                }
            ],
        }

    snapshot_daily_bars_clist(
        config,
        trade_date=trade_date,
        run_id=run_id,
        batch_id="em-clist-snapshot",
        df=full,
    )
    missing_set = set(missing)
    gap_df = full.filter(pl.col("symbol").is_in(list(missing_set)))
    if gap_df.is_empty():
        return {
            "rows_read": full.height,
            "rows_written": 0,
            "filled": False,
            "source_outcomes": {
                "eastmoney": {
                    "status": "source_missing_keys",
                    "requests": 1,
                    "empty_symbols": len(missing),
                }
            },
            "audit_findings": [
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_clist_gapfill",
                    "message": (
                        f"clist had {full.height} rows but none of the "
                        f"{len(missing)} missing tip key(s)"
                    ),
                }
            ],
        }

    gap_df = with_provenance(
        gap_df, source=spec.backup, data_version=data_version_for("daily_bars")
    )
    batch_id = "em-clist-gapfill"
    filled_syms = sorted(set(gap_df["symbol"].to_list()))
    manifest = Manifest(config.manifest_path)
    manifest.start_batch(
        run_id,
        batch_id,
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=filled_syms,
        window_start=trade_date.isoformat(),
        window_end=trade_date.isoformat(),
    )
    StagingWriter(config.staging_root).write_batch("daily_bars", run_id, batch_id, gap_df)
    manifest.finish_batch(
        run_id,
        batch_id,
        "success",
        rows_read=gap_df.height,
        rows_written=gap_df.height,
    )
    logger.warning(
        "daily_bars tip gap-fill: staged %s EastMoney clist row(s) for %s missing key(s)",
        gap_df.height,
        len(missing),
    )
    return {
        "rows_read": gap_df.height,
        "rows_written": gap_df.height,
        "filled": True,
        "complete": len(filled_syms) == len(missing),
        "source_outcomes": {
            "eastmoney": {
                "status": "success" if len(filled_syms) == len(missing) else "partial",
                "requests": 1,
                "rows_written": gap_df.height,
                "empty_symbols": len(missing) - len(filled_syms),
            }
        },
        "audit_findings": [
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_clist_gapfill",
                "message": (
                    f"routed {gap_df.height} tip key(s) through EastMoney clist "
                    f"after TDX left {len(missing)} missing (ADR-0005 routing)"
                ),
                "missing_requested": len(missing),
                "rows_written": gap_df.height,
                "complete": len(filled_syms) == len(missing),
            }
        ],
    }


def _stage_daily_gap_batch(
    config: Config,
    run_id: str,
    *,
    batch_id: str,
    source: str,
    frame: pl.DataFrame,
    symbols: list[str],
    start: date,
    end: date,
) -> int:
    """Merge a gap-fill attempt into its stable staging object."""
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.storage import StagingWriter

    if frame.is_empty():
        return 0
    current_rows = frame.height
    staged = with_provenance(frame, source=source, data_version=data_version_for("daily_bars"))
    writer = StagingWriter(config.staging_root)
    path = config.staging_root / "daily_bars" / f"run_id={run_id}" / f"part-{batch_id}.parquet"
    if path.exists():
        staged = dedupe_by_primary_key(
            pl.concat([pl.read_parquet(path), staged], how="diagonal_relaxed"),
            "daily_bars",
        )
    manifest = Manifest(config.manifest_path)
    manifest.start_batch(
        run_id,
        batch_id,
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=sorted(set(symbols)),
        window_start=start.isoformat(),
        window_end=end.isoformat(),
    )
    writer.write_batch("daily_bars", run_id, batch_id, staged)
    manifest.finish_batch(
        run_id,
        batch_id,
        "success",
        rows_read=current_rows,
        rows_written=current_rows,
    )
    return current_rows


def _gapfill_complete_symbols_via_exchange(
    config: Config,
    run_id: str,
    *,
    symbols: list[str],
    start: date,
    end: date,
) -> dict:
    """Stage compatible SSE quotes when they complete a whole window.

    SZSE report turnover includes trades outside the auction series and is
    reserved for authority checks. It cannot fill auction-based daily bars.
    SSE offers only a current-session snapshot; absent quotes are not proof
    of a halt, and remain owned by the vendor fallback paths.
    """
    import polars as pl

    from cnequity.adapters.exchange.daily_quotes import fetch_sse_daily_quotes
    from cnequity.storage import StagingWriter

    requested = set(dict.fromkeys(symbols))
    sessions = list_trading_dates(config, start, end)
    if not requested or not sessions or not config.sources.get("exchange", False):
        return {
            "rows_read": 0,
            "rows_written": 0,
            "complete_symbols": [],
            "source_outcomes": {"exchange": {"status": "disabled", "requests": 0}},
        }
    if len(sessions) > _EXCHANGE_BULK_GAPFILL_MAX_SESSIONS:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "complete_symbols": [],
            "source_outcomes": {
                "exchange": {
                    "status": "skipped_long_window",
                    "requests": 0,
                    "sessions": len(sessions),
                    "max_sessions": _EXCHANGE_BULK_GAPFILL_MAX_SESSIONS,
                }
            },
        }

    frames: list[pl.DataFrame] = []
    requests = 0
    empty_responses = 0
    sh_symbols = {symbol for symbol in requested if symbol.upper().endswith(".SH")}
    if sh_symbols and len(sessions) == 1:
        requests += 1
        frame = fetch_sse_daily_quotes(sessions[0], config=config)
        if frame.is_empty():
            empty_responses += 1
        else:
            frames.append(frame.filter(pl.col("symbol").is_in(sorted(sh_symbols))))

    fetched = (
        pl.concat([frame for frame in frames if not frame.is_empty()], how="vertical_relaxed")
        if any(not frame.is_empty() for frame in frames)
        else pl.DataFrame()
    )
    writer = StagingWriter(config.staging_root)
    files = writer.list_run_files("daily_bars", run_id)
    existing = (
        pl.scan_parquet([str(path) for path in files])
        .filter(
            (pl.col("trade_date") >= start)
            & (pl.col("trade_date") <= end)
            & pl.col("symbol").is_in(sorted(requested))
        )
        .select("symbol", "trade_date")
        .unique()
        .collect()
        if files
        else pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date})
    )
    existing_keys = set(
        zip(existing["symbol"].to_list(), existing["trade_date"].to_list(), strict=True)
    )
    fetched_keys = (
        set(zip(fetched["symbol"].to_list(), fetched["trade_date"].to_list(), strict=True))
        if not fetched.is_empty()
        else set()
    )
    required_by_symbol = {
        symbol: {(symbol, session) for session in sessions} for symbol in requested
    }
    complete_symbols = {
        symbol
        for symbol, required in required_by_symbol.items()
        if required.issubset(existing_keys | fetched_keys)
    }
    if fetched.is_empty() or not complete_symbols:
        return {
            "rows_read": fetched.height,
            "rows_written": 0,
            "complete_symbols": sorted(complete_symbols),
            "source_outcomes": {
                "exchange": {
                    "status": "empty" if not fetched.height else "partial",
                    "requests": requests,
                    "empty_responses": empty_responses,
                    "rows": fetched.height,
                }
            },
        }

    gap = fetched.filter(pl.col("symbol").is_in(sorted(complete_symbols))).join(
        existing,
        on=["symbol", "trade_date"],
        how="anti",
    )
    if gap.is_empty():
        rows_written = 0
    else:
        rows_written = _stage_daily_gap_batch(
            config,
            run_id,
            batch_id="exchange-gapfill",
            source="exchange",
            frame=gap,
            symbols=sorted(complete_symbols),
            start=start,
            end=end,
        )

    return {
        "rows_read": fetched.height,
        "rows_written": rows_written,
        "complete_symbols": sorted(complete_symbols),
        "audit_findings": [
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_exchange_gapfill",
                "message": (
                    f"official exchange quotes completed {len(complete_symbols)} symbol(s) "
                    f"over {start}..{end} with {requests} bulk request(s)"
                ),
                "symbols": sorted(complete_symbols),
                "rows_written": rows_written,
                "requests": requests,
            }
        ],
        "source_outcomes": {
            "exchange": {
                "status": "success",
                "requests": requests,
                "empty_responses": empty_responses,
                "rows": fetched.height,
                "completed_symbols": len(complete_symbols),
            }
        },
    }


#: Per-fund requests a single gap-fill may spend on exchange day k-lines.
_EXCHANGE_DAYK_MAX_SYMBOLS = 20


def _gapfill_fund_keys_via_exchange_dayk(
    config: Config,
    run_id: str,
    *,
    missing: set[tuple[str, date]],
    start: date,
    end: date,
) -> dict:
    """Stage exact missing exchange-fund keys from the exchange's own day k-line.

    For the sub-lot sessions TDX leaves unpublished (zero lots, positive
    turnover; ``adapters/tdx_protocol/bars.py``). The vendor fallbacks behind
    this one are EastMoney, which bans an address after heavy use, and THS,
    which may not have the day yet: 501217.SH traded 15 shares on 2026-10-26
    and stayed unresolved through both while the SSE published the bar.
    """
    import httpx

    from cnequity.adapters.exchange.fund_volume import fetch_fund_dayk
    from cnequity.domain.symbols import is_etf_symbol, parse_symbol

    def is_fund(symbol: str) -> bool:
        try:
            info = parse_symbol(symbol)
        except ValueError:
            return False
        return info.exchange in {"SH", "SZ"} and is_etf_symbol(info.code, info.exchange)

    wanted: dict[str, set[date]] = {}
    for symbol, day in missing:
        if is_fund(symbol):
            wanted.setdefault(symbol, set()).add(day)
    if not wanted or not config.sources.get("exchange", False):
        return {"rows_read": 0, "rows_written": 0, "source_outcomes": {}}
    symbols = sorted(wanted)[:_EXCHANGE_DAYK_MAX_SYMBOLS]
    frames, failed = [], []
    with httpx.Client(timeout=30.0) as client:
        for symbol in symbols:
            try:
                frame = fetch_fund_dayk(
                    symbol, trade_dates=wanted[symbol], client=client, config=config
                )
            except (httpx.HTTPError, OSError, ValueError) as exc:
                logger.warning("exchange dayk %s failed: %s", symbol, exc)
                failed.append(symbol)
                continue
            if not frame.is_empty():
                frames.append(frame)
    fetched = pl.concat(frames, how="vertical_relaxed") if frames else pl.DataFrame()
    rows_written = 0
    if not fetched.is_empty():
        rows_written = _stage_daily_gap_batch(
            config,
            run_id,
            batch_id="exchange-dayk-gapfill",
            source="exchange",
            frame=fetched,
            symbols=sorted(set(fetched["symbol"].to_list())),
            start=start,
            end=end,
        )
    outcome = {
        "status": "success"
        if rows_written and not failed
        else "partial"
        if rows_written
        else "empty",
        "requests": len(symbols),
        "rows": fetched.height,
        "failed_symbols": failed,
    }
    return {
        "rows_read": fetched.height,
        "rows_written": rows_written,
        "audit_findings": [
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_exchange_dayk_gapfill",
                "message": (
                    f"exchange day k-lines staged {fetched.height} fund session(s) "
                    f"for {len(symbols)} symbol(s)"
                ),
                "symbols": symbols,
                "rows_written": rows_written,
            }
        ],
        "source_outcomes": {"exchange_dayk": outcome},
    }


def _gapfill_missing_keys_via_ths(
    config: Config,
    run_id: str,
    *,
    missing_keys: set[tuple[str, date]],
    start: date,
    end: date,
) -> dict:
    """Use THS only for exact keys still absent after cheaper batch routes."""
    from cnequity.adapters.ths.stock_bars import fetch_stock_bars
    from cnequity.domain.market_profile import serves

    # A key THS is not trusted for stays missing for a source that is.
    missing_keys = {key for key in missing_keys if serves("ths_daily_bars", key[0])}
    ths_enabled = config.sources.get("ths", False)
    if not missing_keys or not ths_enabled:
        # See the baostock link: "disabled" is a claim about configuration, and
        # an earlier link having resolved everything is not one.
        return {
            "rows_read": 0,
            "rows_written": 0,
            "empty_symbols": [],
            "failed_symbols": {},
            "source_outcomes": {
                "ths": {
                    "status": "disabled" if not ths_enabled else "not_needed",
                    "requests": 0,
                }
            },
        }

    by_symbol: dict[str, set[date]] = {}
    for symbol, session in missing_keys:
        by_symbol.setdefault(symbol, set()).add(session)
    rows: list[dict] = []
    empty: list[str] = []
    failed: dict[str, str] = {}
    for symbol, required_dates in sorted(by_symbol.items()):
        try:
            fetched = fetch_stock_bars(
                symbol,
                min(required_dates),
                max(required_dates),
                config=config,
            )
        except Exception as exc:  # noqa: BLE001 — final fallback is isolated per symbol
            failed[symbol] = type(exc).__name__
            logger.warning("THS final daily-bar fallback failed for %s: %s", symbol, exc)
            continue
        selected = [row for row in fetched if (symbol, row.get("trade_date")) in missing_keys]
        if not selected:
            empty.append(symbol)
            continue
        rows.extend(selected)

    frame = pl.DataFrame(rows) if rows else pl.DataFrame()
    written = _stage_daily_gap_batch(
        config,
        run_id,
        batch_id="ths-kline-gapfill",
        source="ths",
        frame=frame,
        symbols=sorted(by_symbol),
        start=start,
        end=end,
    )
    status = "success" if written else ("failed" if failed else "empty")
    findings: list[dict] = []
    if written or failed or empty:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning" if failed or empty else "info",
                "check": "daily_bars_ths_gapfill",
                "message": (
                    f"THS final fallback wrote {written} row(s) for {len(by_symbol)} "
                    f"remaining symbol(s); empty={len(empty)}, failed={len(failed)}"
                ),
                "rows_written": written,
                "empty_symbols": sorted(empty),
                "failed_symbols": sorted(failed),
            }
        )
    return {
        "rows_read": len(rows),
        "rows_written": written,
        "empty_symbols": sorted(empty),
        "failed_symbols": failed,
        "audit_findings": findings,
        "source_outcomes": {
            "ths": {
                "status": status,
                "requests": len(by_symbol),
                "rows": len(rows),
                "empty_symbols": len(empty),
                "failed_symbols": len(failed),
            }
        },
    }


# Bounds the slowest link in the chain. Baostock paces at one request per
# second, so an unbounded residue (a dead primary, not a few stragglers) would
# add over an hour to a run that is already failing.
_BAOSTOCK_GAPFILL_MAX_SYMBOLS = 300


def _gapfill_missing_keys_via_baostock(
    config: Config,
    run_id: str,
    *,
    missing_keys: set[tuple[str, date]],
    start: date,
    end: date,
) -> dict:
    """The last independent per-symbol vendor, reached when the others cannot be.

    `daily_bars` has declared baostock supplementary since the spec was
    written, but nothing in the live chain ever called it: the code path
    existed only for delisted recovery. That left the chain with exactly two
    per-symbol vendors — EastMoney and THS — and EastMoney's history host is
    the one that is unreachable from whole classes of egress (measured: every
    `push2his` host dropped the connection from this vantage while `push2`
    still served clist). When it is down, one vendor remains, and one vendor
    can never certify a no-data key, because certification requires two
    independent empties. That is how a run whose missing names had genuinely
    not traded still failed with "expected key(s) remain unknown".

    Baostock answers the same question from a different failure domain, and
    answers it correctly for this purpose: a suspended session comes back as no
    row rather than as an error, which is the explicit empty the arbitration
    needs. Measured against the session that failed: it served both live
    symbols exactly (matching Sina to the cent) and returned empty, not failed,
    for all of the suspended ones.

    Last on purpose — it logs in per call and walks one symbol at a time, so it
    is the most expensive link and only ever sees the keys nothing else could
    resolve.
    """
    # Named for the recovery path it was written for; it is an ordinary
    # per-symbol bar fetch and carries the same fields the THS link stages.
    from cnequity.adapters.baostock.delisted_bars import fetch_delisted_bars as fetch_baostock_bars

    # Default off like the THS link: a chain that reaches a network vendor
    # nobody enabled is a chain that reaches out from a unit test. Every
    # shipped config sets `[sources.baostock] enabled = true`.
    enabled = config.sources.get("baostock", False)
    if not missing_keys or not enabled:
        # "disabled" only when it really is. An earlier link resolving
        # everything is the chain working, and reporting that as a config
        # problem sends the operator to edit a setting that is already right.
        return {
            "rows_read": 0,
            "rows_written": 0,
            "empty_symbols": [],
            "failed_symbols": {},
            "source_outcomes": {
                "baostock": {
                    "status": "disabled" if not enabled else "not_needed",
                    "requests": 0,
                }
            },
        }

    by_symbol: dict[str, set[date]] = {}
    for symbol, session in missing_keys:
        # Its history covers SH/SZ stocks, not the ETF/LOF and index codes TDX
        # can serve. An unsupported code returning empty proves no absence.
        if not is_baostock_stock(symbol):
            continue
        by_symbol.setdefault(symbol, set()).add(session)
    if not by_symbol:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "empty_symbols": [],
            "failed_symbols": {},
            "source_outcomes": {"baostock": {"status": "skipped", "requests": 0}},
        }
    requested = _research_first(by_symbol)

    if len(requested) > _BAOSTOCK_GAPFILL_MAX_SYMBOLS:
        # This link is for residue. A queue this long means the primary path
        # is broken, and walking it at one request per second would spend more
        # than an hour proving that slowly.
        message = (
            f"baostock final fallback skipped: {len(requested)} symbol(s) exceeds the "
            f"{_BAOSTOCK_GAPFILL_MAX_SYMBOLS}-symbol residue bound for a 1 req/s link; "
            "the primary route is what needs repairing"
        )
        logger.warning("%s", message)
        return {
            "rows_read": 0,
            "rows_written": 0,
            "empty_symbols": [],
            "failed_symbols": {},
            "audit_findings": [
                {
                    "dataset": "daily_bars",
                    "severity": "warning",
                    "check": "daily_bars_baostock_gapfill",
                    "message": message,
                    "requested_symbols": len(requested),
                }
            ],
            "source_outcomes": {
                "baostock": {"status": "skipped", "requests": 0, "pending": len(requested)}
            },
        }

    try:
        fetched, failed_symbols = fetch_baostock_bars(requested, start, end, config=config)
    except Exception as exc:  # noqa: BLE001 — a dead final link must not raise
        logger.warning("baostock final daily-bar fallback failed: %s", exc)
        return {
            "rows_read": 0,
            "rows_written": 0,
            "empty_symbols": [],
            "failed_symbols": {symbol: type(exc).__name__ for symbol in requested},
            "source_outcomes": {
                "baostock": {
                    "status": "failed",
                    "requests": len(requested),
                    "failed_symbols": len(requested),
                }
            },
        }

    rows = [row for row in fetched if (row.get("symbol"), row.get("trade_date")) in missing_keys]
    returned = {row.get("symbol") for row in rows}
    failed = {symbol: "fetch_failed" for symbol in failed_symbols}
    # Empty means answered-and-had-nothing. A symbol the vendor never got to
    # answer for is not evidence of anything.
    empty = sorted(
        symbol for symbol in requested if symbol not in returned and symbol not in failed
    )

    frame = pl.DataFrame(rows) if rows else pl.DataFrame()
    written = _stage_daily_gap_batch(
        config,
        run_id,
        batch_id="baostock-kline-gapfill",
        source="baostock",
        frame=frame,
        symbols=requested,
        start=start,
        end=end,
    )
    status = "success" if written else ("failed" if failed else "empty")
    findings: list[dict] = []
    if written or failed or empty:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning" if failed else "info",
                "check": "daily_bars_baostock_gapfill",
                "message": (
                    f"baostock final fallback wrote {written} row(s) for {len(requested)} "
                    f"remaining symbol(s); empty={len(empty)}, failed={len(failed)}"
                ),
                "rows_written": written,
                "empty_symbols": empty,
                "failed_symbols": sorted(failed),
            }
        )
    return {
        "rows_read": len(rows),
        "rows_written": written,
        "empty_symbols": empty,
        "failed_symbols": failed,
        "audit_findings": findings,
        "source_outcomes": {
            "baostock": {
                "status": status,
                "requests": len(requested),
                "rows": len(rows),
                "empty_symbols": len(empty),
                "failed_symbols": len(failed),
            }
        },
    }


def _research_first(symbols: Iterable[str]) -> list[str]:
    """Order a fallback queue so research-universe symbols are attempted first.

    The per-symbol vendors in the fallback chain open a circuit after a few
    consecutive transport failures and abandon everything still queued behind
    it. Under a scope that deliberately keeps ETF/LOF quote codes
    (``[universe].ingest = "all_instruments"``) an alphabetical queue spends
    that whole budget on 15xxxx/16xxxx codes before it ever reaches 6xxxxx —
    which is how a few unservable fund codes left real A shares unresolved.

    Still fully deterministic: sorted within each class, never shuffled.
    """
    return sorted(symbols, key=lambda symbol: (0 if _is_research_symbol(symbol) else 1, symbol))


def _is_research_symbol(symbol: str) -> bool:
    try:
        info = parse_symbol(symbol)
    except ValueError:
        return False
    return in_ingest_universe(info.code, info.exchange, "all_a")


def _expected_session_keys(
    config: Config,
    symbols: list[str],
    sessions: list[date],
    start: date,
    end: date,
) -> set[tuple[str, date]]:
    """Keys a symbol is actually expected to have over *sessions*.

    Every trading day in the window is the wrong answer, and it was the one
    this chain used: it counted days before a symbol listed, days after it
    delisted, and days the lake already knows it was suspended. A backfill of a
    name with any suspension in its window could therefore never report itself
    complete, which left the failed primary batch unresolved, which left
    `compact` skipping the dataset — rows fetched, staged, and never published.

    The interior-gap gate downstream has always judged by listing span and
    `trading_status`. This is the same rule, applied where completeness is
    first decided, so the two cannot disagree about what "missing" means.
    """
    spans = bar_scope._instrument_spans(config)
    status = load_curated_trading_status(config, start=start, end=end, symbols=sorted(symbols))
    halted: dict[str, set[date]] = {}
    if status is not None and not status.is_empty():
        needed = {"symbol", "trade_date", "is_trading"}
        if needed.issubset(status.columns):
            for row in status.select(*sorted(needed)).iter_rows(named=True):
                if row["is_trading"] is False:
                    halted.setdefault(str(row["symbol"]), set()).add(row["trade_date"])

    keys: set[tuple[str, date]] = set()
    for symbol in symbols:
        list_date, delist_date = spans.get(symbol, (None, None, None))[:2]
        first = max(start, list_date) if list_date is not None else start
        last = min(end, delist_date) if delist_date is not None else end
        closed = halted.get(symbol, set())
        keys.update(
            (symbol, session)
            for session in sessions
            if first <= session <= last and session not in closed
        )
    return keys


def _learn_suspensions_from_baostock(
    config: Config,
    run_id: str,
    missing: set[tuple[str, date]],
    start: date,
    end: date,
) -> tuple[set[tuple[str, date]], dict]:
    """Ask the vendor which of these absences were suspensions, and keep the answer.

    A pre-2016 window has no `trading_status` in the lake to excuse an interior
    gap with, so a backfill of a name that was ever suspended could not report
    itself complete however many bars it actually fetched — and the operator
    had to know to run `cne backfill trading_status` first, for the same
    symbols and window, before the bars would publish. Two commands, in an
    order nothing announced.

    Baostock publishes the fact per symbol-day (`tradestatus`), and is already
    this dataset's declared backfill source, so the step asks for it itself.
    The rows are staged as well as used, so the next run does not ask again.

    Returns the (symbol, session) pairs it confirmed suspended, and an outcome
    record for the receipt.
    """
    from cnequity.adapters.baostock.st_history import fetch_st_history
    from cnequity.steps.http_common import write_fetched

    if not missing or not config.sources.get("baostock", False):
        return set(), {"status": "disabled" if missing else "not_needed", "requests": 0}
    # Unsupported funds, indexes and BJ securities cannot supply suspension
    # evidence merely because their codes can be requested through TDX.
    symbols = sorted({symbol for symbol, _day in missing if is_baostock_stock(symbol)})
    if not symbols:
        return set(), {"status": "skipped", "requests": 0, "reason": "no SH/SZ stock key"}
    if len(symbols) > _BAOSTOCK_GAPFILL_MAX_SYMBOLS:
        return set(), {
            "status": "skipped",
            "requests": 0,
            "pending": len(symbols),
            "reason": "residue exceeds the 1 req/s bound",
        }

    try:
        frame, failed = fetch_st_history(symbols, start, end, config=config)
    except Exception as exc:  # noqa: BLE001 — evidence we could not get is not a new failure
        logger.warning("suspension evidence unavailable for %d symbol(s): %s", len(symbols), exc)
        return set(), {"status": "failed", "requests": len(symbols), "error": type(exc).__name__}
    if frame.is_empty():
        return set(), {"status": "empty", "requests": len(symbols), "failed_symbols": len(failed)}

    halted = frame.filter(~pl.col("is_trading"))
    confirmed = {
        (row["symbol"], row["trade_date"])
        for row in halted.select("symbol", "trade_date").iter_rows(named=True)
    } & missing
    written = write_fetched(
        config,
        run_id,
        "trading_status",
        frame,
        source="baostock",
        # One file per symbol set: a run can ask twice about the same window
        # (the gap-fill and the interior-gap pass), and a shared name let the
        # second pass replace the first's rows after its bars were already
        # excused by them — those names then had neither rows nor a debt.
        batch_id=(
            f"suspension-evidence-{start.isoformat()}-{end.isoformat()}-"
            + hashlib.sha256(",".join(symbols).encode()).hexdigest()[:12]
        ),
    )
    logger.info(
        "learned %d suspended session(s) for %d symbol(s) from baostock; "
        "%s trading_status row(s) staged",
        len(confirmed),
        len(symbols),
        written.get("rows_written", 0),
    )
    # The rows are staged, not curated, until this run's compact — and the
    # interior-gap gate below reads curated. Carry the answer on the config for
    # the rest of this run so the gate does not re-condemn what we just learned.
    from cnequity.run_scope import ensure_run_scoped

    ensure_run_scoped(
        config,
        "_learned_suspensions_run",
        run_id,
        {"_learned_suspensions": lambda: {"run_id": run_id, "keys": set()}},
    )
    config._learned_suspensions["keys"].update(confirmed)
    return confirmed, {
        "status": "success",
        "requests": len(symbols),
        "suspended_keys": len(confirmed),
        "rows_written": int(written.get("rows_written", 0)),
        "failed_symbols": len(failed),
    }
