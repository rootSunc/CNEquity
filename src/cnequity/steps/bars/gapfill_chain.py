"""The multi-day gap-fill chain and its preopen-placeholder guards."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.steps.bars import gapfill, sina_bars
from cnequity.steps.bars._base import logger
from cnequity.steps.common import (
    list_trading_dates,
)

# --------------------------------------------------------------- gap-fill chain
#
# ``_gapfill_multiday_via_kline`` asks a fixed list of sources, in order, for
# the symbol×session keys still missing after the primary fetch. Each source is
# one stage below; the chain is the tuple at the end. Every stage looks its
# fetcher up by module name when it runs, so tests that patch
# ``steps.bars._gapfill_…`` or ``fetch_bars_via_sina`` reach it.
# ``tests/unit/test_daily_bars_gapfill_chain.py`` pins the order, the inputs
# each source is handed and the merged result, and checks every stage's source
# against what ``DATASETS["daily_bars"]`` declares.


@dataclass
class _ChainRun:
    """What the chain has learned so far, shared by its stages."""

    config: Config
    run_id: str
    start: date
    end: date
    requested: list[str]
    require_complete: bool
    missing_keys: Callable[[], set[tuple[str, date]]]
    rows_read: int = 0
    rows_written: int = 0
    findings: list[dict] = field(default_factory=list)
    source_outcomes: dict[str, dict] = field(default_factory=dict)
    empty_evidence: dict[str, set[str]] = field(default_factory=dict)
    exchange_complete: set[str] = field(default_factory=set)

    def pending_symbols(self) -> list[str]:
        """Symbols with a missing key the exchange did not already complete."""
        return gapfill._research_first(
            {symbol for symbol, _day in self.missing_keys()} - self.exchange_complete
        )

    def absorb(self, result: dict, *, empty_source: str | None = None) -> None:
        """Merge one stage's counts, findings and outcomes, in a fixed order."""
        self.rows_read += int(result.get("rows_read", 0))
        self.rows_written += int(result.get("rows_written", 0))
        self.findings.extend(result.get("audit_findings") or [])
        self.source_outcomes.update(result.get("source_outcomes") or {})
        if empty_source:
            self.note_empty(result.get("empty_symbols") or [], empty_source)

    def note_empty(self, symbols: Iterable[str], source: str) -> None:
        for symbol in symbols:
            self.empty_evidence.setdefault(symbol, set()).add(source)


@dataclass(frozen=True)
class GapFillStage:
    """One source in the chain: its name, the registry source it reads, and how."""

    name: str
    #: The ``DatasetSpec`` source this stage reads; must be one daily_bars declares.
    source: str
    run: Callable[[_ChainRun], None]
    #: Stop the chain after this stage when nothing is left for later sources.
    settles_early: bool = False


def _stage_exchange(run: _ChainRun) -> None:
    # Fastest broad recovery first: one official SZSE request per session (and
    # a same-session SSE snapshot) can complete hundreds of symbols at once.
    exchange = gapfill._gapfill_complete_symbols_via_exchange(
        run.config,
        run.run_id,
        symbols=run.requested,
        start=run.start,
        end=run.end,
    )
    run.absorb(exchange)
    run.exchange_complete = set(exchange.get("complete_symbols") or [])


def _stage_exchange_dayk(run: _ChainRun) -> None:
    # Then the exchange's own per-fund day k-line, for the sub-lot fund
    # sessions TDX cannot express and no bulk snapshot carries.
    dayk = gapfill._gapfill_fund_keys_via_exchange_dayk(
        run.config, run.run_id, missing=run.missing_keys(), start=run.start, end=run.end
    )
    run.absorb(dayk)


def _stage_sina(run: _ChainRun) -> None:
    # Sina is a bounded recent-tail request now. It remains skipped for the
    # partial-only path because the bulk/primary staging may already contain
    # valid keys and EastMoney/THS below can filter exact missing pairs.
    if not (run.require_complete and run.config.sources.get("sina", True)):
        return
    sina = sina_bars.fetch_bars_via_sina(
        run.config,
        run.pending_symbols(),
        run.start,
        run.end,
        run.run_id,
        batch_prefix="sina-kline-gapfill",
    )
    run.rows_read += int(sina.get("rows_read", 0))
    run.rows_written += int(sina.get("rows_written", 0))
    run.findings.extend((sina.get("context_updates") or {}).get("audit_findings") or [])
    run.source_outcomes.update(sina.get("source_outcomes") or {})
    run.note_empty(sina.get("empty_symbol_names") or [], "sina")
    if sina.get("empty_symbol_names"):
        run.findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_sina_source_empty",
                "message": (
                    f"Sina returned an explicit empty payload for "
                    f"{len(sina.get('empty_symbol_names') or [])} symbol(s); "
                    "a second independent empty source is required before no-data certification"
                ),
                "symbols": sorted(sina.get("empty_symbol_names") or []),
            }
        )


def _stage_eastmoney(run: _ChainRun) -> None:
    import polars as pl

    from cnequity.adapters.eastmoney.bars import fetch_daily_bars as fetch_em_kline
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.quality.failover import failover_spec, write_backup_snapshot

    config, start, end = run.config, run.start, run.end
    pending = run.missing_keys()
    pending_symbols = gapfill._research_first({symbol for symbol, _day in pending})
    spec = failover_spec(config, "daily_bars")
    if not (pending_symbols and spec is not None and config.sources.get(spec.backup, True)):
        if pending_symbols:
            run.source_outcomes["eastmoney"] = {"status": "disabled", "requests": 0}
        return
    diagnostics: dict = {}
    df = fetch_em_kline(
        pending_symbols,
        start,
        end,
        config=config,
        timeout_sec=8.0,
        diagnostics=diagnostics,
    )
    run.rows_read += df.height
    run.note_empty(diagnostics.get("empty_symbols") or [], "eastmoney")
    failures = diagnostics.get("failed_symbols") or {}
    if df.is_empty():
        status = "failed" if failures else "empty"
        written = 0
    else:
        wanted = pl.DataFrame(
            {
                "symbol": [symbol for symbol, _day in sorted(pending)],
                "trade_date": [day for _symbol, day in sorted(pending)],
            },
            schema={"symbol": pl.Utf8, "trade_date": pl.Date},
        )
        gap_df = df.join(wanted, on=["symbol", "trade_date"], how="inner")
        snapshot = with_provenance(
            df,
            source=spec.backup,
            data_version=data_version_for("daily_bars"),
        )
        write_backup_snapshot(
            config,
            "daily_bars",
            snapshot,
            run_id=run.run_id,
            batch_id="em-kline-snapshot",
            source=spec.backup,
            trade_date=end,
        )
        written = gapfill._stage_daily_gap_batch(
            config,
            run.run_id,
            batch_id="em-kline-gapfill",
            source=spec.backup,
            frame=gap_df,
            symbols=pending_symbols,
            start=start,
            end=end,
        )
        run.rows_written += written
        status = "success" if written else "partial"
    routes = (diagnostics.get("route_outcomes") or {}).values()
    run.source_outcomes["eastmoney"] = {
        "status": status,
        "requests": len(pending_symbols),
        "rows": df.height,
        "rows_written": written,
        "empty_symbols": len(diagnostics.get("empty_symbols") or []),
        "failed_symbols": len(failures),
        "failure_reasons": dict(sorted(failures.items())),
        "proxy_failed": sum(bool(route.get("proxy_failed")) for route in routes),
        "direct_failed": sum(bool(route.get("direct_failed")) for route in routes),
        "direct_succeeded": sum(bool(route.get("direct_succeeded")) for route in routes),
    }
    run.findings.append(
        {
            "dataset": "daily_bars",
            "severity": "warning" if status != "success" else "info",
            "check": "daily_bars_eastmoney_gapfill",
            "message": (
                f"EastMoney historical fallback wrote {written} row(s) for "
                f"{len(pending_symbols)} remaining symbol(s); status={status}"
            ),
            **run.source_outcomes["eastmoney"],
        }
    )


def _stage_ths(run: _ChainRun) -> None:
    ths = gapfill._gapfill_missing_keys_via_ths(
        run.config,
        run.run_id,
        missing_keys=run.missing_keys(),
        start=run.start,
        end=run.end,
    )
    run.absorb(ths, empty_source="ths")


def _stage_baostock(run: _ChainRun) -> None:
    # Everything above can be unavailable at once — EastMoney's history host is
    # unreachable from some egress, THS can be disabled, and the exchange
    # publishes per session rather than per symbol. Reaching a fourth vendor in
    # its own failure domain is what keeps the run from ending on "unknown"
    # when the answer was obtainable; it costs nothing when the chain already
    # resolved everything, because it is only handed what is still missing.
    baostock = gapfill._gapfill_missing_keys_via_baostock(
        run.config,
        run.run_id,
        missing_keys=run.missing_keys(),
        start=run.start,
        end=run.end,
    )
    run.absorb(baostock, empty_source="baostock")


#: The order sources are asked for missing daily-bar keys.
MULTIDAY_GAPFILL_CHAIN: tuple[GapFillStage, ...] = (
    GapFillStage("exchange", "exchange", _stage_exchange),
    GapFillStage("exchange_dayk", "exchange", _stage_exchange_dayk, settles_early=True),
    GapFillStage("sina", "sina", _stage_sina),
    GapFillStage("eastmoney", "eastmoney", _stage_eastmoney),
    GapFillStage("ths", "ths", _stage_ths),
    GapFillStage("baostock", "baostock", _stage_baostock),
)


def _gapfill_multiday_via_kline(
    config: Config,
    run_id: str,
    *,
    symbols: list[str],
    start: date,
    end: date,
    require_complete: bool = True,
) -> dict:
    """Fill exact missing keys through the bounded multi-source route chain."""
    import polars as pl

    from cnequity.storage import StagingWriter

    if not symbols:
        return {"rows_read": 0, "rows_written": 0, "filled": False}

    requested = list(dict.fromkeys(symbols))
    sessions = list_trading_dates(config, start, end)
    expected_keys = gapfill._expected_session_keys(config, requested, sessions, start, end)
    writer = StagingWriter(config.staging_root)

    def staged_keys() -> set[tuple[str, date]]:
        files = writer.list_run_files("daily_bars", run_id)
        if not files:
            return set()
        keys = (
            pl.scan_parquet([str(path) for path in files])
            .filter(
                (pl.col("trade_date") >= start)
                & (pl.col("trade_date") <= end)
                & pl.col("symbol").is_in(requested)
            )
            .select("symbol", "trade_date")
            .unique()
            .collect()
        )
        return set(zip(keys["symbol"].to_list(), keys["trade_date"].to_list(), strict=True))

    def missing_keys() -> set[tuple[str, date]]:
        return expected_keys - staged_keys()

    run = _ChainRun(
        config=config,
        run_id=run_id,
        start=start,
        end=end,
        requested=requested,
        require_complete=require_complete,
        missing_keys=missing_keys,
    )
    for stage in MULTIDAY_GAPFILL_CHAIN:
        stage.run(run)
        if stage.settles_early and not run.pending_symbols():
            # The exchange stages answered for every symbol: nothing is left
            # for the per-symbol vendors, and nothing needs certifying.
            return {
                "rows_read": run.rows_read,
                "rows_written": run.rows_written,
                "filled": bool(run.rows_written),
                "complete": True,
                "audit_findings": run.findings,
                "source_outcomes": run.source_outcomes,
            }
    rows_read, rows_written = run.rows_read, run.rows_written
    findings, source_outcomes, empty_evidence = (
        run.findings,
        run.source_outcomes,
        run.empty_evidence,
    )

    remaining = missing_keys()
    suspended: set[tuple[str, date]] = set()
    if remaining:
        # Nothing above could fill these. Before calling them unresolved, find
        # out whether they are absences the market itself explains.
        suspended, suspension_outcome = gapfill._learn_suspensions_from_baostock(
            config, run_id, remaining, start, end
        )
        source_outcomes["baostock_suspensions"] = suspension_outcome
        if suspended:
            expected_keys -= suspended
            findings.append(
                {
                    "dataset": "daily_bars",
                    "severity": "info",
                    "check": "daily_bars_suspension_evidence",
                    "message": (
                        f"{len(suspended)} absent key(s) confirmed as suspensions by baostock "
                        "and recorded in trading_status"
                    ),
                    "suspended_keys": len(suspended),
                }
            )
    remaining = missing_keys()
    observed = staged_keys()
    expected_no_data = {
        symbol
        for symbol, sources in empty_evidence.items()
        if len(sources) >= 2
        and not any(key[0] == symbol for key in observed)
        and all((symbol, session) in remaining for session in sessions)
    }
    # A vendor saying "suspended on every session you asked about" is not the
    # same claim as two vendors independently returning nothing: the first is a
    # positive statement about the market, the second only says nobody had it.
    # Names that spend a whole window halted for a restructuring are exactly
    # the case the two-empty rule cannot reach — measured on the 2005-2015
    # repair, 19 of them were suspended for every session of 2010 and the run
    # still refused, because no vendor can return rows that never existed.
    halted_symbols = {symbol for symbol, _day in suspended}
    positively_halted = {
        symbol
        for symbol in halted_symbols
        if not any(key[0] == symbol for key in observed)
        and not any(key[0] == symbol for key in expected_keys)
    }
    # Kept apart from `expected_no_data` for the report below: both mean "no
    # rows are owed here", but only one of them was certified by two sources
    # agreeing, and `empty_evidence` holds an entry for that one alone.
    multi_source_no_data = set(expected_no_data)
    if positively_halted:
        expected_no_data |= positively_halted
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_window_fully_suspended",
                "message": (
                    f"{len(positively_halted)} symbol(s) were suspended for every session in "
                    f"{start}..{end} — certified from the vendor's own trading status, not "
                    "from an absence of rows"
                ),
                "symbols": sorted(positively_halted),
            }
        )
    unresolved = {key for key in remaining if key[0] not in expected_no_data}
    if multi_source_no_data:
        # Only the symbols this rule actually certified. A halted name reaches
        # `expected_no_data` through the finding above, on the vendor's trading
        # status rather than on two sources returning nothing — it has no entry
        # in `empty_evidence`, so listing it here both raised `KeyError` and
        # claimed evidence that was never collected.
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "info",
                "check": "daily_bars_multi_source_no_data",
                "message": (
                    f"certified {len(multi_source_no_data)} symbol(s) as source-empty only after "
                    "two independent per-symbol sources agreed"
                ),
                "symbols": sorted(multi_source_no_data),
                "evidence": {
                    symbol: sorted(empty_evidence[symbol])
                    for symbol in sorted(multi_source_no_data)
                },
            }
        )
    if unresolved:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_gapfill_incomplete",
                "message": (
                    f"multi-source gap-fill left {len(unresolved)} symbol×session key(s) "
                    f"unresolved over {start}..{end}"
                ),
                "missing_keys": len(unresolved),
                "sample_keys": [
                    {"symbol": symbol, "trade_date": day.isoformat()}
                    for symbol, day in sorted(unresolved)[:8]
                ],
            }
        )
    return {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "filled": bool(rows_written) or bool(expected_no_data),
        "complete": not unresolved,
        "expected_no_data_symbols": sorted(expected_no_data),
        "audit_findings": findings,
        "source_outcomes": source_outcomes,
        "missing_keys": len(unresolved),
    }


# A bar captured before the session opens is the previous close stamped on every
# field: open==high==low==close and zero volume. A handful of these on any day
# are genuine suspensions, but a whole universe of them means the fetch ran too
# early — 2026-07-22 arrived that way from a pre-open run. Below this share it is
# suspensions; at or above it, it is a mis-timed capture.
_PLACEHOLDER_SHARE_LIMIT = 0.5


def _quarantine_probe_placeholders(
    config: Config, run_id: str, probe: set[str], start: date, end: date
) -> set[str]:
    """Pull zero-volume rows of undated codes that never traded out of staging.

    TDX answers a code between allotment and listing with a flat bar at the
    issue price and no volume. 001381.SZ got one on 2026-10-19, four days
    before its allotment results were even published; the probe read it as a
    listing and the deep window then owed 10-12..10-16 as interior gaps. A row
    nobody traded proves nothing about a listing, so a probed code with no
    traded row in the window keeps none. Any traded row keeps them all.
    """
    from uuid import uuid4

    from cnequity.storage.atomic import write_parquet_atomic
    from cnequity.storage.parquet import StagingWriter

    writer = StagingWriter(config.staging_root)
    window = (
        pl.col("symbol").is_in(sorted(probe))
        & (pl.col("trade_date") >= start)
        & (pl.col("trade_date") <= end)
    )
    files = [(path, pl.read_parquet(path)) for path in writer.list_run_files("daily_bars", run_id)]
    staged = [frame.filter(window) for _path, frame in files if "volume" in frame.columns]
    staged = [frame for frame in staged if not frame.is_empty()]
    if not staged:
        return set()
    rows = pl.concat([frame.select("symbol", "volume") for frame in staged], how="vertical_relaxed")
    traded = set(rows.filter(pl.col("volume") > 0).get_column("symbol").to_list())
    placeholders = set(rows.get_column("symbol").to_list()) - traded
    if not placeholders:
        return set()

    dropped = window & pl.col("symbol").is_in(sorted(placeholders))
    safe_run = "".join(c if c.isalnum() or c in "-_" else "_" for c in run_id)
    quarantine = (
        config.staging_root.parent / "_quarantine" / f"daily_bars-{safe_run}-probe-{uuid4().hex}"
    )
    quarantine.mkdir(parents=True)
    rejected = pl.concat(
        [frame.filter(dropped) for _path, frame in files if "volume" in frame.columns],
        how="diagonal_relaxed",
    )
    write_parquet_atomic(quarantine / "rejected.parquet", rejected, compression="zstd")
    for path, frame in files:
        if "volume" not in frame.columns:
            continue
        kept = frame.filter(~dropped)
        if kept.height == frame.height:
            continue
        sealed = path.with_suffix(".sealed.json").exists()
        path.with_suffix(".sealed.json").unlink(missing_ok=True)
        if kept.is_empty():
            path.unlink()
            continue
        write_parquet_atomic(path, kept, compression="zstd")
        if sealed:
            writer.seal_file(path, "daily_bars", run_id)
    logger.info(
        "daily_bars: %d undated code(s) returned only zero-volume rows; quarantined %d "
        "pre-listing placeholder row(s) (e.g. %s)",
        len(placeholders),
        rejected.height,
        ", ".join(sorted(placeholders)[:5]),
    )
    return placeholders


def _reject_preopen_placeholder(config: Config, run_id: str, trade_date: date) -> None:
    """Fail the step if the freshest staged day is mostly pre-open placeholders.

    Checked against staging, before compact promotes anything, so a mis-timed
    run stays quarantined in staging instead of overwriting a good curated
    partition. `by_date` semantics mean the fix is simply to re-run after the
    close, which a failed step invites rather than hides.
    """
    import polars as pl

    from cnequity.storage import StagingWriter

    files = StagingWriter(config.staging_root).list_run_files("daily_bars", run_id)
    if not files:
        return
    df = (
        pl.scan_parquet([str(f) for f in files])
        .filter(pl.col("trade_date") == trade_date)
        .select("open", "high", "low", "close", "volume")
        .collect()
    )
    if df.is_empty():
        return
    placeholder = df.filter(
        (pl.col("open") == pl.col("close"))
        & (pl.col("high") == pl.col("low"))
        & (pl.col("open") == pl.col("high"))
        & (pl.col("volume") == 0)
    ).height
    share = placeholder / df.height
    if share >= _PLACEHOLDER_SHARE_LIMIT:
        raise RuntimeError(
            f"daily_bars {trade_date}: {placeholder}/{df.height} rows "
            f"({share:.0%}) are pre-open placeholders (OHLC flat, zero volume) — "
            "the capture ran before the close. Re-run after the session closes."
        )
