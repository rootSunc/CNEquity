"""Contracts a run's staged rows must meet before they are merged.

Every source stages its own part, so a rule that needs context — the previous
close, the corporate actions — can only be checked where the staged rows meet
the committed lake: just before compaction. A row that cannot be real is moved
to ``_quarantine`` and its staged part rewritten without it; the committed
value for that key, if any, stays, and the date is recorded as missing so a
later fetch can fill it. A row that is real but incomplete is reported.

Only Beijing daily bars have contracts so far (see
``private/designs/bj-market-profile-2026-09-30.md``, phase 2).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from uuid import uuid4

import polars as pl

from cnequity.config import Config
from cnequity.domain.action_sessions import effective_session
from cnequity.domain.market_profile import BJ, exchange_start
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.atomic import write_json_atomic, write_parquet_atomic

logger = logging.getLogger(__name__)

# A close is priced to the fen, so a move at the limit can overshoot the
# fraction by rounding; half a point is well above that and well below a
# genuine breach.
_LIMIT_TOLERANCE = 0.005
_LOOKBACK_DAYS = 40


def bj_price_limit_breaches(
    staged: pl.DataFrame,
    committed_bars: pl.DataFrame,
    actions: pl.DataFrame,
    instruments: pl.DataFrame,
    market_sessions: list[date] | None = None,
) -> pl.DataFrame:
    """Staged Beijing rows whose close breaks their era's daily limit unexplained.

    ``committed_bars`` and ``staged`` need ``symbol, trade_date, close``;
    ``actions`` needs ``symbol, ex_date``; ``instruments`` needs
    ``symbol, list_date``. A row is a breach only when every reason a move
    may legitimately exceed the limit is ruled out: a first exchange session,
    a resumption after a halt, a corporate action taking effect, an era
    without a limit, or a security the instruments table does not know.

    The previous close must come from the market's previous session: across
    a session the lake is missing, a move is two days' worth and can pass the
    one-day limit honestly. ``market_sessions`` defaults to the dates seen in
    the bars given, which is only as complete as they are.
    """
    rows = staged.filter(pl.col("symbol").str.ends_with(f".{BJ.exchange}"))
    if "volume" in rows.columns:
        rows = rows.filter(pl.col("volume").fill_null(0) > 0)
    if rows.is_empty():
        return rows.clear()
    history = (
        pl.concat(
            [
                committed_bars.select("symbol", "trade_date", "close").join(
                    rows.select("symbol", "trade_date"), on=["symbol", "trade_date"], how="anti"
                ),
                rows.select("symbol", "trade_date", "close"),
            ],
            how="vertical_relaxed",
        )
        .unique(["symbol", "trade_date"], keep="last")
        .sort("symbol", "trade_date")
        .with_columns(
            pl.col("close").shift(1).over("symbol").alias("_prev_close"),
            pl.col("trade_date").shift(1).over("symbol").alias("_prev_date"),
        )
    )
    calendar = sorted(set(market_sessions or history.get_column("trade_date").to_list()))
    previous_session = pl.DataFrame(
        {"trade_date": calendar[1:], "_market_prev": calendar[:-1]},
        schema={"trade_date": pl.Date, "_market_prev": pl.Date},
    )
    candidates = (
        rows.select("symbol", "trade_date")
        .join(history, on=["symbol", "trade_date"])
        .join(previous_session, on="trade_date", how="left")
        .join(instruments.select("symbol", "list_date").unique("symbol"), on="symbol")
        .with_columns(exchange_start(pl.col("list_date")).alias("_start"))
        .filter(
            (pl.col("trade_date") >= pl.col("_start"))
            & pl.col("_prev_date").is_not_null()
            & (pl.col("_prev_date") >= pl.col("_start"))
            # The security's previous bar is the market's previous session:
            # no halt, and no session the lake is missing in between.
            & (pl.col("_prev_date") == pl.col("_market_prev"))
            & (pl.col("_prev_close") > 0)
        )
    )
    if candidates.is_empty():
        return rows.clear()
    limits = pl.DataFrame(
        {
            "_era_start": [e.start or date(1990, 1, 1) for e in BJ.eras],
            "_limit": [e.price_limit for e in BJ.eras],
        },
        schema={"_era_start": pl.Date, "_limit": pl.Float64},
    ).sort("_era_start")
    candidates = candidates.sort("trade_date").join_asof(
        limits, left_on="trade_date", right_on="_era_start", strategy="backward"
    )
    if not actions.is_empty():
        sessions = history.select("symbol", "trade_date")
        effective = (
            effective_session(actions.select("symbol", "ex_date").unique(), sessions)
            .filter(pl.col("effective_session").is_not_null())
            .select("symbol", pl.col("effective_session").alias("trade_date"))
            .unique()
        )
        candidates = candidates.join(effective, on=["symbol", "trade_date"], how="anti")
    breaches = candidates.filter(
        pl.col("_limit").is_not_null()
        & (
            (pl.col("close") / pl.col("_prev_close") - 1).abs()
            > pl.col("_limit") + _LIMIT_TOLERANCE
        )
    )
    return rows.join(
        breaches.select("symbol", "trade_date"), on=["symbol", "trade_date"], how="semi"
    )


def bj_rows_missing_amount(staged: pl.DataFrame, instruments: pl.DataFrame) -> pl.DataFrame:
    """Exchange-era Beijing rows staged without turnover (reported, not rejected)."""
    if "amount" not in staged.columns:
        return staged.clear()
    rows = staged.filter(
        pl.col("symbol").str.ends_with(f".{BJ.exchange}")
        & pl.col("amount").is_null()
        & (pl.col("volume").fill_null(0) > 0 if "volume" in staged.columns else pl.lit(True))
    )
    if rows.is_empty():
        return rows
    known = instruments.select("symbol", "list_date").unique("symbol")
    return (
        rows.join(known, on="symbol", how="left")
        .filter(pl.col("trade_date") >= exchange_start(pl.col("list_date")))
        .drop("list_date")
    )


# A session missing at least this many listed Beijing names, and this share
# of them, is incomplete. Below it a new listing or a late delisting the
# roster has not caught up with would raise it every day.
_SHORTFALL_MIN_SYMBOLS = 5
_SHORTFALL_MIN_SHARE = 0.02


def bj_session_shortfall(
    sessions: list[date],
    present: pl.DataFrame,
    instruments: pl.DataFrame,
    status: pl.DataFrame,
) -> pl.DataFrame:
    """Sessions whose Beijing bars miss listed names nothing says were out.

    ``present`` holds ``symbol, trade_date`` of the bars the lake will have;
    ``instruments`` needs ``symbol, list_date, delist_date``; ``status`` needs
    ``symbol, trade_date, status, source``. A ``derived_*`` status row is
    ignored: it is inferred from missing bars, so it cannot excuse one.
    """
    start = BJ.era("bse").start
    days = sorted(d for d in set(sessions) if d >= start)
    empty = pl.DataFrame(
        schema={
            "trade_date": pl.Date,
            "expected": pl.UInt32,
            "missing": pl.UInt32,
            "sample": pl.List(pl.Utf8),
        }
    )
    if not days:
        return empty
    listed = instruments.filter(pl.col("symbol").str.ends_with(f".{BJ.exchange}")).select(
        "symbol", "list_date", "delist_date"
    )
    roster = (
        pl.DataFrame({"trade_date": days}, schema={"trade_date": pl.Date})
        .join(listed, how="cross")
        .filter(
            pl.col("list_date").is_not_null()
            & (pl.col("list_date") <= pl.col("trade_date"))
            & (pl.col("delist_date").is_null() | (pl.col("trade_date") < pl.col("delist_date")))
        )
        .select("symbol", "trade_date")
    )
    if not status.is_empty():
        out = status.filter(
            ~pl.col("source").str.starts_with("derived_")
            & pl.col("status").is_in(["suspended", "delisted"])
        ).select("symbol", "trade_date")
        roster = roster.join(out, on=["symbol", "trade_date"], how="anti")
    missing = roster.join(
        present.select("symbol", "trade_date").unique(), on=["symbol", "trade_date"], how="anti"
    )
    counts = (
        roster.group_by("trade_date")
        .len("expected")
        .join(
            missing.group_by("trade_date").agg(
                pl.len().alias("missing"), pl.col("symbol").sort().head(20).alias("sample")
            ),
            on="trade_date",
        )
        .filter(
            (pl.col("missing") >= _SHORTFALL_MIN_SYMBOLS)
            & (pl.col("missing") >= _SHORTFALL_MIN_SHARE * pl.col("expected"))
        )
        .sort("trade_date")
    )
    return counts.select("trade_date", "expected", "missing", "sample") if counts.height else empty


def _committed(root: Path | None, dataset: str, **scan) -> pl.DataFrame | None:
    """The committed generation of ``dataset``, one row per primary key."""
    if root is None or not dataset_has_parquet(root):
        return None
    from cnequity.domain.canonical import dedupe_lazy_by_primary_key
    from cnequity.domain.datasets import DATASETS

    lazy = scan_parquet_root(root, partition_col=DATASETS[dataset].partition_col, **scan)
    return dedupe_lazy_by_primary_key(lazy, dataset).collect()


def enforce_daily_bar_contracts(
    config: Config,
    run_id: str,
    staged_files: list[Path],
    *,
    committed_root: Path | None,
    actions_root: Path | None,
    instruments_root: Path | None,
    status_root: Path | None = None,
    staged_actions: pl.DataFrame | None = None,
    staged_status: pl.DataFrame | None = None,
) -> list[dict]:
    """Apply the Beijing daily-bar contracts to a run's staged rows.

    Quarantines impossible rows first, then checks that every session the run
    staged holds its listed Beijing names. Returns audit findings; staged
    files are rewritten only when a row is removed.
    """
    findings = _quarantine_limit_breaches(
        config,
        run_id,
        staged_files,
        committed_root=committed_root,
        actions_root=actions_root,
        instruments_root=instruments_root,
        staged_actions=staged_actions,
    )
    if config.ingest_universe in {"all_a", "all_instruments"}:
        findings.extend(
            _session_completeness(
                config,
                [p for p in staged_files if p.exists()],
                committed_root=committed_root,
                instruments_root=instruments_root,
                status_root=status_root,
                staged_status=staged_status,
            )
        )
    return findings


def _session_completeness(
    config: Config,
    staged_files: list[Path],
    *,
    committed_root: Path | None,
    instruments_root: Path | None,
    status_root: Path | None,
    staged_status: pl.DataFrame | None,
) -> list[dict]:
    if not staged_files:
        return []
    staged = pl.concat(
        [pl.read_parquet(p, columns=["symbol", "trade_date"]) for p in staged_files],
        how="vertical_relaxed",
    )
    sessions = sorted(set(staged.get_column("trade_date").to_list()))
    sessions = [d for d in sessions if d >= BJ.era("bse").start]
    if not sessions:
        return []
    instruments = _committed(instruments_root, "instruments")
    if instruments is None or not {"list_date", "delist_date"} <= set(instruments.columns):
        return []
    committed = _committed(committed_root, "daily_bars", start=sessions[0], end=sessions[-1])
    present = staged.filter(pl.col("symbol").str.ends_with(f".{BJ.exchange}"))
    if committed is not None:
        present = pl.concat(
            [present, committed.select("symbol", "trade_date")], how="vertical_relaxed"
        )
    status_parts = [
        frame.select("symbol", "trade_date", "status", "source")
        for frame in (
            _committed(status_root, "trading_status", start=sessions[0], end=sessions[-1]),
            staged_status,
        )
        if frame is not None and not frame.is_empty()
    ]
    status = (
        pl.concat(status_parts, how="vertical_relaxed")
        if status_parts
        else pl.DataFrame(
            schema={"symbol": pl.Utf8, "trade_date": pl.Date, "status": pl.Utf8, "source": pl.Utf8}
        )
    )
    short = bj_session_shortfall(sessions, present, instruments, status)
    if short.is_empty():
        return []
    from cnequity.storage.state import StateStore

    StateStore(config.meta_root).record_missing_dates(
        "daily_bars", short.get_column("trade_date").to_list(), reason="bj_session_incomplete"
    )
    return [
        {
            "dataset": "daily_bars",
            "severity": "warning",
            "check": "daily_bars_bj_session_incomplete",
            "message": (
                f"{short.height} session(s) miss listed Beijing names no status source "
                "says were suspended; recorded as missing for the next run to refetch"
            ),
            "sessions": [
                {
                    "trade_date": row["trade_date"].isoformat(),
                    "expected": row["expected"],
                    "missing": row["missing"],
                    "sample": row["sample"],
                }
                for row in short.iter_rows(named=True)
            ],
        }
    ]


def _quarantine_limit_breaches(
    config: Config,
    run_id: str,
    staged_files: list[Path],
    *,
    committed_root: Path | None,
    actions_root: Path | None,
    instruments_root: Path | None,
    staged_actions: pl.DataFrame | None = None,
) -> list[dict]:
    """Quarantine staged Beijing rows that break their era's price limit."""
    bj_files: list[tuple[Path, pl.DataFrame]] = []
    for path in staged_files:
        frame = pl.read_parquet(path)
        if "symbol" in frame.columns and frame["symbol"].str.ends_with(".BJ").any():
            bj_files.append((path, frame))
    if not bj_files:
        return []
    staged = pl.concat([frame for _, frame in bj_files], how="diagonal_relaxed").filter(
        pl.col("symbol").str.ends_with(".BJ")
    )
    symbols = staged.get_column("symbol").unique().to_list()
    first = staged.get_column("trade_date").min() - timedelta(days=_LOOKBACK_DAYS)
    last = staged.get_column("trade_date").max()
    instruments = _committed(instruments_root, "instruments", symbols=symbols)
    if instruments is None or "list_date" not in instruments.columns:
        # Without listing dates the exchange era cannot be told from NEEQ OTC
        # quotes, so nothing is judged.
        return []
    committed = _committed(committed_root, "daily_bars", start=first, end=last, symbols=symbols)
    if committed is None:
        committed = pl.DataFrame(
            schema={"symbol": pl.Utf8, "trade_date": pl.Date, "close": pl.Float64}
        )
    committed = committed.filter(pl.col("volume").fill_null(0) > 0)
    actions = _committed(actions_root, "corporate_actions", symbols=symbols)
    parts = [
        a.select("symbol", "ex_date")
        for a in (actions, staged_actions)
        if a is not None and not a.is_empty()
    ]
    all_actions = (
        pl.concat(parts, how="vertical_relaxed")
        if parts
        else pl.DataFrame(schema={"symbol": pl.Utf8, "ex_date": pl.Date})
    )
    from cnequity.query.calendar import list_trading_dates

    # The market's sessions come from the calendar, not from the bars' partition
    # directories: those are months since daily_bars moved to monthly partitions.
    sessions = set(list_trading_dates(config, first, last)) | set(
        staged.get_column("trade_date").to_list()
    )
    breaches = bj_price_limit_breaches(
        staged, committed, all_actions, instruments, market_sessions=sorted(sessions)
    )
    findings: list[dict] = []
    missing_amount = bj_rows_missing_amount(
        staged.join(breaches, on=["symbol", "trade_date"], how="anti"), instruments
    )
    if missing_amount.height:
        findings.append(
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "daily_bars_bj_amount_unavailable",
                "message": (
                    f"{missing_amount.height} Beijing exchange-session row(s) staged without "
                    "turnover; no source that answered this run carries it for those keys"
                ),
                "rows": missing_amount.height,
                "sample": [
                    f"{r['symbol']}|{r['trade_date']}"
                    for r in missing_amount.head(20).iter_rows(named=True)
                ],
            }
        )
    if breaches.is_empty():
        return findings

    keys = breaches.select("symbol", "trade_date")
    safe_run = "".join(c if c.isalnum() or c in "-_" else "_" for c in run_id)
    quarantine = (
        config.staging_root.parent / "_quarantine" / f"daily_bars-{safe_run}-bj-limit-{uuid4().hex}"
    )
    quarantine.mkdir(parents=True)
    write_parquet_atomic(quarantine / "rejected.parquet", breaches, compression="zstd")
    for path, frame in bj_files:
        kept = frame.join(keys, on=["symbol", "trade_date"], how="anti")
        if kept.height == frame.height:
            continue
        if kept.is_empty():
            path.unlink()
            path.with_suffix(".sealed.json").unlink(missing_ok=True)
        else:
            from cnequity.storage.parquet import StagingWriter

            sealed = path.with_suffix(".sealed.json").exists()
            path.with_suffix(".sealed.json").unlink(missing_ok=True)
            write_parquet_atomic(path, kept, compression="zstd")
            if sealed:
                StagingWriter(config.staging_root).seal_file(path, "daily_bars", run_id)
    days = sorted({d for d in breaches.get_column("trade_date").to_list()})
    from cnequity.storage.state import StateStore

    StateStore(config.meta_root).record_missing_dates("daily_bars", days, reason="bj_price_limit")
    report = {
        "dataset": "daily_bars",
        "run_id": run_id,
        "rule": "bj_price_limit",
        "rows_rejected": breaches.height,
        "rejected": [f"{r['symbol']}|{r['trade_date']}" for r in keys.iter_rows(named=True)],
    }
    write_json_atomic(quarantine / "report.json", report, indent=2, default=str)
    logger.warning(
        "daily_bars: quarantined %d Beijing row(s) past the price limit", breaches.height
    )
    findings.append(
        {
            "dataset": "daily_bars",
            "severity": "warning",
            "check": "daily_bars_bj_price_limit_quarantined",
            "message": (
                f"{breaches.height} Beijing row(s) move past their era's daily limit with no "
                "corporate action, listing or halt to explain it; quarantined, committed "
                "values kept, dates left missing for a clean fetch"
            ),
            "rows_rejected": breaches.height,
            "quarantine": str(quarantine),
            "rejected": report["rejected"][:50],
        }
    )
    return findings
