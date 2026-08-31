"""Shared helpers for step implementations."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import polars as pl

from cnequity.adapters.calendar.holidays_cn import CLOSED_DATES
from cnequity.adapters.tdx_protocol.client import fetch_instruments
from cnequity.config import Config
from cnequity.domain.datasets import DATASETS, fetch_semantics
from cnequity.domain.frames import with_columns_unless_blank
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.domain.symbols import is_subscription_placeholder
from cnequity.storage import StagingWriter
from cnequity.storage.state import StateStore

logger = logging.getLogger(__name__)

INCREMENTAL_LOOKBACK_DAYS = 5
BACKFILL_START = date(2016, 1, 1)

# These feeds are rolling *calendar*-date disclosures.  A weekend or exchange
# holiday can still contain a publication, so using ``list_trading_dates``
# here would silently make those rows impossible to fetch.  Keep this small
# and explicit rather than weakening the trading-day contract for market
# observations such as bars, flow, and breadth.
CALENDAR_DATE_DATASETS = frozenset({"announcement_index", "regulatory_events"})


class SnapshotBackfillError(RuntimeError):
    """Raised when backfill is requested for a snapshot-only dataset."""


def write_simple(
    config: Config,
    run_id: str,
    dataset: str,
    df: pl.DataFrame,
    *,
    batch_id: str = "batch-0",
) -> dict:
    writer = StagingWriter(config.staging_root)
    writer.write_batch(dataset, run_id, batch_id, df)
    return {"rows_read": df.height, "rows_written": df.height}


def incremental_window(config: Config, dataset: str, trade_date: date) -> date:
    """Return the start of a dataset's incremental reconciliation window.

    Most event/snapshot feeds retain the historical ``watermark + 1``
    behaviour.  Datasets whose source can revise settled rows declare a
    ``reconciliation_lookback_days`` on their :class:`DatasetSpec`; those
    datasets deliberately overlap the tail on every run.  A
    ``trading_day`` lookback counts sessions from the curated exchange
    calendar, while ``calendar`` counts literal date distance.  The lookback
    includes the watermark (and, when there is no watermark, the as-of day),
    so a value of five fetches the latest five observations and then any new
    sessions after the watermark.
    """
    state = StateStore(config.meta_root)
    watermark = state.get_date(dataset)
    spec = DATASETS.get(dataset)
    lookback = max(int(getattr(spec, "reconciliation_lookback_days", 0) or 0), 0)
    mode = getattr(spec, "reconciliation_lookback_mode", "calendar")

    if lookback:
        anchor = min(watermark, trade_date) if watermark is not None else trade_date
        if mode == "trading_day":
            # Probe a generously padded calendar interval.  The calendar
            # helper itself excludes weekends, exchange holidays, and any
            # curated holiday overrides, so the returned count is a true
            # session count rather than a weekday approximation.
            probe_days = max(lookback * 4 + 14, lookback + 7)
            probe_start = anchor - timedelta(days=probe_days)
            sessions = [
                day for day in list_trading_dates(config, probe_start, anchor) if day <= anchor
            ]
            if sessions:
                start = sessions[max(0, len(sessions) - lookback)]
            else:
                start = anchor - timedelta(days=lookback - 1)
        else:
            start = anchor - timedelta(days=lookback - 1)
        return min(start, trade_date)

    if watermark is not None:
        return min(watermark + timedelta(days=1), trade_date)
    return trade_date - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)


def _load_trading_calendar_df(
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame | None:
    """Load trading_calendar, preferring a lazy hive scan with optional date prune."""
    curated = config.curated_root / "trading_calendar"
    if curated.exists() and any(curated.rglob("*.parquet")):
        from cnequity.query.canonical import dedupe_by_primary_key

        try:
            from cnequity.query.parquet_scan import collect_parquet_root

            return dedupe_by_primary_key(
                collect_parquet_root(curated, partition_col="trade_date", start=start, end=end),
                "trading_calendar",
            )
        except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError) as exc:
            logger.warning(
                "curated trading_calendar scan failed for %s; salvaging readable files: %s",
                curated,
                exc,
            )
        files = list(curated.glob("**/*.parquet"))
        if files:
            try:
                lf = pl.scan_parquet([str(f) for f in files])
                if start is not None:
                    lf = lf.filter(pl.col("trade_date") >= start)
                if end is not None:
                    lf = lf.filter(pl.col("trade_date") <= end)
                return dedupe_by_primary_key(lf.collect(), "trading_calendar")
            except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError) as exc:
                logger.warning(
                    "mixed curated trading_calendar scan failed for %s; reading files individually: %s",
                    curated,
                    exc,
                )
                frames: list[pl.DataFrame] = []
                for path in files:
                    try:
                        frames.append(pl.read_parquet(path))
                    except (
                        FileNotFoundError,
                        OSError,
                        pl.exceptions.PolarsError,
                        ValueError,
                    ) as file_exc:
                        logger.warning(
                            "skipping unreadable trading_calendar file %s: %s", path, file_exc
                        )
                if frames:
                    frame = pl.concat(frames, how="diagonal_relaxed")
                    if "trade_date" in frame.columns:
                        if start is not None:
                            frame = frame.filter(pl.col("trade_date") >= start)
                        if end is not None:
                            frame = frame.filter(pl.col("trade_date") <= end)
                    return dedupe_by_primary_key(frame, "trading_calendar")
    staging_root = config.staging_root / "trading_calendar"
    staging = sorted(staging_root.rglob("*.parquet")) if staging_root.exists() else []
    if staging:
        from cnequity.query.canonical import dedupe_by_primary_key

        df = pl.concat([pl.read_parquet(path) for path in staging], how="diagonal_relaxed")
        if "trade_date" in df.columns:
            df = dedupe_by_primary_key(df, "trading_calendar")
        if start is not None:
            df = df.filter(pl.col("trade_date") >= start)
        if end is not None:
            df = df.filter(pl.col("trade_date") <= end)
        return df
    return None


def load_curated_instruments(config: Config) -> pl.DataFrame | None:
    """Read the merge-style instrument catalog across all surviving shards."""
    root = config.curated_root / "instruments"
    if not root.exists() or not any(root.rglob("*.parquet")):
        return None
    from cnequity.query.canonical import dedupe_by_primary_key
    from cnequity.query.parquet_scan import collect_parquet_root

    try:
        frame = collect_parquet_root(root, hive=False)
    except FileNotFoundError:
        return None
    return dedupe_by_primary_key(frame, "instruments")


def _load_staged_instruments(config: Config) -> pl.DataFrame | None:
    """Read all recoverable instrument fragments when curated data is absent."""
    root = config.staging_root / "instruments"
    files = sorted(root.rglob("*.parquet")) if root.exists() else []
    if not files:
        return None
    from cnequity.query.canonical import dedupe_by_primary_key

    frame = pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")
    return dedupe_by_primary_key(frame, "instruments")


def _without_subscription_placeholders(frame: pl.DataFrame) -> pl.DataFrame:
    """Keep transport-level subscription stubs out of downstream universes."""
    if frame.is_empty() or "name" not in frame.columns:
        return frame
    keep = [not is_subscription_placeholder(name) for name in frame["name"].to_list()]
    return frame.filter(pl.Series(keep))


def list_trading_dates(config: Config, start: date, end: date) -> list[date]:
    """Trading days in [start, end] from curated data or the bundled calendar.

    Never fall back to plain weekdays for the CN market: that would classify
    Spring Festival and National Day as sessions when the lake has not yet
    materialized ``trading_calendar``.
    """
    if start > end:
        return []
    cal = _load_trading_calendar_df(config, start=start, end=end)
    if cal is not None and not cal.is_empty() and "trade_date" in cal.columns:
        covered = set(cal.get_column("trade_date").drop_nulls().to_list())
        expected_days = (end - start).days + 1
        if len(covered) == expected_days:
            out = (
                cal.filter(
                    pl.col("is_trading")
                    & (pl.col("trade_date").dt.weekday() <= 5)
                    & ~pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
                )["trade_date"]
                .sort()
                .to_list()
            )
            if out:
                return out
        else:
            logger.warning(
                "trading_calendar only covers %d/%d calendar day(s) in %s..%s; "
                "rebuilding from the seed and bar evidence",
                len(covered),
                expected_days,
                start.isoformat(),
                end.isoformat(),
            )
    from cnequity.adapters.calendar.exchange_calendar import (
        build_trading_calendar,
        ensure_seed_csv,
    )

    seed_path = config.meta_root / "seeds" / "trading_calendar.csv"
    effective_seed = seed_path if seed_path.exists() else ensure_seed_csv()
    calendar = build_trading_calendar(
        start,
        end,
        seed_path=effective_seed,
        curated_root=config.curated_root if config.curated_root.exists() else None,
    )
    return (
        calendar.filter(
            pl.col("is_trading")
            & (pl.col("trade_date").dt.weekday() <= 5)
            & ~pl.col("trade_date").dt.strftime("%Y-%m-%d").is_in(CLOSED_DATES)
        )["trade_date"]
        .sort()
        .to_list()
    )


def incremental_trade_dates(config: Config, dataset: str, trade_date: date) -> list[date]:
    """Dates to fetch for a daily dataset: [watermark+1, trade_date].

    Event calendars use literal calendar dates because disclosures are not
    constrained to exchange sessions.  All other by-date datasets retain the
    strict curated trading-session walk.
    """
    start = incremental_window(config, dataset, trade_date)
    if dataset in CALENDAR_DATE_DATASETS:
        return [start + timedelta(days=offset) for offset in range((trade_date - start).days + 1)]
    return list_trading_dates(config, start, trade_date)


def is_trading_day(config: Config, trade_date: date) -> bool:
    """Return whether *trade_date* is a trading day per curated calendar or seed."""
    if trade_date.weekday() >= 5 or trade_date.isoformat() in CLOSED_DATES:
        return False
    cal = _load_trading_calendar_df(config, start=trade_date, end=trade_date)
    if cal is not None and not cal.is_empty():
        row = cal.filter(pl.col("trade_date") == trade_date)
        if not row.is_empty():
            return bool(row["is_trading"][0])

    from cnequity.adapters.calendar.exchange_calendar import (
        build_trading_calendar,
        ensure_seed_csv,
    )

    seed_path = config.meta_root / "seeds" / "trading_calendar.csv"
    effective_seed = seed_path if seed_path.exists() else ensure_seed_csv()
    day_cal = build_trading_calendar(
        trade_date,
        trade_date,
        seed_path=effective_seed,
        curated_root=config.curated_root if config.curated_root.exists() else None,
    )
    if day_cal.is_empty():
        raise RuntimeError(
            f"trading calendar returned no row for {trade_date.isoformat()}; "
            "refusing to classify it by weekday"
        )
    return bool(day_cal["is_trading"][0])


def _coverage_gap_findings(dataset: str, gap_dates: list[date]) -> list[dict]:
    if not gap_dates:
        return []
    gap_text = ", ".join(d.isoformat() for d in gap_dates)
    return [
        {
            "dataset": dataset,
            "severity": "warning",
            "check": "coverage_gap",
            "message": (
                f"{dataset}: skipped {len(gap_dates)} trading day(s) ({gap_text}) — "
                "snapshot fetch semantics cannot backfill historical values"
            ),
            "gap_dates": [d.isoformat() for d in gap_dates],
        }
    ]


def _backfill_empty_day_finding(dataset: str, empty_days: list[date]) -> dict:
    """Describe dates that were retried but returned no rows.

    Empty responses are non-fatal because some sources legitimately publish no
    rows on a session.  They must still be visible to the run audit; otherwise
    a backfill can report success while leaving permanent coverage holes.
    """
    return {
        "dataset": dataset,
        "severity": "warning",
        "check": "backfill_empty_days",
        "message": (
            f"{dataset}: {len(empty_days)} trading day(s) returned no rows; "
            "they remain absent and will be retried on the next backfill"
        ),
        "days_empty": len(empty_days),
        "sample_dates": [d.isoformat() for d in empty_days[:8]],
    }


def _dense_empty_day_finding(dataset: str, empty_days: list[date]) -> dict:
    """Keep an allowed empty response visible for a dense daily dataset."""
    return {
        "dataset": dataset,
        "severity": "warning",
        "check": "session_dense_empty_days",
        "message": (
            f"{dataset}: {len(empty_days)} trading day(s) returned no rows; "
            "the dense coverage watermark must not pass these sessions"
        ),
        "days_empty": len(empty_days),
        "sample_dates": [d.isoformat() for d in empty_days[:8]],
    }


def _validate_trade_date(
    df: pl.DataFrame,
    dataset: str,
    trade_date: date,
    *,
    date_col: str | None = None,
) -> None:
    """Reject a response whose date column names another day.

    The historical default checks ``trade_date`` only when present because a
    few period/snapshot datasets have another date key. Callers that fetch an
    exact day under a different key must pass it explicitly; then a missing
    date column is itself a malformed response.
    """
    column = date_col or "trade_date"
    if df.is_empty():
        return
    if column not in df.columns:
        if date_col is not None:
            raise RuntimeError(
                f"{dataset}: fetch for {trade_date.isoformat()} did not return "
                f"the configured date column {date_col!r}"
            )
        return
    parsed_dates = df.get_column(column).cast(pl.Date, strict=False)
    mismatched = int((parsed_dates.is_null() | (parsed_dates != trade_date).fill_null(True)).sum())
    if mismatched:
        raise RuntimeError(
            f"{dataset}: fetch for {trade_date.isoformat()} returned "
            f"{mismatched} row(s) with a different or invalid {column}"
        )


def fetch_incremental_daily(
    config: Config,
    dataset: str,
    trade_date: date,
    fetch_fn: Callable[[date], pl.DataFrame],
    *,
    allow_empty: bool = False,
    date_col: str | None = None,
) -> tuple[pl.DataFrame, list[dict]]:
    """Fetch one or more trading days from watermark+1 through *trade_date*.

    Returns ``(dataframe, audit_findings)``. Snapshot datasets only fetch
    *trade_date*; missed days are reported as ``coverage_gap`` findings.
    """
    semantics = fetch_semantics(dataset)
    if getattr(config, "_backfill", False):
        if semantics == "snapshot":
            raise SnapshotBackfillError(
                f"{dataset}: backfill not supported — fetch semantics are snapshot "
                "(live page stamped with trade_date; historical values unavailable)"
            )
        frame = fetch_fn(trade_date)
        if frame.is_empty() and not allow_empty:
            raise RuntimeError(f"{dataset}: no rows returned for {trade_date.isoformat()}")
        _validate_trade_date(frame, dataset, trade_date, date_col=date_col)
        return frame, []

    spec = DATASETS.get(dataset)
    if semantics == "snapshot" and spec is not None and not spec.watermark:
        # Rolling live windows (for example share_unlock_schedule) are not
        # incremental histories. Do not use a legacy state file to manufacture
        # gap dates; the current snapshot is the only honest request and its
        # future event dates must never become a watermark.
        dates = [trade_date]
    else:
        dates = incremental_trade_dates(config, dataset, trade_date)
    if not dates:
        return pl.DataFrame(), []

    if semantics == "snapshot":
        gap_dates = [d for d in dates if d < trade_date]
        fetch_dates = [trade_date]
        findings = _coverage_gap_findings(dataset, gap_dates)
    else:
        fetch_dates = dates
        findings = []

    frames: list[pl.DataFrame] = []
    empty_days: list[date] = []
    for d in fetch_dates:
        part = fetch_fn(d)
        if part.is_empty():
            if not allow_empty:
                raise RuntimeError(f"{dataset}: no rows returned for {d.isoformat()}")
            spec = DATASETS.get(dataset)
            if spec is not None and spec.coverage_mode == "session_dense":
                empty_days.append(d)
            continue
        # A by-date adapter must not let a vendor page leak rows from a
        # neighbouring session into the requested partition. Validate here
        # before diagonal concatenation; once several days are merged the
        # offending response can no longer be attributed to one request.
        _validate_trade_date(part, dataset, d, date_col=date_col)
        frames.append(part)
    if empty_days:
        findings.append(_dense_empty_day_finding(dataset, empty_days))
    if not frames:
        return pl.DataFrame(), findings
    combined = pl.concat(frames, how="diagonal_relaxed")
    # Rolling reconciliation windows intentionally overlap the watermark. The
    # canonical PK collapse keeps a revised row once while preserving the
    # source/provenance precedence used by lake reads.
    from cnequity.query.canonical import dedupe_by_primary_key

    return dedupe_by_primary_key(combined, dataset), findings


def load_symbols(config: Config) -> list[str]:
    """Universe symbols: curated instruments first, then staging, then source."""
    curated_frame = load_curated_instruments(config)
    staged_frame = _load_staged_instruments(config)
    if curated_frame is not None:
        curated_frame = _without_subscription_placeholders(curated_frame)
        return curated_frame["symbol"].drop_nulls().to_list()
    if staged_frame is not None:
        staged_frame = _without_subscription_placeholders(staged_frame)
        return staged_frame["symbol"].drop_nulls().to_list()
    df = fetch_instruments(
        rate_limit=config.tdx_rate_limit_spec(),
        allow_mock=config.tdx_allow_mock,
        config=config,
    )
    df = _without_subscription_placeholders(df)
    return df["symbol"].to_list()


def instrument_metadata(config: Config) -> pl.DataFrame:
    """Disk-only symbol/listing spans used for deterministic routing."""
    curated_frame = load_curated_instruments(config)
    staged_frame = _load_staged_instruments(config)
    empty = pl.DataFrame(
        schema={
            "symbol": pl.Utf8,
            "list_date": pl.Date,
            "delist_date": pl.Date,
            "asset_type": pl.Utf8,
        }
    )
    if curated_frame is not None:
        frame = _without_subscription_placeholders(curated_frame)
    elif staged_frame is not None:
        frame = _without_subscription_placeholders(staged_frame)
    else:
        return empty
    if "symbol" not in frame.columns:
        return empty
    columns = [
        name
        for name in ("symbol", "list_date", "delist_date", "asset_type")
        if name in frame.columns
    ]
    out = frame.select(columns)
    for name, dtype in (
        ("list_date", pl.Date),
        ("delist_date", pl.Date),
        ("asset_type", pl.Utf8),
    ):
        if name not in out.columns:
            out = with_columns_unless_blank(out, pl.lit(None, dtype=dtype).alias(name))
    return out


def load_curated_trading_status(
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
    symbols: list[str] | None = None,
) -> pl.DataFrame | None:
    """Load the available status evidence without making a network request.

    ``trading_status`` is an advisory but independently fetched daily
    snapshot.  Daily-bar routing may use it to prove that a missing symbol was
    suspended; it must never synthesize a status by treating an absent row as
    ``normal``.  A corrupt or absent status root therefore returns ``None``
    and leaves the symbol in the strict unknown bucket.
    """
    root = config.curated_root / "trading_status"
    if not root.exists() or not any(root.rglob("*.parquet")):
        return None
    try:
        from cnequity.query.canonical import dedupe_by_primary_key
        from cnequity.query.parquet_scan import collect_parquet_root

        frame = collect_parquet_root(
            root,
            partition_col="trade_date",
            start=start,
            end=end,
            symbols=symbols,
        )
    except (FileNotFoundError, OSError, pl.exceptions.PolarsError, ValueError) as exc:
        logger.warning("curated trading_status scan failed; status evidence unavailable: %s", exc)
        return None
    required = {"symbol", "trade_date", "is_trading"}
    if not required.issubset(frame.columns):
        logger.warning(
            "curated trading_status lacks required evidence columns: %s",
            sorted(required - set(frame.columns)),
        )
        return None
    return dedupe_by_primary_key(frame, "trading_status")


def _instrument_identity(config: Config, metadata: pl.DataFrame | None = None) -> dict:
    """Build the identity carried by negative evidence records.

    Revisions are the cheap authoritative invalidation signal after a normal
    compact.  The metadata digest is a fallback for direct step invocations
    and older lakes that predate revision receipts; changing a list/delist
    span or asset type still invalidates an old absence claim there.
    """
    state = StateStore(config.meta_root)
    revision = state.get_revision("instruments")
    frame = metadata if metadata is not None else instrument_metadata(config)
    rows = []
    if frame is not None and not frame.is_empty() and "symbol" in frame.columns:
        columns = [
            column
            for column in ("symbol", "list_date", "delist_date", "asset_type")
            if column in frame.columns
        ]
        for row in frame.select(columns).sort("symbol").iter_rows(named=True):
            rows.append(
                {
                    column: (value.isoformat() if isinstance(value, date) else value)
                    for column, value in row.items()
                }
            )
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str).encode()

    # A cached "no rows" claim is also invalid when trading status changes.
    # Catalog identity alone is insufficient: a symbol can remain the same
    # instrument while moving from suspended to trading (or vice versa).  Use
    # both the authoritative compact revision and a content fingerprint so
    # direct step calls and older lakes without receipts are covered too.
    status_revision = state.get_revision("trading_status")
    status_frame = load_curated_trading_status(config)
    status_rows: list[dict] = []
    if status_frame is not None and not status_frame.is_empty():
        volatile = {"source", "data_version", "fetched_at", "run_id", "capture_id"}
        status_columns = [column for column in status_frame.columns if column not in volatile]
        if status_columns:
            for row in (
                status_frame.select(status_columns).sort(status_columns).iter_rows(named=True)
            ):
                status_rows.append(
                    {
                        column: (value.isoformat() if isinstance(value, date) else value)
                        for column, value in row.items()
                    }
                )
    status_encoded = json.dumps(
        status_rows, sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return {
        "instruments_revision": revision,
        "instruments_fingerprint": hashlib.sha256(encoded).hexdigest(),
        "trading_status_revision": status_revision,
        "trading_status_fingerprint": hashlib.sha256(status_encoded).hexdigest(),
    }


def negative_evidence_ttl_days(config: Config, dataset: str) -> int:
    """Resolve deployment TTL, falling back to the dataset contract."""
    configured = getattr(config, "negative_evidence_ttl_days", None)
    if configured is not None:
        return max(int(configured), 0)
    spec = DATASETS.get(dataset)
    return max(int(getattr(spec, "negative_evidence_ttl_days", 0) or 0), 0)


def load_negative_evidence(
    config: Config,
    dataset: str,
    *,
    metadata: pl.DataFrame | None = None,
    now: datetime | None = None,
) -> list[dict]:
    """Return current negative records for a dataset under its identity."""
    identity = _instrument_identity(config, metadata)
    return StateStore(config.meta_root).get_negative_evidence(
        dataset,
        identity=identity,
        now=now,
    )


def record_negative_evidence(
    config: Config,
    dataset: str,
    symbols: Iterable[str],
    start: date,
    end: date,
    *,
    reason: str,
    source: str,
    metadata: pl.DataFrame | None = None,
    details: dict | None = None,
    now: datetime | None = None,
) -> None:
    """Persist a source-empty/no-data claim for each symbol in a window."""
    unique = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    if not unique:
        return
    entries = [
        {
            "symbol": symbol,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "reason": reason,
            "source": source,
            **(details or {}),
        }
        for symbol in unique
    ]
    StateStore(config.meta_root).record_negative_evidence(
        dataset,
        entries,
        ttl_days=negative_evidence_ttl_days(config, dataset),
        identity=_instrument_identity(config, metadata),
        now=now,
    )


def negative_evidence_covers(
    entry: dict,
    symbol: str,
    start: date,
    end: date,
) -> bool:
    """Whether one persisted claim covers the complete requested window."""
    if str(entry.get("symbol", "")).strip().upper() != str(symbol).strip().upper():
        return False
    try:
        claimed_start = date.fromisoformat(str(entry.get("window_start", entry.get("start"))))
        claimed_end = date.fromisoformat(str(entry.get("window_end", entry.get("end"))))
    except (TypeError, ValueError):
        return False
    return claimed_start <= start and claimed_end >= end


@dataclass
class DailyBarOwnership:
    """One window's explicit generic/dedicated/no-data ownership split.

    ``unknown`` is intentionally separate from ``expected_no_data``. The
    former must remain a fetch/retry obligation; only the latter may be
    omitted from a session-dense expected key set.
    """

    generic: list[str] = field(default_factory=list)
    delegated_delisted: list[str] = field(default_factory=list)
    expected_no_data: list[str] = field(default_factory=list)
    placeholder: list[str] = field(default_factory=list)
    unknown: list[str] = field(default_factory=list)
    negative_cached: list[str] = field(default_factory=list)
    no_data_reasons: dict[str, str] = field(default_factory=dict)


def classify_daily_bar_ownership(
    symbols: list[str],
    spans: dict[
        str,
        tuple[date | None, date | None] | tuple[date | None, date | None, str | None],
    ],
    start: date,
    end: date,
    *,
    bar_universe: set[str] | None = None,
    trading_status: pl.DataFrame | None = None,
    trading_sessions: Iterable[date] | None = None,
    negative_evidence: Iterable[dict] | None = None,
) -> DailyBarOwnership:
    """Route symbols using explicit metadata and evidence, never a ratio.

    A symbol is ``expected_no_data`` only when one of these independently
    inspectable facts holds: its listing/delisting span excludes the complete
    requested window; every covered session has an explicit non-trading status;
    or a still-live, identity-matching negative-evidence record covers the
    complete window. An absent status row, malformed instrument metadata, or
    an incomplete source response remains ``unknown`` and must be retried.

    ``bar_universe`` is historical positive-volume evidence. It is used for
    the narrow undated-ETF placeholder case, where a symbol with no observed
    traded bar is not safe to send through the expensive per-symbol fallback.
    """
    from cnequity.domain.symbols import is_etf_symbol, parse_symbol

    out = DailyBarOwnership()
    normalized_spans = {
        str(symbol).strip().upper(): span for symbol, span in spans.items() if str(symbol).strip()
    }
    normalized_bar_universe = (
        {str(symbol).strip().upper() for symbol in bar_universe if str(symbol).strip()}
        if bar_universe is not None
        else None
    )
    sessions = set(trading_sessions or ())
    status_by_symbol: dict[str, dict[date, bool | None]] = {}
    if trading_status is not None and not trading_status.is_empty():
        required = {"symbol", "trade_date", "is_trading"}
        if required.issubset(trading_status.columns):
            for row in trading_status.select("symbol", "trade_date", "is_trading").iter_rows(
                named=True
            ):
                normalized = str(row["symbol"]).strip().upper()
                raw_day = row["trade_date"]
                if not normalized or not isinstance(raw_day, date):
                    continue
                value = row["is_trading"]
                status_by_symbol.setdefault(normalized, {})[raw_day] = (
                    bool(value) if value is not None else None
                )
    evidence = list(negative_evidence or ())
    for symbol in symbols:
        normalized = str(symbol).strip().upper()
        if not normalized:
            continue
        if normalized not in normalized_spans:
            out.unknown.append(normalized)
            continue
        span = normalized_spans[normalized]
        list_date, delist_date = span[:2]
        asset_type = span[2] if len(span) >= 3 else None
        positive_status = normalized in status_by_symbol and any(
            value is True
            for day, value in status_by_symbol[normalized].items()
            if not sessions or day in sessions
        )
        if list_date is not None and list_date > end:
            out.expected_no_data.append(normalized)
            out.no_data_reasons[normalized] = "not_listed"
        elif delist_date is not None and delist_date < start:
            out.expected_no_data.append(normalized)
            out.no_data_reasons[normalized] = "delisted_before_window"
        elif delist_date is not None and delist_date <= end:
            # Delisted-recovery receipts cover equities/CDRs, not funds. ETF
            # instruments can receive a terminal date from the live snapshot
            # when a subscription/redemption code disappears; routing those
            # codes to the stock-only recovery gate blocks compaction forever.
            try:
                info = parse_symbol(normalized)
                is_fund = is_etf_symbol(info.code, info.exchange)
            except ValueError:
                is_fund = False
            (out.generic if is_fund else out.delegated_delisted).append(normalized)
        elif sessions and normalized in status_by_symbol:
            status = status_by_symbol[normalized]
            # A missing session in the status table is unknown, not a silent
            # non-trading assertion. False is accepted only when every
            # requested exchange session is explicitly covered.
            if sessions.issubset(status) and all(status[day] is False for day in sessions):
                out.expected_no_data.append(normalized)
                out.no_data_reasons[normalized] = "trading_status_non_trading"
                continue
            if positive_status:
                # Positive status evidence means a missing bar is a real
                # coverage obligation, even if an old negative cache exists.
                if len(span) >= 3 and asset_type is None:
                    out.unknown.append(normalized)
                else:
                    out.generic.append(normalized)
                continue
            elif sessions.intersection(status):
                out.unknown.append(normalized)
                continue
            else:
                # A status file that has rows for this symbol but none for
                # the requested sessions is not evidence of a suspension.
                # Keep the key in the strict retry set instead of letting the
                # ``elif`` chain silently drop it.
                out.unknown.append(normalized)
                continue
        elif len(span) >= 3 and asset_type is None:
            # A real instrument row with no asset classification cannot be
            # routed safely: treating it as an equity would either miss a
            # dedicated fallback or certify the wrong no-data reason.
            out.unknown.append(normalized)
        elif (
            asset_type == "etf"
            and list_date is None
            and bar_universe is not None
            and normalized not in normalized_bar_universe
        ):
            # This is likely an issued-but-not-yet-listed fund code, but a
            # delayed list_date enrichment is indistinguishable here. Keep it
            # out of the fetch batch without claiming the absence was proven.
            out.placeholder.append(normalized)
        else:
            out.generic.append(normalized)

        # Cached evidence also applies when status data exists but does not
        # cover the complete requested range. Positive status evidence above
        # intentionally wins, so a newly traded day cannot be hidden by an
        # older cache record.
        if (
            normalized not in out.expected_no_data
            and normalized not in out.unknown
            and not positive_status
        ):
            if any(negative_evidence_covers(item, normalized, start, end) for item in evidence):
                for bucket in (out.generic, out.placeholder):
                    if normalized in bucket:
                        bucket.remove(normalized)
                out.expected_no_data.append(normalized)
                out.negative_cached.append(normalized)
                out.no_data_reasons[normalized] = "negative_evidence"
    return out


def load_bar_universe(config: Config) -> set[str]:
    """Symbols that carry at least one traded ``daily_bars`` row in the lake.

    A suspended/pre-open placeholder can still be present in ``daily_bars`` with
    ``volume=0``. It is not evidence that the symbol ever traded, so when the
    column exists require positive volume. This is the *tradable* universe as
    daily_bars actually realises it: delisted names (source returns no bars) and
    never-traded instrument placeholders (IPO listed but not yet trading) are
    absent. Live snapshots such as the EastMoney valuation clist return those
    dead names, so filtering to this set keeps valuation_metrics in lock-step
    with daily_bars coverage (audit check ``valuation_bars_orphan_symbol``). A
    genuine IPO enters this set the same day it first trades and gets a bar.

    Returns an empty set when no bars exist yet; callers must treat that as
    "cannot reconcile" and skip filtering rather than dropping every row.
    """
    bars_root = config.curated_root / "daily_bars"
    # Canonicalize the daily-bar identity before applying the volume contract.
    # Filtering each file first lets an old positive retry keep a symbol in the
    # valuation universe beside a newer zero-volume canonical row. The shared
    # scanner preserves row-based semantics for legacy files without volume.
    from cnequity.query.parquet_scan import scan_parquet_root

    try:
        scan = scan_parquet_root(
            bars_root,
            partition_col="trade_date",
            traded_only=True,
            dataset="daily_bars",
            meta_root=config.meta_root,
        )
    except FileNotFoundError:
        return set()
    if "symbol" not in scan.collect_schema().names():
        return set()
    return set(scan.select("symbol").unique().collect()["symbol"].to_list())


def _existing_dates(config: Config, dataset: str, date_col: str) -> set[date]:
    root = config.curated_root / dataset
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        return set()
    from cnequity.query.parquet_scan import scan_parquet_files

    return set(scan_parquet_files(files).select(date_col).unique().collect()[date_col].to_list())


def walk_day_backfill(
    config: Config,
    trade_date: date,
    run_id: str,
    dataset: str,
    fetch_one: Callable[[date], pl.DataFrame],
    *,
    source: str,
    date_col: str = "trade_date",
    floor: date = BACKFILL_START,
    flush_days: int = 60,
    existing_dates_fn: Callable[[list[date]], set[date]] | None = None,
    calendar_days: bool = False,
    allow_empty_days: bool = False,
    publish_fn: Callable[[pl.DataFrame, str], object] | None = None,
) -> dict:
    """Walk trading days for a dataset whose fetch answers one day at a time.

    Generalizes ``_backfill_margin_trading`` (steps/capital.py): several
    by-date snapshot datasets — dragon_tiger, block_trades,
    share_unlock_schedule, announcement_index, regulatory_events,
    market_breadth — have an adapter that genuinely serves any historical
    *date_col* value, but until now nothing ever walked a range through it,
    so ``cne backfill <name> --start ...`` silently did nothing: the daily
    step only ever asked for ``trade_date``, never iterated a window.

    Resumable — days already in curated are skipped — and staged every
    *flush_days* so a kill costs only the unflushed chunk, not the sweep so
    far. Single-threaded on purpose: unlike margin_trading this has not been
    measured safe at higher concurrency for these sources, and getting a
    correct sweep once is worth more than a faster wrong one.
    """
    start = getattr(config, "_backfill_start", None) or floor
    end = getattr(config, "_backfill_end", None) or trade_date
    bounded_end = min(end, trade_date)
    if calendar_days:
        days = (
            [start + timedelta(days=offset) for offset in range((bounded_end - start).days + 1)]
            if start <= bounded_end
            else []
        )
    else:
        days = list_trading_dates(config, start, bounded_end)
    have = (
        existing_dates_fn(days)
        if existing_dates_fn is not None
        else _existing_dates(config, dataset, date_col)
    )
    todo = [d for d in days if d not in have]
    if not todo:
        return {"rows_read": 0, "rows_written": 0, "days_skipped": len(days)}

    # Most historical callers use the plain writer.  Critical range-aware
    # adapters (CNINFO announcements/regulatory) supply ``publish_fn`` so the
    # publish boundary can verify the adapter's exact-wire receipt before a
    # staging file is created.  Keep the legacy writer only as the explicit
    # compatibility default for non-archived datasets.
    writer = StagingWriter(config.staging_root) if publish_fn is None else None
    frames: list[pl.DataFrame] = []
    rows_written = 0
    empty_days: list[date] = []
    n_parts = 0

    def flush() -> None:
        nonlocal frames, rows_written, n_parts
        if not frames:
            return
        part = with_provenance(
            pl.concat(frames, how="diagonal_relaxed"),
            source=source,
            data_version=data_version_for(dataset),
        )
        batch_id = f"bf-{n_parts:04d}"
        if publish_fn is None:
            assert writer is not None
            writer.write_batch(dataset, run_id, batch_id, part)
        else:
            # The callback owns the complete staging boundary.  In particular
            # it must validate any source evidence before writing ``part``.
            publish_fn(part, batch_id)
        n_parts += 1
        rows_written += part.height
        frames = []

    for i, d in enumerate(todo, 1):
        try:
            df = fetch_one(d)
            if df.is_empty():
                empty_days.append(d)
            else:
                if date_col not in df.columns:
                    raise RuntimeError(
                        f"{dataset}: fetch for {d.isoformat()} did not return the "
                        f"configured date column {date_col!r}"
                    )
                parsed_dates = df.get_column(date_col).cast(pl.Date, strict=False)
                mismatched = int(
                    (parsed_dates.is_null() | (parsed_dates != d).fill_null(True)).sum()
                )
                if mismatched:
                    raise RuntimeError(
                        f"{dataset}: fetch for {d.isoformat()} returned {mismatched} row(s) "
                        f"with a different or invalid {date_col}"
                    )
                frames.append(df)
        except Exception:
            # The docstring's "a kill costs only the unflushed chunk" promise
            # is empty if a raise skips this flush — measured in production:
            # announcement_index ran 9.6h and landed zero new days because the
            # failure hit mid-window, taking every already-fetched day with it.
            # Response validation errors must preserve the same checkpoint.
            flush()
            raise
        if i % flush_days == 0:
            flush()
            logger.info(
                "%s backfill: %d/%d days (at %s, %d rows staged)",
                dataset,
                i,
                len(todo),
                d.isoformat(),
                rows_written,
            )
    flush()

    if empty_days and not allow_empty_days:
        logger.warning(
            "%s backfill: %d trading day(s) returned no rows (e.g. %s) — "
            "left absent; a rerun retries them",
            dataset,
            len(empty_days),
            empty_days[0].isoformat(),
        )
    result = {
        "rows_read": rows_written,
        "rows_written": rows_written,
        "days_fetched": len(todo) if allow_empty_days else len(todo) - len(empty_days),
        "days_skipped": len(days) - len(todo),
        "days_empty": 0 if allow_empty_days else len(empty_days),
        "days_confirmed_empty": len(empty_days) if allow_empty_days else 0,
    }
    if empty_days and not allow_empty_days:
        # Keep direct step calls truthful as well as engine-managed calls. The
        # engine promotes ``days_empty`` to warning, but callers that invoke a
        # backfill step directly would otherwise receive ``success`` while
        # known dates remain absent and retryable.
        result["status"] = "warning"
        result["context_updates"] = {
            "audit_findings": [_backfill_empty_day_finding(dataset, empty_days)]
        }
    return result
