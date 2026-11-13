"""Shared helpers for step implementations."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import polars as pl

from cnequity.adapters.tdx_protocol.client import fetch_instruments
from cnequity.config import Config
from cnequity.domain.datasets import DATASETS, calendar_scope_datasets, fetch_semantics
from cnequity.domain.frames import with_columns_unless_blank
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.domain.symbols import is_subscription_placeholder
from cnequity.orchestrator.outcomes import (
    CapabilityLimitError,
    SourcePayloadError,
    SourceUnavailableError,
    error_kind,
)
from cnequity.orchestrator.source_gaps import source_gap_dates
from cnequity.run_scope import backfill_of
from cnequity.storage import StagingWriter
from cnequity.storage.instrument_catalog import load_bar_universe as load_bar_universe
from cnequity.storage.instrument_catalog import load_curated_instruments as load_curated_instruments
from cnequity.storage.instrument_catalog import (
    load_curated_trading_status as load_curated_trading_status,
)
from cnequity.storage.state import StateStore

logger = logging.getLogger(__name__)

INCREMENTAL_LOOKBACK_DAYS = 5
BACKFILL_START = date(2016, 1, 1)

# These feeds are rolling *calendar*-date disclosures.  A weekend or exchange
# holiday can still contain a publication, so using ``list_trading_dates``
# here would silently make those rows impossible to fetch.  The membership is
# declared per dataset in ``domain/datasets`` (``session_scope="calendar"``)
# rather than listed here, so one registry entry decides the incremental walk,
# the empty-day guard below, and which steps the events job may schedule.
# Everything else keeps the trading-day contract that market observations such
# as bars, flow and breadth depend on.
CALENDAR_DATE_DATASETS = calendar_scope_datasets()


class SnapshotBackfillError(CapabilityLimitError):
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
    spec = DATASETS.get(dataset)
    if spec is None or not spec.partial_rows:
        writer.write_batch(dataset, run_id, batch_id, df)
        return {"rows_read": df.height, "rows_written": df.height}
    report = writer.write_usable_batch(dataset, run_id, batch_id, df)
    result = {"rows_read": report["rows_read"], "rows_written": report["rows_written"]}
    if not report["rows_rejected"]:
        return result
    rejected_dates = report.get("rejected_dates", [])
    if rejected_dates:
        StateStore(config.meta_root).record_missing_dates(
            dataset, [date.fromisoformat(day) for day in rejected_dates], reason="quarantined_rows"
        )
    result.update(
        status="degraded",
        batch_settled=True,
        rows_rejected=report["rows_rejected"],
        quarantine=report["quarantine"],
        rejected_dates=rejected_dates,
    )
    result["context_updates"] = {
        "audit_findings": [
            {
                "dataset": dataset,
                "severity": "warning",
                "check": "quarantined_rows",
                "message": (
                    f"{report['rows_rejected']} row(s) quarantined, "
                    f"{report['rows_written']} valid row(s) staged; "
                    "the affected dates stay missing until a clean fetch"
                ),
                "rows_rejected": report["rows_rejected"],
                "quarantine": report["quarantine"],
                "rejected_dates": rejected_dates,
                "reasons": report.get("reasons", []),
            }
        ]
    }
    return result


# StateStore field recording the day a tiered feed last completed its deep
# sweep, so a job that runs several times on the deep day (the 6-hourly events
# job ran announcement_index's 30-day sweep four times every Saturday — ~2 h of
# CNINFO requests where one pass was the design) walks the full window once.
_DEEP_RECONCILED_FIELD = "deep_reconciled_on"
# Same for the shallow tail: the 6-hourly events job re-walked the last 7 days
# of announcements on each of its four daily runs (~1,850 CNINFO requests a
# day). The tail is walked once per day; later runs fetch from the watermark.
_TAIL_RECONCILED_FIELD = "tail_reconciled_on"


def _deep_reconciliation_due(config: Config, dataset: str, trade_date: date) -> bool:
    """Whether this run of a tiered feed should walk its full window."""
    spec = DATASETS.get(dataset)
    if not int(getattr(spec, "shallow_reconciliation_lookback_days", 0) or 0):
        return False
    dow = int(getattr(config, "deep_reconciliation_dow", 6) or 0)
    if dow <= 0:
        return True
    # Due on the first run on or after the deep weekday, once a week. A feed
    # that runs every calendar day (announcements) still goes deep on that
    # day; one that runs only on sessions (daily_bars, Saturday never trades)
    # goes deep on its first session after it — before this, never.
    last_deep_day = trade_date - timedelta(days=(trade_date.isoweekday() - dow) % 7)
    state = StateStore(config.meta_root)
    done = state.get_date(dataset, field=_DEEP_RECONCILED_FIELD)
    if done is None:
        # First look at a lake without a record: start the weekly clock at the
        # last deep day instead of sweeping deep now (on the deep day itself,
        # go deep as the day-of-week rule always did).
        done = last_deep_day - timedelta(days=7) if trade_date == last_deep_day else last_deep_day
        state.set_date(dataset, done, field=_DEEP_RECONCILED_FIELD)
    return done < last_deep_day


def _reconciliation_depth(config: Config, dataset: str, trade_date: date) -> str | None:
    """How far back this run of a tiered feed reaches: deep, tail or tip.

    None for a feed without a shallow tier (its full lookback every run).
    """
    spec = DATASETS.get(dataset)
    if not int(getattr(spec, "shallow_reconciliation_lookback_days", 0) or 0):
        return None
    if _deep_reconciliation_due(config, dataset, trade_date):
        return "deep"
    state = StateStore(config.meta_root)
    if state.get_date(dataset, field=_TAIL_RECONCILED_FIELD) == trade_date:
        return "tip"
    return "tail"


def reconciliation_depth(config: Config, dataset: str, trade_date: date) -> str | None:
    """Public spelling of the tier this run of *dataset* walks (deep/tail/tip)."""
    return _reconciliation_depth(config, dataset, trade_date)


def mark_reconciled(config: Config, dataset: str, trade_date: date, depth: str | None) -> None:
    """Record that a run walked its *depth* window, so the next run can go shallower.

    ``fetch_incremental_daily`` records this for the by-date feeds. A feed with
    its own fetch path must call it, or the weekly deep sweep stays due every
    day: ``daily_bars`` never did, and from 2026-10-08 re-swept every symbol
    over a week of sessions on each daily run (~30 min) instead of answering
    the new session from the whole-board quotes (~8 min).
    """
    state = StateStore(config.meta_root)
    if depth == "deep":
        state.set_date(dataset, trade_date, field=_DEEP_RECONCILED_FIELD)
    if depth in {"deep", "tail"}:
        state.set_date(dataset, trade_date, field=_TAIL_RECONCILED_FIELD)


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
    shallow = max(int(getattr(spec, "shallow_reconciliation_lookback_days", 0) or 0), 0)
    depth = _reconciliation_depth(config, dataset, trade_date) if shallow else None
    mode = getattr(spec, "reconciliation_lookback_mode", "calendar")
    if depth in {"tail", "tip"} and shallow == 1 and mode == "trading_day":
        # A one-session tier means the tip alone (daily_bars: a settled session
        # is not re-read daily — its tail is the weekly deep sweep). Counting
        # the watermark session as "one" re-fetched yesterday on every run and
        # sent the whole market through the per-symbol sweep each day.
        if watermark is not None:
            return min(watermark + timedelta(days=1), trade_date)
        return trade_date
    if depth == "tip":
        # Tail already walked today: from the watermark day to the run day.
        lookback = 1
    elif depth == "tail":
        # The deep tail is swept once a week (see `_deep_reconciliation_due`);
        # every other run walks the near tail only. Coverage over a week is
        # unchanged — a late-indexed record is picked up by the next deep sweep
        # instead of the next run — while the per-run page count drops with the
        # window.
        lookback = shallow

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


def _load_staged_instruments(config: Config) -> pl.DataFrame | None:
    """Read all recoverable instrument fragments when curated data is absent."""
    root = config.staging_root / "instruments"
    files = sorted(root.rglob("*.parquet")) if root.exists() else []
    if not files:
        return None
    from cnequity.domain.canonical import dedupe_by_primary_key

    frame = pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")
    return dedupe_by_primary_key(frame, "instruments")


def _without_subscription_placeholders(frame: pl.DataFrame) -> pl.DataFrame:
    """Keep transport-level subscription stubs out of downstream universes."""
    if frame.is_empty():
        return frame
    names = frame["name"].to_list() if "name" in frame.columns else [None] * frame.height
    symbols = frame["symbol"].to_list() if "symbol" in frame.columns else [None] * frame.height
    keep = [
        not is_subscription_placeholder(name, symbol)
        for name, symbol in zip(names, symbols, strict=True)
    ]
    return frame.filter(pl.Series(keep))


def walks_calendar_days(dataset: str) -> bool:
    """True when the incremental walk asks this source for one calendar day at a time.

    Both halves matter. The date axis has to be the natural calendar
    (``session_scope``), and the source has to be queried *per day* rather than
    read as one live page: a page that comes back empty is evidence about the
    page, never about the calendar, so a live snapshot keeps its fail-loud
    contract on an empty response whatever day it is.
    """
    return dataset in CALENDAR_DATE_DATASETS and fetch_semantics(dataset) == "by_date"


def incremental_trade_dates(config: Config, dataset: str, trade_date: date) -> list[date]:
    """Dates to fetch for a daily dataset: [watermark+1, trade_date].

    Event calendars use literal calendar dates because disclosures are not
    constrained to exchange sessions.  All other by-date datasets retain the
    strict curated trading-session walk.

    A calendar-scoped *snapshot* feed keeps the trading-day walk here even so:
    its only request is the run day, and the older days in the window exist
    solely to be reported as coverage gaps.  Widening that list would turn
    every weekend into a warning for a lake whose news sweep runs on weekdays,
    which says nothing about the day being fetched.
    """
    start = incremental_window(config, dataset, trade_date)
    if walks_calendar_days(dataset):
        current = [
            start + timedelta(days=offset) for offset in range((trade_date - start).days + 1)
        ]
    else:
        current = list_trading_dates(config, start, trade_date)
    if fetch_semantics(dataset) == "by_date":
        # A max watermark can move beyond a failed interior day. The durable
        # ledger keeps that day in the plan even after the normal tail expires.
        owed = StateStore(config.meta_root).get_missing_dates(dataset)
        return sorted(set(current) | {day for day in owed if day <= trade_date})
    return current


#: How far back :func:`last_session_on_or_before` will look. The longest CN
#: market closure is the Spring Festival week; a fortnight of slack covers it
#: and any holiday shift, while still failing loudly rather than silently
#: walking back years if the calendar itself is wrong.
MAX_CLOSURE_LOOKBACK_DAYS = 15


def last_session_on_or_before(config: Config, day: date) -> date:
    """The newest trading session at or before *day*.

    The single answer to "which session does this date mean", because the
    alternative was three of them. A daily-bar window that ends on a Saturday
    cannot lose data — no session is skipped by stopping there — but every
    completeness gate downstream reads the end date as a session that must
    have rows: `_staged_daily_bar_symbols(..., end)` filters `trade_date ==
    end` and finds nothing, so a whole market looks unresolved. One backfill
    ending on a Saturday failed with "1 expected key(s) remain unknown" for a
    symbol whose bars were all present through the Friday.

    Normalising here rather than where `_backfill_end` is stored keeps the
    config value the operator's literal request while giving every reader of
    it the same meaning.
    """
    for offset in range(MAX_CLOSURE_LOOKBACK_DAYS + 1):
        candidate = day - timedelta(days=offset)
        if is_trading_day(config, candidate):
            return candidate
    raise RuntimeError(
        f"No trading session found in the {MAX_CLOSURE_LOOKBACK_DAYS} days before "
        f"{day.isoformat()}. The trading calendar is missing or wrong for that "
        "window; rebuild it with `cne run daily --steps trading_calendar`."
    )


def empty_day_is_expected(config: Config, dataset: str, day: date) -> bool:
    """True when *dataset* returning zero rows for *day* proves nothing is wrong.

    A calendar-scoped feed is walked day by day precisely because it publishes
    outside exchange sessions — but a Sunday or a holiday with no disclosure at
    all is the normal case for the market as a whole, not a failed fetch. The
    strict non-empty gate stays exactly where it earns its keep: on a session
    the market did hold, where zero rows means the source is broken.
    """
    return walks_calendar_days(dataset) and not is_trading_day(config, day)


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


def _fetch_failed_day_finding(dataset: str, failures: list[tuple[date, str]]) -> dict:
    """Record days the source could not be read, so a hole is never silent."""
    days = [day for day, _ in failures]
    return {
        "dataset": dataset,
        "severity": "error",
        "check": "fetch_failed_days",
        "message": (
            f"{dataset}: {len(failures)} day(s) could not be fetched "
            f"({', '.join(day.isoformat() for day in days[:8])}); the rest of the "
            "window was staged and these are retried while they remain in the "
            "reconciliation window"
        ),
        "days_failed": len(failures),
        "sample_dates": [day.isoformat() for day in days[:8]],
        "errors": [f"{day.isoformat()}: {error}" for day, error in failures[:8]],
    }


def _reconciliation_window_retries(dataset: str) -> bool:
    """Whether *dataset* re-reads a rolling tail on every incremental run.

    Only such a dataset may stage the days around one it could not read: its
    next runs come back for the failed day on their own. Everything else keeps
    the fail-loud contract, because staging past an unread day there would
    advance the watermark over a hole nothing would ever revisit.
    """
    spec = DATASETS.get(dataset)
    return int(getattr(spec, "reconciliation_lookback_days", 0) or 0) > 0


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
            raise SourcePayloadError(
                f"{dataset}: fetch for {trade_date.isoformat()} did not return "
                f"the configured date column {date_col!r}"
            )
        return
    parsed_dates = df.get_column(column).cast(pl.Date, strict=False)
    mismatched = int((parsed_dates.is_null() | (parsed_dates != trade_date).fill_null(True)).sum())
    if mismatched:
        raise SourcePayloadError(
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
    on_day: Callable[[date, pl.DataFrame], None] | None = None,
    recovered_days: dict[date, pl.DataFrame] | None = None,
    durable_day_checkpoints: bool = False,
    snapshot_only: bool = False,
) -> tuple[pl.DataFrame, list[dict]]:
    """Fetch one or more trading days from watermark+1 through *trade_date*.

    Returns ``(dataframe, audit_findings)``. Snapshot datasets only fetch
    *trade_date*; missed days are reported as ``coverage_gap`` findings.
    ``snapshot_only`` requests just the current observation, without asserting
    earlier snapshot coverage (used by init).
    """
    semantics = fetch_semantics(dataset)
    if backfill_of(config).active:
        if semantics == "snapshot":
            raise SnapshotBackfillError(
                f"{dataset}: backfill not supported — fetch semantics are snapshot "
                "(live page stamped with trade_date; historical values unavailable)"
            )
        frame = fetch_fn(trade_date)
        if (
            frame.is_empty()
            and not allow_empty
            and not empty_day_is_expected(config, dataset, trade_date)
        ):
            raise SourceUnavailableError(
                f"{dataset}: no rows returned for {trade_date.isoformat()}"
            )
        _validate_trade_date(frame, dataset, trade_date, date_col=date_col)
        return frame, []

    spec = DATASETS.get(dataset)
    if semantics == "snapshot" and (snapshot_only or (spec is not None and not spec.watermark)):
        # Init asks for a current observation, not a snapshot history. Rolling
        # live windows (for example share_unlock_schedule) likewise must not
        # use an old state file to manufacture gap dates.
        dates = [trade_date]
    else:
        dates = incremental_trade_dates(config, dataset, trade_date)
    # Decided before the fetch, with the same state the window was built from.
    depth = _reconciliation_depth(config, dataset, trade_date)
    if (
        not dates
        and semantics == "snapshot"
        and spec is not None
        and spec.session_scope == "calendar"
    ):
        # News keeps publishing on closed days. The trading-day gap walk can
        # be empty once Friday is covered, but today's live request must still
        # run (including an empty response's exact archive capture).
        dates = [trade_date]
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
    failed_days: list[tuple[date, str]] = []
    first_failure: Exception | None = None
    fetched_days = 0
    # One unreadable day used to discard the whole window, so a single bad day
    # inside a 30-day reconciliation tail blinded the dataset until it rolled
    # out of that tail. Date-scoped gaps have durable repair receipts even
    # when the normal incremental window has no reconciliation tail.
    tolerate_failed_day = (
        semantics == "by_date" or _reconciliation_window_retries(dataset) or durable_day_checkpoints
    )
    for d in fetch_dates:
        try:
            part = recovered_days[d] if recovered_days and d in recovered_days else fetch_fn(d)
            if (
                part.is_empty()
                and not allow_empty
                and not empty_day_is_expected(config, dataset, d)
            ):
                raise SourceUnavailableError(f"{dataset}: no rows returned for {d.isoformat()}")
            _validate_trade_date(part, dataset, d, date_col=date_col)
        except Exception as exc:  # noqa: BLE001 — re-raised below when total
            if error_kind(exc) not in {
                "source_transient",
                "source_unavailable",
                "source_payload_invalid",
                "capability_limit",
            }:
                raise
            if semantics == "by_date":
                StateStore(config.meta_root).record_missing_dates(dataset, [d], reason=str(exc))
            if not tolerate_failed_day:
                raise
            first_failure = first_failure or exc
            failed_days.append((d, str(exc)))
            logger.warning("%s: %s could not be fetched: %s", dataset, d.isoformat(), exc)
            continue
        fetched_days += 1
        if part.is_empty():
            if empty_day_is_expected(config, dataset, d):
                # The exchanges never opened: nothing was filed market-wide and
                # there is nothing to publish. Move on without a finding — the
                # day is not a gap, and calling it one would bury the real ones.
                logger.debug("%s: %s is not a session and returned no rows", dataset, d)
                continue
            if not allow_empty:
                if semantics == "by_date":
                    StateStore(config.meta_root).record_missing_dates(
                        dataset, [d], reason="unexpected empty response"
                    )
                raise SourceUnavailableError(f"{dataset}: no rows returned for {d.isoformat()}")
            spec = DATASETS.get(dataset)
            if spec is not None and spec.coverage_mode == "session_dense":
                empty_days.append(d)
            continue
        # A by-date adapter must not let a vendor page leak rows from a
        # neighbouring session into the requested partition. Validate here
        # before diagonal concatenation; once several days are merged the
        # offending response can no longer be attributed to one request.
        if on_day is not None and not (recovered_days and d in recovered_days):
            on_day(d, part)
        frames.append(part)
    if first_failure is not None and fetched_days == 0:
        # Nothing was readable: that is a source outage, not one bad day, and
        # there is no partial result worth keeping.
        raise first_failure
    if empty_days:
        findings.append(_dense_empty_day_finding(dataset, empty_days))
    if failed_days:
        findings.append(_fetch_failed_day_finding(dataset, failed_days))
    elif depth in {"deep", "tail"} and not source_gap_dates(dataset):
        # Only a clean sweep counts; a partial one is retried at the same depth.
        state = StateStore(config.meta_root)
        if depth == "deep":
            state.set_date(dataset, trade_date, field=_DEEP_RECONCILED_FIELD)
        state.set_date(dataset, trade_date, field=_TAIL_RECONCILED_FIELD)
    if not frames:
        return pl.DataFrame(), findings
    combined = pl.concat(frames, how="diagonal_relaxed")
    # Rolling reconciliation windows intentionally overlap the watermark. The
    # canonical PK collapse keeps a revised row once while preserving the
    # source/provenance precedence used by lake reads.
    from cnequity.domain.canonical import dedupe_by_primary_key

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


def _status_files_identity(config: Config) -> str:
    """Digest of the committed trading_status files as they sit on disk.

    Direct writes and legacy lakes must still invalidate a status-derived
    absence claim, so the revision alone is not enough. Hashing every status
    row was ~85s per call; paths, sizes and modification times change whenever
    any row does, for a few stat calls.
    """
    from cnequity.storage.published import published

    return published(config, "trading_status").identity()


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
    """Return current negative records whose own identity still holds.

    Read-only apart from expiry: a claim whose basis changed is left out, not
    deleted, so ``cne check`` reading the cache cannot rewrite it.
    """
    store = StateStore(config.meta_root)
    # No absence claims means no identity can affect the routing decision.
    # Check live entries first, avoiding a full trading-status scan on the
    # normal empty-cache path.
    live = store.get_negative_evidence(dataset, now=now)
    if not live:
        return []
    identity = _NegativeEvidenceIdentity(config, dataset, metadata)
    return [item for item in live if item.get("identity") == identity.of(item)]


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
    identity = _NegativeEvidenceIdentity(config, dataset, metadata)
    for entry in entries:
        entry["identity"] = identity.of(entry)
    StateStore(config.meta_root).record_negative_evidence(
        dataset,
        entries,
        ttl_days=negative_evidence_ttl_days(config, dataset),
        now=now,
    )


class _NegativeEvidenceIdentity:
    """What one absence claim rests on, so only a change to that voids it.

    The identity used to be the whole instruments table plus every committed
    trading_status file. The same run's compact republishes trading_status
    seconds after daily_bars certifies its source-empty symbols, and almost
    every day lists or delists something somewhere, so no claim outlived its
    run: `cne check` reported certified names as unexplained gaps and the next
    run asked every vendor again (2026-10-23: 000016.SZ on its delisting day,
    001381.SZ before its IPO).

    A source-empty or not-yet-listed claim is about the vendors' answers for
    one code, so it holds while that code's listing span holds. Only a claim
    derived from status still follows the committed status files.
    """

    def __init__(self, config: Config, dataset: str, metadata: pl.DataFrame | None) -> None:
        self._config = config
        self._dataset = dataset
        self._metadata = metadata
        self._spans: dict[str, list] | None = None
        self._status: dict | None = None

    def of(self, entry: dict) -> dict:
        symbol = str(entry.get("symbol", "")).strip().upper()
        identity: dict = {"instrument": self._span(symbol)}
        if entry.get("reason") == "verified_no_data":
            identity["status"] = self._status_identity()
        if self._dataset == "daily_bars":
            # Before v2, EastMoney data:null could contribute an empty-source vote;
            # before v3, unsupported fund codes could supply a Baostock empty vote.
            # Old votes cannot certify absence after the response contract changes.
            identity["source_response_contract"] = 3
        return identity

    def _span(self, symbol: str) -> list | None:
        if self._spans is None:
            frame = (
                self._metadata if self._metadata is not None else instrument_metadata(self._config)
            )
            self._spans = {}
            if frame is not None and not frame.is_empty() and "symbol" in frame.columns:
                for row in frame.iter_rows(named=True):
                    self._spans[str(row["symbol"]).strip().upper()] = [
                        value.isoformat() if isinstance(value, date) else value
                        for value in (
                            row.get("list_date"),
                            row.get("delist_date"),
                            row.get("asset_type"),
                        )
                    ]
        return self._spans.get(symbol)

    def _status_identity(self) -> dict:
        if self._status is None:
            self._status = {
                "trading_status_revision": StateStore(self._config.meta_root).get_revision(
                    "trading_status"
                ),
                "trading_status_files": _status_files_identity(self._config),
            }
        return self._status


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
    #: Undated, never-traded equities: fetched once per window to find out.
    #: A clean empty answer settles them as not yet listed (see bars.py); a
    #: bar settles them as listed. Funds stay ``placeholder``.
    probe: list[str] = field(default_factory=list)
    unknown: list[str] = field(default_factory=list)
    negative_cached: list[str] = field(default_factory=list)
    no_data_reasons: dict[str, str] = field(default_factory=dict)


def _hold_undated(out: DailyBarOwnership, symbol: str, asset_type: str | None) -> None:
    """Route an undated, never-traded code: equities are probed, funds held."""
    (out.probe if asset_type in {"stock", "cdr"} else out.placeholder).append(symbol)


def _never_started(symbol: str, list_date: date | None, bar_universe: set[str] | None) -> bool:
    """No listing date, and no traded bar in the lake's entire history."""
    return list_date is None and bar_universe is not None and symbol not in bar_universe


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
    no_print_keys: set[tuple[str, date]] | None = None,
) -> DailyBarOwnership:
    """Route symbols using explicit metadata and evidence, never a ratio.

    A symbol is ``expected_no_data`` only when one of these independently
    inspectable facts holds: its listing/delisting span excludes the complete
    requested window; every covered session has an explicit non-trading status;
    or a still-live, identity-matching negative-evidence record covers the
    complete window. An absent status row, malformed instrument metadata, or
    an incomplete source response remains ``unknown`` and must be retried.

    ``bar_universe`` is historical positive-volume evidence. A symbol with no
    list_date and no traded bar anywhere in it has probably not started
    trading. A fund is held as a ``placeholder``: kept out of the expensive
    per-symbol fallback and out of the expected key set, without claiming its
    absence was proven. An equity is a ``probe``: asked for once, because a
    late list_date looks exactly the same and hides a real listing.
    """
    from cnequity.domain.symbols import is_etf_symbol, parse_symbol
    from cnequity.domain.trading_status import independent_suspension_keys

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
    suspended: set[tuple[str, date]] = set()
    if trading_status is not None and not trading_status.is_empty():
        suspended = set(independent_suspension_keys(trading_status).iter_rows())
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
                by_day = status_by_symbol.setdefault(normalized, {})
                # A positive observation cannot be overwritten merely because
                # a conflicting row happens to be visited last.
                previous = by_day.get(raw_day)
                by_day[raw_day] = (
                    True
                    if previous is True or value is True
                    else False
                    if previous is False or value is False
                    else None
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
        elif (
            asset_type in {"etf", "lof"}
            and list_date is not None
            and sessions
            and all((normalized, day) in (no_print_keys or set()) for day in sessions)
        ):
            out.expected_no_data.append(normalized)
            out.no_data_reasons[normalized] = "exchange_fund_no_print"
        elif sessions and normalized in status_by_symbol:
            status = status_by_symbol[normalized]
            # A missing session in the status table is unknown, not a silent
            # non-trading assertion. False is accepted only when every
            # requested exchange session is explicitly covered.
            if all((normalized, day) in suspended for day in sessions):
                out.expected_no_data.append(normalized)
                out.no_data_reasons[normalized] = "trading_status_non_trading"
                continue
            if positive_status:
                # Positive status evidence means a missing bar is a real
                # coverage obligation, even if an old negative cache exists.
                if len(span) >= 3 and asset_type is None:
                    out.unknown.append(normalized)
                elif _never_started(normalized, list_date, normalized_bar_universe):
                    # Except when the security has not started trading. The
                    # status board publishes `is_trading` for a whole universe
                    # from a *suspension list*, so a code that is merely not
                    # suspended reads as trading normally — including one that
                    # has never had a session. Storage already refuses the
                    # mirror image of this (`_symbols_without_bars`: a security
                    # that has never printed a bar cannot have stopped
                    # trading), and the same fact settles it here: an
                    # undated code with no traded bar in the lake's whole
                    # history is not a missing snapshot, it is a code waiting
                    # for its listing. TDX publishes those days early, then
                    # drops them, which leaves a row no enrichment can reach —
                    # so without this a pre-IPO code blocks the market
                    # snapshot every run, permanently.
                    _hold_undated(out, normalized, asset_type)
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
        elif _never_started(normalized, list_date, normalized_bar_universe):
            # Likely an issued-but-not-yet-listed code, but a delayed list_date
            # enrichment is indistinguishable here: the only list_date source
            # for a new listing is EastMoney push2, and while it was down
            # (2026-09-21 on) three Beijing listings were skipped here every
            # day although TDX had their bars. Equities are therefore asked
            # once (``probe``); funds, hundreds of them undated, stay out.
            _hold_undated(out, normalized, asset_type)
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
                for bucket in (out.generic, out.placeholder, out.probe):
                    if normalized in bucket:
                        bucket.remove(normalized)
                out.expected_no_data.append(normalized)
                out.negative_cached.append(normalized)
                out.no_data_reasons[normalized] = "negative_evidence"
    return out


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
    start = backfill_of(config).start or floor
    end = backfill_of(config).end or trade_date
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
    # A failed attempt can already have durable, validated chunks in this
    # run.  Curated-only resume would fetch them again; worse, the old
    # bf-0000 name would replace a longer successful chunk on the next
    # attempt.  Only files written by this helper count as completed days.
    writer = StagingWriter(config.staging_root)
    for path in writer.list_run_files(dataset, run_id):
        if not (path.stem.startswith("part-bf-") or "-bf-" in path.stem):
            continue
        from cnequity.domain.schemas import validate_dataframe

        staged = validate_dataframe(pl.read_parquet(path), dataset)
        if date_col not in staged.columns:
            raise RuntimeError(f"{dataset}: staged backfill part lacks {date_col}: {path}")
        staged_dates = staged.get_column(date_col).cast(pl.Date, strict=False)
        if staged_dates.null_count():
            raise RuntimeError(f"{dataset}: staged backfill part has invalid {date_col}: {path}")
        have.update(staged_dates.to_list())
    # Rows already present can still represent an incomplete or quarantined
    # day. Every recorded date gap must remain eligible for a fresh request.
    have.difference_update(StateStore(config.meta_root).get_missing_dates(dataset))
    todo = [d for d in days if d not in have]
    if not todo:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "days_skipped": len(days),
            "already_covered": len(days),
        }

    # Most historical callers use the plain writer.  Critical range-aware
    # adapters (CNINFO announcements/regulatory) supply ``publish_fn`` so the
    # publish boundary can verify the adapter's exact-wire receipt before a
    # staging file is created.  Keep the legacy writer only as the explicit
    # compatibility default for non-archived datasets.
    writer = writer if publish_fn is None else None
    frames: list[pl.DataFrame] = []
    rows_written = 0
    empty_days: list[date] = []
    failed_days: list[tuple[date, str]] = []
    first_failure: Exception | None = None
    n_parts = 0
    attempt_id = uuid.uuid4().hex

    def flush() -> None:
        nonlocal frames, rows_written, n_parts
        if not frames:
            return
        part = with_provenance(
            pl.concat(frames, how="diagonal_relaxed"),
            source=source,
            data_version=data_version_for(dataset),
        )
        batch_id = f"bf-{attempt_id}-{n_parts:04d}"
        if publish_fn is None:
            assert writer is not None
            writer.write_batch(dataset, run_id, batch_id, part)
        else:
            # The callback owns the complete staging boundary.  In particular
            # it must validate any source evidence before writing ``part``.
            publish_fn(part, batch_id)
        StateStore(config.meta_root).mark_staged_request_days(
            dataset, run_id, part.get_column(date_col).cast(pl.Date).unique().to_list()
        )
        n_parts += 1
        rows_written += part.height
        frames = []

    deferred_days: list[date] = []
    for i, d in enumerate(todo, 1):
        try:
            df = fetch_one(d)
            if df.is_empty():
                empty_days.append(d)
            else:
                if date_col not in df.columns:
                    raise SourcePayloadError(
                        f"{dataset}: fetch for {d.isoformat()} did not return the "
                        f"configured date column {date_col!r}"
                    )
                parsed_dates = df.get_column(date_col).cast(pl.Date, strict=False)
                mismatched = int(
                    (parsed_dates.is_null() | (parsed_dates != d).fill_null(True)).sum()
                )
                if mismatched:
                    raise SourcePayloadError(
                        f"{dataset}: fetch for {d.isoformat()} returned {mismatched} row(s) "
                        f"with a different or invalid {date_col}"
                    )
                frames.append(df)
        except (KeyboardInterrupt, SystemExit):
            # Ctrl-C or a kill used to bypass the flush below and drop up to
            # ``flush_days - 1`` complete days. Stage them, then stop; a
            # staging failure here is logged so the interrupt still wins.
            try:
                flush()
            except Exception:  # noqa: BLE001 — never mask the interrupt
                logger.exception("%s backfill: could not stage fetched days on interrupt", dataset)
            raise
        except Exception as exc:
            kind = error_kind(exc)
            if kind == "storage_failure":
                raise
            # The docstring's "a kill costs only the unflushed chunk" promise
            # is empty if a raise skips this flush — measured in production:
            # announcement_index ran 9.6h and landed zero new days because the
            # failure hit mid-window, taking every already-fetched day with it.
            # Response validation errors must preserve the same checkpoint.
            flush()
            if kind not in {
                "source_transient",
                "source_unavailable",
                "source_payload_invalid",
                "capability_limit",
            }:
                raise
            first_failure = first_failure or exc
            logger.warning("%s backfill: %s unavailable: %s", dataset, d, exc)
            from cnequity.domain.http_policy import SourceCoolingDown

            if isinstance(exc, SourceCoolingDown):
                deferred_days = todo[i - 1 :]
                StateStore(config.meta_root).record_missing_dates(
                    dataset, deferred_days, reason="source_cooling_down"
                )
                break
            failed_days.append((d, str(exc)))
            StateStore(config.meta_root).record_missing_dates(dataset, [d], reason=kind)
            continue
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
    if (
        first_failure is not None
        and not rows_written
        and not have
        and not (allow_empty_days and empty_days)
    ):
        raise first_failure
    if empty_days and not allow_empty_days:
        StateStore(config.meta_root).record_missing_dates(
            dataset, empty_days, reason="source_empty"
        )
    if not rows_written and not have and not (allow_empty_days and empty_days):
        raise SourceUnavailableError(f"{dataset}: no usable requested backfill results")

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
        "days_fetched": len(todo)
        - len(failed_days)
        - len(deferred_days)
        - (0 if allow_empty_days else len(empty_days)),
        "days_skipped": len(days) - len(todo),
        "days_empty": 0 if allow_empty_days else len(empty_days),
        "days_confirmed_empty": len(empty_days) if allow_empty_days else 0,
        "failed_days": len(failed_days),
        "days_deferred": len(deferred_days),
        "usable_result": bool(rows_written or have or (allow_empty_days and empty_days)),
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
    if failed_days:
        result["status"] = "warning"
        result["coverage_status"] = "partial"
        result["reason_code"] = "source_scope_incomplete"
        result.setdefault("context_updates", {}).setdefault("audit_findings", []).append(
            _fetch_failed_day_finding(dataset, failed_days)
        )
    if deferred_days:
        result.update(
            status="warning", coverage_status="partial", reason_code="source_cooling_down"
        )
    return result


def list_trading_dates(config: Config, start: date, end: date) -> list[date]:
    from cnequity.query.calendar import list_trading_dates as read_dates

    return read_dates(config, start, end, include_staging=True)


def is_trading_day(config: Config, trade_date: date) -> bool:
    from cnequity.query.calendar import is_trading_day as read_day

    return read_day(config, trade_date, include_staging=True)
