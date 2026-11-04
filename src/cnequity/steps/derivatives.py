"""Independent exchange/day/kind capture with validated receipts and debt.

Partial exchanges publish with degraded findings. Downloaded content is not a
completion claim until the receipt matches committed rows. Old failures remain
in durable debt beyond the three-session reconciliation tail. Contract builders
consume their own run's validated staging plus canonical bars (ADR-0017).
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.adapters.futures_exchange import (
    members,
)
from cnequity.adapters.futures_exchange.common import (
    SOURCE,
    FuturesDayUnavailable,
    FuturesSourceBlocked,
)
from cnequity.adapters.futures_exchange.registry import (
    DCE_OFFICIAL as DCE_OFFICIAL,
)
from cnequity.adapters.futures_exchange.registry import (
    READERS as READERS,
)
from cnequity.adapters.futures_exchange.registry import (
    ExchangeReader as ExchangeReader,
)
from cnequity.adapters.futures_exchange.registry import (
    Kind as Kind,
)
from cnequity.adapters.futures_exchange.registry import (
    earliest_session as earliest_session,
)
from cnequity.adapters.futures_exchange.registry import (
    enabled_exchanges as enabled_exchanges,
)
from cnequity.adapters.futures_exchange.registry import (
    expected_exchanges as expected_exchanges,
)
from cnequity.adapters.futures_exchange.registry import (
    reader as reader,
)
from cnequity.config import Config
from cnequity.domain.datasets import is_dataset_enabled
from cnequity.domain.derivatives import PUBLISHED_FUTURES_ANOMALIES
from cnequity.orchestrator.outcomes import SourceUnavailableError, error_kind
from cnequity.orchestrator.registry import register_step
from cnequity.steps.common import walk_day_backfill
from cnequity.steps.http_common import run_incremental_fetched, write_fetched

logger = logging.getLogger(__name__)

_BARS_DATASET = {"futures": "futures_bars", "options": "option_bars"}


def _missing_finding(dataset: str, day: date, exchange: str, error: str, severity: str) -> dict:
    return {
        "dataset": dataset,
        "severity": severity,
        "check": "futures_exchange_missing",
        "message": (
            f"{dataset}: {exchange} did not publish {day.isoformat()} ({error}); the other "
            "exchanges were written and the persistent debt ledger retries this one"
        ),
        "exchange": exchange,
        "trade_date": day.isoformat(),
    }


def _quarantine(
    frame: pl.DataFrame, dataset: str, exchange: str, day: date, findings: list[dict], config=None
) -> pl.DataFrame:
    """Drop rows that cannot be true, keep the rest of the exchange's session.

    One impossible row in a file of hundreds is a fact about that row, not
    about the session; refusing the whole write would trade one bad contract
    for every other exchange's data that day.
    """
    from cnequity.domain.schemas import derivative_bar_violations

    predicate = derivative_bar_violations(dataset)
    bad = frame.filter(predicate)
    if bad.is_empty():
        return frame
    if config is not None:
        from cnequity.storage.atomic import write_parquet_atomic

        write_parquet_atomic(
            config.meta_root
            / "derivatives"
            / "quarantine"
            / dataset
            / day.isoformat()
            / f"{exchange}.parquet",
            bad,
        )
    findings.append(
        {
            "dataset": dataset,
            "severity": "warning",
            "check": "futures_row_rejected",
            "message": (
                f"{dataset}: {exchange} {day.isoformat()} had {bad.height} row(s) that break "
                "the bar invariants; they were left out and the rest of the session written"
            ),
            "exchange": exchange,
            "trade_date": day.isoformat(),
            "sample": bad.head(5)
            .select(
                [
                    c
                    for c in ("symbol", "open", "high", "low", "close", "settle")
                    if c in bad.columns
                ]
            )
            .to_dicts(),
        }
    )
    return frame.filter(~predicate)


def fetch_session(
    config: Config,
    day: date,
    kind: Kind,
    *,
    findings: list[dict],
    raise_when_empty: bool = True,
) -> pl.DataFrame:
    """Every expected exchange's *kind* rows for *day*, tolerating stragglers."""
    from cnequity.domain.schemas import validate_dataframe, with_provenance
    from cnequity.storage.derivative_evidence import record_session

    dataset = _BARS_DATASET[kind]
    exchanges = expected_exchanges(config, kind, day)
    frames: list[pl.DataFrame] = []
    failures: list[tuple[str, str]] = []
    for exchange in exchanges:
        # A captured Sina historical session is reused by routine reconciliation.
        # Explicit --refresh is the correction path; zero-trade coverage is
        # still unverified and never promoted to a market completeness proof.
        if (
            exchange == "DCE"
            and config.futures_dce_route == "sina"
            and not getattr(config, "_derivatives_refresh", False)
        ):
            from cnequity.domain.market_time import shanghai_today
            from cnequity.storage.derivative_evidence import session_matches

            old = _scan_bars(config, dataset) if day < shanghai_today() else None
            if old is not None:
                captured = old.filter(
                    (pl.col("trade_date") == day) & (pl.col("exchange") == "DCE")
                ).collect()
                if session_matches(config, dataset, day, exchange, captured):
                    frames.append(captured)
                    continue
        try:
            result = reader(config, exchange).fetch(day, kind=kind, config=config)
        except FuturesSourceBlocked as exc:
            failures.append((exchange, f"blocked: {exc}"))
            continue
        except FuturesDayUnavailable as exc:
            failures.append((exchange, f"not published: {exc}"))
            continue
        except Exception as exc:  # noqa: BLE001 — one exchange must not sink the rest
            if error_kind(exc) not in {
                "source_transient",
                "source_unavailable",
                "source_payload_invalid",
                "capability_limit",
            }:
                raise
            failures.append((exchange, f"{type(exc).__name__}: {exc}"))
            continue
        frame = result.futures if kind == "futures" else result.options
        if frame.is_empty():
            failures.append((exchange, "file listed no contracts of this kind"))
            continue
        accepted = _quarantine(frame, dataset, exchange, day, findings, config)
        if accepted.is_empty():
            failures.append((exchange, "all rows rejected"))
            continue
        accepted = validate_dataframe(
            with_provenance(accepted, source=SOURCE, data_version="v1"), dataset
        )
        rejected = set(frame["symbol"].to_list()) - set(accepted["symbol"].to_list())
        known = PUBLISHED_FUTURES_ANOMALIES.get(day, frozenset())
        record_session(
            config,
            dataset,
            day,
            exchange,
            accepted,
            error=(
                "rows quarantined"
                if accepted.height != frame.height and not (rejected and rejected <= known)
                else None
            ),
            original_rows=frame.height,
        )
        frames.append(accepted)
    for exchange, error in failures:
        record_session(config, dataset, day, exchange, None, error=error)
        logger.warning("%s: %s %s — %s", dataset, exchange, day.isoformat(), error)
        findings.append(
            _missing_finding(dataset, day, exchange, error, "warning" if frames else "error")
        )
    if not frames:
        if exchanges and raise_when_empty:
            raise SourceUnavailableError(
                f"{dataset}: no exchange published {day.isoformat()} "
                f"({'; '.join(f'{e}: {m}' for e, m in failures)})"
            )
        return pl.DataFrame()
    # A reader that is not the exchange itself (Sina for DCE) stamps its own
    # label; the exchanges' files are all `futures_exchange`.
    frames = [
        f if "source" in f.columns else f.with_columns(pl.lit(SOURCE).alias("source"))
        for f in frames
    ]
    return pl.concat(frames, how="diagonal_relaxed")


def _completed_sessions(config: Config, kind: Kind, days: list[date]) -> set[date]:
    """Sessions where every exchange expected that day already has rows."""
    from cnequity.storage.read_context import read_root

    root = read_root(config, _BARS_DATASET[kind])
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        return set()
    from cnequity.query.canonical import dedupe_lazy_by_primary_key
    from cnequity.query.parquet_scan import scan_parquet_files
    from cnequity.storage.derivative_evidence import session_matches

    if getattr(config, "_derivatives_refresh", False):
        return set()
    frame = dedupe_lazy_by_primary_key(
        scan_parquet_files(files).filter(pl.col("trade_date").is_in(days)), _BARS_DATASET[kind]
    ).collect()
    return {
        day
        for day in days
        if all(
            session_matches(
                config,
                _BARS_DATASET[kind],
                day,
                exchange,
                frame.filter(
                    (pl.col("trade_date") == day)
                    & pl.col("exchange").is_in(list(members(exchange)))
                ),
            )
            for exchange in expected_exchanges(config, kind, day)
        )
    }


def _disabled(dataset: str) -> dict:
    return {
        "rows_read": 0,
        "rows_written": 0,
        "note": f"{dataset} capture disabled ([futures].enabled = false)",
    }


def _with_findings(result: dict, findings: list[dict]) -> dict:
    if not findings:
        return result
    updates = dict(result.get("context_updates") or {})
    updates["audit_findings"] = list(updates.get("audit_findings") or []) + findings
    result["context_updates"] = updates
    if result.get("status") in (None, "success"):
        result["status"] = "degraded"
    return result


def _run_bars(config: Config, trade_date: date, run_id: str, kind: Kind) -> dict:
    dataset = _BARS_DATASET[kind]
    if not is_dataset_enabled(dataset, config):
        return _disabled(dataset)
    if getattr(config, "_derivative_http_run", None) != getattr(
        config, "_derivative_batch", run_id
    ):
        config._derivative_http_run = getattr(config, "_derivative_batch", run_id)
        config._derivative_refreshed_requests = set()
        config._derivative_blocked_hosts = set()
    findings: list[dict] = []
    if getattr(config, "_backfill", False):
        floor = earliest_session(config, kind)
        if floor is None:
            return {"rows_read": 0, "rows_written": 0, "note": "no enabled exchange lists these"}
        start = getattr(config, "_backfill_start", None)
        if start is not None and start < floor:
            config._backfill_start = floor
        result = walk_day_backfill(
            config,
            trade_date,
            run_id,
            dataset,
            lambda d: fetch_session(config, d, kind, findings=findings, raise_when_empty=False),
            source=SOURCE,
            floor=floor,
            existing_dates_fn=lambda days: _completed_sessions(config, kind, days),
        )
        return _with_findings(result, findings)
    from cnequity.storage.derivative_evidence import owed_sessions

    recovered = 0
    for owed in owed_sessions(config, dataset, trade_date)[:3]:
        frame = fetch_session(config, owed, kind, findings=findings, raise_when_empty=False)
        if not frame.is_empty():
            recovery = write_fetched(
                config, run_id, dataset, frame, source=SOURCE, batch_id=f"owed-{owed}"
            )
            recovered += recovery.get("rows_written", 0)
    result = run_incremental_fetched(
        config,
        trade_date,
        run_id,
        dataset,
        lambda d: fetch_session(config, d, kind, findings=findings),
        source=SOURCE,
    )
    result["rows_written"] = result.get("rows_written", 0) + recovered
    return _with_findings(result, findings)


@register_step("futures_bars", group="derivatives")
def step_futures_bars(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Per-contract futures daily bars from the exchanges' session files."""
    return _run_bars(config, trade_date, run_id, "futures")


@register_step("option_bars", group="derivatives")
def step_option_bars(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Per-contract option daily bars from the exchanges' session files.

    ``cne backfill option_bars --source tushare`` instead fills the SSE/SZSE
    ETF options, which no exchange archive covers.
    """
    if getattr(config, "_backfill", False) and getattr(config, "_option_source", None) == "tushare":
        return _backfill_etf_options(config, trade_date, run_id)
    return _run_bars(config, trade_date, run_id, "options")


def _backfill_etf_options(config: Config, trade_date: date, run_id: str) -> dict:
    """SSE/SZSE ETF option bars and contracts from Tushare (optional token)."""
    from cnequity.adapters.tushare.client import tushare_enabled
    from cnequity.adapters.tushare.etf_options import (
        ETF_OPTIONS_FLOOR,
        fetch_contracts,
        fetch_day,
    )
    from cnequity.adapters.tushare.etf_options import (
        SOURCE as TUSHARE,
    )
    from cnequity.orchestrator.outcomes import SourceUnavailableError
    from cnequity.steps.common import walk_day_backfill

    if not tushare_enabled(config):
        raise SourceUnavailableError("option_bars: --source tushare needs a Tushare token")
    contracts = fetch_contracts(config)
    if contracts.is_empty():
        raise SourceUnavailableError("option_bars: tushare returned no ETF option contracts")
    written = write_fetched(config, run_id, "option_contracts", contracts, source=TUSHARE)
    result = walk_day_backfill(
        config,
        trade_date,
        run_id,
        "option_bars",
        lambda day: fetch_day(day, contracts, config=config),
        source=TUSHARE,
        floor=ETF_OPTIONS_FLOOR,
        existing_dates_fn=lambda days: _etf_option_days(config, days),
        allow_empty_days=True,
    )
    result["contracts_written"] = written.get("rows_written", 0)
    return result


def _etf_option_days(config: Config, days: list[date]) -> set[date]:
    """Sessions that already hold SSE/SZSE option rows (resume point)."""
    from cnequity.adapters.tushare.etf_options import ETF_OPTION_EXCHANGES

    bars = _scan_bars(config, "option_bars")
    if bars is None or not days:
        return set()
    return set(
        bars.filter(
            pl.col("exchange").is_in(list(ETF_OPTION_EXCHANGES))
            & pl.col("trade_date").is_between(min(days), max(days))
        )
        .select("trade_date")
        .unique()
        .collect()["trade_date"]
        .to_list()
    )


def _scan_bars(config: Config, dataset: str, run_id: str | None = None) -> pl.LazyFrame | None:
    from cnequity.storage.read_context import read_root

    root = read_root(config, dataset)
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if run_id is not None:
        from cnequity.storage.parquet import StagingWriter

        files.extend(StagingWriter(config.staging_root).list_run_files(dataset, run_id))
    if not files:
        return None
    from cnequity.query.canonical import dedupe_lazy_by_primary_key
    from cnequity.query.parquet_scan import scan_parquet_files

    return dedupe_lazy_by_primary_key(scan_parquet_files(files), dataset)


def _reference(config: Config, bars: pl.LazyFrame, findings: list[dict], dataset: str):
    """Latest references normally; explicit backfill windows replay observed sessions."""
    publisher = {member: key for key in READERS for member in members(key)}
    selected = (
        bars.with_columns(pl.col("exchange").replace_strict(publisher, default=pl.col("exchange")))
        .select("exchange", "trade_date")
        .unique()
    )
    start = getattr(config, "_backfill_start", None)
    end = getattr(config, "_backfill_end", None)
    if start is not None or end is not None:
        if start is not None:
            selected = selected.filter(pl.col("trade_date") >= start)
        if end is not None:
            selected = selected.filter(pl.col("trade_date") <= end)
    else:
        selected = selected.group_by("exchange").agg(pl.col("trade_date").max())
    latest = selected.collect().sort("exchange", "trade_date")
    blocked = set()
    frames: list[pl.DataFrame] = []
    for exchange, day in latest.iter_rows():
        if exchange in blocked or exchange not in enabled_exchanges(config):
            continue
        known = READERS.get(exchange)
        if known is None or reader(config, exchange).fetch_reference is None:
            continue
        active = reader(config, exchange)
        if (start is not None or end is not None) and not active.reference_history:
            blocked.add(exchange)
            findings.append(
                {
                    "dataset": dataset,
                    "severity": "warning",
                    "check": "futures_reference_history_unsupported",
                    "message": f"{exchange}: reference endpoint is a current snapshot; historical replay skipped",
                }
            )
            continue
        try:
            reference_kind = "option" if dataset == "option_contracts" else "future"
            kwargs = {"kind": reference_kind} if active.separate_kinds else {}
            frame = active.fetch_reference(day, config=config, **kwargs)
        except Exception as exc:  # noqa: BLE001 — observed dates still stand
            from cnequity.adapters.futures_exchange.common import FuturesSourceBlocked

            if error_kind(exc) not in {
                "source_transient",
                "source_unavailable",
                "source_payload_invalid",
                "capability_limit",
            }:
                raise
            if isinstance(exc, FuturesSourceBlocked):
                blocked.add(exchange)
            findings.append(
                {
                    "dataset": dataset,
                    "severity": "warning",
                    "check": "futures_reference_unavailable",
                    "message": (
                        f"{dataset}: {exchange} reference file for {day.isoformat()} "
                        f"unavailable ({exc}); live contracts keep a null last trading day"
                    ),
                }
            )
            continue
        if not frame.is_empty():
            from cnequity.storage.atomic import write_parquet_atomic

            reference_day = frame["as_of"].max() if "as_of" in frame.columns else day
            write_parquet_atomic(
                config.meta_root
                / "derivatives"
                / "references"
                / exchange
                / f"{reference_day}-{reference_kind}.parquet",
                frame,
            )
            frames.append(frame)
    archived = sorted((config.meta_root / "derivatives" / "references").glob("*/*.parquet"))
    if archived:
        from cnequity.query.parquet_scan import scan_parquet_files

        history = scan_parquet_files(archived).collect()
        frames.insert(0, history)
    if not frames:
        return None
    history = pl.concat(frames, how="diagonal_relaxed")
    # Replaying an old file must not override a newer archived correction.
    return history.sort("as_of", nulls_last=False) if "as_of" in history.columns else history


def _run_contracts(config: Config, run_id: str, dataset: str) -> dict:
    from cnequity.derive.derivative_contracts import (
        build_futures_contracts,
        build_option_contracts,
    )

    if not is_dataset_enabled(dataset, config):
        return _disabled(dataset)
    kind = "option" if dataset == "option_contracts" else "future"
    bars = _scan_bars(config, "option_bars" if kind == "option" else "futures_bars", run_id)
    if bars is None:
        return {"rows_read": 0, "rows_written": 0, "note": "no bars observed yet"}
    if kind == "option":
        from cnequity.adapters.tushare.etf_options import ETF_OPTION_EXCHANGES

        # ETF option contracts come from the exchanges' own list (via Tushare),
        # not from parsing futures-style codes; their rows are left as written.
        bars = bars.filter(~pl.col("exchange").is_in(list(ETF_OPTION_EXCHANGES)))
    findings: list[dict] = []
    reference = _reference(config, bars, findings, dataset)
    if reference is not None:
        reference = reference.filter(pl.col("kind") == kind)
    # Preserve authoritative historical dates when current reference files no
    # longer list expired contracts. Never carry forward inferred expiry dates.
    existing = _scan_bars(config, dataset)
    if existing is not None:
        end_column = "expiry_date" if kind == "option" else "last_trade_date"
        old = (
            existing.filter(pl.col("dates_basis") == "exchange")
            .select("symbol", "list_date", pl.col(end_column).alias("last_trade_date"))
            .collect()
        )
        if not old.is_empty():
            reference = pl.concat(
                [old, reference.select(old.columns)] if reference is not None else [old],
                how="diagonal_relaxed",
            )
    build = build_option_contracts if kind == "option" else build_futures_contracts
    frame = build(bars, reference)
    if frame.is_empty():
        return _with_findings({"rows_read": 0, "rows_written": 0}, findings)
    result = write_fetched(config, run_id, dataset, frame, source=SOURCE)
    return _with_findings(result, findings)


@register_step("futures_contracts", depends_on=["futures_bars"], group="derivatives")
def step_futures_contracts(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Futures contract table, rebuilt from observed bars and reference files."""
    return _run_contracts(config, run_id, "futures_contracts")


@register_step("option_contracts", depends_on=["option_bars"], group="derivatives")
def step_option_contracts(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Option contract table, rebuilt from observed bars and reference files."""
    return _run_contracts(config, run_id, "option_contracts")


def minute_scope(config: Config) -> list[str]:
    """Contracts to capture 1m bars for: named ones, then each product's two
    most-held contracts on the latest session the lake has."""
    chosen: list[str] = list(getattr(config, "futures_minute_contracts", []) or [])
    products = list(getattr(config, "futures_minute_products", []) or [])
    if products:
        bars = _scan_bars(config, "futures_bars")
        if bars is not None:
            for product_symbol in products:
                product, _, exchange = product_symbol.partition(".")
                history = bars.filter(
                    (pl.col("product") == product) & (pl.col("exchange") == exchange)
                )
                latest = history.select(pl.col("trade_date").max()).collect().item()
                held = (
                    history.filter(pl.col("trade_date") == latest)
                    .collect()
                    .sort("open_interest", descending=True)
                    .head(2)["symbol"]
                    .to_list()
                )
                chosen.extend(held)
    unique = list(dict.fromkeys(chosen))
    limit = int(getattr(config, "futures_minute_max_contracts", 100))
    if len(unique) > limit:
        raise ValueError(
            f"minute watchlist has {len(unique)} contracts, exceeds max_contracts={limit}; narrow the scope explicitly"
        )
    return unique


@register_step("futures_minute_bars", group="derivatives")
def step_futures_minute_bars(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """The latest window of 1m bars for the configured futures contracts."""
    from datetime import timedelta

    from cnequity.adapters.sina.futures_minute import fetch_minute_bars
    from cnequity.steps.common import list_trading_dates

    dataset = "futures_minute_bars"
    if not is_dataset_enabled(dataset, config):
        return {
            "rows_read": 0,
            "rows_written": 0,
            "note": f"{dataset} capture disabled ([futures].minute_enabled = false)",
        }
    symbols = minute_scope(config)
    if not symbols:
        return {"rows_read": 0, "rows_written": 0, "note": "no contracts in the minute scope"}
    sessions = list_trading_dates(
        config, trade_date - timedelta(days=21), trade_date + timedelta(days=21)
    )
    frame, failures = fetch_minute_bars(symbols, sessions=sessions, config=config)
    findings: list[dict] = []
    if failures:
        findings.append(
            {
                "dataset": dataset,
                "severity": "warning" if not frame.is_empty() else "error",
                "check": "futures_minute_missing",
                "message": (
                    f"{dataset}: {len(failures)} of {len(symbols)} contract(s) returned no bars "
                    f"(e.g. {', '.join(sorted(failures)[:5])}); Sina keeps about two sessions, "
                    "so a contract missed for longer loses those bars for good"
                ),
                "contracts": failures,
            }
        )
    if frame.is_empty():
        if failures and len(failures) == len(symbols):
            raise SourceUnavailableError(f"{dataset}: no contract returned bars ({failures})")
        return _with_findings({"rows_read": 0, "rows_written": 0}, findings)
    frame = _quarantine(frame, dataset, "SINA", trade_date, findings, config)
    result = write_fetched(config, run_id, dataset, frame, source="sina")
    result["contracts"] = len(symbols)
    return _with_findings(result, findings)
