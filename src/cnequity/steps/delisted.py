"""Reconstruct the delisted universe by sweeping the exchange code space.

The lake's history was backfilled from the *current* listing snapshot, so every
name that ever left the market is missing and every return series it produces is
survivorship-biased (audit: ``universe_survivorship_absent``). Closing that needs
two things no primary source provides: a list of the codes that used to trade,
and their price history.

Neither vendor list is reliably available — baostock's ``query_stock_basic``
answers it in one query but blacklists an IP that has swept it, and EastMoney's
kline host is unreachable from many networks. What is always available is Sina,
and Sina will answer "did this code ever trade" one code at a time. So the
delisted set is reconstructed from the outside: enumerate the issued code space,
subtract what is listed today, and ask about the remainder. A code that answers
is a former listing; one that does not was never issued.

That is ~9,000 requests, so the sweep checkpoints after every batch and resumes
from where it stopped. It is deliberately a separate command from the ingest:
the catalogue is worth reading before committing to a bulk backfill.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import tempfile
import time
from collections import Counter
from contextlib import nullcontext, suppress
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
import polars as pl

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_lazy_by_primary_key
from cnequity.domain.rate_limit import (
    SINA_FETCH_ATTEMPTS,
    SINA_RETRY_STATUS_CODES,
    source_request,
)
from cnequity.domain.symbols import is_cdr_symbol, issued_code_space, parse_symbol
from cnequity.quality.delisted_coverage import (
    _bar_spans as _bar_spans,
)
from cnequity.quality.delisted_coverage import (
    _identity_evidence_path as _identity_evidence_path,
)
from cnequity.quality.delisted_coverage import (
    _in_historical_universe as _in_historical_universe,
)
from cnequity.quality.delisted_coverage import (
    _known_listing_dates as _known_listing_dates,
)
from cnequity.quality.delisted_coverage import (
    _read_catalog as _read_catalog,
)
from cnequity.quality.delisted_coverage import (
    _reference_date as _reference_date,
)
from cnequity.quality.delisted_coverage import (
    _verified_suspended_windows as _verified_suspended_windows,
)
from cnequity.quality.delisted_coverage import (
    catalog_path as catalog_path,
)
from cnequity.quality.delisted_coverage import (
    classify_catalog as classify_catalog,
)
from cnequity.quality.delisted_coverage import (
    delisted_coverage_report as delisted_coverage_report,
)
from cnequity.quality.delisted_coverage import (
    known_delisted_instruments as known_delisted_instruments,
)
from cnequity.quality.delisted_coverage import (
    load_delisted_catalog as load_delisted_catalog,
)
from cnequity.quality.delisted_coverage import (
    load_live_missing as load_live_missing,
)
from cnequity.quality.delisted_coverage import (
    renamed_symbols as renamed_symbols,
)
from cnequity.query.universe import coverage_end_date
from cnequity.steps.common import (
    instrument_metadata,
    is_trading_day,
    load_curated_instruments,
    load_symbols,
)
from cnequity.storage.atomic import replace_with_retry
from cnequity.storage.read_context import ReadContext

logger = logging.getLogger(__name__)

_CATALOG_FILE = "delisted_catalog.json"
# Sina returns HTTP 456 when a bulk history sweep exceeds its anti-abuse
# budget.  Delisted recovery is another daily-kline sweep, so it needs the
# same bounded retry policy as the live BJ fallback instead of turning a
# transient rate limit into a durable unresolved symbol.
_SINA_RETRY_STATUS_CODES = SINA_RETRY_STATUS_CODES
_SINA_FETCH_ATTEMPTS = SINA_FETCH_ATTEMPTS
# Checkpoint cadence. Small enough that an interrupted sweep loses seconds of
# work, large enough that the state file is not rewritten on every request.
_CHECKPOINT_EVERY = 100


@dataclass
class DiscoveryResult:
    probed: int = 0
    delisted: int = 0
    never_issued: int = 0
    failed: list[str] = field(default_factory=list)
    remaining: int = 0

    @property
    def complete(self) -> bool:
        return self.remaining == 0


def _write_catalog(config: Config, payload: dict) -> None:
    path = catalog_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.stem}-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        replace_with_retry(tmp, path)
    except Exception:
        with suppress(OSError):
            os.unlink(tmp)
        raise


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.stem}-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        replace_with_retry(tmp, path)
    except Exception:
        with suppress(OSError):
            os.unlink(tmp)
        raise


def _run_sina_with_retry(
    operation,
    *,
    symbol: str,
    operation_name: str,
    config: Config,
    request_managed: bool = False,
):
    """Run one Sina recovery operation with bounded retries.

    Injected operations are guarded here for callers that do not expose the
    adapter's request boundary. The built-in probe/fetch functions pass the
    config through to ``sina.bars`` instead, where each HTTP call gets its own
    lease (a symbol probe can make two independent calls). Holding another
    lease around those operations would deadlock at a cap of one and would
    count the two wire requests as one QPS event.
    """
    for attempt in range(_SINA_FETCH_ATTEMPTS):
        try:
            context = nullcontext() if request_managed else source_request(config, "sina_bars")
            with context:
                return operation()
        except Exception as exc:  # noqa: BLE001 — classify transport response below
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            retryable = status_code in _SINA_RETRY_STATUS_CODES
            if not retryable or attempt + 1 >= _SINA_FETCH_ATTEMPTS:
                raise
            delay = max(float(getattr(config, "retry_backoff_seconds", 5)), 1.0) * (attempt + 1)
            logger.warning(
                "sina %s transient HTTP %s for %s; retrying in %.1fs (%d/%d)",
                operation_name,
                status_code,
                symbol,
                delay,
                attempt + 1,
                _SINA_FETCH_ATTEMPTS - 1,
            )
            time.sleep(delay)
    raise AssertionError("unreachable")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


# A code the vendor still quotes close to the market's latest session is
# trading, not delisted — it is simply absent from `instruments`. The sweep
# cannot tell the two apart from "has data", and the whole BJ board turned out
# to be exactly this: 328 codes quoting yesterday's close that the lake had
# never heard of. Filing those as delistings would have written a delist_date
# for live stocks and frozen them out of the universe.
#
# 30 days is deliberately generous: a suspended-but-listed name must not be
# mistaken for a delisting, and a genuinely delisted one merely waits for the
# next sweep to age past the threshold. Classification happens at read time, so
# the catalogue needs no migration and corrects itself as time passes.
LIVE_RECENCY_DAYS = 30

# repair_delisted_instruments' orphan scan (below) has the same "suspended
# must not be mistaken for delisted" problem as the catalogue sweep above,
# but for a symbol with no catalogue entry at all: it infers retirement from
# a positive-volume trading gap alone. A run of at least this many vendor
# placeholder rows after the last real trade means the vendor is still
# actively tracking the symbol as listed, so the gap is a halt, not a
# delisting. A handful of trailing placeholder rows (a source's habit of
# tacking a few dummy rows onto a series after the real end) is not.
_ORPHAN_ACTIVE_PLACEHOLDER_MIN_ROWS = 20


def pending_codes(config: Config, read_context: ReadContext | None = None) -> list[str]:
    """Issued codes neither listed today nor already classified by a prior sweep."""
    metadata = (
        load_curated_instruments(config, read_context)
        if read_context is not None
        else instrument_metadata(config)
    )
    if metadata is None:
        metadata = pl.DataFrame()
    if metadata.is_empty():
        live = set() if read_context is not None else set(load_symbols(config))
    else:
        reference = _reference_date(config, read_context)
        live = set(
            metadata.filter(pl.col("delist_date").is_null() | (pl.col("delist_date") > reference))[
                "symbol"
            ].to_list()
        )
    catalog = _read_catalog(config)
    done = set(catalog["delisted"]) | set(catalog["never_issued"]) | renamed_symbols()
    return [s for s in issued_code_space() if s not in live and s not in done]


def discover_delisted(
    config: Config,
    *,
    limit: int | None = None,
    probe=None,
) -> DiscoveryResult:
    """Classify unlisted codes as former listings or never-issued, resumably.

    ``probe(symbol, client) -> date | None`` is injectable for tests; the default
    asks Sina for a single bar. A probe that raises is recorded as failed and
    left pending, so a transient outage never gets misfiled as "never issued" —
    that misfiling would silently and permanently shrink the universe.
    """
    from cnequity.adapters.sina.bars import symbol_exists

    default_probe = probe is None
    probe = probe or (lambda sym, client: symbol_exists(sym, client=client, config=config))
    todo = pending_codes(config)
    if limit is not None:
        todo = todo[:limit]

    catalog = _read_catalog(config)
    result = DiscoveryResult()
    logger.info("delisted discovery: %d code(s) to probe", len(todo))

    with httpx.Client(timeout=20.0) as client:
        for index, symbol in enumerate(todo, start=1):
            try:
                last_seen = _run_sina_with_retry(
                    lambda symbol=symbol: probe(symbol, client),
                    symbol=symbol,
                    operation_name="discovery probe",
                    config=config,
                    request_managed=default_probe,
                )
            except Exception as exc:  # noqa: BLE001 — never misfile an outage
                logger.warning("delisted discovery: probe failed for %s: %s", symbol, exc)
                result.failed.append(symbol)
                continue
            result.probed += 1
            if last_seen is None:
                catalog["never_issued"].append(symbol)
                result.never_issued += 1
            elif symbol in renamed_symbols():
                # Unreachable through `pending_codes`, which already excludes
                # these. Kept because an injected probe or a hand-built todo
                # would otherwise re-file the rename as a delisting, which is
                # the whole thing this is meant to stop.
                logger.info("delisted discovery: %s was renamed, not delisted", symbol)
            else:
                catalog["delisted"][symbol] = last_seen.isoformat()
                result.delisted += 1
                logger.info("delisted discovery: %s last traded %s", symbol, last_seen)

            if index % _CHECKPOINT_EVERY == 0:
                _write_catalog(config, catalog)
                logger.info(
                    "delisted discovery: %d/%d probed (%d delisted so far)",
                    index,
                    len(todo),
                    result.delisted,
                )

    _write_catalog(config, catalog)
    result.remaining = len(pending_codes(config))
    logger.info(
        "delisted discovery: probed=%d delisted=%d never_issued=%d failed=%d remaining=%d",
        result.probed,
        result.delisted,
        result.never_issued,
        len(result.failed),
        result.remaining,
    )
    return result


# --- ingest -----------------------------------------------------------------

_INGESTED_STATE = "delisted_ingested"
_RECOVERY_EVIDENCE_VERSION = 2
_RECOVERY_CLAIM = "delisted_daily_bars_recovery"
_IDENTITY_EVIDENCE_VERSION = 1
_IDENTITY_CLAIM = "delisted_security_identity"
# Stage every N symbols so a long sweep that dies keeps what it already fetched.
_INGEST_CHUNK = 50


def _recovery_symbol_hash(symbols: list[str] | set[str]) -> str:
    return hashlib.sha256(
        json.dumps(sorted(set(symbols)), separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _ingested_symbols(config: Config) -> set[str]:
    path = config.meta_root / "state" / f"{_INGESTED_STATE}.json"
    if not path.exists():
        return set()
    return set(json.loads(path.read_text(encoding="utf-8")).get("completed", []))


def _mark_ingested(config: Config, symbols: list[str]) -> None:
    path = config.meta_root / "state" / f"{_INGESTED_STATE}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    completed = sorted(_ingested_symbols(config) | set(symbols))
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.stem}-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump({"completed": completed}, handle, indent=2)
        replace_with_retry(tmp, path)
    except Exception:
        with suppress(OSError):
            os.unlink(tmp)
        raise


def _asset_type(symbol: str) -> str:
    from cnequity.domain.symbols import is_etf_symbol

    info = parse_symbol(symbol)
    if is_etf_symbol(info.code, info.exchange):
        return "etf"
    if is_cdr_symbol(info.code, info.exchange):
        return "cdr"
    return "stock"


def delisted_symbols_in_window(config: Config, start: date) -> list[str]:
    """Catalogued delistings whose trading overlapped the lake's window.

    A name that stopped trading before the lake starts contributes nothing to a
    backtest over it, so fetching its history would be pure cost.
    """
    catalog = load_delisted_catalog(config)
    already = _ingested_symbols(config)
    return sorted(s for s, last in catalog.items() if last >= start and s not in already)


def write_delisted_identity_evidence(config: Config, delisted: pl.DataFrame) -> Path:
    """Persist a complete source observation of formal delisting identity.

    ``instruments.delist_date`` is intentionally not sufficient evidence here:
    compaction can also populate it from snapshot absence and the recovery path
    can populate it from a last traded bar.  This record is written only after
    Baostock's full security-master query succeeds, preserving the distinction
    between vendor identity and an observed terminal date.
    """
    symbols = {
        row["symbol"]: row["delist_date"].isoformat()
        for row in delisted.select("symbol", "delist_date").iter_rows(named=True)
        if row["delist_date"] is not None
    }
    payload = {
        "schema_version": 1,
        "claim": _IDENTITY_CLAIM,
        "evidence_version": _IDENTITY_EVIDENCE_VERSION,
        "status": "complete",
        "source": "baostock.query_stock_basic",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "delisted_symbols": dict(sorted(symbols.items())),
        "listing_dates": {
            row["symbol"]: row["list_date"].isoformat()
            for row in delisted.select("symbol", "list_date").iter_rows(named=True)
            if row["list_date"] is not None
        }
        if "list_date" in delisted.columns
        else {},
    }
    path = _identity_evidence_path(config)
    if path.exists():
        previous = path.read_bytes()
        history = path.parent / "history" / f"{hashlib.sha256(previous).hexdigest()}.json"
        history.parent.mkdir(parents=True, exist_ok=True)
        if not history.exists():
            history.write_bytes(previous)
    _write_json_atomic(path, payload)
    return path


def delisted_recovery_targets(
    config: Config,
    start: date,
    end: date,
) -> dict[str, dict[str, str]]:
    """Return every target with an explicit fetch/no-data ownership state.

    Formal identity and actual window overlap are separate. A formal delisting
    before the window, or an independently probed terminal before it, is
    expected-no-data. Everything else with formal or aged catalogue authority
    belongs to the dedicated fetch path. Probe-only recent names remain outside
    the set until identity is authoritative.
    """
    formal = known_delisted_instruments(config, end)
    catalog = load_delisted_catalog(config)
    # The chokepoint for both writers below: this set feeds the dedicated
    # fetch, the terminal probe that writes back to the catalogue, and
    # `write_delisting_events`. A renamed code excluded here cannot be
    # re-filed as a delisting by any of the three.
    renamed = renamed_symbols()
    formal = {s: d for s, d in formal.items() if s not in renamed}
    catalog = {s: d for s, d in catalog.items() if s not in renamed}
    targets: dict[str, dict[str, str]] = {}
    for symbol, formal_date in formal.items():
        terminal = catalog.get(symbol)
        expected_no_data = formal_date < start or (terminal is not None and terminal < start)
        targets[symbol] = {
            "ownership": "expected_no_data" if expected_no_data else "dedicated_fetch",
            "basis": "formal_delist_date",
            "formal_delist_date": formal_date.isoformat(),
            **({"last_traded_date": terminal.isoformat()} if terminal else {}),
        }
    for symbol, terminal in catalog.items():
        if terminal < start or symbol in targets:
            continue
        targets[symbol] = {
            "ownership": "dedicated_fetch",
            "basis": "aged_probe_terminal",
            "last_traded_date": terminal.isoformat(),
        }
    return dict(sorted(targets.items()))


def _recovery_scope(start: date, end: date, targets: dict[str, dict[str, str]]) -> dict:
    identity = {
        "evidence_version": _RECOVERY_EVIDENCE_VERSION,
        "source": "sina",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "targets_sha256": hashlib.sha256(
            json.dumps(targets, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }
    return {
        **identity,
        "scope_id": hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "targets": targets,
        "targets_count": len(targets),
    }


def _recovery_checkpoint_path(config: Config, scope_id: str) -> Path:
    return (
        config.meta_root
        / "state"
        / _RECOVERY_CLAIM
        / f"v{_RECOVERY_EVIDENCE_VERSION}"
        / f"{scope_id}.json"
    )


def _load_recovery_checkpoint(config: Config, scope: dict) -> dict:
    path = _recovery_checkpoint_path(config, scope["scope_id"])
    if not path.exists():
        expected = [
            symbol
            for symbol, evidence in scope["targets"].items()
            if evidence["ownership"] == "expected_no_data"
        ]
        return {
            "schema_version": 1,
            "claim": _RECOVERY_CLAIM,
            "scope": scope,
            "status": "pending",
            "recovered_spans": {},
            "expected_no_data_symbols": expected,
            "unresolved_symbols": [],
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("scope") != scope:
        raise RuntimeError(f"delisted recovery checkpoint identity mismatch: {path}")
    return payload


def _write_recovery_checkpoint(config: Config, payload: dict) -> Path:
    payload = dict(payload)
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    path = _recovery_checkpoint_path(config, payload["scope"]["scope_id"])
    _write_json_atomic(path, payload)
    return path


def _publish_recovery_receipt(config: Config, checkpoint: dict) -> Path:
    scope = checkpoint["scope"]
    targets = set(scope["targets"])
    recovered = set(checkpoint.get("recovered_spans", {}))
    no_data = set(checkpoint.get("expected_no_data_symbols", []))
    unresolved = set(checkpoint.get("unresolved_symbols", []))
    if recovered | no_data != targets or recovered & no_data or unresolved:
        raise ValueError("cannot publish incomplete delisted recovery evidence")
    persisted = _bar_spans(config, sorted(recovered))
    missing = []
    for symbol, expected in checkpoint.get("recovered_spans", {}).items():
        observed = persisted.get(symbol)
        expected_first = date.fromisoformat(expected["first_trade_date"])
        expected_last = date.fromisoformat(expected["last_trade_date"])
        if observed is None or observed[0] > expected_first or observed[1] < expected_last:
            missing.append(symbol)
    if missing:
        raise ValueError(
            f"cannot publish delisted recovery before {len(missing)} symbol(s) reach curated storage"
        )
    payload = {
        "schema_version": 2,
        "claim": _RECOVERY_CLAIM,
        "status": "complete",
        "scope": {key: value for key, value in scope.items() if key != "targets"},
        "recovered_symbols": sorted(recovered),
        "expected_no_data_symbols": sorted(no_data),
        "target_symbols_sha256": _recovery_symbol_hash(recovered | no_data),
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    path = config.meta_root / "quality" / "coverage" / _RECOVERY_CLAIM / f"{scope['scope_id']}.json"
    _write_json_atomic(path, payload)
    return path


def publish_delisted_receipts_for_compacted_run(config: Config, run_id: str) -> list[Path]:
    """Publish recovery authority only after the completing run was compacted."""
    root = config.meta_root / "state" / _RECOVERY_CLAIM / f"v{_RECOVERY_EVIDENCE_VERSION}"
    if not root.exists():
        return []
    published: list[Path] = []
    for path in root.glob("*.json"):
        try:
            checkpoint = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if checkpoint.get("status") != "complete":
            continue
        if checkpoint.get("completion_run_id") != run_id:
            continue
        published.append(_publish_recovery_receipt(config, checkpoint))
    return published


def _catalogued_delist_dates(config: Config, symbols: set[str]) -> dict[str, date]:
    """``symbol -> delist_date`` for the requested names that have one."""
    metadata = instrument_metadata(config)
    if metadata.is_empty():
        return {}
    rows = metadata.filter(
        pl.col("symbol").is_in(sorted(symbols)) & pl.col("delist_date").is_not_null()
    )
    return {row["symbol"]: row["delist_date"] for row in rows.iter_rows(named=True)}


def _history_already_in_the_lake(config: Config, symbols: set[str], start: date) -> set[str]:
    """Names whose delisted history the lake already holds, so nothing is pending.

    The recovery sweep exists for names the live catalogue no longer lists and
    the lake has therefore never seen — that is the survivorship hole it fills.
    A name that has traded in this lake for years and has now been marked
    delisted is the opposite case: its history is already here, and there is no
    recovery to wait for.

    Proof is by inspection: traded bars must start by the first required
    session, or a verified zero-volume prefix must cover the earlier sessions.
    They must stop at or before the catalogued delisting. A span that runs
    *past* the delisting contradicts the catalogue and is not accepted here.
    """
    if not symbols:
        return set()
    delist_dates = _catalogued_delist_dates(config, symbols)
    spans = _bar_spans(config, sorted(symbols))
    starts = _delisted_history_starts(config, symbols, start)
    prefix_covered = _zero_volume_prefixes(config, spans, starts)
    proven: set[str] = set()
    for symbol in symbols:
        span = spans.get(symbol)
        delisted_on = delist_dates.get(symbol)
        if span is None or delisted_on is None:
            continue
        if (span[0] <= starts[symbol] or symbol in prefix_covered) and span[1] <= delisted_on:
            proven.add(symbol)
    return proven


def _zero_volume_prefixes(
    config: Config, spans: dict[str, tuple[date, date]], starts: dict[str, date]
) -> set[str]:
    """Accept stored non-trading rows before a known listing's first traded bar.

    Require an authoritative IPO date, later positive-volume history, and an
    explicit zero-volume row for every exchange session in the prefix. A
    zero-only pre-IPO quote or a missing session cannot satisfy this proof.
    """
    from cnequity.query.parquet_scan import scan_parquet_root
    from cnequity.steps.common import list_trading_dates
    from cnequity.storage.read_context import read_root

    listings = _known_listing_dates(config)
    candidates = {
        symbol: (starts[symbol], span[0] - timedelta(days=1))
        for symbol, span in spans.items()
        if symbol in starts
        and span[0] > starts[symbol]
        and symbol in listings
        and listings[symbol] <= starts[symbol]
    }
    if not candidates:
        return set()
    rows = scan_parquet_root(
        read_root(config, "daily_bars"),
        partition_col="trade_date",
        start=min(lo for lo, _ in candidates.values()),
        end=max(spans[symbol][0] for symbol in candidates),
        symbols=sorted(candidates),
        dataset="daily_bars",
        committed=False,
    )
    if "volume" not in rows.collect_schema():
        return set()
    frame = rows.select("symbol", "trade_date", "volume").collect()
    traded = {
        (symbol, day)
        for symbol, day in frame.filter(pl.col("volume") > 0)
        .select("symbol", "trade_date")
        .iter_rows()
    }
    observed: dict[str, set[date]] = {}
    for symbol, day in (
        frame.filter(pl.col("volume") == 0).select("symbol", "trade_date").iter_rows()
    ):
        observed.setdefault(symbol, set()).add(day)
    proven = set()
    for symbol, (lo, hi) in candidates.items():
        sessions = set(list_trading_dates(config, lo, hi))
        # A truncated calendar cannot turn missing days into evidence.
        if (
            (symbol, spans[symbol][0]) in traded
            and sessions
            and min(sessions) == lo
            and sessions.issubset(observed.get(symbol, set()))
        ):
            proven.add(symbol)
    return proven


def _delisted_history_starts(config: Config, symbols: set[str], start: date) -> dict[str, date]:
    """A closed-day window or later IPO owes bars from its first session."""
    from datetime import timedelta

    from cnequity.steps.common import list_trading_dates

    listings = _known_listing_dates(config)
    starts = {symbol: max(start, listings.get(symbol, start)) for symbol in symbols}
    for floor in set(starts.values()):
        sessions = list_trading_dates(config, floor, floor + timedelta(days=31))
        if sessions:
            first = min(sessions)
            starts = {
                symbol: first if value == floor else value for symbol, value in starts.items()
            }
    return starts


def delisted_recovery_covers(
    config: Config,
    start: date,
    end: date,
    symbols: list[str],
) -> bool:
    """Whether *symbols*' delisted history is proven complete for this window.

    Two independent proofs are accepted, and either is enough per symbol:

    * the lake already holds the name's history up to its catalogued delisting
      (``_history_already_in_the_lake``), or
    * a published recovery receipt covers it.

    The receipt path clamps its window to each name's catalogued life. Asking a
    delisted name for bars through the *requested* ``end`` is unsatisfiable by
    construction once ``end`` is today — a name that stopped trading in July
    cannot have a September bar — and that is what turned one freshly delisted
    symbol into a permanent block on the daily compaction.
    """
    required = set(symbols)
    if not required:
        return True
    required -= _history_already_in_the_lake(config, required, start)
    if not required:
        return True
    # A delisted name owes bars only for the part of the window it could trade
    # in. Beyond its delisting, absence is the evidence, not a gap.
    delist_dates = _catalogued_delist_dates(config, required)
    starts = _delisted_history_starts(config, required, start)
    horizon = min(
        (min(end, delist_dates[symbol]) for symbol in required if symbol in delist_dates),
        default=end,
    )
    root = config.meta_root / "quality" / "coverage" / _RECOVERY_CLAIM
    if not root.exists():
        return False
    for path in root.glob("*.json"):
        try:
            receipt = json.loads(path.read_text(encoding="utf-8"))
            scope = receipt["scope"]
            recovered = receipt.get("recovered_symbols", [])
            no_data = receipt.get("expected_no_data_symbols", [])
            if (
                receipt.get("claim") != _RECOVERY_CLAIM
                or receipt.get("schema_version") != 2
                or not isinstance(scope, dict)
                or not isinstance(recovered, list)
                or not isinstance(no_data, list)
                or not all(isinstance(symbol, str) for symbol in recovered + no_data)
                or set(recovered) & set(no_data)
                or scope.get("targets_count") != len(set(recovered) | set(no_data))
                or receipt.get("target_symbols_sha256")
                != _recovery_symbol_hash(recovered + no_data)
            ):
                continue
            identity = {
                key: value
                for key, value in scope.items()
                if key not in {"scope_id", "targets_count"}
            }
            expected_scope_id = hashlib.sha256(
                json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            if scope.get("scope_id") != expected_scope_id:
                continue
            covered = set(recovered)
            if (
                receipt.get("status") == "complete"
                and scope.get("evidence_version") == _RECOVERY_EVIDENCE_VERSION
                and date.fromisoformat(scope["start"]) <= start
                and date.fromisoformat(scope["end"]) >= horizon
                and required <= covered | set(no_data)
            ):
                required_bars = required & covered
                spans = _bar_spans(config, sorted(required_bars))
                if all(
                    symbol in spans
                    and spans[symbol][0] <= starts[symbol]
                    and spans[symbol][1] >= min(horizon, delist_dates.get(symbol, horizon))
                    for symbol in required_bars
                ):
                    return True
        except (KeyError, OSError, ValueError, json.JSONDecodeError):
            continue
    return False


def _instruments_rows(config: Config, spans: dict[str, tuple[date | None, date]]) -> pl.DataFrame:
    """instruments rows for the recovered names, unioned with the live snapshot.

    The union matters: ``compact_instruments`` treats symbols missing from the
    incoming frame as candidate delistings, and staging only the recovered names
    would present all ~7,400 live symbols as absent — tripping the partial-fetch
    guard and emitting a spurious error. Sina's kline carries no company name, so
    ``name`` stays null rather than being invented.
    """
    from cnequity.domain.schemas import INSTRUMENTS_SCHEMA

    cols = ["symbol", "name", "exchange", "asset_type", "list_date", "delist_date", "prev_symbol"]
    rows = [
        {
            "symbol": symbol,
            "name": None,
            "exchange": symbol.split(".")[1],
            "asset_type": _asset_type(symbol),
            "list_date": first,
            # The last day it traded. Universe filters keep a symbol through its
            # delist_date, so this includes the final session.
            "delist_date": last,
            "prev_symbol": None,
        }
        for symbol, (first, last) in sorted(spans.items())
    ]
    if not rows:
        return pl.DataFrame()

    recovered = pl.DataFrame(rows, schema={c: INSTRUMENTS_SCHEMA[c] for c in cols})
    live = load_curated_instruments(config)
    if live is None:
        return recovered
    live = live.drop(["source", "data_version", "fetched_at"], strict=False)
    live = _strip_subscription_placeholders(live)
    # Fill null list/delist dates on live rows from the recovery spans before
    # the unique — otherwise a prior repair that wrote delist_date with a null
    # list_date (no bars yet) permanently shadows the bar-derived list_date
    # from a later backfill (keep="first" would keep the hollow live row).
    if not live.is_empty() and not recovered.is_empty():
        fill = recovered.select(
            [
                "symbol",
                pl.col("list_date").alias("_rec_list_date"),
                pl.col("delist_date").alias("_rec_delist_date"),
            ]
        )
        live = (
            live.join(fill, on="symbol", how="left")
            .with_columns(
                pl.coalesce(pl.col("list_date"), pl.col("_rec_list_date")).alias("list_date"),
                pl.coalesce(pl.col("delist_date"), pl.col("_rec_delist_date")).alias("delist_date"),
            )
            .drop("_rec_list_date", "_rec_delist_date")
        )
    # keep="first" so a live row always wins over a recovered one for the same
    # code — a code reissued after a delisting must stay listed. Catalogued
    # recoveries are absent from the live snapshot (that is the gap), so they
    # append cleanly; subscription stubs are stripped above so they cannot
    # re-enter via the union.
    return pl.concat([live, recovered], how="diagonal_relaxed").unique(
        subset=["symbol"], keep="first"
    )


def delisted_catalog_reconciliation_report(config: Config, *, sample: int = 15, probe=None) -> dict:
    """Compare swept catalogue terminals with curated independent evidence.

    A correction is considered safe only when curated positive-volume bars
    establish a different terminal, that terminal does not exceed the formal
    instrument delisting date, and the catalogue terminal is independently
    impossible (after formal delisting or not an exchange trading day). Other
    mismatches remain visible but are never changed automatically.

    ``probe(symbol) -> date | None`` optionally re-asks the vendor for its last
    positive-volume bar. Terminals swept before the probe rejected zero-volume
    quotes can name a non-printing session; a fresh probe that agrees with the
    curated terminal is the independent evidence for those.
    """
    if sample < 0:
        raise ValueError("sample must be non-negative")

    # The raw sweep also holds codes that still quote near the lake watermark
    # (notably the live BJ board missing from an older instrument snapshot).
    # They are coverage gaps, not retired names, and do not belong in terminal
    # reconciliation.
    catalog = classify_catalog(config)[0]
    symbols = sorted(catalog)
    spans = _bar_spans(config, symbols, positive_volume_only=True)

    instrument_dates: dict[str, date | None] = {}
    instruments = load_curated_instruments(config)
    if instruments is not None:
        instruments = instruments.select(["symbol", "delist_date"])
        instrument_dates = {
            row["symbol"]: row["delist_date"] for row in instruments.iter_rows(named=True)
        }

    safe: list[dict] = []
    unresolved: list[dict] = []
    missing_bars: list[dict] = []
    matching = 0
    for symbol in symbols:
        catalog_last = catalog[symbol]
        span = spans.get(symbol)
        formal = instrument_dates.get(symbol)
        if span is None:
            missing_bars.append({"symbol": symbol, "catalog_last_traded": catalog_last.isoformat()})
            continue
        observed = span[1]
        if observed == catalog_last:
            matching += 1
            continue

        catalog_after_formal = formal is not None and catalog_last > formal
        catalog_not_trading = not is_trading_day(config, catalog_last)
        observed_within_identity = formal is not None and observed <= formal
        reprobed = (
            probe(symbol)
            if probe is not None
            and observed_within_identity
            and not (catalog_after_formal or catalog_not_trading)
            else None
        )
        probe_confirms = reprobed == observed
        finding = {
            "symbol": symbol,
            "catalog_last_traded": catalog_last.isoformat(),
            "observed_last_positive_volume_bar": observed.isoformat(),
            "instrument_delist_date": formal.isoformat() if formal else None,
            "catalog_after_formal_delist": catalog_after_formal,
            "catalog_not_trading_day": catalog_not_trading,
        }
        if reprobed is not None:
            finding["current_probe_last_traded"] = reprobed.isoformat()
        if observed_within_identity and (
            catalog_after_formal or catalog_not_trading or probe_confirms
        ):
            finding["proposed_last_traded"] = observed.isoformat()
            finding["evidence"] = [
                reason
                for reason, present in (
                    ("catalog_after_formal_delist", catalog_after_formal),
                    ("catalog_not_trading_day", catalog_not_trading),
                    ("current_probe_positive_volume_terminal", probe_confirms),
                    ("curated_positive_volume_terminal", True),
                )
                if present
            ]
            safe.append(finding)
        else:
            finding["reason"] = (
                "observed_terminal_exceeds_formal_delist"
                if formal is not None and observed > formal
                else "catalog_terminal_not_independently_disproven"
            )
            unresolved.append(finding)

    return {
        "claim": "delisted_catalog_terminal_reconciliation",
        "read_only": True,
        "counts": {
            "catalogued": len(catalog),
            "matching": matching,
            "safe_correction": len(safe),
            "unresolved_mismatch": len(unresolved),
            "missing_curated_bars": len(missing_bars),
        },
        "samples": {
            "safe_correction": safe[:sample],
            "unresolved_mismatch": unresolved[:sample],
            "missing_curated_bars": missing_bars[:sample],
        },
        # The complete safe set is retained for deterministic apply even when
        # the human-readable samples are capped.
        "safe_corrections": safe,
    }


def reconcile_delisted_catalog(config: Config, *, sample: int = 15, probe=None) -> dict:
    """Apply only high-confidence terminal corrections with backup and receipt."""
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.orchestrator.run_lock import run_lock

    with run_lock(config.meta_root, "delisted_catalog_reconciliation"):
        running = [
            {"run_id": row["run_id"], "job_name": row["job_name"]}
            for row in Manifest(config.manifest_path).list_runs()
            if row["status"] == "running"
        ]
        if running:
            names = ", ".join(f"{row['job_name']}:{row['run_id']}" for row in running[:5])
            raise RuntimeError(
                "refusing to reconcile the delisted catalogue while ingestion runs are active: "
                + names
            )

        report = delisted_catalog_reconciliation_report(config, sample=sample, probe=probe)
        corrections = report["safe_corrections"]
        now = datetime.now(timezone.utc)
        stamp = now.strftime("%Y%m%dT%H%M%SZ")
        source = catalog_path(config)
        if not source.exists():
            raise RuntimeError("delisted catalogue does not exist")

        backup = config.meta_root / "state" / "history" / f"delisted_catalog-{stamp}.json"
        backup.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, backup)
        before_sha256 = _file_sha256(backup)

        payload = _read_catalog(config)
        for correction in corrections:
            payload["delisted"][correction["symbol"]] = correction["proposed_last_traded"]
        payload["last_reconciled_at"] = now.isoformat()
        _write_catalog(config, payload)
        after_sha256 = _file_sha256(source)

        receipt = {
            **report,
            "read_only": False,
            "applied": len(corrections),
            "applied_at": now.isoformat(),
            "backup": str(backup),
            "backup_sha256": before_sha256,
            "catalog_sha256": after_sha256,
        }
        receipt_path = (
            config.meta_root / "quality" / f"delisted-catalog-reconciliation-{stamp}.json"
        )
        _write_json_atomic(receipt_path, receipt)
        return {**receipt, "receipt": str(receipt_path)}


def _strip_subscription_placeholders(df: pl.DataFrame) -> pl.DataFrame:
    from cnequity.domain.symbols import is_subscription_placeholder

    if df.is_empty() or "name" not in df.columns:
        return df
    keep = [not is_subscription_placeholder(n) for n in df["name"].to_list()]
    return df.filter(pl.Series(keep))


def purge_subscription_placeholders(config: Config) -> int:
    """Remove ``认购款`` stubs from curated instruments. Returns rows dropped."""
    from cnequity.storage.atomic import write_parquet_atomic

    existing = load_curated_instruments(config)
    if existing is None:
        return 0
    path = config.curated_root / "instruments" / "part-merged.parquet"
    cleaned = _strip_subscription_placeholders(existing)
    dropped = existing.height - cleaned.height
    if dropped:
        write_parquet_atomic(path, cleaned, compression="zstd")
        for stale in path.parent.rglob("*.parquet"):
            if stale != path:
                stale.unlink()
        logger.info("purged %d subscription-placeholder instrument row(s)", dropped)
    return dropped


def repair_delisted_instruments(
    config: Config,
    run_id: str,
    *,
    start: date | None = None,
) -> dict:
    """Wire catalogued delistings into ``instruments`` from bars already in the lake.

    The baostock bars backfill closed the survivorship gap in ``daily_bars`` but
    never wrote matching ``instruments`` rows, so ``universe="all_a"`` kept
    selecting dead names forever (audit: ``retired_symbol_missing_delist_date``).
    Re-fetching those bars would be pure cost — derive ``list_date`` /
    ``delist_date`` from the spans that are already on disk, stage the union with
    the live snapshot, and mark the catalogued symbols ingested so
    ``cne backfill daily_bars --profile delisted`` only fetches the true gaps.
    """
    from cnequity.quality.cross_checks import RETIRED_GAP_DAYS
    from cnequity.steps.http_common import write_fetched

    catalog = load_delisted_catalog(config)
    if start is not None:
        catalog = {s: last for s, last in catalog.items() if last >= start}

    # Orphan bars: series that ended well before the lake's last session but
    # carry no delist_date (or no instruments row at all). Same structural
    # tell the survivorship audit uses.
    retired_orphans: list[str] = []
    bars_root = config.curated_root / "daily_bars"
    lake_last = coverage_end_date(config, "daily_bars") if bars_root.exists() else None
    if lake_last is not None:
        cutoff = lake_last - timedelta(days=RETIRED_GAP_DAYS)
        from cnequity.query.parquet_scan import scan_parquet_root

        bars = scan_parquet_root(
            bars_root, partition_col="trade_date", hive=False, traded_only=True
        )
        last_bars = (
            bars.group_by("symbol")
            .agg(pl.col("trade_date").max().alias("last_bar"))
            .filter(pl.col("last_bar") < cutoff)
            .collect()
        )
        marked: set[str] = set()
        inst = load_curated_instruments(config)
        if inst is not None:
            marked = set(inst.filter(pl.col("delist_date").is_not_null())["symbol"].to_list())
        candidates = [s for s in last_bars["symbol"].to_list() if s not in marked]

        # A stock that is merely halted (bankruptcy reorganisation, a
        # regulatory investigation — both real, sometimes year-plus patterns
        # in this market) but still formally listed keeps getting a daily
        # placeholder row from the vendor throughout the halt. That is a
        # trading gap identical to a dead stock's under the traded-only
        # filter above, so the gap alone cannot tell the two apart. A run of
        # placeholder rows past the last trade is the tell for "still being
        # tracked as listed"; a token trailing row or two is not — require a
        # real run before trusting a candidate is genuinely retired.
        retired_orphans = candidates
        if candidates:
            last_bar_by_symbol = last_bars.filter(pl.col("symbol").is_in(candidates)).select(
                "symbol", "last_bar"
            )
            tail_counts = (
                dedupe_lazy_by_primary_key(
                    scan_parquet_root(bars_root, partition_col="trade_date", hive=False),
                    "daily_bars",
                )
                .filter(pl.col("symbol").is_in(candidates))
                .join(last_bar_by_symbol.lazy(), on="symbol", how="inner")
                .filter(pl.col("trade_date") > pl.col("last_bar"))
                .group_by("symbol")
                .agg(pl.len().alias("tail_rows"))
                .filter(pl.col("tail_rows") >= _ORPHAN_ACTIVE_PLACEHOLDER_MIN_ROWS)
                .collect()
            )
            still_tracked = set(tail_counts["symbol"].to_list())
            retired_orphans = [s for s in candidates if s not in still_tracked]

    targets = sorted(set(catalog) | set(retired_orphans))
    # Only stocks/CDRs — ETF-prefix orphans are noise for the equity universe.
    targets = [s for s in targets if _asset_type(s) in ("stock", "cdr")]
    spans = _bar_spans(config, targets)

    instrument_spans: dict[str, tuple[date, date]] = {}
    for symbol in targets:
        if symbol in spans:
            first, last = spans[symbol]
            # Prefer the later of catalog last / bar last so a consolidation
            # tail past the Sina probe date is not cut off.
            delist = max(catalog[symbol], last) if symbol in catalog else last
            instrument_spans[symbol] = (first, delist)
        elif symbol in catalog:
            # No bars yet — still record the delisting so all_a stops selecting
            # it; list_date stays unknown until a backfill lands.
            instrument_spans[symbol] = (None, catalog[symbol])

    instruments = _instruments_rows(config, instrument_spans)
    instruments = _strip_subscription_placeholders(instruments)
    rows_written = 0
    if not instruments.is_empty():
        out = write_fetched(config, run_id, "instruments", instruments, source="sina")
        rows_written = int(out.get("rows_written", 0))

    # Symbols whose bars are already in the lake need no sina re-fetch.
    already_haved = sorted(s for s in catalog if s in spans)
    if already_haved:
        _mark_ingested(config, already_haved)

    purged = purge_subscription_placeholders(config)

    still_need_bars = sorted(s for s in catalog if s not in spans)
    result = {
        "rows_read": rows_written,
        "rows_written": rows_written,
        "targets": len(targets),
        "from_catalog": len(catalog),
        "from_orphan_bars": len(retired_orphans),
        "with_bars": len(spans),
        "instruments_spans": len(instrument_spans),
        "marked_ingested": len(already_haved),
        "purged_placeholders": purged,
        "still_need_bars": still_need_bars,
    }
    if still_need_bars:
        result["status"] = "warning"
    return result


def backfill_delisted_bars(
    config: Config,
    run_id: str,
    start: date,
    *,
    end: date | None = None,
    recovery_targets: dict[str, dict[str, str]] | None = None,
    fetch=None,
    probe_last=None,
) -> dict:
    """Recover dedicated delisted history with explicit three-state evidence.

    Each target ends as recovered, proven expected-no-data, or unresolved. A
    target whose curated bars already reach the catalogue terminal is reused
    without another provider request. A complete receipt is published only
    when every target has one of the first two states. The legacy global
    ``delisted_ingested`` marker is not completion authority because it was not
    window-scoped and marked empty fetches done.
    """
    from cnequity.adapters.sina.bars import fetch_daily_bars_sina, symbol_exists
    from cnequity.steps.http_common import write_fetched

    end = end or _reference_date(config)
    if start > end:
        raise ValueError("delisted recovery start must be on or before end")
    default_fetch = fetch is None
    default_probe = probe_last is None
    fetch = fetch or (
        lambda symbol, client: fetch_daily_bars_sina(
            symbol, start=start, end=end, client=client, config=config
        )
    )
    probe_last = probe_last or (
        lambda symbol, client: symbol_exists(symbol, client=client, config=config)
    )
    # Init pins admitted targets before its first recovery publication. A
    # partial compact changes the lake's reference date; it must not silently
    # remove the unswept names from this already accepted obligation.
    targets = (
        delisted_recovery_targets(config, start, end)
        if recovery_targets is None
        else {symbol: dict(target) for symbol, target in recovery_targets.items()}
    )
    scope = _recovery_scope(start, end, targets)
    checkpoint = _load_recovery_checkpoint(config, scope)
    recovered_spans = dict(checkpoint.get("recovered_spans", {}))
    if recovered_spans:
        persisted = _bar_spans(config, sorted(recovered_spans))
        recovered_spans = {
            symbol: expected
            for symbol, expected in recovered_spans.items()
            if symbol in persisted
            and persisted[symbol][0] <= date.fromisoformat(expected["first_trade_date"])
            and persisted[symbol][1] >= date.fromisoformat(expected["last_trade_date"])
        }
    expected_no_data = set(checkpoint.get("expected_no_data_symbols", []))
    unresolved = set(checkpoint.get("unresolved_symbols", []))
    dedicated = {
        symbol for symbol, evidence in targets.items() if evidence["ownership"] == "dedicated_fetch"
    }
    # The first recovery pass may follow a broad source backfill (for example
    # Baostock) that already covers most catalogue terminals. Re-fetching those
    # symbols from Sina wastes provider budget and can create competing staged
    # versions. Curated positive-volume bars are sufficient evidence here when
    # their observed terminal reaches the catalogue terminal; anything shorter
    # remains on the dedicated fetch path.
    existing_spans = _bar_spans(config, sorted(dedicated))
    reused = 0
    for symbol in sorted(dedicated):
        terminal = targets[symbol].get("last_traded_date")
        observed = existing_spans.get(symbol)
        if terminal is None or observed is None:
            continue
        expected_terminal = date.fromisoformat(terminal)
        # Require the existing span to start no later than the catalogue
        # terminal as well. A reused security code can have only a newer
        # listing on disk; its later bars must not be mistaken for history of
        # the retired listing.
        if observed[0] > expected_terminal or observed[1] < expected_terminal:
            continue
        recovered_spans.setdefault(
            symbol,
            {
                "first_trade_date": observed[0].isoformat(),
                "last_trade_date": observed[1].isoformat(),
            },
        )
        expected_no_data.discard(symbol)
        reused += 1
    # A pre-window terminal probe is terminal evidence too: retrying it on
    # every run defeats the scoped checkpoint and wastes a provider request.
    todo = sorted(dedicated - set(recovered_spans) - expected_no_data)
    if not targets:
        checkpoint["status"] = "complete"
        checkpoint["completion_run_id"] = run_id
        checkpoint_path = _write_recovery_checkpoint(config, checkpoint)
        return {
            "rows_read": 0,
            "rows_written": 0,
            "scope_id": scope["scope_id"],
            "checkpoint": str(checkpoint_path),
            "coverage_pending_compact": True,
            "note": "no delisted recovery targets",
        }

    logger.info("delisted bars: %d symbol(s) to fetch from %s", len(todo), start.isoformat())
    rows_written = 0
    failed: list[str] = []
    empty_unresolved: list[str] = []
    events: list[dict] = []
    pending_frames: list[pl.DataFrame] = []
    pending_spans: dict[str, tuple[date, date]] = {}

    def flush(chunk_index: int) -> None:
        nonlocal rows_written
        if pending_frames:
            merged = pl.concat(pending_frames, how="diagonal_relaxed")
            out = write_fetched(
                config,
                run_id,
                "daily_bars",
                merged,
                source="sina",
                batch_id=f"delisted-{chunk_index:04d}",
            )
            rows_written += int(out.get("rows_written", 0))
        for symbol, (first, last) in pending_spans.items():
            recovered_spans[symbol] = {
                "first_trade_date": first.isoformat(),
                "last_trade_date": last.isoformat(),
            }
            unresolved.discard(symbol)
        checkpoint.update(
            {
                "status": "incomplete",
                "recovered_spans": recovered_spans,
                "expected_no_data_symbols": sorted(expected_no_data),
                "unresolved_symbols": sorted(unresolved),
            }
        )
        _write_recovery_checkpoint(config, checkpoint)
        pending_frames.clear()
        pending_spans.clear()

    with httpx.Client(timeout=30.0) as client:
        for index, symbol in enumerate(todo, start=1):
            try:
                bars = _run_sina_with_retry(
                    lambda symbol=symbol: fetch(symbol, client),
                    symbol=symbol,
                    operation_name="bars",
                    config=config,
                    request_managed=default_fetch,
                )
            except Exception as exc:  # noqa: BLE001 — one dead symbol must not stop the sweep
                from cnequity.orchestrator.outcomes import error_kind

                if error_kind(exc) not in {
                    "source_transient",
                    "source_unavailable",
                    "source_payload_invalid",
                    "capability_limit",
                }:
                    if error_kind(exc) != "storage_failure":
                        flush(index)
                    raise
                logger.warning("delisted bars: fetch failed for %s: %s", symbol, exc)
                failed.append(symbol)
                unresolved.add(symbol)
                checkpoint["unresolved_symbols"] = sorted(unresolved)
                _write_recovery_checkpoint(config, checkpoint)
                continue
            if bars.is_empty():
                try:
                    terminal = _run_sina_with_retry(
                        lambda symbol=symbol: probe_last(symbol, client),
                        symbol=symbol,
                        operation_name="terminal probe",
                        config=config,
                        request_managed=default_probe,
                    )
                except Exception as exc:  # noqa: BLE001 — unresolved stays retryable
                    from cnequity.orchestrator.outcomes import error_kind

                    if error_kind(exc) not in {
                        "source_transient",
                        "source_unavailable",
                        "source_payload_invalid",
                        "capability_limit",
                    }:
                        if error_kind(exc) != "storage_failure":
                            flush(index)
                        raise
                    logger.warning("delisted bars: terminal probe failed for %s: %s", symbol, exc)
                    empty_unresolved.append(symbol)
                    unresolved.add(symbol)
                    checkpoint["unresolved_symbols"] = sorted(unresolved)
                    _write_recovery_checkpoint(config, checkpoint)
                    continue
                if terminal is None or terminal >= start:
                    empty_unresolved.append(symbol)
                    unresolved.add(symbol)
                    checkpoint["unresolved_symbols"] = sorted(unresolved)
                    _write_recovery_checkpoint(config, checkpoint)
                    continue
                catalog = _read_catalog(config)
                catalog["delisted"][symbol] = terminal.isoformat()
                _write_catalog(config, catalog)
                expected_no_data.add(symbol)
                unresolved.discard(symbol)
                checkpoint["expected_no_data_symbols"] = sorted(expected_no_data)
                checkpoint["unresolved_symbols"] = sorted(unresolved)
                _write_recovery_checkpoint(config, checkpoint)
                continue

            # A provider can return a final quote-day placeholder with zero
            # volume. Keep that raw row in curated storage, but make the
            # recovery span agree with the positive-volume coverage contract;
            # otherwise receipt publication fails after compact even though
            # the terminal is a known last-quote/last-print difference.
            evidence = bars.filter(pl.col("volume") > 0) if "volume" in bars.columns else bars
            if evidence.is_empty():
                empty_unresolved.append(symbol)
                unresolved.add(symbol)
                checkpoint["unresolved_symbols"] = sorted(unresolved)
                _write_recovery_checkpoint(config, checkpoint)
                continue
            first = evidence["trade_date"].min()
            last = evidence["trade_date"].max()
            pending_frames.append(bars)
            pending_spans[symbol] = (first, last)
            events.append(
                {
                    "symbol": symbol,
                    "first_trade_date": first,
                    "last_trade_date": last,
                    **classify_ending(bars),
                }
            )
            if index % _INGEST_CHUNK == 0:
                flush(index // _INGEST_CHUNK)
                logger.info(
                    "delisted bars: %d/%d fetched (%d rows)", index, len(todo), rows_written
                )
        flush(0)

    spans = {
        symbol: (
            date.fromisoformat(value["first_trade_date"]),
            date.fromisoformat(value["last_trade_date"]),
        )
        for symbol, value in recovered_spans.items()
    }
    instruments = _instruments_rows(config, spans)
    if not instruments.is_empty():
        write_fetched(config, run_id, "instruments", instruments, source="sina")
    write_delisting_events(config, events)

    resolved = set(recovered_spans) | expected_no_data
    all_targets = set(targets)
    unresolved |= all_targets - resolved
    complete = resolved == all_targets and not unresolved
    checkpoint.update(
        {
            "status": "complete" if complete else "incomplete",
            "recovered_spans": recovered_spans,
            "expected_no_data_symbols": sorted(expected_no_data),
            "unresolved_symbols": sorted(unresolved),
        }
    )
    if complete:
        checkpoint["completion_run_id"] = run_id
    else:
        checkpoint.pop("completion_run_id", None)
    checkpoint_path = _write_recovery_checkpoint(config, checkpoint)
    if recovered_spans:
        # Compatibility only; v2 checkpoint/receipt above is the authority.
        _mark_ingested(config, sorted(recovered_spans))

    result: dict = {
        "rows_read": rows_written,
        "rows_written": rows_written,
        "scope_id": scope["scope_id"],
        "checkpoint": str(checkpoint_path),
        "symbols": len(targets),
        "recovered": len(recovered_spans),
        "reused_curated": reused,
        "to_fetch": len(todo),
        "expected_no_data": len(expected_no_data),
        "expected_no_data_symbols": sorted(expected_no_data),
        "ending_patterns": dict(Counter(e["ending_pattern"] for e in events)),
        "usable_result": bool(recovered_spans or expected_no_data or not targets),
        "coverage_status": "unknown" if complete else "partial",
    }
    if complete:
        result["coverage_pending_compact"] = True
    else:
        result["status"] = "warning" if result["usable_result"] else "failed"
        result["reason_code"] = "source_unavailable"
        result["failed_symbols"] = len(failed)
        result["empty_symbols"] = len(empty_unresolved)
        result["unresolved_symbols"] = len(unresolved)
        result.setdefault("context_updates", {})["audit_findings"] = [
            {
                "dataset": "daily_bars",
                "severity": "warning",
                "check": "delisted_backfill_incomplete",
                "message": (
                    f"{len(unresolved)}/{len(targets)} delisted targets remain unresolved "
                    f"({len(failed)} failed, {len(empty_unresolved)} empty); re-run to resume"
                ),
            }
        ]
    return result


# --- ending pattern ---------------------------------------------------------
# Whether a recovered series runs through the 退市整理期 decides whether a
# backtest realises the final loss or marks the position at its last
# pre-suspension price. On this lake that period is worth -27% to -92%.
#
# The shapes, measured over 24 recent delistings:
#   consolidation   — a halt of 14-56 days, then a -27% to -92% resumption day,
#                     then a short tail. The series is complete through the worst.
#   abrupt_decline  — no halt, ends low after a grind at the ±5% ST limit. The
#                     signature of a trading-rule delisting (面值/市值), which has
#                     no consolidation period — but a vendor series truncated at
#                     the suspension looks identical, so this bucket is the one
#                     that needs a sensitivity check before being trusted.
#   abrupt_stable   — no halt, ends at an ordinary price with a flat or positive
#                     tail: absorption/merger or a voluntary delisting.
_FINAL_WINDOW = 60
_HALT_GAP_DAYS = 10
_CONSOLIDATION_DROP = -0.25
_DECLINE_TAIL_RETURN = -0.40
_DECLINE_MAX_CLOSE = 2.0
_MIN_BARS_TO_CLASSIFY = 30


def classify_ending(bars: pl.DataFrame) -> dict:
    """Describe how a price series ends, with the evidence behind the label."""
    out = {
        "ending_pattern": "insufficient",
        "final_close": None,
        "halt_gap_days": None,
        "worst_final_return": None,
        "final_window_return": None,
        "bars": bars.height,
    }
    if bars.height < _MIN_BARS_TO_CLASSIFY:
        return out

    tail = bars.sort("trade_date").tail(_FINAL_WINDOW)
    days = tail["trade_date"].to_list()
    gap = max((days[i] - days[i - 1]).days for i in range(1, len(days)))
    rets = tail.select((pl.col("close") / pl.col("close").shift(1) - 1).alias("r")).drop_nulls()[
        "r"
    ]
    worst = float(rets.min())
    window_return = float(tail["close"][-1] / tail["close"][0] - 1)
    final_close = float(tail["close"][-1])

    if gap > _HALT_GAP_DAYS and worst < _CONSOLIDATION_DROP:
        pattern = "consolidation"
    elif window_return < _DECLINE_TAIL_RETURN and final_close < _DECLINE_MAX_CLOSE:
        pattern = "abrupt_decline"
    else:
        pattern = "abrupt_stable"

    out.update(
        ending_pattern=pattern,
        final_close=final_close,
        halt_gap_days=gap,
        worst_final_return=worst,
        final_window_return=window_return,
    )
    return out


def write_delisting_events(config: Config, events: list[dict]) -> int:
    """Merge *events* into ``derived/delisting_events`` (one row per symbol)."""
    from cnequity.domain.schemas import DELISTING_EVENTS_SCHEMA, with_provenance
    from cnequity.query.canonical import dedupe_by_primary_key
    from cnequity.storage.atomic import write_parquet_atomic

    if not events:
        return 0
    incoming = with_provenance(
        pl.DataFrame(
            events,
            schema={
                k: v
                for k, v in DELISTING_EVENTS_SCHEMA.items()
                if k not in ("source", "data_version", "fetched_at")
            },
        ),
        source="sina",
        data_version="v1",
    )
    out_dir = config.derived_root / "delisting_events"
    out_path = out_dir / "part-merged.parquet"
    frames = [incoming]
    if out_dir.exists():
        frames.extend(pl.read_parquet(path) for path in sorted(out_dir.rglob("*.parquet")))
    merged = (
        pl.concat(frames, how="diagonal_relaxed")
        .pipe(dedupe_by_primary_key, "delisting_events")
        .sort("last_trade_date", descending=True)
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(out_path, merged, compression="zstd")
    for stale in out_dir.rglob("*.parquet"):
        if stale != out_path:
            stale.unlink()
    return merged.height
