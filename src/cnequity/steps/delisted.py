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
from cnequity.domain.symbols import is_all_a_symbol, is_cdr_symbol, issued_code_space, parse_symbol
from cnequity.query.universe import coverage_end_date
from cnequity.steps.common import (
    instrument_metadata,
    is_trading_day,
    load_curated_instruments,
    load_symbols,
)

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


def _write_catalog(config: Config, payload: dict) -> None:
    path = catalog_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.stem}-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        os.replace(tmp, path)
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
        os.replace(tmp, path)
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


def _reference_date(config: Config) -> date:
    """The market's latest session, as the lake sees it."""
    from cnequity.domain.market_time import shanghai_today

    return coverage_end_date(config, "daily_bars") or shanghai_today()


def classify_catalog(config: Config) -> tuple[dict[str, date], dict[str, date]]:
    """Split the swept catalogue into (delisted, live-but-missing)."""
    raw = _read_catalog(config)["delisted"]
    reference = _reference_date(config)
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
    instruments = load_curated_instruments(config)
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


def load_delisted_catalog(config: Config) -> dict[str, date]:
    """Symbols that genuinely stopped trading -> their last trading date."""
    return classify_catalog(config)[0]


def load_live_missing(config: Config) -> dict[str, date]:
    """Symbols still trading that the lake's instrument list does not carry.

    Not a survivorship problem — a coverage hole. These need adding to the daily
    pipeline, not a historical backfill of a dead name.
    """
    return classify_catalog(config)[1]


def pending_codes(config: Config) -> list[str]:
    """Issued codes neither listed today nor already classified by a prior sweep."""
    metadata = instrument_metadata(config)
    if metadata.is_empty():
        live = set(load_symbols(config))
    else:
        reference = _reference_date(config)
        live = set(
            metadata.filter(pl.col("delist_date").is_null() | (pl.col("delist_date") > reference))[
                "symbol"
            ].to_list()
        )
    catalog = _read_catalog(config)
    done = set(catalog["delisted"]) | set(catalog["never_issued"])
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
        os.replace(tmp, path)
    except Exception:
        with suppress(OSError):
            os.unlink(tmp)
        raise


def _asset_type(symbol: str) -> str:
    from cnequity.domain.symbols import is_cdr_symbol, is_etf_symbol, parse_symbol

    info = parse_symbol(symbol)
    if is_etf_symbol(info.code, info.exchange):
        return "etf"
    if is_cdr_symbol(info.code, info.exchange):
        return "cdr"
    return "stock"


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


def delisted_symbols_in_window(config: Config, start: date) -> list[str]:
    """Catalogued delistings whose trading overlapped the lake's window.

    A name that stopped trading before the lake starts contributes nothing to a
    backtest over it, so fetching its history would be pure cost.
    """
    catalog = load_delisted_catalog(config)
    already = _ingested_symbols(config)
    return sorted(s for s, last in catalog.items() if last >= start and s not in already)


def _identity_evidence_path(config: Config) -> Path:
    return (
        config.meta_root
        / "quality"
        / "evidence"
        / _IDENTITY_CLAIM
        / f"baostock-v{_IDENTITY_EVIDENCE_VERSION}.json"
    )


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
    }
    path = _identity_evidence_path(config)
    _write_json_atomic(path, payload)
    return path


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

    Proof is by inspection, not by trust: the name must have bars, they must
    start no later than the requested window, and they must stop at or before
    the catalogued delisting. A span that runs *past* the delisting contradicts
    the catalogue and is deliberately not accepted here.
    """
    if not symbols:
        return set()
    delist_dates = _catalogued_delist_dates(config, symbols)
    spans = _bar_spans(config, sorted(symbols))
    proven: set[str] = set()
    for symbol in symbols:
        span = spans.get(symbol)
        delisted_on = delist_dates.get(symbol)
        if span is None or delisted_on is None:
            continue
        if span[0] <= start and span[1] <= delisted_on:
            proven.add(symbol)
    return proven


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
                and required <= covered
            ):
                spans = _bar_spans(config, sorted(required))
                if all(
                    symbol in spans
                    and spans[symbol][0] <= start
                    and spans[symbol][1] >= min(horizon, delist_dates.get(symbol, horizon))
                    for symbol in required
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


def _bar_spans(
    config: Config,
    symbols: list[str],
    *,
    positive_volume_only: bool = True,
    start: date | None = None,
    end: date | None = None,
) -> dict[str, tuple[date, date]]:
    """``symbol -> (first/last traded date)`` for existing daily bars."""
    if not symbols:
        return {}
    root = config.curated_root / "daily_bars"
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
    end = end or _reference_date(config)
    if start > end:
        raise ValueError("start must be on or before end")
    if sample < 0:
        raise ValueError("sample must be non-negative")
    if universe not in {"all_a", "all_a_sh_sz"}:
        raise ValueError(f"unsupported historical universe: {universe!r}")

    catalog = {
        symbol: last
        for symbol, last in load_delisted_catalog(config).items()
        if _in_historical_universe(symbol, universe)
    }
    recent = {
        symbol: terminal
        for symbol, terminal in load_live_missing(config).items()
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
    bars_root = config.curated_root / "daily_bars"
    if symbols and bars_root.exists() and any(bars_root.rglob("*.parquet")):
        from cnequity.query.parquet_scan import scan_parquet_root

        bars = scan_parquet_root(
            bars_root,
            partition_col="trade_date",
            start=start,
            end=end,
            symbols=symbols,
            traded_only=True,
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
    instruments = load_curated_instruments(config)
    if instruments is not None:
        instruments = instruments.select(["symbol", "delist_date"])
        instrument_dates = {
            row["symbol"]: row["delist_date"] for row in instruments.iter_rows(named=True)
        }

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
            finding = {"symbol": symbol, "catalog_last_traded": catalog_last.isoformat()}
            if overlap_is_definite:
                missing_bars.append(finding)
            else:
                unknown_overlap.append(finding)
                continue
        else:
            proven_overlap += 1
            if overlap_is_definite and span[1] != catalog_last:
                formal_delist = instrument_dates.get(symbol)
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
        symbol for symbol in pending_codes(config) if _in_historical_universe(symbol, universe)
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


def delisted_catalog_reconciliation_report(config: Config, *, sample: int = 15) -> dict:
    """Compare swept catalogue terminals with curated independent evidence.

    A correction is considered safe only when curated positive-volume bars
    establish a different terminal, that terminal does not exceed the formal
    instrument delisting date, and the catalogue terminal is independently
    impossible (after formal delisting or not an exchange trading day). Other
    mismatches remain visible but are never changed automatically.
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
        finding = {
            "symbol": symbol,
            "catalog_last_traded": catalog_last.isoformat(),
            "observed_last_positive_volume_bar": observed.isoformat(),
            "instrument_delist_date": formal.isoformat() if formal else None,
            "catalog_after_formal_delist": catalog_after_formal,
            "catalog_not_trading_day": catalog_not_trading,
        }
        if observed_within_identity and (catalog_after_formal or catalog_not_trading):
            finding["proposed_last_traded"] = observed.isoformat()
            finding["evidence"] = [
                reason
                for reason, present in (
                    ("catalog_after_formal_delist", catalog_after_formal),
                    ("catalog_not_trading_day", catalog_not_trading),
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


def reconcile_delisted_catalog(config: Config, *, sample: int = 15) -> dict:
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

        report = delisted_catalog_reconciliation_report(config, sample=sample)
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
    ``cne delisted backfill`` only fetches the true gaps.
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
    targets = delisted_recovery_targets(config, start, end)
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
        "ending_patterns": dict(Counter(e["ending_pattern"] for e in events)),
    }
    if complete:
        result["coverage_pending_compact"] = True
    else:
        result["status"] = "warning"
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
