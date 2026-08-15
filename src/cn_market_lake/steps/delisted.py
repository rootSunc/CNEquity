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
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.domain.symbols import issued_code_space
from cn_market_lake.steps.common import instrument_metadata, is_trading_day, load_symbols

logger = logging.getLogger(__name__)

_CATALOG_FILE = "delisted_catalog.json"
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


def _reference_date(config: Config) -> date:
    """The market's latest session, as the lake sees it."""
    from cn_market_lake.query.parquet_scan import list_partitions

    parts = list_partitions(config.curated_root / "daily_bars", "trade_date")
    return parts[-1].end if parts else date.today()


def classify_catalog(config: Config) -> tuple[dict[str, date], dict[str, date]]:
    """Split the swept catalogue into (delisted, live-but-missing)."""
    raw = _read_catalog(config)["delisted"]
    cutoff = _reference_date(config) - timedelta(days=LIVE_RECENCY_DAYS)
    delisted: dict[str, date] = {}
    live: dict[str, date] = {}
    for sym, value in raw.items():
        last = date.fromisoformat(value)
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
    from cn_market_lake.adapters.sina.bars import symbol_exists

    probe = probe or (lambda sym, client: symbol_exists(sym, client=client))
    todo = pending_codes(config)
    if limit is not None:
        todo = todo[:limit]

    catalog = _read_catalog(config)
    result = DiscoveryResult()
    logger.info("delisted discovery: %d code(s) to probe", len(todo))

    with httpx.Client(timeout=20.0) as client:
        for index, symbol in enumerate(todo, start=1):
            config.rate_limit("sina")
            try:
                last_seen = probe(symbol, client)
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
    from cn_market_lake.domain.symbols import is_cdr_symbol, is_etf_symbol, parse_symbol

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
        "schema_version": 1,
        "claim": _RECOVERY_CLAIM,
        "status": "complete",
        "scope": {key: value for key, value in scope.items() if key != "targets"},
        "recovered_symbols": sorted(recovered),
        "expected_no_data_symbols": sorted(no_data),
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


def delisted_recovery_covers(
    config: Config,
    start: date,
    end: date,
    symbols: list[str],
) -> bool:
    """Whether a complete receipt proves dedicated recovery for *symbols*."""
    required = set(symbols)
    if not required:
        return True
    root = config.meta_root / "quality" / "coverage" / _RECOVERY_CLAIM
    if not root.exists():
        return False
    for path in root.glob("*.json"):
        try:
            receipt = json.loads(path.read_text(encoding="utf-8"))
            scope = receipt["scope"]
            covered = set(receipt.get("recovered_symbols", []))
            if (
                receipt.get("status") == "complete"
                and scope.get("evidence_version") == _RECOVERY_EVIDENCE_VERSION
                and date.fromisoformat(scope["start"]) <= start
                and date.fromisoformat(scope["end"]) >= end
                and required <= covered
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
    from cn_market_lake.domain.schemas import INSTRUMENTS_SCHEMA

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
    live_path = config.curated_root / "instruments" / "part-merged.parquet"
    if not live_path.exists():
        return recovered
    live = pl.read_parquet(live_path).drop(["source", "data_version", "fetched_at"], strict=False)
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
    positive_volume_only: bool = False,
) -> dict[str, tuple[date, date]]:
    """``symbol -> (first_bar, last_bar)`` for symbols that already have daily_bars."""
    if not symbols:
        return {}
    root = config.curated_root / "daily_bars"
    if not root.exists() or not any(root.rglob("*.parquet")):
        return {}
    from cn_market_lake.query.parquet_scan import parquet_glob

    bars = pl.scan_parquet(parquet_glob(root)).filter(pl.col("symbol").is_in(symbols))
    if positive_volume_only and "volume" in bars.collect_schema().names():
        bars = bars.filter(pl.col("volume") > 0)
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

    catalog = load_delisted_catalog(config)
    candidates = {symbol: last for symbol, last in catalog.items() if last >= start}
    formal = known_delisted_instruments(config, end)
    formal_in_window = {symbol: value for symbol, value in formal.items() if value >= start}
    symbols = sorted(set(candidates) | set(formal_in_window))

    spans: dict[str, tuple[date, date]] = {}
    bars_root = config.curated_root / "daily_bars"
    if symbols and bars_root.exists() and any(bars_root.rglob("*.parquet")):
        from cn_market_lake.query.parquet_scan import scan_parquet_root

        bars = scan_parquet_root(
            bars_root,
            partition_col="trade_date",
            start=start,
            end=end,
            symbols=symbols,
        )
        # EastMoney and baostock may retain suspended/formally-delisting rows
        # with zero volume after the final actual trade. The catalogue records
        # the last *traded* session, so those placeholders must not move the
        # terminal date. Older/minimal lakes without volume retain the best
        # evidence they have instead of failing schema discovery.
        if "volume" in bars.collect_schema().names():
            bars = bars.filter(pl.col("volume") > 0)
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
    instruments_path = config.curated_root / "instruments" / "part-merged.parquet"
    if instruments_path.exists():
        instruments = pl.read_parquet(instruments_path, columns=["symbol", "delist_date"])
        instrument_dates = {
            row["symbol"]: row["delist_date"] for row in instruments.iter_rows(named=True)
        }

    missing_bars: list[dict] = []
    unknown_overlap: list[dict] = []
    terminal_mismatches: list[dict] = []
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
        elif instrument_dates[symbol] is None or instrument_dates[symbol] < catalog_last:
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
    }
    recent = load_live_missing(config)
    recent_quarantined = [
        {"symbol": symbol, "probe_last_traded": terminal.isoformat()}
        for symbol, terminal in sorted(recent.items())
        if terminal >= start and symbol not in formal_in_window
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

    pending = pending_codes(config)
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
    instruments_path = config.curated_root / "instruments" / "part-merged.parquet"
    if instruments_path.exists():
        instruments = pl.read_parquet(instruments_path, columns=["symbol", "delist_date"])
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
    from cn_market_lake.orchestrator.manifest import Manifest
    from cn_market_lake.orchestrator.run_lock import run_lock

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
    from cn_market_lake.domain.symbols import is_subscription_placeholder

    if df.is_empty() or "name" not in df.columns:
        return df
    keep = [not is_subscription_placeholder(n) for n in df["name"].to_list()]
    return df.filter(pl.Series(keep))


def purge_subscription_placeholders(config: Config) -> int:
    """Remove ``认购款`` stubs from curated instruments. Returns rows dropped."""
    from cn_market_lake.storage.atomic import write_parquet_atomic

    path = config.curated_root / "instruments" / "part-merged.parquet"
    if not path.exists():
        return 0
    existing = pl.read_parquet(path)
    cleaned = _strip_subscription_placeholders(existing)
    dropped = existing.height - cleaned.height
    if dropped:
        write_parquet_atomic(path, cleaned, compression="zstd")
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
    ``cml delisted backfill`` only fetches the true gaps.
    """
    from cn_market_lake.quality.cross_checks import RETIRED_GAP_DAYS
    from cn_market_lake.query.parquet_scan import list_partitions
    from cn_market_lake.steps.http_common import write_fetched

    catalog = load_delisted_catalog(config)
    if start is not None:
        catalog = {s: last for s, last in catalog.items() if last >= start}

    # Orphan bars: series that ended well before the lake's last session but
    # carry no delist_date (or no instruments row at all). Same structural
    # tell the survivorship audit uses.
    retired_orphans: list[str] = []
    bars_root = config.curated_root / "daily_bars"
    parts = list_partitions(bars_root, "trade_date") if bars_root.exists() else []
    if parts:
        lake_last = parts[-1].end
        cutoff = lake_last - timedelta(days=RETIRED_GAP_DAYS)
        from cn_market_lake.query.parquet_scan import parquet_glob

        last_bars = (
            pl.scan_parquet(parquet_glob(bars_root))
            .group_by("symbol")
            .agg(pl.col("trade_date").max().alias("last_bar"))
            .filter(pl.col("last_bar") < cutoff)
            .collect()
        )
        inst_path = config.curated_root / "instruments" / "part-merged.parquet"
        marked: set[str] = set()
        if inst_path.exists():
            inst = pl.read_parquet(inst_path)
            marked = set(inst.filter(pl.col("delist_date").is_not_null())["symbol"].to_list())
        retired_orphans = [s for s in last_bars["symbol"].to_list() if s not in marked]

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

    return {
        "rows_read": rows_written,
        "rows_written": rows_written,
        "targets": len(targets),
        "from_catalog": len(catalog),
        "from_orphan_bars": len(retired_orphans),
        "with_bars": len(spans),
        "instruments_spans": len(instrument_spans),
        "marked_ingested": len(already_haved),
        "purged_placeholders": purged,
        "still_need_bars": sorted(s for s in catalog if s not in spans),
    }


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
    complete receipt is published only when every target has one of the first
    two states. The legacy global ``delisted_ingested`` marker is not completion
    authority because it was not window-scoped and marked empty fetches done.
    """
    from cn_market_lake.adapters.sina.bars import fetch_daily_bars_sina, symbol_exists
    from cn_market_lake.steps.http_common import write_fetched

    end = end or _reference_date(config)
    if start > end:
        raise ValueError("delisted recovery start must be on or before end")
    fetch = fetch or (
        lambda symbol, client: fetch_daily_bars_sina(symbol, start=start, end=end, client=client)
    )
    probe_last = probe_last or (lambda symbol, client: symbol_exists(symbol, client=client))
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
    todo = sorted(dedicated - set(recovered_spans))
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
            config.rate_limit("sina")
            try:
                bars = fetch(symbol, client)
            except Exception as exc:  # noqa: BLE001 — one dead symbol must not stop the sweep
                logger.warning("delisted bars: fetch failed for %s: %s", symbol, exc)
                failed.append(symbol)
                unresolved.add(symbol)
                checkpoint["unresolved_symbols"] = sorted(unresolved)
                _write_recovery_checkpoint(config, checkpoint)
                continue
            if bars.is_empty():
                config.rate_limit("sina")
                try:
                    terminal = probe_last(symbol, client)
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

            first = bars["trade_date"].min()
            last = bars["trade_date"].max()
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
    from cn_market_lake.domain.schemas import DELISTING_EVENTS_SCHEMA, with_provenance
    from cn_market_lake.storage.atomic import write_parquet_atomic

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
    out_path = config.derived_root / "delisting_events" / "part-merged.parquet"
    frames = [incoming]
    if out_path.exists():
        frames.append(pl.read_parquet(out_path))
    merged = (
        pl.concat(frames, how="diagonal_relaxed")
        .sort("fetched_at")
        .unique(subset=["symbol"], keep="last")
        .sort("last_trade_date", descending=True)
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(out_path, merged, compression="zstd")
    return merged.height
