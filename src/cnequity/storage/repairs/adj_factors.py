"""Unified, bounded factor repair using cached evidence and retained revisions."""

from __future__ import annotations

import hashlib
import json
import sys
from contextlib import nullcontext, redirect_stdout
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import polars as pl

from cnequity.config import Config
from cnequity.derive.factor_arbitration import (
    _baostock_factors,
    _cached_batches,
    arbitrate_factor_sources,
    baostock_factor_symbol,
    source_overrides,
)
from cnequity.derive.fund_factors import apply_fund_share_factors
from cnequity.derive.stock_factors import apply_stock_event_factors
from cnequity.domain.contracts import contract_fingerprint, dataset_contract
from cnequity.domain.schemas import with_provenance
from cnequity.file_lock import lake_mutation_lock
from cnequity.quality.cross_checks import factor_action_contradictions
from cnequity.quality.publication import check_repair_publication
from cnequity.query.canonical import dedupe_by_primary_key
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.instrument_catalog import load_curated_instruments
from cnequity.storage.parquet import StagingWriter, compact_dataset
from cnequity.storage.read_context import ReadContext
from cnequity.storage.revisions import RevisionStore, sha256_file

DEPENDENCIES = (
    "daily_bars",
    "instruments",
    "corporate_actions",
    "adj_factors",
    "share_structure",
)
KEYS = ["symbol", "trade_date", "adjust_type"]
ALGORITHM = "factor-repair-v1"


def _scan(context, dataset, column, symbols=None):
    root = context.roots[dataset]
    if not dataset_has_parquet(root, committed=False):
        return pl.DataFrame()
    return dedupe_by_primary_key(
        scan_parquet_root(root, partition_col=column, symbols=symbols, committed=False).collect(),
        dataset,
    )


def _identities(config, context, symbols):
    identities = {}
    for name, root in context.roots.items():
        # Retained generations are immutable. Legacy paths have no such
        # guarantee, so bind every file, not just its size or timestamp.
        store = RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=False
        )
        current = store.current_root(name)
        identities[name] = (
            str(root)
            if current == root
            else {
                p.relative_to(root).as_posix(): sha256_file(p)
                for p in sorted(root.rglob("*.parquet"))
            }
        )
    for symbol in symbols:
        path = config.meta_root / "adj_factors_cache" / f"{symbol.replace('.', '_')}_hfq.parquet"
        identities[f"raw:{symbol}"] = sha256_file(path) if path.exists() else None
    folder = config.meta_root / "adj_factor_arbitration_cache"
    identities["peer_cache"] = {
        p.name: sha256_file(p) for p in sorted(folder.glob("*")) if p.is_file()
    }
    return identities


def _requested_symbols(config, context, explicit):
    if explicit is not None:
        return sorted(set(explicit))
    contradictions, _ = factor_action_contradictions(config)
    wanted = set(contradictions["symbol"].to_list())
    from cnequity.derive.adj_factors import _uncovered_symbols

    wanted.update(_uncovered_symbols(config))
    instruments = load_curated_instruments(config, context)
    if instruments is not None and "asset_type" in instruments.columns:
        funds = instruments.filter(pl.col("asset_type").is_in(["etf", "lof"]))["symbol"].to_list()
        actions = _scan(context, "corporate_actions", "ex_date", funds) if funds else pl.DataFrame()
        if not actions.is_empty():
            wanted.update(
                actions.filter(pl.col("action_type") != "cash_dividend")["symbol"].to_list()
            )
    # Explicit native-unavailable symbols remain inspectable, but do not make
    # every normal historical pass retry all retired securities.
    return sorted(wanted)


def _chunk(config, context, wanted, explicit, limit):
    path = config.meta_root / "quality" / "factor_repairs" / "cursor.json"
    roots = {n: str(r) for n, r in context.roots.items() if n != "adj_factors"}
    try:
        cursor = json.loads(path.read_text()) if path.exists() else {}
    except (OSError, ValueError):
        cursor = {}
    offset = (
        max(0, int(cursor.get("offset", 0)))
        if explicit is None and cursor.get("roots") == roots and cursor.get("wanted") == wanted
        else 0
    )
    if offset >= len(wanted):
        offset = 0
    return wanted[offset : offset + limit], offset, roots, path


def _diagnose(config):
    from cnequity.quality.factor_diagnostics import publish_factor_diagnostics

    try:
        return publish_factor_diagnostics(config)
    except Exception as exc:
        # Publication has already committed. A failed optional observation is
        # not a failed transaction and must not trigger a numerical retry.
        return {"assessed": False, "reason": f"diagnosis unavailable: {type(exc).__name__}"}


def _restore_coverage(config, before, bars, peer):
    """Fill stored-key gaps from the original source; never mix vendor scales."""
    restored, observations = [], []
    traded = bars.filter(pl.col("volume") > 0) if "volume" in bars.columns else bars
    for symbol in traded["symbol"].unique().sort().to_list():
        keys = traded.filter(pl.col("symbol") == symbol).select("symbol", "trade_date").unique()
        present = before.filter(pl.col("symbol") == symbol)
        if not present.is_empty():
            # New sessions belong to daily derivation. An old native cache
            # does not prove that no new event occurred after the stored tip.
            keys = keys.filter(pl.col("trade_date") <= present["trade_date"].max())
        missing = keys.join(
            present.select("symbol", "trade_date"), on=["symbol", "trade_date"], how="anti"
        )
        if missing.is_empty():
            continue
        native = config.meta_root / "adj_factors_cache" / f"{symbol.replace('.', '_')}_hfq.parquet"
        source = "sina"
        if native.exists() and (
            present.is_empty() or set(present["source"].drop_nulls()) == {"sina"}
        ):
            raw = pl.read_parquet(native).select("trade_date", "factor")
            if present.is_empty() and not raw.is_empty():
                missing = missing.filter(pl.col("trade_date") <= raw["trade_date"].max())
        elif present.is_empty() and not peer.is_empty() and baostock_factor_symbol(symbol):
            raw = peer.filter(pl.col("symbol") == symbol).select("trade_date", "factor")
            source = "baostock"
        else:
            observations.append(
                {
                    "symbol": symbol,
                    "reason": "missing coverage requires original same-source factors",
                    "missing_rows": missing.height,
                }
            )
            continue
        if (
            raw.is_empty()
            or raw.filter(
                pl.col("factor").is_null() | ~pl.col("factor").is_finite() | (pl.col("factor") <= 0)
            ).height
        ):
            observations.append(
                {
                    "symbol": symbol,
                    "reason": "native factor coverage unavailable",
                    "missing_rows": missing.height,
                }
            )
            continue
        if not present.is_empty():
            controls = (
                present.select("trade_date", pl.col("factor").alias("stored"))
                .sort("trade_date")
                .join_asof(raw.sort("trade_date"), on="trade_date", strategy="backward")
            )
            if controls.filter(
                pl.col("factor").is_null()
                | ((pl.col("factor") / pl.col("stored") - 1).abs() > 1e-12)
            ).height:
                observations.append(
                    {
                        "symbol": symbol,
                        "reason": "original source changed; coverage cannot be spliced",
                        "missing_rows": missing.height,
                    }
                )
                continue
        fill = (
            missing.sort("trade_date")
            .join_asof(raw.sort("trade_date"), on="trade_date", strategy="backward")
            .drop_nulls("factor")
            .with_columns(pl.lit("hfq").alias("adjust_type"), pl.lit(source).alias("source"))
        )
        restored.append(fill)
        observations.append(
            {
                "symbol": symbol,
                "restored_rows": fill.height,
                "missing_rows": missing.height - fill.height,
                "source": source,
            }
        )
    return (
        pl.concat([before, *restored], how="diagonal_relaxed") if restored else before
    ), observations


def repair_adj_factors(
    config: Config,
    *,
    apply: bool = False,
    refresh_sources: bool = False,
    symbols: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    max_symbols: int = 100,
    plan_id: str | None = None,
    fetch=None,
) -> dict:
    """Preview offline; refresh explicitly; recompute and publish under a lock.

    A supplied plan id requires the same inputs, scope and proposed rows. No
    source override changes before the factor revision's commit marker.
    """
    if max_symbols < 1 or symbols == [] or (start and end and start > end):
        raise ValueError("invalid factor repair scope")
    requested_context = None
    requested = None
    if refresh_sources:
        context = ReadContext.capture(config, DEPENDENCIES, {})
        requested_context = context
        requested = _requested_symbols(config, context, symbols)
        names, _, _, _ = _chunk(config, context, requested, symbols, max_symbols)
        bars = _scan(context, "daily_bars", "trade_date", names)
        if not bars.is_empty():
            from cnequity.adapters.baostock.adj_factors import (
                fetch_adjust_factor_events_baostock_many,
            )

            eligible = [s for s in names if baostock_factor_symbol(s)]
            if eligible:
                try:
                    # The vendor SDK prints login/logout messages. Keep the
                    # command's stdout usable as one JSON report.
                    with redirect_stdout(sys.stderr):
                        _baostock_factors(
                            config,
                            eligible,
                            bars["trade_date"].max(),
                            fetch or fetch_adjust_factor_events_baostock_many,
                        )
                except Exception:
                    # Failed/partial fetches never become native evidence.
                    # Continue using complete cached evidence for other names.
                    import logging

                    logging.getLogger(__name__).warning(
                        "independent factor refresh unavailable", exc_info=True
                    )
    with lake_mutation_lock(config.meta_root) if apply else nullcontext():
        store = RevisionStore(
            config.meta_root, config.curated_root, config.derived_root, create=apply
        )
        context = ReadContext.capture(config, DEPENDENCIES, {})
        wanted = (
            requested
            if requested_context is not None and context.roots == requested_context.roots
            else _requested_symbols(config, context, symbols)
        )
        # Continue a repeated bounded pass after the last fully considered
        # chunk. Changed inputs start a new pass; explicit scopes do not rotate.
        names, offset, roots, cursor_path = _chunk(config, context, wanted, symbols, max_symbols)
        report = {
            "applied": False,
            "rows": 0,
            "symbols": 0,
            "algorithm": ALGORITHM,
            "gate_impact": "none",
            "scope": names,
            "deferred_symbols": max(0, len(wanted) - offset - len(names)),
            "start": str(start) if start else None,
            "end": str(end) if end else None,
        }
        if not names:
            return report
        identities = _identities(config, context, names)
        before = _scan(context, "adj_factors", "trade_date", names)
        bars = _scan(context, "daily_bars", "trade_date", names)
        if bars.is_empty():
            report["reason"] = "no daily bars in scope"
            return report
        if before.is_empty():
            before = pl.DataFrame(
                schema={
                    "symbol": pl.String,
                    "trade_date": pl.Date,
                    "adjust_type": pl.String,
                    "factor": pl.Float64,
                    "source": pl.String,
                }
            )
        before = before.filter(pl.col("adjust_type") == "hfq")
        peer_frames, _ = _cached_batches(config, bars["trade_date"].max())
        peer = pl.concat(peer_frames) if peer_frames else pl.DataFrame()
        covered, coverage_events = _restore_coverage(config, before, bars, peer)
        arbitration = arbitrate_factor_sources(
            config, symbols=names, refresh_sources=False, save_evidence=refresh_sources
        )
        corrected, fund_events = apply_fund_share_factors(
            config, covered, read_context=context, start=start, end=end
        )
        corrected, stock_events = apply_stock_event_factors(
            config, corrected, read_context=context, peer=peer, start=start, end=end
        )
        report["events"] = [*coverage_events, *fund_events, *stock_events]
        report["arbitration"] = arbitration
        switched = arbitration.get("switch") or []
        if start or end:
            # Whole-source switches cannot be scoped to individual events.
            switched = []
        replacements = []
        for symbol in switched:
            raw = (
                peer.filter(pl.col("symbol") == symbol)
                .select("trade_date", "factor")
                .sort("trade_date")
            )
            rows = before.filter(pl.col("symbol") == symbol)
            aligned = (
                rows.select(*KEYS)
                .sort("trade_date")
                .join_asof(raw, on="trade_date", strategy="backward")
            )
            if (
                aligned["factor"].null_count()
                or aligned.filter(~pl.col("factor").is_finite() | (pl.col("factor") <= 0)).height
            ):
                continue
            replacements.append(aligned.with_columns(pl.lit("baostock").alias("_vendor")))
        if replacements:
            replaced = pl.concat(replacements)
            corrected = pl.concat(
                [
                    corrected.filter(
                        ~pl.col("symbol").is_in(replaced["symbol"].unique().to_list())
                    ),
                    replaced,
                ],
                how="diagonal_relaxed",
            )
        vendor = (
            pl.coalesce("_vendor", "source") if "_vendor" in corrected.columns else pl.col("source")
        )
        corrected = corrected.with_columns(vendor.alias("source"))
        changed = (
            corrected.join(
                before.select(*KEYS, pl.col("factor").alias("_before")), on=KEYS, how="left"
            )
            .filter(
                pl.col("_before").is_null()
                | (((pl.col("factor") / pl.col("_before")) - 1.0).abs() > 1e-12)
            )
            .select(*KEYS, "factor", "source")
        )
        if changed.filter(~pl.col("factor").is_finite() | (pl.col("factor") <= 0)).height:
            raise ValueError("factor repair produced invalid stored numbers")
        report.update(
            rows=changed.height,
            symbols=changed["symbol"].n_unique(),
            changed_symbols=changed["symbol"].unique().sort().to_list(),
            input_identities=identities,
        )
        serialized = changed.sort(KEYS).write_json()
        plan = {
            "algorithm": ALGORITHM,
            "inputs": identities,
            "scope": names,
            "start": report["start"],
            "end": report["end"],
            "rows_sha256": hashlib.sha256(serialized.encode()).hexdigest(),
        }
        current_plan = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
        if plan_id is not None and plan_id != current_plan:
            raise ValueError("factor repair inputs or candidate changed; refresh the preview")
        report["plan_id"] = current_plan
        folder = config.meta_root / "quality" / "factor_repairs"
        write_json_atomic(folder / "plans" / f"{current_plan}.json", report, indent=2, default=str)
        if (
            apply
            and _identities(config, ReadContext.capture(config, DEPENDENCIES, {}), names)
            != identities
        ):
            raise ValueError("factor repair inputs changed; refresh the preview")
        if not apply or changed.is_empty():
            if apply:
                report["diagnostics"] = _diagnose(config)
                if symbols is None:
                    write_json_atomic(
                        cursor_path,
                        {"roots": roots, "wanted": wanted, "offset": offset + len(names)},
                        indent=2,
                    )
            return report
        rows = with_provenance(
            changed.drop("source"), source="derived_stock_actions", data_version="v1"
        ).with_columns(changed["source"])
        run_id = f"adj-factors-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
        store.ensure_current("adj_factors")
        store.materialize_current("adj_factors")
        overrides = source_overrides(config)
        for item in replacements:
            symbol = item["symbol"][0]
            overrides[symbol] = {"source": "baostock", "plan_id": current_plan}
            from cnequity.derive.adj_factors import _save_cache

            _save_cache(
                config,
                symbol,
                "hfq_baostock",
                peer.filter(pl.col("symbol") == symbol).select("trade_date", "factor"),
            )
        for symbol in changed.filter(pl.col("source") == "baostock")["symbol"].unique().to_list():
            if before.filter(pl.col("symbol") == symbol).is_empty():
                overrides[symbol] = {"source": "baostock", "plan_id": current_plan}
                from cnequity.derive.adj_factors import _save_cache

                _save_cache(
                    config,
                    symbol,
                    "hfq_baostock",
                    peer.filter(pl.col("symbol") == symbol).select("trade_date", "factor"),
                )
        latest = store.latest("adj_factors")
        proofs = dict(latest.metadata.get("factor_event_proofs", {})) if latest is not None else {}
        for event in stock_events:
            if "correction" in event:
                proofs[f"{event['symbol']}|{event['trade_date']}"] = event
        changed_files = []
        try:
            with TemporaryDirectory(prefix="cne-adj-factors-") as directory:
                staging = Path(directory)
                StagingWriter(staging).write_batch("adj_factors", run_id, "repair-0", rows)
                compact_dataset(
                    staging,
                    config.derived_root,
                    "adj_factors",
                    run_id,
                    changed_files=changed_files,
                    base_root=context.roots["adj_factors"],
                )
            publication = check_repair_publication(
                config,
                "adj_factors",
                run_id,
                changed_files,
                symbols=frozenset(report["changed_symbols"]),
            )
            contract = dataset_contract("adj_factors")
            revision = store.commit(
                "adj_factors",
                run_id=run_id,
                changed_files=changed_files,
                schema_version=int(contract["schema_version"]),
                contract_fingerprint=contract_fingerprint(contract),
                metadata={
                    "reason": "factor_repair",
                    "factor_repair": report,
                    "publication_audit": publication["report_path"],
                    "factor_source_overrides": overrides,
                    "factor_event_proofs": proofs,
                },
            )
            if revision is None:
                raise RuntimeError("factor repair produced no revision")
        except Exception:
            store.materialize_current("adj_factors")
            raise
        report.update(
            applied=True, revision=revision.revision, publication_audit=publication["report_path"]
        )
        report["diagnostics"] = _diagnose(config)
        write_json_atomic(folder / "applied" / f"{run_id}.json", report, indent=2, default=str)
        if symbols is None:
            write_json_atomic(
                cursor_path,
                {"roots": roots, "wanted": wanted, "offset": offset + len(names)},
                indent=2,
            )
        return report
