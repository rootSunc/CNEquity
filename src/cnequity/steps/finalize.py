"""Finalize steps: compact, derive_adj_factors, derive_industry_index, audit."""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.domain.datasets import PARTITION_COLS, RESAMPLED_MINUTE_DATASETS, WATERMARK_SKIP
from cnequity.orchestrator.registry import register_step
from cnequity.progress import sweep_progress
from cnequity.storage import StagingWriter, compact_dataset
from cnequity.storage.instruments import compact_instruments
from cnequity.storage.observation import commit_observation
from cnequity.storage.state import StateStore

logger = logging.getLogger(__name__)


def _record_dataset_result(
    config: Config,
    run_id: str,
    dataset: str,
    stage: str,
    status: str,
    *,
    criticality: str,
    revision_id: str | None = None,
    rows_written: int = 0,
    error_code: str | None = None,
    error_message: str | None = None,
    coverage_status: str | None = None,
    publication_status: str | None = None,
) -> None:
    """Persist a logical dataset receipt without coupling steps to the engine."""
    from cnequity.orchestrator.manifest import Manifest

    Manifest(config.manifest_path).record_dataset_result(
        run_id,
        dataset,
        stage,
        status,
        criticality=criticality,
        revision_id=revision_id,
        rows_written=rows_written,
        error_code=error_code,
        error_message=error_message,
        coverage_status=coverage_status,
        publication_status=publication_status,
    )


def _dataset_criticality(dataset: str) -> str:
    """Classify a physical dataset for the run-level degraded policy.

    A registered dataset takes its support level's criticality, whichever
    schedule group runs it; anything else falls back to its step's group.
    """
    if dataset in {"compact", "audit"}:
        return "core"
    from cnequity.domain.datasets import dataset_criticality

    by_support = dataset_criticality(dataset)
    if by_support is not None:
        return by_support
    try:
        from cnequity.orchestrator.registry import get_step

        group = get_step(dataset).group
    except KeyError:
        group = "advisory"
    if group in {"core", "finalize"}:
        return "core"
    if group == "research":
        return "research"
    return "advisory"


def _max_partition_date(config: Config, dataset: str, partition_col: str) -> date | None:
    """Freshest date actually present, for the watermark.

    Reads the column rather than trusting the directory name: under month/year
    granularity a ``trade_date=2026`` directory says nothing about how far into
    the year the data goes, and taking the period start as the watermark would
    rewind it by up to a year and re-fetch everything since.
    """
    from cnequity.query.parquet_scan import list_partitions, partition_dir

    root = config.curated_root / dataset
    if not root.exists():
        return None

    parts = list_partitions(root, partition_col)
    files = list(root.glob("*.parquet"))
    if parts:
        # Newest period only — earlier ones cannot hold a later date.
        files.extend(
            sorted(partition_dir(root, partition_col, parts[-1].value).glob("**/*.parquet"))
        )
    elif not files:
        files = list(root.glob("**/*.parquet"))
    if not files:
        return None
    combined = pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
    if partition_col not in combined.columns:
        return None
    return combined[partition_col].max()


def _watermarked_datasets() -> list[tuple[str, str]]:
    return [
        (dataset, pcol)
        for dataset, pcol in PARTITION_COLS.items()
        if pcol is not None and dataset not in WATERMARK_SKIP
    ]


def _layer_file_identity(root: Path) -> dict[str, tuple[int, str]]:
    """Capture parquet bytes without following links for a derive COW diff."""
    from cnequity.storage.revisions import RevisionStore, sha256_file

    if not root.exists():
        return {}
    return {
        path.relative_to(root).as_posix(): (path.stat().st_size, sha256_file(path))
        for path in RevisionStore._walk_files(root)
    }


DERIVE_INPUTS = {
    "adj_factors": ("daily_bars", "corporate_actions", "instruments"),
    "industry_index": ("daily_bars", "industry_members", "adj_factors", "trading_calendar"),
    "futures_continuous": ("futures_bars", "futures_contracts"),
    "option_greeks": ("option_bars", "futures_bars", "option_contracts", "macro_indicators"),
    **{name: ("minute_bars", "minute_bars_5m") for name in RESAMPLED_MINUTE_DATASETS},
}


def _capture_derive_inputs(config: Config, dataset: str) -> dict:
    """Record immutable input identities and the available coverage evidence."""
    from cnequity.storage.revisions import RevisionStore

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    state = StateStore(config.meta_root)
    inputs = {}
    for name in DERIVE_INPUTS.get(dataset, ()):
        store.ensure_current(name)
        pointer = store.current_pointer(name) or {}
        gaps = state.get_payload(name)
        metadata = {}
        if pointer.get("receipt"):
            metadata = json.loads((config.meta_root / pointer["receipt"]).read_text()).get(
                "metadata", {}
            )
        inputs[name] = {
            key: pointer.get(key) for key in ("revision", "revision_id", "content_digest")
        }
        inputs[name]["coverage_status"] = (
            "partial"
            if any(gaps.get(key) for key in ("missing_ranges", "missing_units", "outstanding_keys"))
            or metadata.get("coverage_status") == "partial"
            or metadata.get("coverage", {}).get("status") == "partial"
            else "unknown"
        )
    return inputs


def _publish_derived_revision(
    config: Config,
    dataset: str,
    run_id: str,
    trade_date: date,
    before: dict[str, tuple[int, str]],
    *,
    input_revisions: dict | None = None,
    coverage_status: str = "unknown",
    factor_findings: list[dict] | None = None,
) -> dict | None:
    """Publish one immutable COW generation for a derived dataset."""
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.storage.revisions import RevisionStore

    root = config.derived_root / dataset
    after = _layer_file_identity(root)
    changed = [
        root / relative for relative, identity in after.items() if before.get(relative) != identity
    ]
    if not changed:
        return None
    from cnequity.file_lock import lake_mutation_lock
    from cnequity.quality.publication import evaluate_publication

    revisions = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    with lake_mutation_lock(config.meta_root, blocking=True):
        for name, identity in (input_revisions or {}).items():
            current = revisions.current_pointer(name) or {}
            if any(
                current.get(key) != identity.get(key)
                for key in ("revision", "revision_id", "content_digest")
            ):
                from cnequity.orchestrator.outcomes import InputUnavailableError

                revisions.quarantine_candidate(
                    dataset, run_id=run_id, reason="input_revision_changed"
                )
                raise InputUnavailableError(
                    f"{dataset}: input {name} changed during calculation; re-run derive"
                )
        if any(row.get("coverage_status") == "partial" for row in (input_revisions or {}).values()):
            coverage_status = "partial"
        publication = evaluate_publication(
            config, f"{run_id}-{dataset}", trade_date, {dataset: root}, {dataset: changed}
        )
        if publication["blocked"]:
            revisions.quarantine_candidate(dataset, run_id=run_id, reason="publication_gate")
            raise RuntimeError(
                f"publication gate blocked {dataset}: {publication.get('report_path')}"
            )
        factor_metadata = {}
        if dataset == "adj_factors" and factor_findings:
            previous = revisions.latest(dataset)
            proofs = dict(previous.metadata.get("factor_event_proofs", {})) if previous else {}
            for item in factor_findings:
                if item.get("check") == "stock_event_factor" and "correction" in item:
                    proofs[f"{item['symbol']}|{item['trade_date']}"] = item
            factor_metadata["factor_event_proofs"] = proofs
        contract = dataset_contract(dataset)
        revision = revisions.commit(
            dataset,
            run_id=run_id,
            changed_files=changed,
            schema_version=int(contract["schema_version"]),
            contract_fingerprint=contract_fingerprint(contract),
            metadata={
                "trade_date": trade_date.isoformat(),
                "layer": "derived",
                "input_revisions": input_revisions or {},
                "coverage_status": coverage_status,
                "publication_audit": {
                    key: publication.get(key) for key in ("mode", "blocked", "report_path")
                },
                "changed_files_count": len(changed),
                **factor_metadata,
            },
            _locked=True,
        )
    if revision is None:
        return None
    _record_dataset_result(
        config,
        run_id,
        dataset,
        "publish_revision",
        "success",
        criticality=_dataset_criticality(dataset),
        revision_id=revision.revision_id,
        coverage_status=coverage_status,
        publication_status="partial" if coverage_status == "partial" else "published",
    )
    return {
        "revision": revision.revision,
        "revision_id": revision.revision_id,
        "content_digest": revision.content_digest,
        "changed_partitions": list(revision.changed_partitions),
        "coverage_status": coverage_status,
        "publication_status": "partial" if coverage_status == "partial" else "published",
    }


def _watermark_date_for(config: Config, dataset: str, partition_col: str) -> date | None:
    """Freshest date that is safe to claim as covered.

    For ``valuation_metrics`` a sparse tip (partial baostock refill at 20% of
    bars) must not advance the watermark — walk back to the last dense day.
    For session-dense datasets, also stop before the first interior trading-day
    hole so the next incremental window retries the missing session instead of
    starting after the raw maximum.
    """
    if dataset == "valuation_metrics":
        from cnequity.quality.cross_checks import last_dense_valuation_date

        dense = last_dense_valuation_date(config)
        if dense is not None:
            return dense
        # Once daily bars exist, an undense valuation tip is not a safe
        # watermark. Falling back to its raw maximum would let a partial
        # snapshot make the next incremental run start after an unobserved
        # valuation day. A bars-less first run has no independent universe to
        # reconcile against, so it may still establish an initial watermark.
        from cnequity.query.parquet_scan import dataset_has_parquet

        if dataset_has_parquet(config.curated_root / "daily_bars"):
            return None
        # No dense day yet — fall through to raw max so a brand-new lake can
        # still establish a watermark from its first complete EM snapshot.
    from cnequity.domain.datasets import DATASETS
    from cnequity.quality.verify import last_contiguous_dense_date

    spec = DATASETS.get(dataset)
    if spec is not None and spec.coverage_mode == "session_dense":
        if dataset in ("index_bars", "daily_bars"):
            # THS supplies legacy index history before the reliable TDX daily
            # source begins. The old history contains source/calendar holes
            # (for example 1991 Saturdays), which must remain audit findings
            # but must not pin the live incremental watermark in 2000. Delisted
            # recovery likewise lands lone 1990s stock histories (000508.SZ
            # pinned daily_bars at 1993-09-16).
            from cnequity.steps.common import BACKFILL_START

            return last_contiguous_dense_date(config, spec, start=BACKFILL_START)
        return last_contiguous_dense_date(config, spec)
    return _max_partition_date(config, dataset, partition_col)


def _update_watermarks(
    config: Config,
    datasets: frozenset[str] | None,
    trade_date: date,
) -> None:
    """Advance each compacted dataset's watermark to the freshest date it holds.

    The watermark answers "through what date do we have data", so it is read
    back from the lake rather than assumed from the run date. Snapshot datasets
    used to be stamped with ``trade_date`` unconditionally, which made the
    watermark lie whenever a snapshot produced nothing: a partial baostock
    valuation backfill writing rows for an *earlier* day still pushed the
    watermark to today, so lake_health called valuation_metrics fresh on a day
    it had zero rows, and the missed days never showed up as coverage gaps.
    Snapshot fetching only ever requests trade_date, so a truthful (possibly
    older) watermark cannot trigger a re-fetch storm — it just surfaces the hole.
    """
    state = StateStore(config.meta_root)
    from cnequity.domain.datasets import fetch_semantics

    for dataset, pcol in _watermarked_datasets():
        if datasets is not None and dataset not in datasets:
            continue
        if dataset == "daily_bars":
            from cnequity.storage.fund_no_print import reconcile_no_print_coverage

            reconcile_no_print_coverage(config)
            from cnequity.storage.coverage import (
                resolved_bj_session_dates,
                resolved_prelisting_keys,
            )

            state.clear_missing_dates(
                dataset, resolved_bj_session_dates(config, state.get_payload(dataset))
            )
            state.clear_outstanding_keys(
                dataset, resolved_prelisting_keys(config, state.get_outstanding_keys(dataset))
            )
        max_dt = _watermark_date_for(config, dataset, pcol)
        if max_dt is not None:
            state.update_max_date(dataset, max_dt)
        if fetch_semantics(dataset) == "by_date":
            observed = _max_partition_date(config, dataset, pcol)
            with state.transaction(dataset) as payload:
                missing = {
                    date.fromisoformat(row["start"]) for row in payload.get("missing_ranges", [])
                }
                missing.update(
                    date.fromisoformat(row["trade_date"])
                    for row in payload.get("outstanding_keys", [])
                )
                if observed is not None:
                    payload["observed_max"] = observed.isoformat()
                complete = max_dt
                if complete is not None and missing:
                    complete = min(complete, min(missing) - timedelta(days=1))
                if complete is not None:
                    payload["complete_through"] = complete.isoformat()
                else:
                    payload.pop("complete_through", None)
                payload["coverage_status"] = (
                    "incomplete"
                    if missing
                    or payload.get("missing_units")
                    or (observed is not None and (complete is None or observed > complete))
                    else "complete"
                )


def _reconcile_watermarks(config: Config) -> list[dict]:
    """Pull back any watermark that claims data the lake does not have.

    Advancing only covers datasets that compacted this run, so a dataset whose
    source went dark — writing nothing, therefore never compacting — would keep
    a stale-but-fresh-looking watermark forever, which is exactly the case that
    hides an outage. This runs over every watermarked dataset and only ever
    corrects downward, so it cannot mask a real advance.
    """
    state = StateStore(config.meta_root)
    from cnequity.query.parquet_scan import dataset_has_parquet

    findings: list[dict] = []
    for dataset, pcol in _watermarked_datasets():
        current = state.get_date(dataset)
        if current is None:
            continue
        max_dt = _watermark_date_for(config, dataset, pcol)
        if (
            max_dt is None
            and dataset == "valuation_metrics"
            and dataset_has_parquet(config.curated_root / "daily_bars")
        ):
            # A pre-existing watermark can itself have been written by the old
            # raw-max fallback. Once daily bars are available and no valuation
            # day clears the coverage gate, remove that unsafe claim so the
            # next run cannot skip the missing snapshot window.
            state.clear_date(dataset)
            findings.append(
                {
                    "dataset": dataset,
                    "severity": "warning",
                    "check": "valuation_watermark_coverage_gate",
                    "message": (
                        f"valuation watermark {current.isoformat()} was cleared: no day "
                        "reaches the 70% coverage gate against daily_bars"
                    ),
                    "claimed": current.isoformat(),
                    "actual": None,
                }
            )
            logger.warning(
                "%s: watermark %s cleared because no dense valuation day exists",
                dataset,
                current.isoformat(),
            )
            continue
        if max_dt is None or current <= max_dt:
            continue
        claimed = current
        state.set_date(dataset, max_dt)
        check = (
            "valuation_watermark_coverage_gate"
            if dataset == "valuation_metrics"
            else "watermark_ahead_of_data"
        )
        findings.append(
            {
                "dataset": dataset,
                "severity": "warning",
                "check": check,
                "message": (
                    f"watermark claimed {claimed.isoformat()} but the freshest "
                    f"complete {pcol} in curated is {max_dt.isoformat()}; corrected"
                    + (
                        " (coverage below 70% of daily_bars on newer tip days)"
                        if dataset == "valuation_metrics"
                        else ". The source produced nothing for the days in between"
                    )
                ),
                "claimed": claimed.isoformat(),
                "actual": max_dt.isoformat(),
            }
        )
        logger.warning(
            "%s: watermark %s ahead of complete data %s; corrected",
            dataset,
            claimed.isoformat(),
            max_dt.isoformat(),
        )
    return findings


@register_step("compact", group="finalize", parallelizable=False)
def step_compact(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    from cnequity.orchestrator.run_lock import run_lock

    # Compact does read-merge-write on shared curated partitions; overlapping
    # runs (cron group + manual run) must serialize here or lose rows.
    with run_lock(config.meta_root, "compact", blocking=True):
        return _compact_locked(config, trade_date, run_id, context)


def _compact_locked(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    from cnequity.domain.canonical import canonical_policy
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.orchestrator.compact_gate import compact_allowed, publication_files
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.storage.coverage import (
        candidate_gaps,
        gap_summary,
        resolved_outstanding_keys,
    )
    from cnequity.storage.provenance import runtime_lineage
    from cnequity.storage.revisions import RevisionStore

    manifest = Manifest(config.manifest_path)
    writer = StagingWriter(config.staging_root)
    staged = [ds for ds in PARTITION_COLS if writer.list_run_files(ds, run_id)]
    total = 0
    compacted: set[str] = set()
    committed_revisions: dict[str, dict] = {}
    partial_datasets: set[str] = set()
    coverage_by_dataset: dict[str, dict] = {}
    resolved_keys: dict[str, set[tuple[str, str]]] = {}
    fully_selected: set[str] = set()
    pending: list[tuple] = []
    skipped: list[dict] = []
    audit_findings: list[dict] = []
    revisions = RevisionStore(config.meta_root, config.curated_root)
    lineage = runtime_lineage(config)

    # Compacting a full init's staging is a read-merge-write over every
    # partition of every dataset, and it ran without a word: the last phase of
    # a run that had already taken hours was also its quietest. Reported on the
    # way into each dataset, so the count names the one currently being merged.
    report = sweep_progress(logger, "compact", len(staged), every=1, unit="datasets")

    for index, ds in enumerate(staged, start=1):
        report(index)
        allowed, incomplete_count = compact_allowed(
            manifest,
            run_id,
            ds,
            stale_after_seconds=config.batch_stale_seconds,
        )
        if not allowed:
            skipped.append(
                {
                    "dataset": ds,
                    "incomplete_batches": incomplete_count,
                }
            )
            _record_dataset_result(
                config,
                run_id,
                ds,
                "compact",
                "blocked",
                criticality=_dataset_criticality(ds),
                error_code="incomplete_batches",
                error_message=f"{incomplete_count} incomplete batch(es) block compact",
            )
            continue

        selected_files = publication_files(manifest, run_id, ds, config.staging_root)
        if not selected_files:
            continue
        full_staging = len(selected_files) == len(writer.list_run_files(ds, run_id))
        if full_staging:
            fully_selected.add(ds)
        receipts = [r for r in manifest.get_dataset_results(run_id) if r["dataset"] == ds]
        dataset_coverage = {r["coverage_status"] for r in receipts} - {"not_applicable"}
        gaps = StateStore(config.meta_root).get_payload(ds)
        if (
            incomplete_count
            or "partial" in dataset_coverage
            or any(gaps.get(k) for k in ("missing_ranges", "missing_units", "outstanding_keys"))
            or len(selected_files) < len(writer.list_run_files(ds, run_id))
        ):
            partial_datasets.add(ds)

        pcol = PARTITION_COLS[ds]
        changed_files: list[Path] = []
        change_log: list[dict] = []
        # Establish revision zero before touching the legacy-compatible
        # curated path, then always merge against the immutable committed
        # generation. A previous process may have died after replacing only
        # some mutable partitions; it must never become the next compact's
        # implicit base.
        committed_root = revisions.ensure_current(ds)
        # Candidate audits must see the same base that commit will publish.
        # Discard only the disposable compatibility copy of an interrupted run.
        revisions.materialize_current(ds)
        if ds == "instruments":
            rows, inst_findings = compact_instruments(
                config.staging_root,
                config.curated_root,
                run_id,
                trade_date,
                changed_files=changed_files,
                base_root=committed_root,
                staging_files=selected_files,
                allow_delist_inference="partial" not in dataset_coverage,
            )
            if rows:
                compacted.add(ds)
            total += rows
            if inst_findings:
                audit_findings.extend(inst_findings)
            if changed_files:
                from cnequity.query.parquet_scan import collect_parquet_root
                from cnequity.storage.changes import summarize_changes

                try:
                    previous = collect_parquet_root(committed_root, hive=False, committed=False)
                except FileNotFoundError:
                    previous = pl.DataFrame()
                current = collect_parquet_root(
                    config.curated_root / ds, hive=False, committed=False
                )
                change_log.append({"partition": None, **summarize_changes(previous, current, ds)})
        else:
            if ds == "daily_bars":
                # Rules that need the previous close or the corporate actions
                # can only be checked where staged rows meet the committed lake.
                from cnequity.storage.staged_contracts import enforce_daily_bar_contracts

                staged_action_files = publication_files(
                    manifest, run_id, "corporate_actions", config.staging_root
                )
                staged_status_files = publication_files(
                    manifest, run_id, "trading_status", config.staging_root
                )
                audit_findings.extend(
                    enforce_daily_bar_contracts(
                        config,
                        run_id,
                        selected_files,
                        committed_root=committed_root,
                        actions_root=revisions.current_root("corporate_actions"),
                        instruments_root=revisions.current_root("instruments"),
                        status_root=revisions.current_root("trading_status"),
                        staged_status=(
                            pl.concat(
                                [
                                    pl.read_parquet(f).select(
                                        "symbol", "trade_date", "status", "source"
                                    )
                                    for f in staged_status_files
                                ],
                                how="vertical_relaxed",
                            )
                            if staged_status_files
                            else None
                        ),
                        staged_actions=(
                            pl.concat(
                                [
                                    pl.read_parquet(f).select("symbol", "ex_date")
                                    for f in staged_action_files
                                ],
                                how="vertical_relaxed",
                            )
                            if staged_action_files
                            else None
                        ),
                    )
                )
                selected_files = [f for f in selected_files if f.exists()]
                if any(finding.get("rows_rejected") for finding in audit_findings):
                    partial_datasets.add(ds)
                if not selected_files:
                    continue
            rows = compact_dataset(
                config.staging_root,
                config.curated_root,
                ds,
                run_id,
                partition_col=pcol,
                changed_files=changed_files,
                base_root=committed_root,
                change_log=change_log,
                staging_files=selected_files,
            )
            if rows:
                compacted.add(ds)
            total += rows
            if ds == "daily_bars" and selected_files:
                from cnequity.research.preview import retire_preview_symbols

                try:
                    published_symbols = (
                        pl.concat(
                            [
                                pl.read_parquet(path, columns=["symbol"])
                                for path in selected_files
                                if path.exists()
                            ],
                            how="vertical_relaxed",
                        )
                        .get_column("symbol")
                        .unique()
                        .to_list()
                    )
                except (OSError, ValueError, pl.exceptions.PolarsError):
                    published_symbols = []
                retire_preview_symbols(config, [str(symbol) for symbol in published_symbols])

        _record_dataset_result(
            config,
            run_id,
            ds,
            "compact",
            "success",
            criticality=_dataset_criticality(ds),
            rows_written=rows,
        )

        payload = StateStore(config.meta_root).get_payload(ds)
        resolved_keys[ds] = resolved_outstanding_keys(
            ds, config.curated_root / ds, payload.get("outstanding_keys", []), committed=False
        )
        remaining = candidate_gaps(payload, run_id, resolved_keys[ds], full_staging=full_staging)
        is_partial = (
            incomplete_count > 0
            or not full_staging
            or "partial" in dataset_coverage
            or any(remaining.values())
            or any(f.get("dataset") == ds and f.get("rows_rejected") for f in audit_findings)
        )
        if is_partial:
            partial_datasets.add(ds)
        else:
            partial_datasets.discard(ds)
        coverage_by_dataset[ds] = {
            "status": "partial"
            if is_partial
            else "complete"
            if "complete" in dataset_coverage
            else "unknown",
            "requested_scope": manifest.get_run_metadata(run_id).get("backfill_scope")
            or {
                "start": manifest.get_run_metadata(run_id).get("history_start"),
                "end": trade_date.isoformat(),
                "universe": config.ingest_universe,
            },
            "gaps": gap_summary(remaining),
            "gaps_source": "dataset_state",
            "resolved_outstanding_keys": len(resolved_keys[ds]),
        }

        if changed_files:
            # Run the optional independent-source gate against the complete
            # mutable candidate before publishing its immutable generation.
            # ``diff_dataset`` normally resolves current.json, so pass the
            # candidate root explicitly here; otherwise it would compare the
            # previous committed day and allow a bad candidate through.
            gate_spec = next(
                (
                    spec
                    for spec in config.failover_datasets
                    if spec.name == ds and spec.revision_gate
                ),
                None,
            )
            if gate_spec is not None:
                from cnequity.quality.source_diff import (
                    diff_dataset,
                    source_diff_blocks_revision,
                )

                gate_findings = diff_dataset(
                    config,
                    gate_spec,
                    trade_date=trade_date,
                    candidate_root=config.curated_root / ds,
                )
                audit_findings.extend(gate_findings)
                if source_diff_blocks_revision(gate_findings):
                    quarantine = revisions.quarantine_candidate(
                        ds,
                        run_id=run_id,
                        reason="source_diff_gate",
                    )
                    compacted.discard(ds)
                    skipped.append(
                        {
                            "dataset": ds,
                            "reason": "source_diff_gate",
                            "findings": gate_findings,
                            "quarantine": str(quarantine) if quarantine else None,
                        }
                    )
                    _record_dataset_result(
                        config,
                        run_id,
                        ds,
                        "publish_revision",
                        "blocked",
                        criticality=_dataset_criticality(ds),
                        rows_written=rows,
                        error_code="source_diff_gate",
                        error_message="independent source drift exceeds configured tolerance",
                    )
                    continue
            pending.append((ds, pcol, rows, changed_files, change_log, gate_spec))

    from cnequity.quality.publication import evaluate_publication

    publication = evaluate_publication(
        config,
        run_id,
        trade_date,
        {item[0]: config.curated_root / item[0] for item in pending},
        {item[0]: item[3] for item in pending},
    )
    blocked_datasets = set(publication.get("blocked_datasets") or [])
    if publication["blocked"]:
        for ds, _pcol, rows, *_ in pending:
            if ds not in blocked_datasets:
                continue
            quarantine = revisions.quarantine_candidate(
                ds, run_id=run_id, reason="publication_gate"
            )
            compacted.discard(ds)
            skipped.append(
                {
                    "dataset": ds,
                    "reason": "publication_gate",
                    "quarantine": str(quarantine) if quarantine else None,
                }
            )
            _record_dataset_result(
                config,
                run_id,
                ds,
                "publish_revision",
                "blocked",
                criticality=_dataset_criticality(ds),
                rows_written=rows,
                error_code="publication_gate",
                error_message="candidate audit introduced errors; see publication report",
            )
        audit_findings.extend(publication["new_errors"])
        pending = [item for item in pending if item[0] not in blocked_datasets]
    for ds, pcol, rows, changed_files, change_log, gate_spec in pending:
        try:
            contract = dataset_contract(ds)
            revision = revisions.commit(
                ds,
                run_id=run_id,
                changed_files=changed_files,
                schema_version=int(contract["schema_version"]),
                contract_fingerprint=contract_fingerprint(contract),
                metadata={
                    "trade_date": trade_date.isoformat(),
                    "partition_col": pcol,
                    "rows_written": rows,
                    "canonical_policy": canonical_policy(ds),
                    "changes": change_log,
                    "publication_audit": {
                        "mode": publication.get("mode"),
                        "blocked": False,
                        "report_path": publication.get("report_path"),
                    },
                    "validation": {
                        "schema": "passed",
                        "staging_seals": "passed"
                        if all(
                            writer.is_sealed(f, ds, run_id)
                            for f in publication_files(manifest, run_id, ds, config.staging_root)
                        )
                        else "legacy_batch_gate",
                        "batch_completeness": "partial" if ds in partial_datasets else "passed",
                        "source_diff_gate": "passed" if gate_spec is not None else "not_configured",
                    },
                    "coverage": coverage_by_dataset[ds],
                    **lineage,
                },
                # ``_compact_locked`` already holds the shared compact
                # run lock, whose path is intentionally the same as the
                # low-level lake mutation lock.  Re-entering it would
                # deadlock on POSIX; direct RevisionStore callers still
                # acquire the lock through the default path.
                _locked=True,
            )
        except Exception as exc:
            _record_dataset_result(
                config,
                run_id,
                ds,
                "publish_revision",
                "failed",
                criticality=_dataset_criticality(ds),
                rows_written=rows,
                error_code=type(exc).__name__,
                error_message=str(exc),
            )
            raise
        if ds in {"futures_bars", "option_bars"}:
            from cnequity.storage.derivative_evidence import confirm_receipts

            confirm_receipts(config, ds)
        if revision is not None:
            committed_revisions[ds] = {
                "revision": revision.revision,
                "revision_id": revision.revision_id,
                "content_digest": revision.content_digest,
                "changed_partitions": list(revision.changed_partitions),
            }
            _record_dataset_result(
                config,
                run_id,
                ds,
                "publish_revision",
                "success",
                criticality=_dataset_criticality(ds),
                revision_id=revision.revision_id,
                rows_written=rows,
                coverage_status=coverage_by_dataset[ds]["status"],
                publication_status="partial" if ds in partial_datasets else "published",
            )
        else:
            _record_dataset_result(
                config,
                run_id,
                ds,
                "publish_revision",
                "skipped",
                criticality=_dataset_criticality(ds),
                rows_written=rows,
            )
    for published_dataset in compacted:
        published_state = StateStore(config.meta_root)
        if published_dataset in fully_selected:
            published_state.commit_staged_request_days(published_dataset, run_id)
            published_state.commit_staged_units(published_dataset, run_id)
            commit_observation(config, published_dataset, run_id)
        if resolved_keys.get(published_dataset):
            published_state.clear_outstanding_keys(
                published_dataset, resolved_keys[published_dataset]
            )
        # Committing staged request days marks them covered; a partially
        # accepted batch must leave its rejected dates in the missing ledger.
        writer = StagingWriter(config.staging_root)
        for report in writer.quality_receipts(published_dataset, run_id):
            if report.get("rows_rejected"):
                published_state.record_missing_dates(
                    published_dataset,
                    [date.fromisoformat(day) for day in report.get("rejected_dates", [])],
                    reason="quarantined_rows",
                )
    if compacted:
        _update_watermarks(config, frozenset(compacted), trade_date)
    audit_findings.extend(_reconcile_watermarks(config))

    from cnequity.query.views import ensure_duckdb_views

    ensure_duckdb_views(config)

    skipped_datasets = {item["dataset"] for item in skipped}
    coverage_receipts: list[str] = []
    if "trading_status" not in skipped_datasets:
        from cnequity.quality.st_coverage import publish_st_receipts_for_compacted_run

        coverage_receipts.extend(
            str(path) for path in publish_st_receipts_for_compacted_run(config, run_id)
        )
    if not ({"daily_bars", "instruments"} & skipped_datasets):
        from cnequity.steps.delisted import publish_delisted_receipts_for_compacted_run

        coverage_receipts.extend(
            str(path) for path in publish_delisted_receipts_for_compacted_run(config, run_id)
        )

    result: dict = {"rows_read": total, "rows_written": total}
    if partial_datasets:
        result.update(
            status="warning", coverage_status="partial", reason_code="source_scope_incomplete"
        )
    if publication.get("report_path"):
        result["publication_audit"] = publication["report_path"]
    if skipped:
        # Skipping is an intentional integrity gate, but it is not a
        # successful compact: callers must surface the run as retryable and
        # must not report that every staged dataset reached curated storage.
        result["status"] = "warning"
    if coverage_receipts:
        result["coverage_receipts"] = coverage_receipts
    if committed_revisions:
        result["dataset_revisions"] = committed_revisions
    context_updates: dict = {}
    if skipped:
        context_updates["compact_skipped_datasets"] = skipped
    if audit_findings:
        context_updates["audit_findings"] = audit_findings
    if context_updates:
        result["context_updates"] = context_updates
    return result


@register_step(
    "derive_adj_factors",
    group="finalize",
    parallelizable=False,
    depends_on=["daily_bars", "compact"],
    input_datasets=("daily_bars",),
)
def step_derive_adj_factors(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    from cnequity.derive.adj_factors import AdjFactorsDeriveError, compute_adj_factors

    rebackfill = list(context.get("symbols_to_rebackfill") or [])
    from cnequity.derive.factor_arbitration import settle_recent_unstepped_factors

    # Before the audit gate. A price-proven miss switches onto Baostock and is
    # refreshed below; a miss Baostock cannot repair stays a warning.
    switched = settle_recent_unstepped_factors(config, trade_date)
    if switched:
        rebackfill = sorted(set(rebackfill) | set(switched))
    from cnequity.storage.revisions import RevisionStore

    derived_revisions = RevisionStore(
        config.meta_root,
        config.curated_root,
        config.derived_root,
    )
    try:
        # Keep the mutable writer tree seeded from the last committed
        # generation.  This is especially important after an operator removes
        # the legacy derived path: an append-only derive must not publish only
        # its new tip and lose retained history.
        derived_revisions.ensure_current("adj_factors")
        derived_revisions.materialize_current("adj_factors")
        before_files = _layer_file_identity(config.derived_root / "adj_factors")
        input_revisions = _capture_derive_inputs(config, "adj_factors")
        result = compute_adj_factors(config, refresh_symbols=rebackfill)
        published_revision = _publish_derived_revision(
            config,
            "adj_factors",
            run_id,
            trade_date,
            before_files,
            input_revisions=input_revisions,
            coverage_status="partial" if result.failed else "unknown",
            factor_findings=result.findings,
        )
    except Exception as exc:
        _record_dataset_result(
            config,
            run_id,
            "adj_factors",
            "derive",
            "failed",
            criticality="research",
            error_code=type(exc).__name__,
            error_message=str(exc),
        )
        raise
    out: dict = {
        "rows_read": result.rows,
        "rows_written": result.rows,
        "usable_result": bool(result.rows or not result.task_count),
    }
    if result.findings:
        out["context_updates"] = {"audit_findings": result.findings}
    if published_revision is not None:
        out["dataset_revision"] = published_revision
        if published_revision["coverage_status"] == "partial":
            out.update(
                status="warning", coverage_status="partial", reason_code="input_coverage_limited"
            )
        _record_dataset_result(
            config,
            run_id,
            "adj_factors",
            "publish_revision",
            "success",
            criticality="research",
            revision_id=published_revision["revision_id"],
            rows_written=result.rows,
            coverage_status=published_revision["coverage_status"],
            publication_status=published_revision["publication_status"],
        )
    if result.failed:
        # Publish valid independent symbols while keeping retry evidence.
        out["failed_tasks"] = len(result.failed)
        out["status"] = "warning"
    if result.failed and result.rows == 0:
        exc = AdjFactorsDeriveError(
            "adj_factors: no usable requested results after source failures",
            findings=result.findings,
        )
        _record_dataset_result(
            config,
            run_id,
            "adj_factors",
            "derive",
            "failed",
            criticality="research",
            rows_written=result.rows,
            error_code=type(exc).__name__,
            error_message=str(exc),
        )
        raise exc
    status = str(out.get("status", "success"))
    _record_dataset_result(
        config,
        run_id,
        "adj_factors",
        "derive",
        status,
        criticality="research",
        rows_written=result.rows,
        error_code="partial_fetch" if result.failed else None,
        error_message=(
            f"{len(result.failed)} symbol×type fetch failure(s)" if result.failed else None
        ),
    )
    status_finding = _fill_traded_status(config, trade_date)
    if status_finding is not None:
        updates = out.setdefault("context_updates", {})
        updates["audit_findings"] = [*(updates.get("audit_findings") or []), status_finding]
    return out


# Sessions back the daily fill re-reads: a late bar repair can land a print a
# few days after the status step ran.
_TRADED_STATUS_LOOKBACK_DAYS = 10


def _fill_traded_status(config: Config, trade_date: date) -> dict | None:
    """Give a status row to every listed key that printed but no board listed.

    Some funds print on a day no board the status step reads lists them
    (158036.SZ on 2026-10-23), and that key went without a row until a manual
    `cne repair status-traded`. A positive-volume bar is first-hand proof of
    trading; this runs here, after compact, because the status step runs
    before the day's bars exist. It only adds missing keys, and a zero-volume
    bar proves nothing, so a fund that did not trade stays without a row.
    """
    from cnequity.storage.repairs.status_traded import repair_status_traded

    try:
        report = repair_status_traded(
            config,
            start=trade_date - timedelta(days=_TRADED_STATUS_LOOKBACK_DAYS),
            end=trade_date,
            apply=True,
        )
    except Exception as exc:  # noqa: BLE001 — a missing status row is not a factor failure
        logger.warning("trading_status: traded-status fill failed: %s", exc)
        return {
            "dataset": "trading_status",
            "severity": "warning",
            "check": "trading_status_traded_fill_failed",
            "message": str(exc),
        }
    if report.get("rows"):
        logger.info("trading_status: filled %d traded key(s) from bars", report["rows"])
        return {
            "dataset": "trading_status",
            "severity": "info",
            "check": "trading_status_traded_fill",
            "message": f"{report['rows']} printed key(s) without a status row filled from bars",
            "sample": report.get("sample", [])[:10],
        }
    return None


@register_step(
    "derive_futures_continuous",
    group="finalize",
    parallelizable=False,
    depends_on=["compact"],
)
def step_derive_futures_continuous(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    """Main and second-month continuous futures from curated futures_bars."""
    from cnequity.derive.futures_continuous import derive_futures_continuous
    from cnequity.domain.datasets import is_dataset_enabled
    from cnequity.storage.revisions import RevisionStore

    if not is_dataset_enabled("futures_continuous", config):
        return {
            "rows_read": 0,
            "rows_written": 0,
            "note": "futures_continuous disabled ([futures].enabled = false)",
        }
    revisions = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    revisions.ensure_current("futures_continuous")
    revisions.materialize_current("futures_continuous")
    before = _layer_file_identity(config.derived_root / "futures_continuous")
    inputs = _capture_derive_inputs(config, "futures_continuous")
    summary = derive_futures_continuous(config)
    published = _publish_derived_revision(
        config, "futures_continuous", run_id, trade_date, before, input_revisions=inputs
    )
    rows = int(summary.get("rows") or 0)
    out: dict = {"rows_read": rows, "rows_written": rows}
    if published is not None:
        out["dataset_revision"] = published
    if summary.get("note"):
        out["note"] = summary["note"]
    return out


@register_step(
    "derive_option_greeks",
    group="finalize",
    parallelizable=False,
    depends_on=["compact"],
)
def step_derive_option_greeks(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """The lake's own IV and Greeks for new option settlements."""
    from cnequity.derive.option_greeks import derive_option_greeks
    from cnequity.domain.datasets import is_dataset_enabled
    from cnequity.storage.revisions import RevisionStore

    if not is_dataset_enabled("option_greeks", config):
        return {
            "rows_read": 0,
            "rows_written": 0,
            "note": "option_greeks disabled ([futures].enabled / options = false)",
        }
    revisions = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    revisions.ensure_current("option_greeks")
    revisions.materialize_current("option_greeks")
    before = _layer_file_identity(config.derived_root / "option_greeks")
    inputs = _capture_derive_inputs(config, "option_greeks")
    summary = derive_option_greeks(config)
    published = _publish_derived_revision(
        config, "option_greeks", run_id, trade_date, before, input_revisions=inputs
    )
    rows = int(summary.get("rows") or 0)
    out: dict = {"rows_read": rows, "rows_written": rows}
    if published is not None:
        out["dataset_revision"] = published
    if summary.get("note"):
        out["note"] = summary["note"]
    return out


@register_step(
    "derive_industry_index",
    group="finalize",
    parallelizable=False,
    depends_on=["derive_adj_factors"],
    input_datasets=("daily_bars", "industry_members"),
)
def step_derive_industry_index(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    from cnequity.derive.industry_index import derive_industry_index
    from cnequity.storage.revisions import RevisionStore

    derived_revisions = RevisionStore(
        config.meta_root,
        config.curated_root,
        config.derived_root,
    )
    try:
        derived_revisions.ensure_current("industry_index")
        derived_revisions.materialize_current("industry_index")
        before_files = _layer_file_identity(config.derived_root / "industry_index")
        inputs = _capture_derive_inputs(config, "industry_index")
        summary = derive_industry_index(config)
        published_revision = _publish_derived_revision(
            config,
            "industry_index",
            run_id,
            trade_date,
            before_files,
            input_revisions=inputs,
        )
    except Exception as exc:
        _record_dataset_result(
            config,
            run_id,
            "industry_index",
            "derive",
            "failed",
            criticality="research",
            error_code=type(exc).__name__,
            error_message=str(exc),
        )
        raise
    rows = int(summary.get("rows") or 0)
    out: dict = {"rows_read": rows, "rows_written": rows}
    if published_revision is not None:
        out["dataset_revision"] = published_revision
        if published_revision["coverage_status"] == "partial":
            out.update(
                status="warning", coverage_status="partial", reason_code="input_coverage_limited"
            )
        _record_dataset_result(
            config,
            run_id,
            "industry_index",
            "publish_revision",
            "success",
            criticality="research",
            revision_id=published_revision["revision_id"],
            rows_written=rows,
            coverage_status=published_revision["coverage_status"],
            publication_status=published_revision["publication_status"],
        )
    note = str(summary.get("note") or "")
    if rows == 0 and "already current" not in note and "no 申万 membership rows" not in note:
        out["status"] = "warning"
        out["context_updates"] = {
            "audit_findings": [
                {
                    "dataset": "industry_index",
                    "severity": "warning",
                    "check": "derived_empty",
                    "message": (
                        "industry_index produced no rows for the requested derive window; "
                        f"{note or 'required membership, bars, or adjustment-factor inputs are missing'}"
                    ),
                }
            ]
        }
    status = str(out.get("status", "success"))
    _record_dataset_result(
        config,
        run_id,
        "industry_index",
        "derive",
        status,
        criticality="research",
        rows_written=rows,
        error_code="derived_empty" if status == "warning" else None,
        error_message=(str(summary.get("note") or "") if status == "warning" else None),
    )
    return out


@register_step(
    "audit",
    group="finalize",
    parallelizable=False,
    depends_on=["compact", "derive_adj_factors", "derive_industry_index"],
)
def step_audit(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    from cnequity.quality.audit import run_audit
    from cnequity.quality.factor_policy import factor_advisory
    from cnequity.quality.publication import coverage_only

    findings = run_audit(config, run_id, trade_date, context)
    by_severity = context.get("audit_by_severity", {}) if isinstance(context, dict) else {}
    errors = int(by_severity.get("error", 0))
    coverage_errors = sum(coverage_only(item) for item in context.get("audit_error_findings", []))
    errors -= coverage_errors
    errors -= sum(factor_advisory(item) for item in context.get("audit_error_findings", []))

    # This step depends on `compact`, so by the time it runs the rows are
    # already in curated: the audit has never been able to *prevent* anything,
    # only describe it afterwards. `aggregate_run_status` already fails a run
    # on a core step that failed, so the missing piece was only ever this
    # status. Recording the verdict in shadow mode first means the gate can be
    # switched on against measured evidence rather than a guess about how many
    # runs it would have stopped.
    gate = getattr(config, "audit_gate", "shadow")
    would_block = errors > 0
    status = (
        "failed"
        if (would_block and gate == "block")
        else "warning"
        if coverage_errors
        else "success"
    )
    if would_block and gate != "off":
        _record_audit_gate_verdict(config, run_id, trade_date, gate, by_severity)
        logger.warning(
            "audit gate (%s): %d error finding(s)%s",
            gate,
            errors,
            " — failing this run" if gate == "block" else " would have failed this run",
        )

    out = {"status": status, "rows_read": findings, "rows_written": findings}
    _record_dataset_result(
        config,
        run_id,
        "audit",
        "audit",
        status,
        criticality="core",
        rows_written=findings,
    )
    return out


def _record_audit_gate_verdict(
    config: Config,
    run_id: str,
    trade_date: date,
    gate: str,
    by_severity: dict,
) -> None:
    """Append one line of evidence about what the gate did, or would have done.

    A JSONL so a quarter of runs can be read back with one pass before anyone
    decides to switch `[quality].audit_gate` to "block".
    """
    path = config.meta_root / "quality" / "audit_gate.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "run_id": run_id,
        "trade_date": trade_date.isoformat(),
        "gate": gate,
        "blocked": gate == "block",
        "by_severity": dict(by_severity),
    }
    try:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    except OSError:
        # Evidence is useful, not load-bearing; never fail a run over it.
        logger.debug("could not append audit gate verdict to %s", path)


@register_step(
    "ths_official_snapshot",
    group="core",
    depends_on=["compact"],
    description="Capture licensed peer snapshots for the arbitration checks",
)
def step_ths_official_snapshot(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    """Refresh the peer snapshots ``audit`` arbitrates against.

    Placed between ``compact`` and ``audit`` because it needs the session just
    published and its output is what the arbitration checks read. Without it
    those checks are permanently silent: they read ``meta/source_snapshots`` and
    nothing else writes there.

    Valuations are the reason this is daily rather than occasional. The upstream
    publishes no valuation history at all, so a session not captured on the day
    is gone — the accumulating record is the whole product. Bars and corporate
    actions are refreshed alongside because both feed arbitration and both move.

    Statements are **not** here. They change on disclosure days, so a daily
    sweep of 300 securities buys nothing; `cne ths-official capture --what
    financials` covers them when it matters.

    Never fails the run. No key, verification switched off, or an unreachable
    source all return a skipped result — an absent second opinion is not a
    reason to fail a day's ingestion.
    """
    from cnequity.steps.bars import snapshot_daily_bars_ths_official
    from cnequity.steps.capital import snapshot_valuations_ths_official
    from cnequity.steps.fundamentals import snapshot_corporate_actions_ths_official

    if not config.sources.get("ths_official", False):
        return {"rows_written": 0, "status": "skipped", "reason": "source not enabled"}
    if not getattr(config, "ths_official_verify_enabled", True):
        return {"rows_written": 0, "status": "skipped", "reason": "verify off"}
    if not getattr(config, "ths_official_api_key", None):
        return {"rows_written": 0, "status": "skipped", "reason": "no api key"}

    out: dict = {"rows_written": 0}
    for label, call in (
        ("valuations", lambda: snapshot_valuations_ths_official(config, run_id, as_of=trade_date)),
        (
            "daily_bars",
            lambda: snapshot_daily_bars_ths_official(
                config, run_id, start=trade_date - timedelta(days=45), end=trade_date
            ),
        ),
        (
            "corporate_actions",
            lambda: snapshot_corporate_actions_ths_official(config, run_id),
        ),
    ):
        try:
            result = call()
        except Exception as exc:  # noqa: BLE001 — a peer outage is not a failed day
            logger.warning("ths_official snapshot: %s failed: %s", label, exc)
            out[label] = {"status": "error", "reason": str(exc)[:200]}
            continue
        out[label] = result
        out["rows_written"] += int(result.get("rows_written", 0) or 0)
    return out
