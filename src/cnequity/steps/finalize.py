"""Finalize steps: compact, derive_adj_factors, derive_industry_index, audit."""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.derive.adj_factors import INPUTS as ADJ_FACTOR_INPUTS
from cnequity.derive.minute_resample import INPUTS as MINUTE_RESAMPLE_INPUTS
from cnequity.derive.option_greeks import INPUTS as OPTION_GREEK_INPUTS
from cnequity.domain.datasets import PARTITION_COLS, RESAMPLED_MINUTE_DATASETS
from cnequity.orchestrator.outcomes import CoverageStatus, PublicationStatus, summarize_coverage
from cnequity.orchestrator.registry import register_step
from cnequity.progress import sweep_progress
from cnequity.steps.compact_hooks import hooks_for
from cnequity.storage import StagingWriter, compact_dataset
from cnequity.storage.instruments import compact_instruments

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


DERIVE_INPUTS = {
    "adj_factors": ADJ_FACTOR_INPUTS,
    "industry_index": ("daily_bars", "industry_members", "adj_factors", "trading_calendar"),
    "futures_continuous": ("futures_bars", "futures_contracts"),
    "option_greeks": OPTION_GREEK_INPUTS,
    **{name: MINUTE_RESAMPLE_INPUTS for name in RESAMPLED_MINUTE_DATASETS},
}


def _planner_owes(config: Config, dataset: str, trade_date: date | None = None) -> bool:
    """Whether the planner sees a gap in *dataset*'s recent cells."""
    from cnequity.domain.market_time import shanghai_today
    from cnequity.quality.planned_coverage import gap_days, gap_keys

    day = trade_date or shanghai_today()
    return bool(gap_days(config, dataset, day) or gap_keys(config, dataset, day))


def capture_derive_inputs(config: Config, dataset: str, *, trade_date: date | None = None) -> dict:
    """Record immutable input identities and the available coverage evidence."""
    from cnequity.storage.revisions import RevisionStore

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    inputs = {}
    for name in DERIVE_INPUTS.get(dataset, ()):
        pointer = store.current_pointer(name) or {}
        latest = store.latest(name) if pointer.get("receipt") else None
        metadata = {} if latest is None else latest.metadata
        inputs[name] = {
            key: pointer.get(key) for key in ("revision", "revision_id", "content_digest")
        }
        owed = (
            _planner_owes(config, name)
            if trade_date is None
            else _planner_owes(config, name, trade_date=trade_date)
        )
        inputs[name]["coverage_status"] = (
            "partial"
            if owed
            or metadata.get("coverage_status") == "partial"
            or metadata.get("coverage", {}).get("status") == "partial"
            else "unknown"
        )
    return inputs


def publish_derived_revision(
    config: Config,
    dataset: str,
    run_id: str,
    trade_date: date,
    output,
    *,
    input_revisions: dict | None = None,
    coverage_status: str = "unknown",
    factor_findings: list[dict] | None = None,
) -> dict | None:
    """Publish what a derivation wrote into *output* as one gated revision.

    *output* is the ``DerivedOutput`` the derivation wrote: a checkout of
    the partitions it changed.
    """
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.quality.publication import evaluate_publication
    from cnequity.storage.lake_write import DatasetPublish, InputChanged
    from cnequity.storage.revisions import RevisionStore

    if not output.changed():
        return None
    changed = output.changed_files()
    revisions = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
    lake_write = output.lake_write

    def reject() -> None:
        # Published data is untouched; the candidate is kept aside.
        lake_write.discard(dataset)

    def input_changed(name: str):
        from cnequity.orchestrator.outcomes import InputChangedError

        reject()
        return InputChangedError(
            f"{dataset}: input {name} changed during calculation; re-run derive"
        )

    # The write holds the lake lock only while it commits, and checks the
    # inputs again there.
    for name, identity in (input_revisions or {}).items():
        current = revisions.current_pointer(name) or {}
        if any(
            current.get(key) != identity.get(key)
            for key in ("revision", "revision_id", "content_digest")
        ):
            raise input_changed(name)
        lake_write.expect(name, identity.get("revision_id"))
    if (
        summarize_coverage(
            row.get("coverage_status", CoverageStatus.NOT_APPLICABLE)
            for row in (input_revisions or {}).values()
        )
        == CoverageStatus.PARTIAL
    ):
        coverage_status = CoverageStatus.PARTIAL.value
    publication = evaluate_publication(
        config,
        f"{run_id}-{dataset}",
        trade_date,
        {dataset: output.root},
        {dataset: changed},
        lake_write=lake_write,
    )
    if publication["blocked"]:
        reject()
        raise RuntimeError(f"publication gate blocked {dataset}: {publication.get('report_path')}")
    factor_metadata = {}
    if dataset == "adj_factors" and factor_findings:
        previous = revisions.latest(dataset)
        proofs = dict(previous.metadata.get("factor_event_proofs", {})) if previous else {}
        for item in factor_findings:
            if item.get("check") == "stock_event_factor" and "correction" in item:
                proofs[f"{item['symbol']}|{item['trade_date']}"] = item
        factor_metadata["factor_event_proofs"] = proofs
    contract = dataset_contract(dataset)
    schema_version = int(contract["schema_version"])
    fingerprint = contract_fingerprint(contract)
    metadata = {
        "trade_date": trade_date.isoformat(),
        "layer": "derived",
        "input_revisions": input_revisions or {},
        "coverage_status": coverage_status,
        "publication_audit": {
            key: publication.get(key) for key in ("mode", "blocked", "report_path")
        },
        "changed_files_count": len(changed),
        **factor_metadata,
    }
    try:
        revision = lake_write.publish(
            {dataset: DatasetPublish(schema_version, fingerprint, metadata)},
            message=f"{dataset} revision from {run_id}",
        ).get(dataset)
    except InputChanged as exc:
        raise input_changed(exc.dataset) from exc
    if revision is None:
        return None
    publication_status = (
        PublicationStatus.PARTIAL
        if coverage_status == CoverageStatus.PARTIAL
        else PublicationStatus.PUBLISHED
    )
    _record_dataset_result(
        config,
        run_id,
        dataset,
        "publish_revision",
        "success",
        criticality=_dataset_criticality(dataset),
        revision_id=revision.revision_id,
        coverage_status=coverage_status,
        publication_status=publication_status.value,
    )
    return {
        "revision": revision.revision,
        "revision_id": revision.revision_id,
        "content_digest": revision.content_digest,
        "changed_partitions": list(revision.changed_partitions),
        "coverage_status": coverage_status,
        "publication_status": publication_status.value,
    }


def _settle_published_datasets(
    config: Config, datasets: set[str], *, run_id: str, fully_selected: set[str]
) -> None:
    """Settle what the datasets compact published leave behind outside the lake.

    Runs once per compact after every revision commit, and — when a later
    commit fails — for the datasets committed before it.
    """
    # Coverage is the planner's, read off the published rows and the ledger:
    # a quarantined day has no published row, so it is simply a gap.
    if "sector_bars" in datasets and "sector_bars" in fully_selected:
        from cnequity.steps.rotation import commit_sector_bars_staged

        commit_sector_bars_staged(config, run_id)


def _record_resume_units(
    config: Config, run_id: str, findings: list[dict], datasets: set[str]
) -> list[dict]:
    """Record complete resume units when compact had no changed files to publish."""
    if not datasets:
        return []
    from cnequity.domain.findings import finding
    from cnequity.storage import resume_units

    try:
        resume_units.record(config, run_id, findings, datasets)
    except Exception as exc:  # a lost checkpoint only re-asks the units
        logger.warning("backfill units not recorded for %s: %s", run_id, exc)
        return [
            finding(
                dataset="",
                check="resume_units_record",
                severity="warning",
                message=f"the backfill's published units were not recorded: {exc}",
            )
        ]
    return []


def _record_answers(config: Config, run_id: str, answers: list[dict]) -> list[dict]:
    """Record this run's answers in the coverage ledger; never fails a compact.

    *answers* are the findings in which this run's fetch steps reported what
    they asked about and how it was answered. Refreshes of datasets refreshed
    as a whole come from the run records and are recorded beside them.
    """
    from cnequity.domain.findings import finding
    from cnequity.quality.planned_coverage import record_answers, record_refreshes

    try:
        record_answers(config, run_id, answers)
        record_refreshes(config, run_id)
    except Exception as exc:  # a lost answer is only asked again
        logger.warning("coverage ledger not updated for %s: %s", run_id, exc)
        return [
            finding(
                dataset="",
                check="attempts_ledger_sync",
                severity="warning",
                message=f"the coverage ledger was not brought up to date: {exc}",
            )
        ]
    return []


@register_step("compact", group="finalize", parallelizable=False)
def step_compact(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    from cnequity.orchestrator.run_lock import reentrant_run_lock
    from cnequity.storage.catalog_bridge import lake_root
    from cnequity.storage.lake_write import planned_on
    from equitylake_core.lock import lake_read

    # The run owns its staging. Another run may prepare its own candidate
    # concurrently; publish rechecks the bases and inputs under the write lock.
    with (
        lake_read(lake_root(config)),
        reentrant_run_lock(config.meta_root, run_id),
        planned_on(config) as plan,
    ):
        return _compact_run(config, trade_date, run_id, context, plan)


def _compact_run(config: Config, trade_date: date, run_id: str, context: dict, plan) -> dict:
    from cnequity.storage.catalog_migration import require_catalog_lake
    from cnequity.storage.lake_write import LakeWrite

    require_catalog_lake(config)
    lake_write = LakeWrite(config, run_id)
    try:
        if plan is not None:
            # The publication audit may read any declared dataset. With the
            # gate off, the daily-bar pre-merge contract has explicit inputs.
            from cnequity.domain.datasets import DATASETS

            if config.publication_gate != "off":
                inputs = DATASETS
            elif lake_write.config.staging_root.joinpath("daily_bars", f"run_id={run_id}").is_dir():
                inputs = ("corporate_actions", "instruments", "trading_status")
            else:
                inputs = ()
            for name in inputs:
                lake_write.expect(name, plan.revisions.get(name))
        result = _compact_datasets(config, trade_date, run_id, context, lake_write)
    except BaseException:
        lake_write.close(keep=True)
        raise
    lake_write.close()
    return result


def _compact_datasets(
    config: Config, trade_date: date, run_id: str, context: dict, lake_write
) -> dict:
    from cnequity.domain.canonical import canonical_policy
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.orchestrator.compact_gate import compact_allowed, publication_files
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.storage.coverage import (
        gap_summary,
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
        dataset_coverage = summarize_coverage(r["coverage_status"] for r in receipts)
        gaps = _ledger_view(config, ds, context)
        if (
            incomplete_count
            or dataset_coverage == CoverageStatus.PARTIAL
            or any(gaps.get(k) for k in ("missing_ranges", "missing_units", "outstanding_keys"))
            or len(selected_files) < len(writer.list_run_files(ds, run_id))
        ):
            partial_datasets.add(ds)

        pcol = PARTITION_COLS[ds]
        changed_files: list[Path] = []
        change_log: list[dict] = []
        # The partitions this run changes, checked out of the catalog: the
        # merge reads and writes them in place, and only they publish.
        from cnequity.storage.read_context import read_root

        checkout = lake_write.checkout_staged(ds, selected_files)
        committed_root = read_root(config, ds)
        merge_base, out_root, candidate_root = checkout.root, checkout.layer_root, checkout.root
        if ds == "instruments":
            rows, inst_findings = compact_instruments(
                config.staging_root,
                config.curated_root,
                run_id,
                trade_date,
                changed_files=changed_files,
                base_root=merge_base,
                staging_files=selected_files,
                allow_delist_inference=dataset_coverage != CoverageStatus.PARTIAL,
                out_root=out_root,
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
                current = collect_parquet_root(candidate_root, hive=False, committed=False)
                change_log.append({"partition": None, **summarize_changes(previous, current, ds)})
        else:
            hooks = hooks_for(ds)
            if hooks.before_merge is not None:
                checked = hooks.before_merge(
                    config,
                    run_id,
                    lambda name: publication_files(manifest, run_id, name, config.staging_root),
                    revisions,
                    committed_root,
                    selected_files,
                )
                audit_findings.extend(checked.findings)
                selected_files = checked.files
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
                base_root=merge_base,
                change_log=change_log,
                staging_files=selected_files,
                out_root=out_root,
            )
            if rows:
                compacted.add(ds)
            total += rows
            if hooks.after_merge is not None:
                hooks.after_merge(config, selected_files)

        _record_dataset_result(
            config,
            run_id,
            ds,
            "compact",
            "success",
            criticality=_dataset_criticality(ds),
            rows_written=rows,
        )

        remaining = _ledger_view(config, ds, context)
        is_partial = (
            incomplete_count > 0
            or not full_staging
            or dataset_coverage == CoverageStatus.PARTIAL
            or any(remaining.values())
            or any(f.get("dataset") == ds and f.get("rows_rejected") for f in audit_findings)
        )
        if is_partial:
            partial_datasets.add(ds)
        else:
            partial_datasets.discard(ds)
        coverage_by_dataset[ds] = {
            "status": CoverageStatus.PARTIAL.value
            if is_partial
            else CoverageStatus.COMPLETE.value
            if dataset_coverage == CoverageStatus.COMPLETE
            else CoverageStatus.UNKNOWN.value,
            "requested_scope": manifest.get_run_metadata(run_id).get("backfill_scope")
            or {
                "start": manifest.get_run_metadata(run_id).get("history_start"),
                "end": trade_date.isoformat(),
                "universe": config.ingest_universe,
            },
            "gaps": gap_summary(remaining),
            "gaps_source": "coverage_ledger",
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

                if pcol == checkout.spec.partition_col:
                    # A checkout holds only the partitions this run changes;
                    # the comparison needs the session's rows even when the
                    # run staged other days. An unchanged partition is not
                    # published.
                    checkout.partition(
                        f"{pcol}={checkout.spec.partition_for(trade_date)}" if pcol else ""
                    )
                gate_findings = diff_dataset(
                    config,
                    gate_spec,
                    trade_date=trade_date,
                    candidate_root=candidate_root,
                )
                audit_findings.extend(gate_findings)
                if source_diff_blocks_revision(gate_findings):
                    quarantine = lake_write.discard(ds)
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
        {item[0]: lake_write.checkout(item[0]).root for item in pending},
        {item[0]: item[3] for item in pending},
        lake_write=lake_write,
    )
    blocked_datasets = set(publication.get("blocked_datasets") or [])
    if publication["blocked"]:
        for ds, _pcol, rows, *_ in pending:
            if ds not in blocked_datasets:
                continue
            quarantine = lake_write.discard(ds)
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

    def _revision_metadata(ds, pcol, rows, change_log, gate_spec) -> dict:
        return {
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
        }

    findings = list(context.get("audit_findings") or ())
    resume_datasets = compacted & fully_selected

    def add_publication_records(write) -> None:
        from cnequity.quality.planned_coverage import attach_answers
        from cnequity.storage.resume_units import attach

        attach_answers(write, run_id, findings)
        attach(write, run_id, findings, resume_datasets)

    catalog_revisions: dict = {}
    catalog_pending = [item for item in pending if lake_write.has(item[0])]
    if catalog_pending:
        from cnequity.storage.lake_write import DatasetPublish

        try:
            catalog_revisions = lake_write.publish(
                {
                    ds: DatasetPublish(
                        schema_version=int(dataset_contract(ds)["schema_version"]),
                        contract_fingerprint=contract_fingerprint(dataset_contract(ds)),
                        metadata=_revision_metadata(ds, pcol, rows, change_log, gate_spec),
                    )
                    for ds, pcol, rows, _files, change_log, gate_spec in catalog_pending
                },
                message=f"compact run {run_id}",
                record=add_publication_records,
            )
        except Exception as exc:
            # Report that the caller did not receive a result; a failure at
            # COMMIT or afterwards can leave a committed snapshot.
            for ds, _pcol, rows, *_ in catalog_pending:
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
    # One catalog transaction published every dataset above, or none of them.
    for ds, _pcol, rows, *_rest in pending:
        revision = catalog_revisions.get(ds)
        after_commit = hooks_for(ds).after_commit
        if after_commit is not None:
            after_commit(config, ds)
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
                publication_status=(
                    PublicationStatus.PARTIAL
                    if ds in partial_datasets
                    else PublicationStatus.PUBLISHED
                ).value,
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
    _settle_published_datasets(config, compacted, run_id=run_id, fully_selected=fully_selected)
    if not catalog_revisions:
        audit_findings.extend(_record_resume_units(config, run_id, findings, resume_datasets))
    audit_findings.extend(_record_answers(config, run_id, [] if catalog_revisions else findings))

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
    outputs=("adj_factors",),
)
def step_derive_adj_factors(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    from cnequity.derive.adj_factors import AdjFactorsDeriveError, compute_adj_factors

    rebackfill = list(context.get("symbols_to_rebackfill") or [])
    from cnequity.storage.repairs.adj_factors import settle_recent_unstepped_factors

    # Before the audit gate. A price-proven miss switches onto Baostock and is
    # refreshed below; a miss Baostock cannot repair stays a warning.
    switched = settle_recent_unstepped_factors(config, trade_date)
    if switched:
        rebackfill = sorted(set(rebackfill) | set(switched))
    from cnequity.storage.derived_output import DerivedOutput

    try:
        with DerivedOutput(config, "adj_factors", run_id) as output:
            input_revisions = capture_derive_inputs(config, "adj_factors", trade_date=trade_date)
            result = compute_adj_factors(config, refresh_symbols=rebackfill, output=output)
            published_revision = publish_derived_revision(
                config,
                "adj_factors",
                run_id,
                trade_date,
                output,
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
        if published_revision["coverage_status"] == CoverageStatus.PARTIAL:
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
    outputs=("futures_continuous",),
)
def step_derive_futures_continuous(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    """Main and second-month continuous futures from curated futures_bars."""
    from cnequity.derive.futures_continuous import derive_futures_continuous
    from cnequity.domain.datasets import is_dataset_enabled
    from cnequity.storage.derived_output import DerivedOutput

    if not is_dataset_enabled("futures_continuous", config):
        return {
            "rows_read": 0,
            "rows_written": 0,
            "note": "futures_continuous disabled ([futures].enabled = false)",
        }
    with DerivedOutput(config, "futures_continuous", run_id) as output:
        inputs = capture_derive_inputs(config, "futures_continuous", trade_date=trade_date)
        summary = derive_futures_continuous(config, output=output)
        published = publish_derived_revision(
            config, "futures_continuous", run_id, trade_date, output, input_revisions=inputs
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
    outputs=("option_greeks",),
)
def step_derive_option_greeks(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """The lake's own IV and Greeks for new option settlements."""
    from cnequity.derive.option_greeks import derive_option_greeks
    from cnequity.domain.datasets import is_dataset_enabled
    from cnequity.storage.derived_output import DerivedOutput

    if not is_dataset_enabled("option_greeks", config):
        return {
            "rows_read": 0,
            "rows_written": 0,
            "note": "option_greeks disabled ([futures].enabled / options = false)",
        }
    with DerivedOutput(config, "option_greeks", run_id) as output:
        inputs = capture_derive_inputs(config, "option_greeks", trade_date=trade_date)
        summary = derive_option_greeks(config, output=output)
        published = publish_derived_revision(
            config, "option_greeks", run_id, trade_date, output, input_revisions=inputs
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
    outputs=("industry_index",),
)
def step_derive_industry_index(
    config: Config, trade_date: date, run_id: str, context: dict
) -> dict:
    from cnequity.derive.industry_index import derive_industry_index
    from cnequity.storage.derived_output import DerivedOutput

    try:
        with DerivedOutput(config, "industry_index", run_id) as output:
            inputs = capture_derive_inputs(config, "industry_index", trade_date=trade_date)
            summary = derive_industry_index(config, output=output)
            published_revision = publish_derived_revision(
                config,
                "industry_index",
                run_id,
                trade_date,
                output,
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
        if published_revision["coverage_status"] == CoverageStatus.PARTIAL:
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
    from cnequity.quality.gate import coverage_only, is_contradiction

    findings = run_audit(config, run_id, trade_date, context)
    by_severity = context.get("audit_by_severity", {}) if isinstance(context, dict) else {}
    error_findings = context.get("audit_error_findings", [])
    # A run's status asks whether what it wrote is right, so only
    # contradictions count (see `cnequity.quality.gate`); a coverage gap
    # marks it a warning.
    errors = int(by_severity.get("error", 0))
    errors -= sum(not is_contradiction(item) for item in error_findings)
    coverage_errors = sum(coverage_only(item) for item in error_findings)

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
    from cnequity.steps.bars.ths_official import snapshot_daily_bars_ths_official
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


def _ledger_view(config, dataset: str, context: dict | None = None) -> dict:
    """*dataset*'s owed work, less the days this run's answers settle (recorded after compact)."""
    from cnequity.quality.planned_coverage import ANSWER_CHECK, ledger_view

    view = ledger_view(config, dataset)
    settled = {
        day
        for item in (context or {}).get("audit_findings") or ()
        if item.get("dataset") == dataset
        and item.get("check") == ANSWER_CHECK
        and item.get("outcome") == "complete"
        for day in item.get("periods") or ()
    }
    if settled:
        view["missing_ranges"] = [
            row for row in view.get("missing_ranges", []) if row.get("start") not in settled
        ]
    return view
