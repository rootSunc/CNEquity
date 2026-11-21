"""Offline fallback guidance for completed attempts with coverage gaps."""

from __future__ import annotations

import shlex

from cnequity.domain.datasets import DATASETS, history_mode_for, keyed_source_ready
from cnequity.domain.market_time import shanghai_today
from cnequity.domain.source_routes import source_route_enabled
from cnequity.i18n import _


def fallback_options(config, manifest, run_id: str) -> list[dict]:
    """Describe retained facts and real recovery routes without probing sources.

    Registered source roles are capabilities, not evidence of attempts. Keep
    them labelled so a report never claims a provider was tried or available.
    """
    from cnequity.query.parquet_scan import dataset_has_parquet
    from cnequity.storage.read_context import read_root

    limitations: dict[str, set[str]] = {}
    for row in manifest.aggregate_run_status(run_id)["degraded_results"]:
        if row["execution_status"] not in {"completed", "skipped"}:
            continue
        dataset = {"trading_status_derive": "trading_status"}.get(row["dataset"], row["dataset"])
        if dataset in DATASETS:
            limitations.setdefault(dataset, set()).add(row["reason_code"] or "coverage_limited")
    if not limitations:
        return []
    metadata = manifest.get_run_metadata(run_id)
    run = manifest.get_run(run_id)
    config_args = ["--config", str(config.config_path)] if config.config_path else []
    command = ["cne", "run", "retry", "--run-id", run_id, *config_args]
    if run["job_name"] == "delisted_backfill" and metadata.get("parent_init_run_id"):
        command = [
            "cne",
            "init",
            "--resume",
            "--run-id",
            metadata["parent_init_run_id"],
            *config_args,
        ]
    elif run["job_name"] == "delisted_backfill":
        command = ["cne", "backfill", "daily_bars", "--profile", "delisted"]
        if metadata.get("since"):
            command += ["--start", metadata["since"]]
        command += config_args
    elif run["job_name"] == "derive_trading_status":
        command = ["cne", "derive", "trading_status", *config_args]
        for key, flag in (("derive_start", "--start"), ("derive_end", "--end")):
            if metadata.get(key):
                command += [flag, metadata[key]]
    elif run["job_name"].startswith("derive:"):
        command = ["cne", "derive", run["job_name"].split(":", 1)[1], *config_args]
    options = []
    for dataset, reasons in sorted(limitations.items()):
        spec = DATASETS[dataset]
        sources = [
            {
                "source": source,
                "role": role,
                "enabled": source_route_enabled(config, source)
                and (source != spec.keyed_backfill_source or keyed_source_ready(source, config)),
            }
            for role, names in (
                ("primary", (spec.primary_source,)),
                ("backup", (spec.backup_source,)),
                ("backfill", (spec.backfill_source or spec.keyed_backfill_source,)),
                ("supplementary", spec.supplementary_sources),
            )
            for source in names
            if source
        ]
        mode = history_mode_for(spec, config)
        earliest = spec.history_bounds(shanghai_today(), config)[0]
        collection_commands = [
            shlex.join(["cne", "run", job, "--group", name, *config_args])
            for job, groups in (("daily", config.schedule_groups), ("events", config.events_groups))
            for name, group in groups.items()
            if dataset in group.steps
        ]
        options.append(
            {
                "dataset": dataset,
                "reason_codes": sorted(reasons),
                "strategy": "use_available_data",
                "available_data": dataset_has_parquet(read_root(config, dataset)),
                "action": (
                    _(
                        "Keep the existing snapshot; add this dataset to the daily configuration so it accumulates from later collections."
                    )
                    if mode == "snapshot_only"
                    else _(
                        "Keep the existing lake and deliver the data that passed verification; the gaps stay in the coverage record and can be backfilled later."
                    )
                ),
                "registered_sources": sources,
                "scope": metadata.get("backfill_scope")
                or {
                    "start": metadata.get("start")
                    or metadata.get("since")
                    or metadata.get("history_start"),
                    "end": metadata.get("end") or metadata.get("trade_date"),
                },
                "retry_command": shlex.join(command) if mode != "snapshot_only" else None,
                "collection_commands": collection_commands,
                "earliest_available": earliest.isoformat() if earliest else None,
                "source_retired_date": (
                    spec.source_retired_date.isoformat() if spec.source_retired_date else None
                ),
                "history_mode": mode,
                "history_note": (
                    _(
                        "A snapshot source cannot recreate missed history; a retry only obtains the observations the source can still offer."
                    )
                    if mode == "snapshot_only"
                    else _(
                        "Retry with the original range; ranges beyond the source's capability stay as gaps."
                    )
                ),
            }
        )
    return options
