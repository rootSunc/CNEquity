"""Whitelisted dashboard operations.

Each operation is one Click command plus the flags the page is allowed to set.
``--config`` is never taken from the request. The alignment test walks this
table against Click, so a renamed flag fails there instead of 404ing a button.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from cnequity.domain.market_time import shanghai_today
from cnequity.i18n import N_, _, use_language
from cnequity.serve.display import en, zh

# Targets ``cne derive`` accepts that the page may run. The command's other
# names rewrite data only with ``--apply`` or have no schedule at all; those
# stay on the CLI. The test compares this tuple with ``derive``'s branches.
DERIVE_NAMES = (
    "adj_factors",
    "industry_index",
    "trading_status",
    "futures_continuous",
    "option_greeks",
    "minute_bars_15m",
    "minute_bars_30m",
    "minute_bars_60m",
)

# Click choices the page deliberately does not offer.
EXCLUDED_CHOICES: dict[tuple[str, str], frozenset[str]] = {
    ("init.start", "profile"): frozenset({"demo", "sample"}),
}

WRITE_LOCKS = frozenset({"ingest-daily", "ingest-events", "lake-write", "init"})


class OpsError(ValueError):
    """The page can show this and must not start a process."""


class UnknownOp(OpsError):
    """No such operation id."""


@dataclass(frozen=True)
class Param:
    name: str
    kind: str  # bool, date, choice, multi, symbols, text, run_id
    label: str
    flag: str | None = None
    help: str = ""
    required: bool = False
    choices: tuple[str, ...] = ()
    choice_source: str | None = None
    position: int = 0
    empty_reason: str = ""
    repeat: bool = False


@dataclass(frozen=True)
class OpSpec:
    id: str
    title: str
    summary: str
    group: str
    command: tuple[str, ...]
    params: tuple[Param, ...] = ()
    fixed: tuple[str, ...] = ()
    lock_class: str = "read"  # or "run" — decided from the target run
    confirm: str = "standard"  # none, standard, heavy
    result: str = "run_json"  # run_json, report, gate
    available_in: str = "normal"  # normal, setup, both
    needs: str | None = None
    needs_reason: str = ""
    heavy_text: str = ""


STANDARD_ACK = {"id": "confirm", "text": N_("I confirm starting this operation now.")}


def _ops() -> tuple[OpSpec, ...]:
    trade_date = Param(
        "trade_date", "date", N_("Trading day"), flag="--trade-date", help=N_("Defaults to today.")
    )
    backfill = Param(
        "backfill",
        "bool",
        N_("Run as catch-up"),
        flag="--backfill",
        help=N_("Re-run a specific day instead of the incremental window."),
    )
    return (
        OpSpec(
            id="snapshot.create",
            title=N_("Create data backup"),
            group=N_("Backup and restore"),
            summary=N_(
                "Copy the selected datasets into a verifiable lake snapshot. Does not contact data sources."
            ),
            command=("snapshot", "create"),
            lock_class="lake-write",
            result="report",
            params=(
                Param("name", "snapshot_name", N_("New backup name"), required=True),
                Param(
                    "snapshot_root",
                    "directory",
                    N_("Backup directory (optional, absolute path)"),
                    flag="--snapshot-root",
                ),
                Param(
                    "datasets",
                    "multi",
                    N_("Datasets to back up"),
                    flag="--dataset",
                    required=True,
                    choice_source="stored_datasets",
                    repeat=True,
                    empty_reason=N_("No published data yet."),
                ),
            ),
        ),
        OpSpec(
            id="snapshot.verify",
            title=N_("Verify data backup"),
            group=N_("Backup and restore"),
            summary=N_(
                "Recompute backup file digests and check for missing or corrupt files and manifest mismatches."
            ),
            command=("snapshot", "verify"),
            result="gate",
            params=(
                Param("name", "snapshot_name", N_("Backup name"), required=True),
                Param(
                    "snapshot_root",
                    "directory",
                    N_("Backup directory (optional, absolute path)"),
                    flag="--snapshot-root",
                ),
            ),
        ),
        OpSpec(
            id="snapshot.restore",
            title=N_("Restore data backup"),
            group=N_("Backup and restore"),
            summary=N_(
                "Verify, then restore into a new or empty directory. You can inspect the result afterward."
            ),
            command=("snapshot", "restore"),
            lock_class="lake-write",
            result="report",
            confirm="heavy",
            heavy_text=N_(
                "I confirm the restore target is outside the current data lake and understand only the selected backup's contents are restored."
            ),
            params=(
                Param("name", "snapshot_name", N_("Backup name"), required=True),
                Param(
                    "snapshot_root",
                    "directory",
                    N_("Backup directory (optional, absolute path)"),
                    flag="--snapshot-root",
                ),
                Param(
                    "target",
                    "path",
                    N_("Restore directory (absolute path)"),
                    required=True,
                    position=1,
                ),
            ),
        ),
        OpSpec(
            id="daily.full",
            title=N_("Run a day's update"),
            summary=N_(
                "Run every daily-update schedule group in config order, then the event streams. Continues if a group fails."
            ),
            group=N_("Daily update"),
            command=("run", "daily"),
            params=(
                trade_date,
                backfill,
                Param("no_events", "bool", N_("Skip event streams"), flag="--no-events"),
                Param(
                    "pack",
                    "multi",
                    N_("Research packs"),
                    flag="--pack",
                    repeat=True,
                    choices=("market", "fundamentals", "universe"),
                    help=N_(
                        "Leave empty to run all schedule groups and event streams. If selected, only the matching schedule groups run."
                    ),
                ),
            ),
            lock_class="ingest-daily",
            result="run_json",
        ),
        OpSpec(
            id="daily.group",
            title=N_("Run a schedule group"),
            summary=N_(
                "Run only the selected daily-update schedule group. Does not mark today as done, so the scheduled full run still happens."
            ),
            group=N_("Daily update"),
            command=("run", "daily"),
            params=(
                Param(
                    "group",
                    "choice",
                    N_("Schedule group"),
                    flag="--group",
                    required=True,
                    choice_source="schedule_groups",
                    empty_reason=N_("This config has no daily-update schedule groups."),
                ),
                trade_date,
                backfill,
            ),
            lock_class="ingest-daily",
            result="run_json",
        ),
        OpSpec(
            id="daily.stale",
            title=N_("Catch up lagging datasets"),
            summary=N_(
                "Re-fetch only datasets still behind the last trading day. Optionally limit to schedule groups or to snapshots."
            ),
            group=N_("Catch-up and recovery"),
            command=("run", "daily"),
            fixed=("--stale-only",),
            params=(
                Param("snapshots_only", "bool", N_("Snapshots only"), flag="--snapshots-only"),
                Param(
                    "groups",
                    "multi",
                    N_("Limit to schedule groups"),
                    flag="--groups",
                    choice_source="schedule_groups",
                    help=N_("Leave empty for all enabled groups."),
                ),
            ),
            lock_class="ingest-daily",
            result="run_json",
        ),
        OpSpec(
            id="daily.maintain",
            title=N_("Run maintenance"),
            summary=N_(
                "After the day's update: run audit, freshness gate, source probes, run evidence, metadata backup and expiry preview."
            ),
            group=N_("Catch-up and recovery"),
            command=("run", "maintain"),
            params=(
                Param(
                    "notify",
                    "bool",
                    N_("Desktop notification on problems (macOS)"),
                    flag="--notify",
                ),
            ),
            lock_class="ingest-daily",
            # Exit 1 is findings (stale data, audit errors), not a failed command.
            result="gate",
        ),
        OpSpec(
            id="events.run",
            title=N_("Run event streams"),
            summary=N_(
                "Announcements, regulatory events and news. Also runs on weekends and holidays."
            ),
            group=N_("Daily update"),
            command=("run", "events"),
            params=(
                Param(
                    "group",
                    "choice",
                    N_("Event groups"),
                    flag="--group",
                    choice_source="events_groups",
                    help=N_("Leave empty to run all in config order."),
                ),
                Param(
                    "trade_date",
                    "date",
                    N_("Calendar day"),
                    flag="--trade-date",
                    help=N_("Defaults to today, including weekends and holidays."),
                ),
            ),
            lock_class="ingest-events",
            needs="events_groups",
            needs_reason=N_("This config has no event-stream schedule groups."),
            result="run_json",
        ),
        OpSpec(
            id="run.retry",
            title=N_("Retry a run"),
            summary=N_(
                "Retry the failed batches in a run. Initialization runs resume as initialization."
            ),
            group=N_("Catch-up and recovery"),
            command=("run", "retry"),
            params=(Param("run_id", "run_id", "run", flag="--run-id", required=True),),
            lock_class="run",
            result="run_json",
        ),
        OpSpec(
            id="run.retry_failed_groups",
            title=N_("Retry failed daily-update groups"),
            summary=N_("Retry the latest failed run of each daily-update schedule group once."),
            group=N_("Catch-up and recovery"),
            command=("run", "retry"),
            fixed=("--failed-groups",),
            lock_class="ingest-daily",
            result="run_json",
        ),
        OpSpec(
            id="run.compact",
            title=N_("Publish staging"),
            summary=N_("Publish the finished data this run left in staging to the lake."),
            group=N_("Catch-up and recovery"),
            command=("run", "compact"),
            params=(Param("run_id", "run_id", "run", flag="--run-id", required=True),),
            lock_class="lake-write",
            result="run_json",
        ),
        OpSpec(
            id="diag.doctor",
            title=N_("Environment health check"),
            summary=N_(
                "Offline check of config, dependencies and silent failure points. No network access."
            ),
            group=N_("Initialize"),
            command=("doctor",),
            fixed=("--json",),
            lock_class="read",
            confirm="none",
            result="gate",
            available_in="both",
        ),
        OpSpec(
            id="init.start",
            title=N_("Initialize data lake"),
            summary=N_(
                "All market instruments. quick covers the last 3 years; full starts from each dataset's history start, with daily bars from 2016-01-01."
            ),
            group=N_("Initialize"),
            command=("init",),
            # The page reads the machine result; the terminal summary is for people.
            fixed=("--json",),
            params=(
                Param(
                    "profile",
                    "choice",
                    N_("Depth"),
                    flag="--profile",
                    required=True,
                    choices=("quick", "full"),
                    help=N_("quick is the last 3 years; full goes deeper."),
                ),
                Param(
                    "since",
                    "date",
                    N_("History start"),
                    flag="--since",
                    help=N_("Overrides the depth above when set."),
                ),
                Param(
                    "pack",
                    "multi",
                    N_("Research packs"),
                    flag="--pack",
                    repeat=True,
                    choices=("market", "fundamentals", "universe"),
                    help=N_(
                        "Does not change what initialization downloads. market: quotes, fundamentals: fundamentals, universe: historical ST."
                    ),
                ),
                Param(
                    "keep_going", "bool", N_("Continue after a stage fails"), flag="--keep-going"
                ),
            ),
            lock_class="init",
            confirm="heavy",
            heavy_text=N_(
                "I understand initialization fetches from data sources at the selected depth and may take hours."
            ),
            result="run_json",
        ),
        OpSpec(
            id="init.resume",
            title=N_("Resume initialization"),
            summary=N_(
                "Resume the latest unfinished initialization, keeping batches that succeeded."
            ),
            group=N_("Initialize"),
            command=("init",),
            # The page reads the machine result; the terminal summary is for people.
            fixed=("--json",),
            params=(
                Param("run_id", "run_id", "init run", flag="--run-id", required=True),
                Param(
                    "keep_going", "bool", N_("Continue after a stage fails"), flag="--keep-going"
                ),
            ),
            lock_class="init",
            result="run_json",
        ),
        OpSpec(
            id="backfill.run",
            title=N_("Backfill a dataset"),
            summary=N_(
                "Shows the source, range and fetch method first; fetching starts only after you confirm."
            ),
            group=N_("Targeted backfill"),
            command=("backfill",),
            params=(
                Param(
                    "dataset",
                    "choice",
                    N_("Dataset"),
                    required=True,
                    choice_source="datasets",
                    position=0,
                    empty_reason=N_("No datasets can be backfilled from the dashboard."),
                ),
                Param("start", "date", N_("Start"), flag="--start"),
                Param("end", "date", N_("End"), flag="--end", help=N_("Defaults to today.")),
                Param(
                    "symbols",
                    "symbols",
                    N_("Instruments"),
                    flag="--symbols",
                    help=N_(
                        "Comma-separated, e.g. 600519.SH,000001.SZ. Leave empty for the dataset's default range."
                    ),
                ),
            ),
            lock_class="lake-write",
            confirm="standard",
            heavy_text=N_(
                "I confirm the backfill for the previewed range. No instrument limit or more than a year means heavy data source requests."
            ),
            result="run_json",
        ),
        OpSpec(
            id="derive.run",
            title=N_("Rebuild derived data"),
            summary=(
                N_(
                    "Recompute from published data. Full mode rewrites partitions. 15 / 30 / 60-minute bars are skipped by default; select them to write them to the lake here."
                )
            ),
            group=N_("Targeted backfill"),
            command=("derive",),
            params=(
                Param(
                    "name",
                    "choice",
                    N_("Derived"),
                    required=True,
                    choices=DERIVE_NAMES,
                    position=0,
                ),
                Param("start", "date", N_("Start"), flag="--start"),
                Param("end", "date", N_("End"), flag="--end"),
                Param("full", "bool", N_("Full rewrite"), flag="--full"),
            ),
            lock_class="lake-write",
            heavy_text=N_("I confirm a full rebuild that rewrites partitions."),
            result="run_json",
        ),
        OpSpec(
            id="check.status",
            title=N_("Freshness"),
            summary=N_("View coverage and freshness by dataset. Read-only."),
            group=N_("Inspection"),
            command=("status",),
            fixed=("--datasets",),
            params=(
                Param(
                    "groups",
                    "multi",
                    N_("Limit to schedule groups"),
                    flag="--groups",
                    choice_source="schedule_groups",
                ),
            ),
            lock_class="read",
            confirm="none",
            result="report",
        ),
        OpSpec(
            id="check.verify",
            title=N_("Verify coverage"),
            summary=N_(
                "Read-only check of lake coverage. Exit code 1 means findings, not a command error."
            ),
            group=N_("Inspection"),
            command=("verify",),
            lock_class="read",
            confirm="none",
            result="gate",
        ),
        OpSpec(
            id="check.audit",
            title=N_("Full lake audit"),
            summary=N_("Run a full lake audit and save a snapshot."),
            group=N_("Inspection"),
            command=("audit",),
            fixed=("--full",),
            lock_class="report",
            confirm="none",
            result="gate",
        ),
        OpSpec(
            id="check.sources_probe",
            title=N_("Probe data sources"),
            summary=N_(
                "Reuses collection evidence from the last 12 hours and probes only untouched sources. Contacts third-party hosts."
            ),
            group=N_("Inspection"),
            command=("sources", "probe"),
            fixed=("--stale-only",),
            params=(
                Param(
                    "vantage",
                    "text",
                    N_("Vantage label"),
                    flag="--vantage",
                    help=N_("e.g. local, cn. Defaults to local."),
                ),
            ),
            lock_class="report",
            result="report",
        ),
        OpSpec(
            id="maint.stats_rebuild",
            title=N_("Rebuild metrics table"),
            summary=N_("Recompute row counts, sizes and source distribution. No fetching."),
            group=N_("Inspection"),
            command=("stats", "rebuild"),
            lock_class="report",
            confirm="none",
            result="report",
        ),
        OpSpec(
            id="config.upgrade_preview",
            title=N_("Preview config upgrade"),
            summary=N_(
                "List the steps and schedule groups this version would add to the config. Writes no files."
            ),
            group=N_("Inspection"),
            command=("config", "upgrade"),
            fixed=("--dry-run",),
            lock_class="read",
            confirm="none",
            result="report",
        ),
    )


OPS: tuple[OpSpec, ...] = _ops()
OPS_BY_ID: dict[str, OpSpec] = {spec.id: spec for spec in OPS}

GROUP_ORDER = (
    N_("Daily update"),
    N_("Catch-up and recovery"),
    N_("Initialize"),
    N_("Targeted backfill"),
    N_("Backup and restore"),
    N_("Inspection"),
)


def spec_for(op_id: str) -> OpSpec:
    try:
        return OPS_BY_ID[op_id]
    except KeyError as exc:
        raise UnknownOp(_("Unknown operation {op_id}").format(op_id=op_id)) from exc


def flags_of(spec: OpSpec) -> list[str]:
    """Option strings this operation passes, for the Click alignment test."""
    flags = [param.flag for param in spec.params if param.flag]
    flags.extend(token for token in spec.fixed if token.startswith("--"))
    return flags


def backfill_datasets(config) -> list[str]:
    """Datasets the page may backfill: registered, enabled, not derived."""
    import cnequity.steps  # noqa: F401 — registration is the import
    from cnequity.domain.datasets import DATASETS, is_dataset_enabled
    from cnequity.orchestrator.registry import STEP_REGISTRY

    names = [
        name
        for name, dataset in DATASETS.items()
        if dataset.layer != "derived" and name in STEP_REGISTRY and is_dataset_enabled(name, config)
    ]
    return sorted(names)


def choices_for(source: str, config) -> list[str]:
    if config is None:
        return []
    if source == "schedule_groups":
        return sorted(getattr(config, "schedule_groups", {}) or {})
    if source == "events_groups":
        return sorted(getattr(config, "events_groups", {}) or {})
    if source == "datasets":
        return backfill_datasets(config)
    if source == "derive":
        return list(DERIVE_NAMES)
    if source in {"stored_datasets", "snapshots"}:
        from cnequity.ops.backups import snapshot_names, stored_datasets

        return stored_datasets(config) if source == "stored_datasets" else snapshot_names(config)
    return []


def _choice_list(param: Param, config) -> list[str]:
    if param.choice_source:
        return choices_for(param.choice_source, config)
    return list(param.choices)


def unavailable_reason(spec: OpSpec, config, *, setup: bool) -> str | None:
    if setup and spec.available_in == "normal":
        return _("Finish first-time setup first.")
    if not setup and spec.available_in == "setup":
        return _("Only used during first-time setup.")
    if config is None:
        return None if spec.available_in != "normal" else _("Finish first-time setup first.")
    if spec.needs and not choices_for(spec.needs, config):
        return _(spec.needs_reason) if spec.needs_reason else _("The config has no such entry.")
    for param in spec.params:
        if param.required and param.choice_source and not _choice_list(param, config):
            if param.empty_reason:
                return _(param.empty_reason)
            return _("No {value} to choose from.").format(value=_(param.label))
    return None


def command_template(spec: OpSpec) -> str:
    """The command a person can read. No ``--config`` path from this machine."""
    tokens = ["cne", *spec.command, *spec.fixed]
    positionals = sorted(
        (param for param in spec.params if param.flag is None), key=lambda param: param.position
    )
    for param in (*positionals, *(param for param in spec.params if param.flag)):
        if param.flag is None:
            piece = param.name.upper()
        elif param.kind == "bool":
            piece = param.flag
        else:
            piece = f"{param.flag} {param.name.upper()}"
        tokens.append(piece if param.required else f"[{piece}]")
    return " ".join(tokens)


def describe(config, *, setup: bool) -> list[dict[str, Any]]:
    """Cards and form fields. Choices come from the loaded config."""
    cards = []
    for spec in OPS:
        # Computed text, so it is rendered once in each language rather than
        # looked up afterwards.
        with use_language("zh_CN"):
            reason = unavailable_reason(spec, config, setup=setup)
        with use_language("en"):
            reason_en = unavailable_reason(spec, config, setup=setup)
        cards.append(
            {
                "id": spec.id,
                "title": zh(spec.title),
                "title_en": en(spec.title),
                "summary": zh(spec.summary),
                "summary_en": en(spec.summary),
                "group": zh(spec.group),
                "group_en": en(spec.group),
                "command": command_template(spec),
                "confirm": spec.confirm,
                "result": spec.result,
                "available": reason is None,
                "unavailable_reason": reason,
                "unavailable_reason_en": reason_en,
                "params": [
                    {
                        "name": param.name,
                        "kind": param.kind,
                        "label": zh(param.label),
                        "label_en": en(param.label),
                        "help": zh(param.help),
                        "help_en": en(param.help),
                        "required": param.required,
                        "choices": _choice_list(param, config) or None,
                    }
                    for param in spec.params
                ],
            }
        )
    return cards


def _parse_date(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise OpsError(_("{label} must be YYYY-MM-DD.").format(label=label))
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise OpsError(_("{label} must be YYYY-MM-DD.").format(label=label)) from exc
    if parsed > shanghai_today():
        raise OpsError(_("{label} cannot be later than today (Beijing time).").format(label=label))
    return parsed.isoformat()


def _parse_symbols(value: object) -> list[str]:
    from cnequity.domain.symbols import parse_symbol

    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",") if part.strip()]
    elif isinstance(value, list):
        parts = [str(part).strip() for part in value if str(part).strip()]
    else:
        raise OpsError(_("Symbols must be comma-separated codes."))
    if len(parts) > 200:
        raise OpsError(_("At most 200 symbols at a time."))
    parsed = []
    for part in parts:
        try:
            parsed.append(parse_symbol(part).symbol)
        except ValueError as exc:
            raise OpsError(_("Unrecognized symbol {part}").format(part=part)) from exc
    return parsed


def _parse_run_id(value: object, config) -> str:
    if not isinstance(value, str) or not value or len(value) > 80:
        raise OpsError(_("Invalid run id."))
    if any(
        char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.:-"
        for char in value
    ):
        raise OpsError(_("Invalid run id."))
    if config is None or not Path(config.manifest_path).exists():
        raise OpsError(_("No run records yet."))
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.orchestrator.run_lock import is_run_locked

    record = Manifest(config.manifest_path).get_run(value)
    if record is None:
        raise OpsError(_("No run {value}.").format(value=value))
    if record["status"] == "running" and is_run_locked(config.meta_root, value):
        raise OpsError(_("Run {value} is still running.").format(value=value))
    return value


def _parse_text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise OpsError(_("{label} is invalid.").format(label=label))
    if not value:
        return ""
    if len(value) > 32 or any(
        char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
        for char in value
    ):
        raise OpsError(
            _("{label} may only contain letters, digits, underscores and hyphens.").format(
                label=label
            )
        )
    return value


def normalize(spec: OpSpec, raw: dict[str, Any] | None, config) -> dict[str, Any]:
    """Validate *raw* and return JSON-ready values for every parameter."""
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise OpsError(_("Invalid parameter format."))
    known = {param.name for param in spec.params}
    extra = sorted(set(raw) - known)
    if extra:
        raise OpsError(_("Cannot be specified: ") + _(", ").join(extra))
    values: dict[str, Any] = {}
    for param in spec.params:
        if param.name not in raw or raw[param.name] is None or raw[param.name] == "":
            if param.required:
                raise OpsError(_("Fill in {value}.").format(value=_(param.label)))
            if param.kind == "bool":
                values[param.name] = False
            elif param.kind == "multi":
                values[param.name] = []
            else:
                values[param.name] = None
            continue
        value = raw[param.name]
        if param.kind == "bool":
            if not isinstance(value, bool):
                raise OpsError(_("{value} must be a boolean.").format(value=_(param.label)))
            values[param.name] = value
        elif param.kind == "date":
            values[param.name] = _parse_date(value, _(param.label))
        elif param.kind == "choice":
            if not isinstance(value, str):
                raise OpsError(_("{value} is invalid.").format(value=_(param.label)))
            allowed = _choice_list(param, config)
            if value not in allowed:
                raise OpsError(_("{value} is not among the choices.").format(value=_(param.label)))
            values[param.name] = value
        elif param.kind == "multi":
            if isinstance(value, str):
                items = [part.strip() for part in value.split(",") if part.strip()]
            elif isinstance(value, list) and all(isinstance(part, str) for part in value):
                items = [part.strip() for part in value if part.strip()]
            else:
                raise OpsError(_("{value} is invalid.").format(value=_(param.label)))
            allowed = set(_choice_list(param, config))
            unknown = [item for item in items if item not in allowed]
            if unknown:
                raise OpsError(
                    _("{value} is not among the choices: {value2}").format(
                        value=_(param.label), value2=_(", ").join(unknown)
                    )
                )
            values[param.name] = items
        elif param.kind == "symbols":
            values[param.name] = _parse_symbols(value)
        elif param.kind == "text":
            values[param.name] = _parse_text(value, _(param.label)) or None
        elif param.kind == "run_id":
            values[param.name] = _parse_run_id(value, config)
        elif param.kind == "snapshot_name":
            from cnequity.storage.snapshots import SnapshotStore

            if not isinstance(value, str):
                raise OpsError(_("Invalid backup name."))
            try:
                values[param.name] = SnapshotStore._validate_name(value)
            except ValueError as exc:
                raise OpsError(
                    _("A backup name must be 1–80 letters, digits, dots, underscores or hyphens.")
                ) from exc
        elif param.kind == "path":
            if (
                not isinstance(value, str)
                or len(value) > 4096
                or any(ord(char) < 32 for char in value)
            ):
                raise OpsError(_("Invalid directory path."))
            from cnequity.ops.backups import restore_target

            values[param.name] = str(restore_target(config, value, raw.get("snapshot_root")))
        elif param.kind == "directory":
            from cnequity.storage.snapshots import _reject_symlink_path

            if (
                not isinstance(value, str)
                or not value
                or len(value) > 4096
                or any(ord(char) < 32 for char in value)
            ):
                raise OpsError(_("Invalid backup directory path."))
            path = Path(value).expanduser()
            if not path.is_absolute():
                raise OpsError(_("The backup directory must be an absolute path."))
            _reject_symlink_path(path, label="snapshot root")
            if path.exists() and not path.is_dir():
                raise OpsError(_("The backup storage directory is not a directory."))
            values[param.name] = str(path)
        else:
            raise OpsError(_("Unknown parameter type {kind}").format(kind=param.kind))
    _check_cross(spec, values, config)
    return values


def _check_cross(spec: OpSpec, values: dict[str, Any], config) -> None:
    start, end = values.get("start"), values.get("end")
    if start and end and start > end:
        raise OpsError(_("The start cannot be after the end."))
    if spec.id == "snapshot.create" and not values.get("datasets"):
        raise OpsError(_("Select at least one dataset to back up."))
    if spec.id == "snapshot.create":
        from cnequity.ops.backups import create_root

        create_root(config, values.get("snapshot_root"))
    if spec.id == "init.resume":
        if config is None:
            raise OpsError(_("Not configured yet."))
        from cnequity.orchestrator.manifest import Manifest

        latest = None
        if Path(config.manifest_path).exists():
            latest = Manifest(config.manifest_path).latest_incomplete_init_run()
        if latest is None or latest["run_id"] != values["run_id"]:
            raise OpsError(_("Only the latest unfinished init can be resumed."))
    if spec.id == "init.start" and values.get("profile") not in {"quick", "full"}:
        raise OpsError(_("Init depth can only be quick or full."))


def confirm_level(spec: OpSpec, params: dict[str, Any]) -> str:
    if spec.id == "backfill.run":
        symbols = params.get("symbols") or []
        start, end = params.get("start"), params.get("end")
        if not symbols or not start:
            return "heavy"
        finish = date.fromisoformat(end) if end else shanghai_today()
        if (finish - date.fromisoformat(start)).days > 366:
            return "heavy"
        return "standard"
    if spec.id == "derive.run" and params.get("full"):
        return "heavy"
    return spec.confirm


def acknowledgements(spec: OpSpec, level: str) -> list[dict[str, str]]:
    # Clicking Start after a preview is the confirmation for routine work; a
    # checkbox that only repeats "start this now" adds a click and no
    # information. Heavy work still names its specific cost.
    if level != "heavy":
        return []
    if spec.heavy_text:
        return [{"id": "heavy", "text": zh(spec.heavy_text), "text_en": en(spec.heavy_text)}]
    text = STANDARD_ACK["text"]
    return [{**STANDARD_ACK, "text": zh(text), "text_en": en(text)}]


def lock_class_for(spec: OpSpec, params: dict[str, Any], config) -> str:
    if spec.lock_class != "run":
        return spec.lock_class
    from cnequity.orchestrator.engine import job_family
    from cnequity.orchestrator.manifest import Manifest

    record = Manifest(config.manifest_path).get_run(params["run_id"])
    family = job_family(str(record["job_name"]))
    if family == "daily":
        return "ingest-daily"
    if family == "events":
        return "ingest-events"
    if family == "init":
        return "init"
    return "lake-write"


def target_run_id(spec: OpSpec, params: dict[str, Any]) -> str | None:
    if spec.id in {"run.retry", "init.resume", "run.compact"}:
        return params.get("run_id")
    return None


def build_argv(spec: OpSpec, params: dict[str, Any], config_path: Path | None) -> list[str]:
    argv = [*spec.command, *spec.fixed]
    for param in sorted(
        (item for item in spec.params if item.flag is None), key=lambda item: item.position
    ):
        value = params.get(param.name)
        if value is not None:
            argv.append(str(value))
    for param in spec.params:
        if param.flag is None:
            continue
        value = params.get(param.name)
        if param.kind == "bool":
            if value:
                argv.append(param.flag)
            continue
        if value is None or value == [] or value == "":
            continue
        if param.kind in {"multi", "symbols"}:
            if param.repeat:
                for item in value:
                    argv.extend([param.flag, item])
                continue
            argv.extend([param.flag, ",".join(value)])
            continue
        argv.extend([param.flag, str(value)])
    if config_path is not None:
        argv.extend(["--config", str(config_path)])
    return argv


def command_line(argv: list[str]) -> str:
    return "cne " + shlex.join(argv)


def config_file(config) -> Path | None:
    path = getattr(config, "config_path", None) if config is not None else None
    if path is None:
        return None
    resolved = Path(path).expanduser().resolve()
    return resolved if resolved.is_file() else None


def pass_config(spec: OpSpec, config) -> Path | None:
    """Absolute ``--config``, or None when doctor is running before a file exists."""
    path = config_file(config)
    if path is None and spec.id == "diag.doctor":
        return None
    return path


def prepared(spec: OpSpec, raw: dict[str, Any] | None, config) -> dict[str, Any]:
    """Normalized parameters, argv and the confirmation this request needs."""
    reason = unavailable_reason(spec, config, setup=config is None)
    if reason and spec.id != "diag.doctor":
        raise OpsError(reason)
    params = normalize(spec, raw, config)
    if spec.id.startswith("snapshot."):
        from cnequity.ops.backups import store_for

        params["snapshot_root"] = str(store_for(config, params.get("snapshot_root")).root)
    path = pass_config(spec, config)
    if path is None and spec.id != "diag.doctor":
        raise OpsError(
            _(
                "This service has no writable config file path, so it cannot be started from the dashboard."
            )
        )
    level = confirm_level(spec, params)
    argv = build_argv(spec, params, path)
    return {
        "params": params,
        "argv": argv,
        "command": command_line(argv),
        "lock_class": (
            spec.lock_class if spec.lock_class != "run" else lock_class_for(spec, params, config)
        ),
        "confirm": level,
        "acknowledgements": acknowledgements(spec, level),
        "target_run_id": target_run_id(spec, params),
        "config_path": str(path) if path is not None else None,
        "result": spec.result,
        "title": zh(spec.title),
        "title_en": en(spec.title),
    }
