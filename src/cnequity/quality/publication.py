"""Offline candidate-lake audit before any compact pointer is published."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from collections import Counter
from contextlib import closing
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS
from cnequity.quality.factor_policy import advisory_factor_findings, factor_advisory
from cnequity.quality.gate import contradictions
from cnequity.sqlite_readonly import connect_readonly
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.locator import layer_root, logical_root
from cnequity.storage.read_context import read_root

# Coverage gaps (absent observations) remain audit findings and explicit
# quality gates may reject them; they do not invalidate independent
# publishable rows, so publication counts contradictions only (see
# ``cnequity.quality.gate``).


def _link_read_only(source: str, target: str) -> str:
    # The audit never writes Parquet. Metadata is copied separately; no link
    # escapes the temporary audit view and no linked file is ever published.
    try:
        os.link(source, target)
    except OSError:
        shutil.copy2(source, target)
    return target


def _clone_lake(config: Config, view: Config, catalog_datasets: set[str]) -> None:
    """Give *view* a lake catalog of its own that holds what *config*'s publishes.

    Data files are immutable, so they are hard links; the catalog database is
    copied. Datasets published only into the catalog are found through their
    pointer and receipt, which are copied too. Calling it again resets the
    view's lake, undoing a rehearsed publish.
    """
    from cnequity.query import catalog_read
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core.catalog import CATALOG_FILE, DATA_DIR

    source, target = lake_root(config), lake_root(view)
    catalog_read.close(view)
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    shutil.copytree(source / DATA_DIR, target / DATA_DIR, copy_function=_link_read_only)
    with (
        closing(connect_readonly(source / CATALOG_FILE)) as original,
        closing(sqlite3.connect(target / CATALOG_FILE)) as copied,
    ):
        original.backup(copied)
        copied.execute("PRAGMA journal_mode=WAL")
    # A rehearsed publish leaves its pointer and state in the view; drop them,
    # so a dataset the real lake keeps as a generation is read as one again.
    for stale in (view.meta_root / "revisions").glob("*/current.json"):
        stale.unlink()
        real_state = config.meta_root / "state" / f"{stale.parent.name}.json"
        view_state = view.meta_root / "state" / real_state.name
        if real_state.is_file():
            shutil.copy2(real_state, view_state)
        else:
            view_state.unlink(missing_ok=True)
    for name in sorted(catalog_datasets):
        pointer = config.meta_root / "revisions" / name / "current.json"
        if not pointer.is_file():
            continue
        payload = json.loads(pointer.read_text(encoding="utf-8"))
        for relative in (pointer.relative_to(config.meta_root), Path(payload.get("receipt", ""))):
            if relative.name and (config.meta_root / relative).is_file():
                stored = view.meta_root / relative
                stored.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(config.meta_root / relative, stored)
        state = config.meta_root / "state" / f"{name}.json"
        if state.is_file():
            (view.meta_root / "state").mkdir(parents=True, exist_ok=True)
            shutil.copy2(state, view.meta_root / "state" / state.name)


def _read_candidate_tree(view: Config, names) -> None:
    """Make *view* read each of *names* from its dataset tree, not from the catalog.

    A candidate handed over as a whole tree replaces the dataset in the view.
    A pointer left there would send readers to the catalog's published rows
    and the candidate would never be audited.
    """
    for name in names:
        (view.meta_root / "revisions" / name / "current.json").unlink(missing_ok=True)


def _read_published(config: Config, view: Config, name: str) -> None:
    """Make *view* read *name* as the lake publishes it from its catalog again."""
    pointer = config.meta_root / "revisions" / name / "current.json"
    stored = view.meta_root / "revisions" / name / "current.json"
    stored.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(pointer, stored)


def _has_catalog(config: Config) -> bool:
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core.catalog import CATALOG_FILE

    return (lake_root(config) / CATALOG_FILE).exists()


def _catalog_datasets(config: Config) -> set[str]:
    """Datasets whose published revision lives only in the lake catalog."""
    names = set()
    for name in DATASETS:
        pointer = config.meta_root / "revisions" / name / "current.json"
        try:
            if json.loads(pointer.read_text(encoding="utf-8")).get("storage") == "catalog":
                names.add(name)
        except (OSError, ValueError):
            continue
    return names


def _rehearse(lake_write, view: Config, names) -> None:
    """Publish the checkouts of *names* into *view*'s lake, as the run would into the real one."""
    from cnequity.domain.contracts import contract_fingerprint, dataset_contract
    from cnequity.query import catalog_read
    from cnequity.storage.lake_write import DatasetPublish

    catalog_read.close(view)
    lake_write.rehearse(
        view,
        {
            name: DatasetPublish(
                schema_version=int(dataset_contract(name)["schema_version"]),
                contract_fingerprint=contract_fingerprint(dataset_contract(name)),
            )
            for name in names
        },
        message="publication rehearsal",
    )


def _errors(
    config: Config, day: date, scope: dict[str, frozenset[str] | None] | None = None
) -> list[dict]:
    from cnequity.quality.audit import _collect_lake_findings, _profile_adjusted_findings
    from cnequity.quality.source_diff import run_source_diffs

    findings = _profile_adjusted_findings(
        config, _collect_lake_findings(config, day, full=True, offline=True, scope=scope)
    )
    findings.extend(run_source_diffs(config, "publication-view", day))
    return contradictions(findings)


def _factor_action_errors(config: Config, symbols: frozenset[str]) -> list[dict]:
    """A complete per-security contradiction set for a repair candidate."""
    if not symbols:
        return []
    from cnequity.quality.cross_checks import factor_action_contradictions

    contradictions, _ = factor_action_contradictions(config, symbols=symbols)
    rows = contradictions.to_dicts()
    if not rows:
        return []
    return [
        {
            "dataset": "adj_factors",
            "check": "repair_factor_action_contradictions",
            "severity": "error",
            "violations_complete": True,
            "violations": {f"{row['symbol']}|{row['ex_date']}|{row['kind']}": 1 for row in rows},
        }
    ]


def _repair_actions(view: Config, symbols: frozenset[str]) -> pl.DataFrame:
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = view.curated_root / "corporate_actions"
    if not symbols or not dataset_has_parquet(root):
        return pl.DataFrame(schema={"symbol": pl.Utf8, "ex_date": pl.Date, "action_type": pl.Utf8})
    return (
        scan_parquet_root(root, partition_col="ex_date", symbols=sorted(symbols))
        .select("symbol", "ex_date", "action_type")
        .unique()
        .collect()
    )


def _halt_dated_removals(view: Config, before: pl.DataFrame, after: pl.DataFrame) -> list[dict]:
    """Actions a repair removed from a date the market traded but the security did not.

    Such a date is an ex-date inside a trading halt: genuine, with its factor
    step on resumption. Moving it there leaves every factor check unchanged,
    so only this invariant can catch it. A date the whole market was shut
    (a Saturday in an old TDX record) is not protected.
    """
    from cnequity.query.parquet_scan import scan_parquet_root

    removed = before.join(after, on=["symbol", "ex_date", "action_type"], how="anti")
    if removed.is_empty():
        return []
    bars_root = view.curated_root / "daily_bars"
    from cnequity.query.parquet_scan import dataset_has_parquet

    if not dataset_has_parquet(bars_root):
        # No bars, no evidence the market was open on any of these days.
        return []
    # "The market was open" means some security has a bar that day. Ask the
    # rows, not the layout: a day directory only answered it while daily_bars
    # was day-partitioned, and a month partition would have read every
    # ex-date as a closed market and waved every removal through.
    ex_dates = removed.get_column("ex_date").unique()
    open_days = (
        scan_parquet_root(
            bars_root,
            partition_col="trade_date",
            start=ex_dates.min(),
            end=ex_dates.max(),
        )
        .filter(pl.col("trade_date").is_in(ex_dates.implode()))
        .select(pl.col("trade_date").alias("ex_date"))
        .unique()
        .collect()
    )
    market_open = removed.join(open_days, on="ex_date", how="semi")
    if market_open.is_empty():
        return []
    traded = (
        scan_parquet_root(
            bars_root,
            partition_col="trade_date",
            symbols=market_open.get_column("symbol").unique().to_list(),
            traded_only=True,
        )
        .select("symbol", pl.col("trade_date").alias("ex_date"))
        .unique()
        .collect()
    )
    halted = market_open.join(traded, on=["symbol", "ex_date"], how="anti")
    if halted.is_empty():
        return []
    return [
        {
            "dataset": "corporate_actions",
            "check": "repair_removed_halt_dated_action",
            "severity": "error",
            "message": (
                f"repair removes {halted.height} action(s) dated inside a trading halt; "
                "a halt-dated ex-date is genuine and its step lands on resumption"
            ),
            "violations_complete": True,
            "violations": {
                f"{row['symbol']}|{row['ex_date']}|{row['action_type']}": 1
                for row in halted.iter_rows(named=True)
            },
        }
    ]


# Where a finding is. Its identity survives a changed count, message or
# sample: a finding that shrinks from 2 bad rows to 1 is the same issue,
# improved, not a new error.
_SCOPE_KEYS = (
    "dataset",
    "check",
    "partition_col",
    "partition_value",
    "partition",
    "column",
    "source",
    "trade_date",
    "exchange",
    "exchanges",
    "code",
    "item_code",
    "report_period",
)
# How much of it there is; a larger value is a worse finding. A list counts
# by its length (``missing_symbols`` is a count in some checks, a list in
# others).
_MEASURE_KEYS = (
    "duplicate_rows",
    "invalid_rows",
    "rows",
    "rows_rejected",
    "missing_keys",
    "missing_symbols",
    "disagreeing_symbols",
    "implausible_share",
    "worst_bps",
)


def issue_key(finding: dict) -> str:
    """Stable identity of a finding: rule plus scope, without its measurements."""
    scope = {key: finding[key] for key in _SCOPE_KEYS if finding.get(key) is not None}
    return json.dumps(scope, sort_keys=True, default=str)


def _measures(finding: dict) -> dict[str, float]:
    out: dict[str, float] = {}
    for key in _MEASURE_KEYS:
        value = finding.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            out[key] = float(value)
        elif isinstance(value, (list, tuple, set, dict)):
            out[key] = float(len(value))
    return out


# Complete sets of named entities (source label → datasets): a name absent
# from the baseline is a new problem even when the count stays the same.
_ENTITY_KEYS = ("sources", "mismatches")


def compare_issue(baseline: dict, candidate: dict) -> str:
    """``worsened``, ``improved`` or ``unchanged`` for two findings of one key.

    Complete per-entity evidence decides first: any entity absent from the
    baseline, or with more excess rows, is a regression even when the total
    fell. Otherwise shared measures decide.
    """
    old, new = baseline.get("violations"), candidate.get("violations")
    if (
        isinstance(old, dict)
        and isinstance(new, dict)
        and baseline.get("violations_complete")
        and candidate.get("violations_complete")
    ):
        if any(key not in old or value > old[key] for key, value in new.items()):
            return "worsened"
        return "improved" if new != old else "unchanged"
    for key in _ENTITY_KEYS:
        old_names, new_names = baseline.get(key), candidate.get(key)
        if isinstance(old_names, dict) and isinstance(new_names, dict):
            if any(
                name not in old_names or set(value) - set(old_names[name])
                for name, value in new_names.items()
            ):
                return "worsened"
    before, after = _measures(baseline), _measures(candidate)
    shared = before.keys() & after.keys()
    if any(after[key] > before[key] for key in shared):
        return "worsened"
    if any(after[key] < before[key] for key in shared):
        return "improved"
    return "unchanged"


def classify_issues(baseline: list[dict], candidate: list[dict]) -> dict[str, list[dict]]:
    """Sort candidate findings against the baseline by stable issue key."""

    def grouped(findings: list[dict]) -> dict[str, list[dict]]:
        out: dict[str, list[dict]] = {}
        for finding in findings:
            out.setdefault(issue_key(finding), []).append(finding)
        for items in out.values():
            items.sort(key=lambda item: json.dumps(item, sort_keys=True, default=str))
        return out

    before, after = grouped(baseline), grouped(candidate)
    result: dict[str, list[dict]] = {
        name: [] for name in ("introduced", "worsened", "improved", "unchanged", "resolved")
    }
    for key in sorted(before.keys() | after.keys()):
        old, new = before.get(key, []), after.get(key, [])
        for previous, current in zip(old, new, strict=False):
            result[compare_issue(previous, current)].append(current)
        result["introduced"].extend(new[len(old) :])
        result["resolved"].extend(old[len(new) :])
    return result


def _new_errors(baseline: list[dict], candidate: list[dict]) -> list[dict]:
    """Candidate errors that are new or measurably worse than the baseline."""
    changes = classify_issues(baseline, candidate)
    return [*changes["introduced"], *changes["worsened"]]


def _summary(changes: dict[str, list[dict]]) -> dict:
    return {
        name: [{"issue": issue_key(item), **_measures(item)} for item in items]
        for name, items in changes.items()
    }


def audit_scope(
    candidates: dict[str, Path], changed: dict[str, list[Path]] | None
) -> dict[str, frozenset[str] | None]:
    """Datasets (and partitions) whose structural findings can differ.

    Without a change list, or when a root file changed, the whole dataset is
    in scope.
    """
    scope: dict[str, frozenset[str] | None] = {}
    for name, root in candidates.items():
        files = (changed or {}).get(name)
        if not files:
            scope[name] = None
            continue
        tops = {Path(path).relative_to(root).parts for path in files}
        scope[name] = (
            None if any(len(parts) < 2 for parts in tops) else frozenset(parts[0] for parts in tops)
        )
    return scope


def evaluate_publication(
    config: Config,
    run_id: str,
    day: date,
    candidates: dict[str, Path],
    changed: dict[str, list[Path]] | None = None,
    *,
    repair_symbols: frozenset[str] = frozenset(),
    lake_write=None,
) -> dict:
    """Audit the candidates against the committed lake; block new or worse errors.

    Structural checks run only for the candidate datasets, limited to the
    partitions in ``changed`` when given; the deep historical sweep of every
    dataset belongs to ``cne audit --full``.

    A lake that publishes into its catalog gets a clone of the catalog in the
    audit view, so datasets that exist only there are read as published. A
    candidate that *lake_write* holds as a checkout is a set of changed
    partitions, not a whole dataset tree: the checkouts are published into the
    clone as the run would publish them. A candidate given as a whole tree (a
    repair's working tree) replaces the dataset in the view.
    """
    mode = config.publication_gate
    if mode == "off" or not candidates:
        return {"mode": mode, "blocked": False, "new_errors": []}
    scope = audit_scope(candidates, changed)
    report = {
        "mode": mode,
        "datasets": sorted(candidates),
        "run_id": run_id,
        "trade_date": str(day),
        "repair_symbols": sorted(repair_symbols),
        "audit_scope": {
            name: None if parts is None else sorted(parts) for name, parts in scope.items()
        },
    }
    try:
        with tempfile.TemporaryDirectory(prefix="cnequity-publication-") as directory:
            root = Path(directory)
            view = replace(config, data_root=root)
            for name, spec in DATASETS.items():
                source = read_root(config, name)
                if source.is_dir():
                    target = logical_root(view, spec)
                    shutil.copytree(source, target, copy_function=_link_read_only)
            # Isolate every metadata write performed by audit helpers. No
            # pointers are copied: they would resolve back to published data.
            for relative in (
                "state",
                "quality/coverage",
                "quality/evidence",
                "source_snapshots",
                "research",
                "seeds",
            ):
                source = config.meta_root / relative
                if source.is_dir():
                    shutil.copytree(source, view.meta_root / relative)
            view.meta_root.mkdir(parents=True, exist_ok=True)
            if config.manifest_path.exists():
                # SQLite's context manager ends transactions but does not close
                # connections. Windows needs both handles closed before cleanup.
                with (
                    closing(connect_readonly(config.manifest_path)) as original,
                    closing(sqlite3.connect(view.manifest_path)) as copied,
                ):
                    original.backup(copied)
            rehearsed = sorted(
                name for name in candidates if lake_write is not None and lake_write.has(name)
            )
            trees = sorted(name for name in candidates if name not in rehearsed)
            catalog_only = _catalog_datasets(config) if _has_catalog(config) else set()
            cloned = lake_write is not None or bool(catalog_only)
            if cloned:
                _clone_lake(config, view, catalog_only)
            baseline = [item for item in _errors(view, day, scope) if not factor_advisory(item)]
            factor_baseline = advisory_factor_findings(_factor_action_errors(view, repair_symbols))
            guard_actions = "corporate_actions" in candidates and bool(repair_symbols)
            actions_before = _repair_actions(view, repair_symbols) if guard_actions else None
            for name, source in candidates.items():
                if name in rehearsed:
                    continue
                layer = layer_root(view, name)
                target = layer / name
                if target.exists():
                    shutil.rmtree(target)
                shutil.copytree(source, target, copy_function=_link_read_only)
            _read_candidate_tree(view, trees)
            if rehearsed:
                _rehearse(lake_write, view, rehearsed)
            candidate_errors = [
                item for item in _errors(view, day, scope) if not factor_advisory(item)
            ]
            report["factor_diagnostics"] = {
                "baseline": factor_baseline,
                "candidate": advisory_factor_findings(_factor_action_errors(view, repair_symbols)),
                "gate_impact": "none",
            }
            if actions_before is not None:
                candidate_errors.extend(
                    _halt_dated_removals(
                        view, actions_before, _repair_actions(view, repair_symbols)
                    )
                )
            changes = classify_issues(baseline, candidate_errors)
            introduced = [*changes["introduced"], *changes["worsened"]]
            report.update(
                baseline_errors=baseline,
                candidate_errors=candidate_errors,
                new_errors=introduced,
                issue_changes=_summary(changes),
            )
            blocked_datasets: set[str] = set()
            if mode == "block" and introduced and len(candidates) == 1:
                # One candidate: nothing else could have introduced it.
                blocked_datasets.update(candidates)
            elif mode == "block" and introduced:
                # Attribute a new finding by removing each candidate in turn.
                # This also catches cross-dataset checks: if either side's
                # removal resolves the finding, both sides are held together.
                introduced_keys = Counter(issue_key(item) for item in introduced)
                for name, source in candidates.items():
                    if name in rehearsed:
                        # Rehearse the publish again without this candidate.
                        _clone_lake(config, view, catalog_only)
                        _read_candidate_tree(view, trees)
                        others = [other for other in rehearsed if other != name]
                        if others:
                            _rehearse(lake_write, view, others)
                        remaining = Counter(
                            issue_key(item)
                            for item in _new_errors(baseline, _errors(view, day, scope))
                        )
                        if any(remaining[key] < count for key, count in introduced_keys.items()):
                            blocked_datasets.add(name)
                        continue
                    layer = (
                        view.derived_root
                        if DATASETS[name].layer == "derived"
                        else view.curated_root
                    )
                    target = layer / name
                    shutil.rmtree(target)
                    original = read_root(config, name)
                    if original.is_dir():
                        shutil.copytree(original, target, copy_function=_link_read_only)
                    elif name in catalog_only:
                        _read_published(config, view, name)
                    try:
                        remaining = Counter(
                            issue_key(item)
                            for item in _new_errors(baseline, _errors(view, day, scope))
                        )
                    finally:
                        if target.exists():
                            shutil.rmtree(target)
                        shutil.copytree(source, target, copy_function=_link_read_only)
                        _read_candidate_tree(view, [name])
                    if any(remaining[key] < count for key, count in introduced_keys.items()):
                        blocked_datasets.add(name)
                if not blocked_datasets:
                    # A check with no attributable input is unsafe to publish.
                    blocked_datasets.update(candidates)
            report["blocked_datasets"] = sorted(blocked_datasets)
            if cloned:
                # Release the clone's catalog before its directory is removed.
                from cnequity.query import catalog_read

                catalog_read.close(view)
    except Exception as exc:
        # In block mode an audit that could not inspect the candidate is not a
        # successful publication check. Preserve the reason for a safe retry.
        report["new_errors"] = [
            {
                "dataset": "publication",
                "check": "candidate_audit_failed",
                "severity": "error",
                "message": str(exc),
            }
        ]
        report["blocked_datasets"] = sorted(candidates) if mode == "block" else []
    finally:
        if lake_write is not None or _has_catalog(config):
            from cnequity.query import catalog_read

            catalog_read.close_all()
    report["blocked"] = bool(report.get("blocked_datasets"))
    path = config.meta_root / "quality" / "publication" / f"{run_id}.json"
    write_json_atomic(path, report, indent=2, default=str)
    report["report_path"] = str(path)
    return report


def check_repair_publication(
    config: Config,
    dataset: str,
    run_id: str,
    changed_files: list[Path],
    *,
    symbols: frozenset[str] = frozenset(),
    lake_write=None,
) -> dict:
    """Require a clean candidate comparison before an offline repair commits.

    The candidate is the dataset's working tree, or, when *lake_write* holds
    a checkout of it, the partitions that write would publish.
    """
    from cnequity.storage.revisions import RevisionStore

    checked_out = lake_write is not None and lake_write.has(dataset)
    candidate = (
        lake_write.checkout(dataset).root if checked_out else layer_root(config, dataset) / dataset
    )
    report = evaluate_publication(
        replace(config, publication_gate="block"),
        run_id,
        datetime.now(timezone.utc).date(),
        {dataset: candidate},
        {dataset: changed_files},
        repair_symbols=symbols,
        lake_write=lake_write if checked_out else None,
    )
    if report["blocked"]:
        if checked_out:
            # Published data was never touched; keep the checkout for inspection.
            quarantine = lake_write.discard(dataset)
        else:
            store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
            quarantine = store.quarantine_candidate(dataset, run_id=run_id, reason="repair_gate")
        raise RuntimeError(
            f"repair publication gate blocked {dataset}: {report.get('report_path')}"
            + (f"; candidate: {quarantine}" if quarantine else "")
        )
    return report
