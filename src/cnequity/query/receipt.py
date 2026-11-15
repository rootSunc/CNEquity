"""A data read and the evidence needed to interpret or replay that read.

The receipt describes the rows actually returned. In particular, observed date
bounds are not a claim that every security or session in the window is covered.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from cnequity.config import Config
from cnequity.domain.market_profile import OTC_SCOPED_DATASETS
from cnequity.query.reader import (
    ADJUSTABLE_DATASETS,
    DATE_COLUMNS,
    PIT_DATASETS,
    _has_listing_dates,
    _merge_revision_selection,
    _revision_for_dataset,
    load,
    resolve_config,
)
from cnequity.query.state import DatasetState, dataset_state
from cnequity.storage.revisions import RevisionStore

logger = logging.getLogger(__name__)


class ReadReceiptError(RuntimeError):
    """The requested read cannot carry the claimed data identity."""


@dataclass(frozen=True)
class ReadResult:
    frame: pl.DataFrame
    receipt: dict[str, Any]


def _dependencies(dataset: str, options: Mapping[str, Any], config: Config) -> set[str]:
    dependencies = {dataset}
    if dataset == "flash_news_wire":
        dependencies.add("news_headlines")
    if dataset in ADJUSTABLE_DATASETS and options.get("adjust"):
        dependencies.add("adj_factors")
    if options.get("adjust") == "total_return":
        dependencies.update({"corporate_actions", "instruments"})
    if options.get("universe") or options.get("profile") or options.get("universe_profile"):
        dependencies.update({"instruments", "trading_status", "trading_calendar"})
    if (
        dataset in OTC_SCOPED_DATASETS
        and not options.get("include_otc")
        and _has_listing_dates(config)
    ):
        dependencies.add("instruments")
    return dependencies


def _receipt_for_revision(
    config: Config, dataset: str, revision_id: str, current: DatasetState
) -> dict[str, Any]:
    if revision_id == current.revision_id:
        return {
            "revision": current.revision,
            "revision_id": current.revision_id,
            "revision_at": current.revision_at,
            "schema_version": current.schema_version,
            "contract_fingerprint": current.contract_fingerprint,
            "content_digest": current.content_digest,
            "revision_receipt": current.revision_receipt,
        }
    from cnequity.storage.revisions import RevisionStore

    store = RevisionStore(config.meta_root, config.curated_root, config.derived_root, create=False)
    recorded = store.recorded_revision(dataset, revision_id)
    if recorded is not None:
        relative = f"revisions/{dataset}/{recorded.revision:08d}-{revision_id}.json"
        return {
            "revision": recorded.revision,
            "revision_id": revision_id,
            "revision_at": recorded.committed_at,
            "schema_version": recorded.schema_version,
            "contract_fingerprint": recorded.contract_fingerprint,
            "content_digest": recorded.content_digest,
            "revision_receipt": relative if (config.meta_root / relative).is_file() else None,
        }
    directory = config.meta_root / "revisions" / dataset
    if directory.is_symlink():
        raise ReadReceiptError(f"revision receipt directory is a symlink: {dataset}")
    matches = list(directory.glob(f"*-{revision_id}.json"))
    if len(matches) != 1 or matches[0].is_symlink():
        raise ReadReceiptError(f"historical revision receipt unavailable: {dataset}/{revision_id}")
    payload = json.loads(matches[0].read_text(encoding="utf-8"))
    if payload.get("dataset") != dataset or payload.get("revision_id") != revision_id:
        raise ReadReceiptError(f"historical revision receipt mismatch: {dataset}/{revision_id}")
    return {
        "revision": payload.get("revision"),
        "revision_id": revision_id,
        "revision_at": payload.get("committed_at"),
        "schema_version": payload.get("schema_version"),
        "contract_fingerprint": payload.get("contract_fingerprint"),
        "content_digest": payload.get("content_digest"),
        "revision_receipt": matches[0].relative_to(config.meta_root).as_posix(),
    }


def _counts(frame: pl.DataFrame, column: str) -> dict[str, int]:
    if column not in frame.columns or frame.is_empty():
        return {}
    return {
        str(row[column]): int(row["count"])
        for row in frame.group_by(column).agg(pl.len().alias("count")).sort(column).to_dicts()
    }


def _bound(frame: pl.DataFrame, column: str, operation: str) -> str | None:
    if frame.is_empty() or column not in frame.columns:
        return None
    value = getattr(frame[column], operation)()
    return value.isoformat() if isinstance(value, (date, datetime)) else str(value)


def _request_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_request_value(item) for item in value]
    return value


def load_with_receipt(
    dataset: str,
    *,
    config: Config | None = None,
    data_root: str | Path | None = None,
    require_replayable: bool = False,
    **options: Any,
) -> ReadResult:
    """Read once with explicit generations and return a JSON-safe receipt.

    Each available dependency revision is captured before the query and passed
    to ``load``. A legacy dependency without a revision remains readable, but
    the receipt declares it non-replayable. ``require_replayable`` fails before
    the scan in that case. Quality and coverage are reported as observations;
    they are never inferred from a watermark or the existence of a revision.
    """

    cfg = resolve_config(config=config, data_root=data_root)
    dependencies = _dependencies(dataset, options, cfg)
    states = {name: dataset_state(name, config=cfg) for name in sorted(dependencies)}
    selection = _merge_revision_selection(options.get("revision"), options.get("revision_map"))
    pins: dict[str, int | str] = {}
    for name in sorted(dependencies):
        requested = (
            _revision_for_dataset(selection, name, fallback_primary=name == dataset)
            if name == dataset or isinstance(selection, Mapping)
            else None
        )
        selected = requested if requested is not None else states[name].revision_id
        if selected is not None:
            pins[name] = selected
    unpinned = sorted(dependencies - pins.keys())
    if require_replayable and unpinned:
        raise ReadReceiptError(f"no committed revision for: {', '.join(unpinned)}")

    identities: dict[str, dict[str, Any]] = {}
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root, create=False)
    for name in sorted(dependencies):
        state = states[name]
        if name in pins:
            root = store.current_root(name, revision=pins[name])
            if root is None:
                raise ReadReceiptError(f"revision generation unavailable: {name}/{pins[name]}")
            identities[name] = _receipt_for_revision(cfg, name, root.name, state)
        else:
            identities[name] = {
                "revision": None,
                "revision_id": None,
                "revision_at": None,
                "schema_version": state.schema_version,
                "contract_fingerprint": state.contract_fingerprint,
                "content_digest": None,
                "revision_receipt": None,
                "updated_at": state.updated_at,
            }

    read_options = {**options, "revision": None, "revision_map": pins}
    frame = load(dataset, config=cfg, **read_options)
    for name in unpinned:
        if dataset_state(name, config=cfg).updated_at != states[name].updated_at:
            raise ReadReceiptError(f"unversioned dataset changed during read: {name}")

    date_column = DATE_COLUMNS.get(dataset)
    request_keys = (
        "start",
        "end",
        "as_of",
        "adjust",
        "universe",
        "profile",
        "universe_profile",
        "symbols",
        "items",
        "strict_adj",
        "strict_universe",
        "all_vintages",
        "pit_mode",
    )
    request = {key: _request_value(options[key]) for key in request_keys if key in options}
    pit_mode = options.get("pit_mode") if dataset in PIT_DATASETS else "not_applicable"
    if dataset in PIT_DATASETS and pit_mode is None:
        pit_mode = "best_effort"
    payload: dict[str, Any] = {
        "schema_version": 1,
        "dataset": dataset,
        "request": request,
        "dependencies": identities,
        "replayable": not unpinned,
        "unpinned_datasets": unpinned,
        "coverage": {
            "kind": "observed_rows_only",
            "requested_start": request.get("start"),
            "requested_end": request.get("end"),
            "date_column": date_column,
            "observed_start": _bound(frame, date_column, "min") if date_column else None,
            "observed_end": _bound(frame, date_column, "max") if date_column else None,
            "row_count": frame.height,
            "completeness": "not_assessed",
        },
        "provenance": {
            "returned_row_sources": _counts(frame, "source"),
            "returned_row_data_versions": _counts(frame, "data_version"),
            "fetched_at_start": _bound(frame, "fetched_at", "min"),
            "fetched_at_end": _bound(frame, "fetched_at", "max"),
        },
        "pit": {
            "mode": pit_mode,
            "quality_counts": _counts(frame, "pit_quality"),
            "exact_counts": _counts(frame, "pit_is_exact"),
        },
    }
    if options.get("adjust") and dataset in ADJUSTABLE_DATASETS:
        from cnequity.quality.factor_diagnostics import factor_read_quality
        from cnequity.query.reader import _read_dataset
        from cnequity.storage.read_context import ReadContext

        context = ReadContext.capture(cfg, ("adj_factors",), pins)
        factors = None
        if not frame.is_empty():
            try:
                factors = _read_dataset(
                    cfg,
                    "adj_factors",
                    symbols=frame["symbol"].unique().to_list(),
                    start=frame["trade_date"].min(),
                    end=frame["trade_date"].max(),
                    read_context=context,
                )
            except (OSError, ValueError, pl.exceptions.PolarsError):
                pass  # Optional diagnostics cannot invalidate a successful read.
        try:
            payload["adjustment"] = factor_read_quality(
                cfg, frame, factor_root=context.roots["adj_factors"], factors=factors
            )
        except Exception as exc:  # noqa: BLE001 — diagnostics never fail a read
            logger.warning("factor diagnostics unavailable for this read: %s", exc)
            payload["adjustment"] = {
                "gate_impact": "none",
                "historical_accuracy": "not_certified",
                "diagnostics_assessed": False,
            }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["receipt_id"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return ReadResult(frame=frame, receipt=payload)
