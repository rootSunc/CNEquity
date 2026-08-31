"""Shared step helper for EastMoney / CNINFO HTTP datasets."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.steps.common import fetch_incremental_daily, write_simple
from cnequity.storage.raw_archive import (
    RawArchiveError,
    RawPayloadArchive,
    RawPayloadRecord,
    capture_is_consumed,
    capture_publish,
    captured_records,
)
from cnequity.storage.raw_archive import (
    capture_nonce as active_capture_nonce,
)
from cnequity.storage.state import StateStore

_RAW_ARCHIVE_EVIDENCE_SEAL = object()


@dataclass(frozen=True, slots=True, init=False)
class RawArchiveEvidence:
    """A sealed receipt for exact wire observations already in the archive.

    Page-oriented adapters archive before they return their normalized frame,
    so ``write_fetched`` cannot receive source bytes a second time.  The
    receipt is intentionally not a boolean compatibility flag: it carries the
    concrete sidecar/hash identities and is revalidated at the publish
    boundary before any staging write is allowed.
    """

    dataset: str
    run_id: str
    source: str
    request_scope: str
    capture_nonce: str
    record_keys: tuple[tuple[str, str], ...]
    observation_ids: tuple[str, ...]
    _seal: object = field(repr=False, compare=False)

    def __init__(
        self,
        dataset: str,
        run_id: str,
        source: str,
        request_scope: str,
        capture_nonce: str,
        record_keys: tuple[tuple[str, str], ...],
        observation_ids: tuple[str, ...],
        *,
        _seal: object,
    ) -> None:
        if _seal is not _RAW_ARCHIVE_EVIDENCE_SEAL:
            raise TypeError("RawArchiveEvidence must be created by archive verification")
        object.__setattr__(self, "dataset", str(dataset))
        object.__setattr__(self, "run_id", str(run_id))
        object.__setattr__(self, "source", str(source))
        object.__setattr__(self, "request_scope", str(request_scope))
        object.__setattr__(self, "capture_nonce", str(capture_nonce))
        object.__setattr__(self, "record_keys", tuple(record_keys))
        object.__setattr__(self, "observation_ids", tuple(str(item) for item in observation_ids))
        object.__setattr__(self, "_seal", _seal)

    @classmethod
    def _verified(
        cls,
        dataset: str,
        run_id: str,
        source: str,
        request_scope: str,
        capture_nonce: str,
        record_keys: tuple[tuple[str, str], ...],
        observation_ids: tuple[str, ...],
    ) -> RawArchiveEvidence:
        return cls(
            dataset,
            run_id,
            source,
            request_scope,
            capture_nonce,
            record_keys,
            observation_ids,
            _seal=_RAW_ARCHIVE_EVIDENCE_SEAL,
        )


def _raw_archive(config: Config, dataset: str) -> RawPayloadArchive:
    """Construct the strict archive view used for receipt validation."""
    return RawPayloadArchive(
        config.meta_root,
        enabled=True,
        datasets=[dataset],
        compression=config.raw_archive_compression,
        max_payload_bytes=config.raw_archive_max_payload_bytes,
    )


def verify_raw_archive(
    config: Config,
    dataset: str,
    run_id: str,
    *,
    source: str,
    request_scope: str,
    records: list[RawPayloadRecord] | tuple[RawPayloadRecord, ...] | None = None,
) -> RawArchiveEvidence:
    """Verify and seal exact wire evidence for one run/dataset.

    This is the only factory for :class:`RawArchiveEvidence`.  Listing the
    archive also integrity-checks payload bytes; the additional metadata check
    rejects an adapter that wrote a normalized or otherwise non-wire payload.
    """
    if not run_id:
        raise RawArchiveError(f"{dataset}: raw archive evidence requires a non-empty run_id")
    if not source or not request_scope:
        raise RawArchiveError(f"{dataset}: raw archive evidence requires source and request_scope")
    nonce = active_capture_nonce(
        config,
        dataset,
        str(run_id),
        source=source,
        request_scope=request_scope,
    )
    if not nonce:
        raise RawArchiveError(
            f"{dataset}: archive capture is not active for source {source!r}, "
            f"scope {request_scope!r}"
        )
    if capture_is_consumed(
        config,
        dataset,
        str(run_id),
        source=source,
        request_scope=request_scope,
        nonce=nonce,
    ):
        raise RawArchiveError(f"{dataset}: archive capture was already consumed")
    archive = _raw_archive(config, dataset)
    current = captured_records(
        config,
        dataset,
        str(run_id),
        source=source,
        request_scope=request_scope,
    )
    if records is None:
        selected = current
    else:
        selected = list(records)
        current_keys = {
            (item.metadata_path, item.payload_sha256, item.observation_id) for item in current
        }
        if any(
            item.capture_nonce != nonce
            or (item.metadata_path, item.payload_sha256, item.observation_id) not in current_keys
            for item in selected
        ):
            raise RawArchiveError(
                f"{dataset}: supplied archive records are not from the active capture"
            )
    if not selected:
        raise RawArchiveError(
            f"{dataset}: archive is required but no exact wire observation exists "
            f"for run_id {run_id!r}, source {source!r}, scope {request_scope!r}"
        )
    keys: list[tuple[str, str]] = []
    observation_ids: list[str] = []
    for record in selected:
        if (
            record.dataset != str(dataset)
            or record.run_id != str(run_id)
            or record.source != str(source)
            or record.request_scope != str(request_scope)
            or record.capture_nonce != nonce
            or not record.observation_id
        ):
            raise RawArchiveError(
                f"{dataset}: archive observation is outside the current source/scope"
            )
        if (
            not isinstance(record.http_metadata, dict)
            or record.http_metadata.get("wire_exact") is not True
        ):
            raise RawArchiveError(
                f"{dataset}: archive observation {record.metadata_path!r} "
                "does not carry verified exact wire evidence"
            )
        # ``records`` already reads each payload, but keep this explicit so a
        # receipt is tied to the exact payload it names rather than just the
        # existence of a JSON sidecar.
        archive.record(record.metadata_path)
        keys.append((record.metadata_path, record.payload_sha256))
        observation_ids.append(str(record.observation_id))
    return RawArchiveEvidence._verified(
        dataset,
        str(run_id),
        str(source),
        str(request_scope),
        str(nonce),
        tuple(keys),
        tuple(observation_ids),
    )


def _validate_raw_archive_evidence(
    config: Config,
    dataset: str,
    run_id: str,
    evidence: RawArchiveEvidence | None,
    *,
    source: str,
    df: pl.DataFrame | None = None,
) -> None:
    """Revalidate a receipt immediately before the curated publish."""
    if (
        not isinstance(evidence, RawArchiveEvidence)
        or evidence._seal is not _RAW_ARCHIVE_EVIDENCE_SEAL
    ):
        raise RawArchiveError(
            f"{dataset}: raw archive requires exact wire evidence; caller did not provide "
            "a verified evidence receipt"
        )
    if (
        evidence.dataset != dataset
        or evidence.run_id != str(run_id)
        or evidence.source != str(source)
        or not evidence.request_scope
        or not evidence.capture_nonce
        or not evidence.record_keys
        or len(evidence.record_keys) != len(evidence.observation_ids)
        or not all(evidence.observation_ids)
    ):
        raise RawArchiveError(
            f"{dataset}: raw archive evidence does not match source/scope/run {run_id!r}"
        )
    active_nonce = active_capture_nonce(
        config,
        dataset,
        str(run_id),
        source=source,
        request_scope=evidence.request_scope,
    )
    if not active_nonce or active_nonce != evidence.capture_nonce:
        raise RawArchiveError(f"{dataset}: raw archive evidence capture is no longer active")
    if capture_is_consumed(
        config,
        dataset,
        str(run_id),
        source=source,
        request_scope=evidence.request_scope,
        nonce=evidence.capture_nonce,
    ):
        raise RawArchiveError(f"{dataset}: raw archive evidence capture was already consumed")
    current_keys = {
        (item.metadata_path, item.payload_sha256, item.observation_id)
        for item in captured_records(
            config,
            dataset,
            str(run_id),
            source=source,
            request_scope=evidence.request_scope,
        )
        if item.capture_nonce == active_nonce
    }
    if any(
        (metadata_path, payload_sha256, observation_id) not in current_keys
        for (metadata_path, payload_sha256), observation_id in zip(
            evidence.record_keys, evidence.observation_ids, strict=True
        )
    ):
        raise RawArchiveError(
            f"{dataset}: raw archive evidence records are not from the active capture"
        )
    if df is not None and "source" in df.columns:
        observed_sources = {
            str(value)
            for value in df.get_column("source").drop_nulls().unique().to_list()
            if str(value).strip()
        }
        if observed_sources and observed_sources != {str(source)}:
            raise RawArchiveError(
                f"{dataset}: normalized rows contain source(s) {sorted(observed_sources)!r}, "
                f"not the archived source {source!r}"
            )
    archive = _raw_archive(config, dataset)
    for (metadata_path, payload_sha256), observation_id in zip(
        evidence.record_keys, evidence.observation_ids, strict=True
    ):
        try:
            record = archive.record(metadata_path)
        except (FileNotFoundError, RawArchiveError) as exc:
            if isinstance(exc, RawArchiveError):
                raise
            raise RawArchiveError(
                f"{dataset}: raw archive evidence observation is missing for run_id {run_id!r}"
            ) from exc
        if (
            record.run_id != str(run_id)
            or record.dataset != str(dataset)
            or record.source != str(source)
            or record.request_scope != evidence.request_scope
            or record.observation_id != observation_id
        ):
            raise RawArchiveError(
                f"{dataset}: raw archive evidence observation is outside the current scope"
            )
        if record.payload_sha256 != payload_sha256:
            raise RawArchiveError(f"{dataset}: raw archive evidence payload hash changed")
        if (
            not isinstance(record.http_metadata, dict)
            or record.http_metadata.get("wire_exact") is not True
        ):
            raise RawArchiveError(f"{dataset}: raw archive evidence is not exact wire data")
        archive.read(record)


def call_with_run_id(
    fetch_fn: Callable,
    value,
    *,
    pipeline_config: Config,
    dataset: str,
    run_id: str,
    **kwargs,
):
    """Call an adapter with provenance, rejecting captureless critical paths.

    Optional compatibility doubles may still have the historical narrow
    signature when raw archival is disabled.  Once the dataset is governed by
    the archive policy, a callable that cannot receive the run id cannot prove
    that its wire observations belong to this run and is rejected before it
    can publish.
    """
    try:
        parameters = inspect.signature(fetch_fn).parameters.values()
    except (TypeError, ValueError):
        parameters = ()
    supports_run_id = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters
    ) or any(parameter.name == "run_id" for parameter in parameters)
    if not supports_run_id:
        if pipeline_config.should_archive_raw(dataset):
            raise RawArchiveError(
                f"{dataset}: archive is required but the adapter cannot receive run_id"
            )
        return fetch_fn(value, **kwargs)
    return fetch_fn(value, run_id=run_id, **kwargs)


def _mark_snapshot_capture(config: Config, dataset: str, snapshot_date: date) -> None:
    """Record a live-window capture without turning future dates into a watermark."""
    spec = DATASETS.get(dataset)
    if spec is not None and not spec.watermark and spec.fetch_semantics == "snapshot":
        StateStore(config.meta_root).update_max_date(
            dataset,
            snapshot_date,
            field="last_snapshot_date",
        )


def _archive_fetched_payload(
    config: Config,
    run_id: str,
    dataset: str,
    df: pl.DataFrame,
    *,
    source: str,
    raw_payload: object | None = None,
    raw_archive_evidence: RawArchiveEvidence | None = None,
    request_params: dict | None = None,
    url: str | None = None,
) -> None:
    """Retain a replayable payload for configured critical HTTP datasets.

    Adapters that expose the original response can pass ``raw_payload``.
    Page-oriented adapters instead pass a sealed ``RawArchiveEvidence`` receipt
    for the sidecars they wrote before returning their normalized dataframe. A
    canonical dataframe is not an exact source observation and must never be
    advertised as replayable wire evidence.
    """
    # Config owns the policy.  In particular, an empty explicit list means
    # "use the built-in critical set", not "archive every dataframe".
    if not config.should_archive_raw(dataset):
        return
    if raw_payload is None:
        # This check deliberately happens before write_simple: a critical
        # dataframe must not become visible when its source observation is
        # absent or merely claimed by a compatibility boolean.
        _validate_raw_archive_evidence(
            config,
            dataset,
            run_id,
            raw_archive_evidence,
            source=source,
            df=df,
        )
        return
    if raw_archive_evidence is not None:
        _validate_raw_archive_evidence(
            config,
            dataset,
            run_id,
            raw_archive_evidence,
            source=source,
            df=df,
        )
    if not isinstance(raw_payload, (bytes, bytearray, memoryview)):
        raise RawArchiveError(
            f"{dataset}: raw archive requires exact response bytes; "
            "refusing to archive a normalized payload"
        )
    if isinstance(raw_payload, bytearray):
        raw_payload = bytes(raw_payload)
    elif isinstance(raw_payload, memoryview):
        raw_payload = raw_payload.tobytes()
    archive = RawPayloadArchive(
        config.meta_root,
        enabled=config.raw_archive_enabled,
        # The policy has already selected this dataset.  Passing a singleton
        # keeps the archive itself strict even when the config list is empty.
        datasets=[dataset],
        compression=config.raw_archive_compression,
        max_payload_bytes=config.raw_archive_max_payload_bytes,
    )
    archive.archive(
        dataset,
        raw_payload,
        source=source,
        request_params={"run_id": run_id, **(request_params or {})},
        run_id=run_id,
        url=url,
        payload_format="bytes",
        http_metadata={"wire_exact": True},
    )


def write_fetched(
    config: Config,
    run_id: str,
    dataset: str,
    df: pl.DataFrame,
    *,
    source: str,
    batch_id: str = "batch-0",
    raw_payload: object | None = None,
    raw_archive_evidence: RawArchiveEvidence | None = None,
    request_params: dict | None = None,
    url: str | None = None,
    snapshot_date: date | None = None,
) -> dict:
    df = with_provenance(df, source=source, data_version=data_version_for(dataset))
    publish_context = nullcontext()
    if config.should_archive_raw(dataset) and isinstance(raw_archive_evidence, RawArchiveEvidence):
        publish_context = capture_publish(
            config,
            dataset,
            run_id,
            source=raw_archive_evidence.source,
            request_scope=raw_archive_evidence.request_scope,
            nonce=raw_archive_evidence.capture_nonce,
        )
    with publish_context:
        _archive_fetched_payload(
            config,
            run_id,
            dataset,
            df,
            source=source,
            raw_payload=raw_payload,
            raw_archive_evidence=raw_archive_evidence,
            request_params=request_params,
            url=url,
        )
        result = write_simple(config, run_id, dataset, df, batch_id=batch_id)
    if snapshot_date is not None:
        _mark_snapshot_capture(config, dataset, snapshot_date)
    return result


def _incomplete_window_status(findings: list[dict]) -> str | None:
    """Public status for a window that did not come back whole.

    ``warning`` blocks this run's compact, which is what a missing *session* of
    a dense dataset needs: publishing past it would let the watermark claim a
    session nobody observed. A day the source refused is different — the days
    around it are complete in themselves, and holding them back would let one
    bad day inside the reconciliation tail blind the dataset for as long as it
    stays in that tail. Those publish, and say so as ``degraded``.
    """
    checks = {finding.get("check") for finding in findings}
    if "session_dense_empty_days" in checks:
        return "warning"
    if "fetch_failed_days" in checks:
        return "degraded"
    return None


def run_incremental_fetched(
    config: Config,
    trade_date: date,
    run_id: str,
    dataset: str,
    fetch_fn: Callable[[date], pl.DataFrame],
    *,
    source: str,
    allow_empty: bool = False,
    universe: set[str] | None = None,
    date_col: str | None = None,
    raw_payload: object | None = None,
    raw_archive_evidence: RawArchiveEvidence | None = None,
    raw_archive_evidence_factory: Callable[[], RawArchiveEvidence] | None = None,
    request_params: dict | None = None,
    url: str | None = None,
) -> dict:
    df, findings = fetch_incremental_daily(
        config,
        dataset,
        trade_date,
        fetch_fn,
        allow_empty=allow_empty,
        date_col=date_col,
    )
    if universe and not df.is_empty():
        # Constrain a live snapshot (e.g. EastMoney valuation clist) to the
        # tradable universe: the source returns delisted / never-traded names the
        # lake must not carry. An empty universe means "cannot reconcile" — skip
        # filtering rather than dropping every row.
        source_rows = df.height
        if "symbol" not in df.columns:
            raise RuntimeError(f"{dataset}: cannot reconcile source rows without a symbol column")
        df = df.filter(pl.col("symbol").is_in(list(universe)))
        if df.is_empty():
            raise RuntimeError(
                f"{dataset}: source returned {source_rows} row(s), but none matched the "
                f"reconciled universe ({len(universe)} symbol(s))"
            )
    if df.is_empty():
        out: dict = {"rows_read": 0, "rows_written": 0}
        # An allowed empty live snapshot is still a successful observation.
        # Record its capture date so stale scheduling does not retry forever,
        # while preserving the separate no-watermark contract for rolling
        # windows.  Required/watermarked feeds do not reach this branch with
        # ``allow_empty=True`` unless their caller explicitly accepts an
        # empty response, so they remain retryable by their own gate.
        if allow_empty:
            if config.should_archive_raw(dataset) and raw_payload is None:
                evidence = raw_archive_evidence
                if evidence is None:
                    if raw_archive_evidence_factory is None:
                        raise RawArchiveError(
                            f"{dataset}: archive-enabled empty snapshot requires an "
                            "explicit source/request evidence factory"
                        )
                    evidence = raw_archive_evidence_factory()
                _validate_raw_archive_evidence(
                    config,
                    dataset,
                    run_id,
                    evidence,
                    source=source,
                    df=df,
                )
            _mark_snapshot_capture(config, dataset, trade_date)
        if findings:
            out["context_updates"] = {"audit_findings": findings}
            # Snapshot coverage gaps are deliberately not promoted here: those
            # missed historical snapshots cannot be replayed without
            # manufacturing point-in-time values.
            status = _incomplete_window_status(findings)
            if status is not None:
                out["status"] = status
        return out
    evidence = raw_archive_evidence
    if config.should_archive_raw(dataset) and raw_payload is None and evidence is None:
        if raw_archive_evidence_factory is None:
            raise RawArchiveError(
                f"{dataset}: archive-enabled snapshot requires an explicit "
                "source/request evidence factory"
            )
        evidence = raw_archive_evidence_factory()
    result = write_fetched(
        config,
        run_id,
        dataset,
        df,
        source=source,
        raw_payload=raw_payload,
        raw_archive_evidence=evidence,
        request_params=request_params,
        url=url,
    )
    _mark_snapshot_capture(config, dataset, trade_date)
    if findings:
        result["context_updates"] = {"audit_findings": findings}
        status = _incomplete_window_status(findings)
        if status is not None:
            result["status"] = status
    return result


def empty_ok(df: pl.DataFrame, dataset: str, trade_date: date) -> None:
    if df.is_empty():
        raise RuntimeError(f"{dataset}: no rows returned for {trade_date.isoformat()}")
