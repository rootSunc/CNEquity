"""Compact eligibility: skip datasets with incomplete batches in the current run."""

from __future__ import annotations

from cn_market_lake.orchestrator.manifest import Manifest


def refresh_batch_liveness(
    manifest: Manifest,
    run_id: str,
    *,
    stale_after_seconds: float,
) -> int:
    """Promote heartbeat-expired running batches to stale before compact gating."""
    return manifest.promote_running_to_stale(run_id, stale_after_seconds=stale_after_seconds)


def datasets_with_incomplete_batches(manifest: Manifest, run_id: str) -> frozenset[str]:
    """Return dataset names that still have non-success batches for *run_id*."""
    counts = manifest.incomplete_batch_counts_by_dataset(run_id)
    return frozenset(ds for ds, n in counts.items() if n > 0)


def compact_allowed(
    manifest: Manifest,
    run_id: str,
    dataset: str,
    *,
    stale_after_seconds: float | None = None,
) -> tuple[bool, int]:
    """Return (allowed, incomplete_batch_count) for compacting *dataset* in *run_id*."""
    if stale_after_seconds is not None:
        refresh_batch_liveness(manifest, run_id, stale_after_seconds=stale_after_seconds)
    incomplete = manifest.incomplete_batch_counts_by_dataset(run_id).get(dataset, 0)
    return incomplete == 0, incomplete
