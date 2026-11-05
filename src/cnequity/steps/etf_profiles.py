"""Current ETF classification snapshots; no historical directory backfill."""

from __future__ import annotations

from datetime import date

from cnequity.adapters.exchange.etf_profiles import fetch_exchange_etf_profiles
from cnequity.config import Config
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.outcomes import SourceUnavailableError
from cnequity.orchestrator.registry import register_step
from cnequity.run_scope import backfill_of
from cnequity.steps.common import SnapshotBackfillError
from cnequity.steps.http_common import verify_raw_archive, write_fetched
from cnequity.storage.raw_archive import RawPayloadArchive, begin_capture


@register_step("etf_profiles", group="research", requires_workers=False)
def step_etf_profiles(config: Config, trade_date: date, run_id: str, context: dict) -> dict:
    """Publish both official directories or retain the previous snapshot."""
    if not config.sources.get("exchange", True):
        raise SourceUnavailableError("etf_profiles: exchange source disabled in config")
    if backfill_of(config).active:
        raise SnapshotBackfillError(
            "etf_profiles: historical exchange directories are unavailable; "
            "a current snapshot cannot be backdated"
        )
    if not config.should_archive_raw("etf_profiles"):
        raise RuntimeError(
            "etf_profiles requires raw archive for exchange directories and index methodologies"
        )
    observed = shanghai_today()
    frame, wire = fetch_exchange_etf_profiles(config, observed)
    scope = f"exchange-etf-directories:{observed.isoformat()}"
    nonce = begin_capture(config, "etf_profiles", run_id, source="exchange", request_scope=scope)
    archive = RawPayloadArchive(
        config.meta_root,
        enabled=True,
        datasets=["etf_profiles"],
        compression=config.raw_archive_compression,
        max_payload_bytes=config.raw_archive_max_payload_bytes,
        capture_owner=config,
        capture_run_id=run_id,
        capture_source="exchange",
        capture_scope=scope,
        capture_nonce=nonce,
    )
    records = [
        archive.archive(
            "etf_profiles",
            payload,
            source="exchange",
            request_params=params,
            run_id=run_id,
            url=url,
            payload_format=kind,
            http_metadata={"wire_exact": True},
            observation_id=f"{run_id}:{scope}:{index}",
            request_scope=scope,
        )
        for index, (url, payload, params, kind) in enumerate(wire)
    ]
    evidence = verify_raw_archive(
        config,
        "etf_profiles",
        run_id,
        source="exchange",
        request_scope=scope,
        records=records,
    )
    return write_fetched(
        config,
        run_id,
        "etf_profiles",
        frame,
        source="exchange",
        raw_archive_evidence=evidence,
        snapshot_date=observed,
    )
