"""One incremental fast-news fact table with a flash-wire compatibility view.

Both datasets are views of the same EastMoney 7×24 list. They used to page it
separately — twice per 15-minute events run, the whole day each time. Now the
first of the two steps in a run fetches everything published since the older
of the two datasets' newest items (less a small overlap); the other reuses it.
Only ``news_headlines`` stores new facts and exact page bytes. Existing
``flash_news_wire`` revisions remain readable through the compatibility view.
"""

from __future__ import annotations

import base64
import logging
import threading
from datetime import date, datetime, time, timedelta

import polars as pl

from cnequity.config import Config
from cnequity.domain.market_time import SHANGHAI_TZ
from cnequity.orchestrator.outcomes import SourceUnavailableError

logger = logging.getLogger(__name__)
CST = SHANGHAI_TZ
# Items keep their news_id, so re-reading a few minutes costs nothing but a
# duplicate the compact collapses; it guards against items that appear late.
_OVERLAP = timedelta(minutes=30)
_LOCK = threading.Lock()
_STAGE_LOCK = threading.Lock()
_CACHE: dict[str, tuple[pl.DataFrame, list[dict], bool]] = {}
_STAGED: set[str] = set()


def _newest_held(config: Config, dataset: str) -> datetime | None:
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / dataset
    if not dataset_has_parquet(root):
        return None
    newest = (
        scan_parquet_root(root, partition_col="publish_date")
        .select(
            (
                pl.col("publish_date").cast(pl.Date).cast(pl.Utf8)
                + pl.lit("T")
                + pl.col("publish_time").cast(pl.Utf8)
            ).max()
        )
        .collect()
        .item()
    )
    return datetime.fromisoformat(newest).replace(tzinfo=CST) if newest else None


def since_for(config: Config, trade_date: date) -> datetime:
    """Where the next canonical fetch starts."""
    held = _newest_held(config, "news_headlines")
    if held is None:
        return datetime.combine(trade_date, time(0, 0), CST)
    return held - _OVERLAP


def shared_fetch(
    config: Config, trade_date: date, run_id: str
) -> tuple[pl.DataFrame, list[dict], bool]:
    """The run's fetch, made once; (rows, raw pages, reached *since*)."""
    from cnequity.adapters.eastmoney import news_pause
    from cnequity.adapters.eastmoney.news_feed import fetch_news_since

    with _LOCK:
        cached = _CACHE.get(run_id)
        if cached is None:
            news_pause.ensure_open(config)
            since = since_for(config, trade_date)
            pages: list[dict] = []
            try:
                rows, reached = fetch_news_since(since, config=config, recorder=pages)
            except Exception as exc:
                news_pause.note_outcome(config, exc)
                raise
            news_pause.note_outcome(config, None)
            if not reached:
                logger.warning(
                    "fast news: the API stopped before %s; anything older was not served",
                    since.isoformat(timespec="minutes"),
                )
            logger.info(
                "fast news: %d item(s) since %s in %d page(s)",
                rows.height,
                since.isoformat(timespec="minutes"),
                len(pages),
            )
            cached = (rows, pages, reached)
            _CACHE.clear()  # one run at a time per process; keep only this one
            _STAGED.clear()
            _CACHE[run_id] = cached
        return cached


class _Replayed:
    def __init__(self, page: dict) -> None:
        self.content = base64.b64decode(page["content_b64"])
        self.url = page["url"]
        self.status_code = page["status"]
        self.headers = {"content-type": page.get("content_type") or "application/json"}


def _archive(config: Config, dataset: str, run_id: str, scope: str, pages: list[dict]) -> None:
    from cnequity.adapters.eastmoney.raw import archive_response, configured_archive

    archive = configured_archive(config, dataset, run_id=run_id, request_scope=scope)
    for page in pages:
        archive_response(
            archive,
            dataset,
            _Replayed(page),
            request_params={**page["params"], "page": page["page"]},
            run_id=run_id,
            url=page["url"],
            observation_id=f"{run_id or 'anonymous'}:{dataset}:{page['page']}",
            pagination={"page": page["page"], "sort_end": page["sort_end"]},
            captured_at=datetime.fromisoformat(page["captured_at"]),
        )


def stage_news(config: Config, trade_date: date, run_id: str, dataset: str) -> dict:
    """Stage the canonical news rows once; old wire step is a thin alias."""
    with _STAGE_LOCK:
        return _stage_news_locked(config, trade_date, run_id, dataset)


def _stage_news_locked(config: Config, trade_date: date, run_id: str, dataset: str) -> dict:
    from cnequity.query.parquet_scan import dataset_has_parquet
    from cnequity.steps.http_common import verify_raw_archive, write_fetched
    from cnequity.storage import StagingWriter

    if not config.sources.get("eastmoney", True):
        raise SourceUnavailableError(f"{dataset}: eastmoney source disabled in config")
    if dataset not in {"news_headlines", "flash_news_wire"}:
        raise ValueError(f"unsupported news dataset {dataset!r}")
    canonical = "news_headlines"
    staged = StagingWriter(config.staging_root).list_run_files(canonical, run_id)
    if staged:
        from cnequity.domain.schemas import validate_dataframe

        for path in staged:
            try:
                stored = validate_dataframe(pl.read_parquet(path), canonical)
            except (OSError, pl.exceptions.PolarsError, ValueError) as exc:
                raise RuntimeError(f"news_headlines: unreadable staging part {path}") from exc
            if stored.is_empty():
                raise RuntimeError(f"news_headlines: empty staging part {path}")
    if (_CACHE.get(run_id) is not None and run_id in _STAGED) or staged:
        return {"rows_read": 0, "rows_written": 0, "dataset": canonical, "compat_view": dataset}
    rows, pages, reached = shared_fetch(config, trade_date, run_id)
    scope = f"snapshot:{trade_date.isoformat()}"
    frame = rows
    # Archived even when empty: a quiet window is still an observed response.
    _archive(config, canonical, run_id, scope, pages)
    if frame.is_empty():
        from cnequity.quality.verify import covered_through

        if (
            not dataset_has_parquet(config.curated_root / canonical)
            and covered_through(config, canonical) is None
        ):
            # A first capture that finds nothing must not look like a clean start
            # (an empty success once left the dataset unregistered in curated).
            raise SourceUnavailableError(
                f"{dataset}: no fast news returned and the lake holds none"
            )
        return {"rows_read": 0, "rows_written": 0, "pages": len(pages), "dataset": canonical}
    evidence = (
        verify_raw_archive(config, canonical, run_id, source="eastmoney", request_scope=scope)
        if config.should_archive_raw(canonical)
        else None
    )
    result = write_fetched(
        config, run_id, canonical, frame, source="eastmoney", raw_archive_evidence=evidence
    )
    _STAGED.add(run_id)
    result["dataset"] = canonical
    result["compat_view"] = dataset
    result["pages"] = len(pages)
    result["publish_dates"] = sorted({d.isoformat() for d in frame["publish_date"].to_list()})
    if not reached:
        result.setdefault("context_updates", {})["audit_findings"] = [
            {
                "dataset": dataset,
                "severity": "warning",
                "check": "news_gap_beyond_source",
                "message": (
                    f"{dataset}: the fast-news API stopped paging before the lake's "
                    "newest item; items older than what it served are missing"
                ),
            }
        ]
    return result
