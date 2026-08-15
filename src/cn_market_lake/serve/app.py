"""The read-only lake dashboard: JSON API plus one self-contained page.

**Nothing here writes to the lake.** There is no endpoint that runs, retries or
cleans anything, and there will not be: an unauthenticated local HTTP service
that can trigger ingestion is a liability, and the CLI is already the right
front door for those. The page shows the command to run and lets you copy it.

The one exception proves the rule — ``meta/stats`` is regenerated in the
background when ingestion has moved on, because it is a cache of the lake rather
than part of it, and a dashboard serving numbers from last week is worse than
one that refreshes its own cache.

Responses are pydantic models so ``/api/docs`` documents the real contract:
the OpenAPI page is generated from the handlers and cannot drift from them.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from cn_market_lake.config import Config
from cn_market_lake.serve.lake import LakeView

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

# Poll interval for the run stream. Batches finish on the order of seconds to
# minutes, so this is responsive without making the manifest a hot file.
_STREAM_POLL_SECONDS = 2.0

_STATIC_ASSETS = ("/static/styles.css", "/static/bundle.js")


def _asset_stamp(source: str) -> str:
    """Cache-busting stamp for a dashboard asset URL.

    The file's mtime rather than the package version: installing a wheel
    refreshes it, so upgrades bust the cache — and so does a local rebuild,
    which a version string would not, leaving a developer testing yesterday's
    JavaScript against today's code. One stat per page load.
    """
    asset = STATIC_DIR / Path(source).name
    return str(int(asset.stat().st_mtime)) if asset.exists() else "dev"


class Health(BaseModel):
    anchor: date = Field(description="Last trading day; freshness is judged against this.")
    datasets: int
    fresh: int
    stale: int
    empty: int
    not_applicable: int
    stale_datasets: list[str]
    empty_optional: list[str] = Field(description="Empty and required=False — expected, not a gap.")
    empty_required: list[str] = Field(description="Empty and required — a real gap.")
    rows: int
    bytes: int
    findings_by_severity: dict[str, int]
    audit_trade_date: str | None
    stats_stale: bool
    stats_reason: str | None
    stats_generated_at: datetime | None


class Tier(BaseModel):
    tier: str
    label: str
    datasets: int
    fresh: int
    stale: int
    empty: int
    rows: int
    bytes: int
    members: list[str]


class Dataset(BaseModel):
    dataset: str
    tier: str
    tier_label: str
    layer: str
    granularity: str | None
    date_col: str | None
    fetch_semantics: str
    history_mode: str
    backfill_source: str | None
    history_horizon_days: int | None
    pit: bool
    required: bool
    intraday: str | None
    row_grain: str | None = Field(
        default=None,
        description=(
            "What one row covers when finer than a day: '1m', '5m', 'tick'. "
            "Descriptive only. Set even where `intraday` is null — trade_ticks "
            "is intraday without holding bars."
        ),
    )
    has_data: bool
    coverage_start: date | None
    coverage_end: date | None
    watermarked: bool
    watermark: date | None
    freshness: str
    row_count: int | None
    bytes: int | None
    partitions: int | None


class Column(BaseModel):
    column: str
    dtype: str


class PartitionStat(BaseModel):
    partition: str | None
    granularity: str | None
    period_start: date | None
    period_end: date | None
    row_count: int
    bytes: int


class Gaps(BaseModel):
    missing: list[str] = Field(description="Missing partition values, capped at 60.")
    total: int
    unit: str = Field(description="Counted in the dataset's own period, not in days.")


class Command(BaseModel):
    cmd: str
    why: str


class Batch(BaseModel):
    run_id: str
    batch_id: str
    status: str
    window_start: str | None
    window_end: str | None
    rows_written: int | None
    retry_count: int | None
    started_at: str | None
    finished_at: str | None
    error_message: str | None


class DatasetDetail(Dataset):
    partition_col: str | None
    max_staleness_days: int
    backfill_chunk_days: int | None
    backfill_chunk_symbols: int | None
    earliest_available: date | None = Field(
        description="Source floor, not this lake's backlog: earlier windows return nothing."
    )
    history_floor_date: date | None = Field(
        default=None,
        description=(
            "Set when the source's edge is a fixed calendar date rather than the "
            "rolling trading-day count in history_horizon_days. Both can produce an "
            "earliest_available, so this is what distinguishes them."
        ),
    )
    adjustable: bool = Field(description="load() can join adj_factors for this dataset.")
    primary_key: list[str]
    schema_columns: list[Column] = Field(alias="schema")
    gaps: Gaps
    findings: list[dict]
    commands: list[Command]
    batches: list[Batch]


class DateOptions(BaseModel):
    kind: str = Field(
        description="Which control fits: trading_day, event_day, period, "
        "report_period, or none for a merge-style dataset."
    )
    column: str | None
    granularity: str | None
    values: list[str] = Field(description="Only values that exist, newest first.")
    total: int
    note: str | None


class RowPage(BaseModel):
    columns: list[str]
    rows: list[list[object | None]]
    total: int = Field(description="Rows matching the filter, before paging.")
    offset: int
    limit: int


class RunSummary(BaseModel):
    run_id: str
    job_name: str
    status: str
    started_at: str
    finished_at: str | None
    rows_read: int | None
    rows_written: int | None
    error_message: str | None
    batches: int
    batch_status: dict[str, int]


class RunBatch(BaseModel):
    batch_id: str
    dataset: str
    status: str
    window_start: str | None
    window_end: str | None
    rows_read: int | None
    rows_written: int | None
    retry_count: int | None
    started_at: str | None
    finished_at: str | None
    heartbeat_at: str | None
    error_message: str | None
    stalled: bool = Field(
        description="Still 'running' but silent past batch_stale_seconds — what "
        "the engine will promote to failed on the next run."
    )
    silent_seconds: int | None = None


class RunDetail(BaseModel):
    run_id: str
    job_name: str
    status: str
    started_at: str
    finished_at: str | None
    rows_read: int | None
    rows_written: int | None
    error_message: str | None
    metadata_json: str | None
    stale_after_seconds: float
    batches: list[RunBatch]


class FindingsRun(BaseModel):
    run_id: str
    trade_date: str | None
    total: int
    by_severity: dict[str, int]
    top_checks: list[tuple[str, int]]


class DiffRun(BaseModel):
    run_id: str
    trade_date: str | None
    diff_count: int
    by_check: dict[str, int]


class QuarantineEntry(BaseModel):
    name: str
    files: int
    bytes: int
    modified: str


class OnDemandEntry(BaseModel):
    dataset: str
    entries: int
    bytes: int
    newest: str | None


class Quality(BaseModel):
    findings_runs: list[FindingsRun]
    diff_runs: list[DiffRun]
    quarantine: list[QuarantineEntry] = Field(
        description="Pulled out of curated and kept as evidence, not a wastebasket."
    )
    on_demand: list[OnDemandEntry] = Field(
        description="Per-symbol caches; empty means nobody has queried one yet."
    )


class QualityRun(BaseModel):
    run_id: str
    trade_date: str | None
    findings: list[dict]
    diffs: list[dict]


class Provenance(BaseModel):
    source: str
    data_version: str
    row_count: int
    fetched_at_min: datetime | None
    fetched_at_max: datetime | None


class ProvenancePoint(BaseModel):
    """One (period, source, data_version) — the source mix as it moved."""

    period_start: date
    source: str
    data_version: str
    row_count: int


class ProvenanceSeries(BaseModel):
    bucket: str = Field(description="Width each point spans: day, month or year.")
    points: list[ProvenancePoint]


class HeatmapRow(BaseModel):
    dataset: str
    tier: str
    granularity: str | None
    freshness: str
    cadence_days: int = Field(description="Days this dataset may lag before it counts as stale.")
    gap_meaning: str = Field(
        description="'fault' — a daily by_date dataset is genuinely missing a day. "
        "'cadence' — the source is not daily, or is snapshot and a missed day "
        "can never be filled honestly."
    )
    cells: str = Field(description="One char per day; see `legend`.")


class Heatmap(BaseModel):
    days: list[date]
    legend: dict[str, str]
    rows: list[HeatmapRow]


def get_view(request: Request) -> LakeView:
    return request.app.state.view


# Annotated rather than a `= Depends(...)` default: the same wiring, but the call
# stays out of the signature's defaults, where it is both a bugbear finding
# (B008) and evaluated once at import.
View = Annotated[LakeView, Depends(get_view)]


def create_app(config: Config, *, token: str | None = None) -> FastAPI:
    """Build the dashboard app for *config*.

    *token*, when set, is required as ``Authorization: Bearer <token>`` or
    ``?token=``. The CLI makes it mandatory for a non-loopback bind — this
    service has no other access control and should not be reachable without one.
    """
    app = FastAPI(
        title="cn-market-lake dashboard",
        description="Read-only view of one lake: coverage, freshness and provenance.",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
    )
    app.state.view = LakeView(config)
    app.state.token = token

    @app.middleware("http")
    async def _authenticate(request: Request, call_next):
        expected = request.app.state.token
        if expected:
            header = request.headers.get("authorization", "")
            supplied = header[7:] if header.lower().startswith("bearer ") else None
            supplied = supplied or request.query_params.get("token")
            if supplied != expected:
                from fastapi.responses import JSONResponse

                return JSONResponse({"detail": "unauthorized"}, status_code=401)
        return await call_next(request)

    # Same-origin only: the bundle is packaged beside the page, never fetched
    # from a CDN. `html=False` so a missing asset 404s rather than silently
    # serving index.html as JavaScript.
    app.mount("/static", StaticFiles(directory=STATIC_DIR, html=False), name="static")

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def index() -> HTMLResponse:
        page = STATIC_DIR / "index.html"
        if not page.exists():  # pragma: no cover — packaging failure
            raise HTTPException(500, "dashboard page missing from the installed package")
        # Version-stamp the built assets. StaticFiles sends no Cache-Control, so
        # a browser may otherwise reuse stale CSS or JavaScript after an upgrade.
        # A changed query string is a changed URL.
        html = page.read_text(encoding="utf-8")
        for source in _STATIC_ASSETS:
            html = html.replace(source, f"{source}?v={_asset_stamp(source)}")
        return HTMLResponse(html)

    @app.get("/source-health", response_class=HTMLResponse, include_in_schema=False)
    def source_health(request: Request) -> HTMLResponse:
        """Render whatever `cml sources` last wrote into the lake.

        Reads, never probes. Probing reaches out to a dozen third-party hosts,
        and a GET that does it turns an unauthenticated local service into
        something a stray browser tab can point at other people's endpoints —
        the same reason nothing here triggers ingestion. The CLI owns that.
        """
        import json as _json

        from cn_market_lake.diagnostics.health_page import render_page
        from cn_market_lake.diagnostics.source_health import HealthReport

        root = request.app.state.view.config.meta_root / "source_health"
        reports = []
        for path in sorted(root.glob("*.json")) if root.exists() else []:
            try:
                reports.append(HealthReport.from_dict(_json.loads(path.read_text("utf-8"))))
            except (ValueError, OSError) as exc:  # a half-written or hand-edited file
                logger.warning("skipping source-health report %s: %s", path.name, exc)
        if not reports:
            return HTMLResponse(
                "<!doctype html><meta charset=utf-8>"
                "<style>body{font:15px/1.7 system-ui;margin:3rem auto;max-width:34rem}"
                "code{background:#f6f8fa;padding:.1rem .3rem;border-radius:4px}</style>"
                "<h1>还没有探测记录</h1>"
                "<p>先跑一次探测，报告会写进湖里，这个页面读它：</p>"
                "<pre><code>cml sources --vantage cn</code></pre>"
                "<p>探测放在 CLI 上是有意的——这个面板只读，不会替你去请求十几个第三方主机。</p>",
                status_code=404,
            )
        return HTMLResponse(render_page(reports))

    @app.get("/api/health", response_model=Health)
    def health(view: View) -> Health:
        # The overview page loads this first, so it is where the cache gets its
        # chance to notice the lake moved. Returns immediately either way.
        view.refresh_stats_in_background()
        return Health(**view.health())

    @app.get("/api/tiers", response_model=list[Tier])
    def tiers(view: View) -> list[Tier]:
        return [Tier(**row) for row in view.tiers()]

    @app.get("/api/datasets", response_model=list[Dataset])
    def datasets(
        view: View,
        tier: Annotated[str | None, Query(description="Restrict to one L0–L8 tier.")] = None,
    ) -> list[Dataset]:
        return [Dataset(**row) for row in view.datasets(tier=tier)]

    def _known(dataset: str) -> None:
        from cn_market_lake.domain.datasets import DATASETS

        if dataset not in DATASETS:
            raise HTTPException(404, f"unknown dataset {dataset!r}")

    @app.get("/api/datasets/{dataset}", response_model=DatasetDetail)
    def dataset_detail(dataset: str, view: View) -> DatasetDetail:
        _known(dataset)
        return DatasetDetail(**view.dataset_detail(dataset))

    @app.get("/api/datasets/{dataset}/partitions", response_model=list[PartitionStat])
    def dataset_partitions(dataset: str, view: View) -> list[PartitionStat]:
        _known(dataset)
        return [PartitionStat(**row) for row in view.partitions(dataset)]

    @app.get("/api/datasets/{dataset}/dates", response_model=DateOptions)
    def dataset_dates(dataset: str, view: View) -> DateOptions:
        _known(dataset)
        return DateOptions(**view.date_options(dataset))

    @app.get("/api/datasets/{dataset}/rows", response_model=RowPage)
    def dataset_rows(
        dataset: str,
        view: View,
        period: Annotated[
            str | None, Query(description="A value from /dates — a day or a period.")
        ] = None,
        symbol: Annotated[str | None, Query(description="e.g. 600519.SH")] = None,
        as_of: Annotated[date | None, Query(description="PIT cutoff on announce_date.")] = None,
        adjust: Annotated[str | None, Query(pattern="^(hfq|qfq)$")] = None,
        # Capped rather than unbounded: this endpoint is a viewer, and a full
        # market-day of a wide dataset is not something to hand back by accident.
        # Bulk extraction is `load()` in Python, not a paging URL.
        limit: Annotated[int, Query(ge=1, le=1000)] = 200,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> RowPage:
        _known(dataset)
        try:
            return RowPage(
                **view.rows(
                    dataset,
                    period=period,
                    symbol=symbol,
                    as_of=as_of,
                    adjust=adjust,
                    limit=limit,
                    offset=offset,
                )
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.get("/api/datasets/{dataset}/provenance", response_model=list[Provenance])
    def provenance(dataset: str, view: View) -> list[Provenance]:
        _known(dataset)
        return [Provenance(**row) for row in view.provenance(dataset)]

    @app.get("/api/datasets/{dataset}/provenance/series", response_model=ProvenanceSeries)
    def provenance_series(dataset: str, view: View) -> ProvenanceSeries:
        _known(dataset)
        return ProvenanceSeries(**view.provenance_series(dataset))

    @app.get("/api/quality", response_model=Quality)
    def quality(
        view: View,
        limit: Annotated[int, Query(ge=1, le=200)] = 30,
    ) -> Quality:
        return Quality(**view.quality(limit=limit))

    @app.get("/api/quality/runs/{run_id}", response_model=QualityRun)
    def quality_run(run_id: str, view: View) -> QualityRun:
        detail = view.quality_run(run_id)
        if detail is None:
            raise HTTPException(404, f"no quality artefacts for run {run_id!r}")
        return QualityRun(**detail)

    @app.get("/api/runs", response_model=list[RunSummary])
    def runs(
        view: View,
        limit: Annotated[int, Query(ge=1, le=200)] = 40,
    ) -> list[RunSummary]:
        return [RunSummary(**row) for row in view.runs(limit=limit)]

    @app.get("/api/runs/{run_id}", response_model=RunDetail)
    def run_detail(run_id: str, view: View) -> RunDetail:
        detail = view.run_detail(run_id)
        if detail is None:
            raise HTTPException(404, f"unknown run {run_id!r}")
        return RunDetail(**detail)

    @app.get("/api/stream/runs/{run_id}", include_in_schema=False)
    async def stream_run(run_id: str, view: View, request: Request):
        """Server-sent events: the run's batches as they change.

        Polls the manifest rather than being pushed to, because the writers are
        separate worker processes and SQLite has no notification channel. A
        cheap fingerprint decides whether anything is worth sending, so an idle
        subscriber costs one small query per interval and no traffic.

        The poll runs in a thread: SQLite is blocking, and doing it on the event
        loop would stall every other request for the duration.
        """
        import asyncio
        import json as _json

        from fastapi.responses import StreamingResponse
        from starlette.concurrency import run_in_threadpool

        if view.run_detail(run_id) is None:
            raise HTTPException(404, f"unknown run {run_id!r}")

        async def events():
            last = None
            while True:
                if await request.is_disconnected():
                    return
                fingerprint = await run_in_threadpool(view.run_fingerprint, run_id)
                if fingerprint != last:
                    last = fingerprint
                    detail = await run_in_threadpool(view.run_detail, run_id)
                    if detail is None:
                        return
                    yield f"data: {_json.dumps(detail, default=str)}\n\n"
                    # A finished run cannot change again; close rather than
                    # leave the client holding an idle connection forever.
                    if detail["status"] not in ("running",):
                        return
                else:
                    # Comment frame: keeps proxies from reaping an idle stream.
                    yield ": keepalive\n\n"
                await asyncio.sleep(_STREAM_POLL_SECONDS)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/api/heatmap", response_model=Heatmap)
    def heatmap(
        view: View,
        days: Annotated[
            int, Query(ge=1, le=750, description="Trading days back from the anchor.")
        ] = 90,
    ) -> Heatmap:
        return Heatmap(**view.heatmap(days=days))

    return app
