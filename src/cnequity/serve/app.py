"""Lake dashboard.

Data views stay read-only. Storage cleanup, the whitelisted operations page,
and the lake switches are the writes, all behind same-origin checks.
``--read-only`` registers none of them. A server started without a config file
answers only the setup wizard until ``activate_lake`` installs one.
"""

from __future__ import annotations

import logging
import secrets
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from cnequity.config import Config
from cnequity.i18n import _, normalize_language, use_language
from cnequity.serve.lake import RUN_STATUSES, LakeView

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
    no_source: int = Field(
        default=0,
        description="Registered but permanently sourceless — counted apart from empty.",
    )
    not_applicable: int
    stale_datasets: list[str]
    empty_optional: list[str] = Field(description="Empty and required=False — expected, not a gap.")
    empty_required: list[str] = Field(description="Empty and required — a real gap.")
    rows: int
    bytes: int
    findings_by_severity: dict[str, int]
    audit_trade_date: str | None
    historical_universe: str = Field(
        default="all_a",
        description="Historical research universe checked by the last audit.",
    )
    historical_universe_ready: bool | None = Field(
        default=None,
        description=(
            "Whether the last audit's historical validity contract passed; "
            "null when no audit snapshot contains that contract."
        ),
    )
    historical_universe_window: dict[str, str | None] | None = Field(
        default=None,
        description="Inclusive historical window checked by the last audit.",
    )
    historical_all_a_st_evidence: dict[str, object] | None = Field(
        default=None,
        description=(
            "All-A historical ST evidence baseline from the operational audit; "
            "independent of the selected research universe."
        ),
    )
    historical_universe_blockers: list[str] = Field(
        default_factory=list,
        description="Human-readable reasons the historical contract is blocked.",
    )
    source_limitations: list[str] = Field(
        default_factory=list,
        description="Known upstream/source limitations recorded by the last full audit.",
    )
    stats_stale: bool
    stats_reason: str | None
    stats_generated_at: datetime | None


class ReadReceiptPage(BaseModel):
    """A bounded dataset read and the exact identity/coverage of those rows."""

    rows: list[dict[str, Any]]
    receipt: dict[str, Any]


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
    op: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


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


class ResampleBacklog(BaseModel):
    pending: int = Field(description="Sessions a default `cne derive` would rebuild.")
    input_sessions: int = Field(description="Sessions the lake's 1m and 5m cover.")
    derived: bool = Field(description="Whether this resample has been stored at all.")


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
    resample: ResampleBacklog | None = Field(
        default=None,
        description="Only for minute_bars_15m/30m/60m: how far the stored bars trail 1m/5m.",
    )
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
    adjustment: dict[str, Any] | None = None


class RunFailure(BaseModel):
    dataset: str
    status: str
    error_message: str = Field(
        description="The batch's own error, first line only. This is the reason "
        "a failed or degraded run stopped, not the run-level summary."
    )


class RunSummary(BaseModel):
    run_id: str
    job_name: str
    status: str
    started_at: str
    finished_at: str | None
    launch_id: str | None = None
    rows_read: int | None
    rows_written: int | None
    error_message: str | None
    batches: int
    batch_status: dict[str, int]
    failures: list[RunFailure] = Field(
        default_factory=list,
        description="Distinct batch failures that still need action. A batch already "
        "retried to success, superseded, or marked stale is omitted.",
    )
    has_staging: bool = False


class RunPage(BaseModel):
    total: int = Field(
        description="Runs matching the status filter, or every run when the filter is omitted. "
        "With open=true, only runs that still need attention."
    )
    offset: int
    limit: int
    runs: list[RunSummary]
    failed_daily_groups: int | None = Field(
        default=None,
        description="When open=true, daily schedule groups whose latest attempt failed. "
        "Null on an ordinary list.",
    )


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
    launch_id: str | None = None
    has_staging: bool = False


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
    view = request.app.state.view
    if view is None:
        raise HTTPException(409, _("Not configured yet"))
    return view


def activate_lake(app: FastAPI, config: Config) -> None:
    """Switch a setup-mode server onto a config it just wrote.

    Routes read ``app.state`` on each request, so this does not require a
    restart and does not recapture a closed-over maintenance object.
    """
    from cnequity.serve.storage import StorageMaintenance

    view = LakeView(config)
    maintenance = StorageMaintenance(config, invalidate=view.invalidate, csrf=app.state.csrf)
    view.maintenance_gate = maintenance.gate
    app.state.config = config
    app.state.view = view
    app.state.storage_maintenance = maintenance
    app.state.setup = False
    if config.config_path is not None:
        app.state.config_path = Path(config.config_path).resolve()
    app.state.ops.bind(config)


def reload_lake_config(app: FastAPI) -> Config:
    """Point the running server at the config file after a settings write."""
    from cnequity.config import load_config
    from cnequity.serve.ops.catalog import OpsError

    path = app.state.config_path
    if path is None:
        raise OpsError(_("There is no config file to write."))
    try:
        config = load_config(path)
    except (OSError, ValueError) as exc:
        raise OpsError(
            _("The config was written, but the service could not reload it; restart serve.")
        ) from exc
    app.state.config = config
    if app.state.view is not None:
        app.state.view.config = config
    if app.state.storage_maintenance is not None:
        app.state.storage_maintenance.config = config
    if config.config_path is not None:
        app.state.config_path = Path(config.config_path).resolve()
    app.state.ops.bind(config)
    app.state.schedule_service = None
    app.state.settings_service = None
    return config


# Annotated rather than a `= Depends(...)` default: the same wiring, but the call
# stays out of the signature's defaults, where it is both a bugbear finding
# (B008) and evaluated once at import.
View = Annotated[LakeView, Depends(get_view)]


def request_language(headers) -> str | None:
    """The language a request asks for: the page's choice, then the browser's.

    The page sends `X-CNE-Lang` with every API call; a page opened directly,
    such as /source-health, only has `Accept-Language`. None leaves the
    server's own language in force.
    """
    chosen = normalize_language(headers.get("x-cne-lang"))
    if chosen:
        return chosen
    for part in headers.get("accept-language", "").split(","):
        if language := normalize_language(part.split(";", 1)[0]):
            return language
    return None


def create_app(
    config: Config | None,
    *,
    token: str | None = None,
    read_only: bool = False,
    allow_remote_ops: bool = False,
    setup: bool = False,
    config_path: Path | None = None,
) -> FastAPI:
    """Build the dashboard app for *config*.

    *token*, when set, is required as ``Authorization: Bearer <token>`` or
    ``?token=``. The CLI makes it mandatory for a non-loopback bind — this
    service has no other access control and should not be reachable without one.

    ``setup`` is the first-run wizard, used only when the default config file
    does not exist yet. ``read_only`` registers none of the write routes.
    Remote ingestion stays off unless ``allow_remote_ops`` is set; storage
    cleanup keeps its existing token rule.
    """
    if config is not None:
        setup = False
    app = FastAPI(
        title="cnequity dashboard",
        description=(
            "Lake coverage, freshness and provenance. Whitelisted ingestion can "
            "be started from the operations page. Lake switches are previewed "
            "and written back to the config file. Storage cleanup still requires "
            "explicit web confirmation."
        ),
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
    )
    app.state.token = token
    app.state.read_only = read_only
    app.state.allow_remote_ops = allow_remote_ops
    app.state.setup = setup
    app.state.csrf = secrets.token_urlsafe(32)
    app.state.config = None if setup else config
    app.state.view = None if setup or config is None else LakeView(config)
    if config is not None and config.config_path is not None:
        app.state.config_path = Path(config.config_path).resolve()
    elif config_path is not None:
        app.state.config_path = Path(config_path).expanduser().resolve()
    else:
        app.state.config_path = None
    from cnequity.serve.ops.routes import install_ops_routes
    from cnequity.serve.ops.runner import OpsService
    from cnequity.serve.storage import StorageMaintenance
    from cnequity.serve.storage_routes import install_storage_routes

    if app.state.view is None:
        app.state.storage_maintenance = None
    else:
        maintenance = StorageMaintenance(
            config, invalidate=app.state.view.invalidate, csrf=app.state.csrf
        )
        app.state.storage_maintenance = maintenance
        app.state.view.maintenance_gate = maintenance.gate
    install_storage_routes(app, mutations=not read_only)
    app.state.ops = OpsService(
        None if setup else config,
        storage_busy=lambda: bool(
            app.state.storage_maintenance and app.state.storage_maintenance.busy
        ),
    )
    install_ops_routes(app, mutations=not read_only)

    @app.middleware("http")
    async def _authenticate(request: Request, call_next):
        expected = request.app.state.token
        if expected:
            header = request.headers.get("authorization", "")
            supplied = header[7:] if header.lower().startswith("bearer ") else None
            supplied = supplied or request.query_params.get("token")
            if not supplied or not secrets.compare_digest(
                supplied.encode("utf-8"), expected.encode("utf-8")
            ):
                from fastapi.responses import JSONResponse

                return JSONResponse({"detail": "unauthorized"}, status_code=401)
        path = request.url.path
        # Operations and storage answer while a purge has closed the read gate,
        # and they must not count as readers the purge is waiting on.
        unguarded = (
            path.startswith("/api/storage")
            or path.startswith("/api/ops")
            or path.startswith("/api/setup")
            or path.startswith("/api/stream/ops")
            # The API description reads no lake, and first-time setup prints
            # this link in its banner: it must open before there is one.
            or path in {"/api/docs", "/api/openapi.json"}
        )
        if unguarded:
            response = await call_next(request)
            response.headers["Cache-Control"] = "no-store"
            return response
        if not path.startswith("/api/"):
            return await call_next(request)
        if request.app.state.setup or request.app.state.view is None:
            from fastapi.responses import JSONResponse

            return JSONResponse({"detail": _("Not configured yet")}, status_code=409)
        maintenance = request.app.state.storage_maintenance
        if maintenance is None or not maintenance.gate.enter():
            from fastapi.responses import JSONResponse

            return JSONResponse(
                {
                    "detail": _(
                        "Storage maintenance is running and reads are paused for now; refresh shortly."
                    )
                },
                status_code=503,
            )
        try:
            return await call_next(request)
        finally:
            maintenance.gate.leave()

    @app.middleware("http")
    async def _browser_boundary(request: Request, call_next):
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = "frame-ancestors 'none'"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    # Registered last, so it wraps every other middleware: their own replies
    # ("not configured yet") are in the reader's language too.
    @app.middleware("http")
    async def _language(request: Request, call_next):
        with use_language(request_language(request.headers)):
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
        # The shell itself is not version-stamped. Revalidate it so a browser
        # does not keep an empty page from the previous build.
        return HTMLResponse(html, headers={"Cache-Control": "no-cache"})

    @app.get("/source-health", response_class=HTMLResponse, include_in_schema=False)
    def source_health(request: Request) -> HTMLResponse:
        """Render whatever `cne sources probe` last wrote into the lake.

        Reads, never probes. Probing reaches out to a dozen third-party hosts,
        and a GET that does it turns a page view into traffic those hosts did
        not ask for. The operations page can start ``cne sources probe
        --stale-only``; this page only renders the report that command wrote.
        """
        import json as _json

        from cnequity.diagnostics.health_page import render_page
        from cnequity.diagnostics.source_health import HealthReport

        if request.app.state.view is None:
            return HTMLResponse(_("Not configured yet."), status_code=409)
        root = request.app.state.view.config.meta_root / "source_health"
        reports = []
        # Latest reports are ``<vantage>.json``. The incident ledger and any
        # other object without a vantage share this directory and are not columns.
        for path in sorted(root.glob("*.json")) if root.exists() else []:
            try:
                raw = _json.loads(path.read_text("utf-8"))
                vantage = raw.get("vantage") if isinstance(raw, dict) else None
                if not isinstance(vantage, str) or not vantage.strip():
                    continue
                reports.append(HealthReport.from_dict(raw))
            except (ValueError, OSError, TypeError) as exc:  # half-written or hand-edited
                logger.warning("skipping source-health report %s: %s", path.name, exc)
        if not reports:
            return HTMLResponse(
                _(
                    "<!doctype html><meta charset=utf-8><style>body{font:15px/1.7 system-ui;margin:3rem auto;max-width:34rem}code{background:#f6f8fa;padding:.1rem .3rem;border-radius:4px}</style><h1>No probe records yet</h1><p>Run a probe first; the report is written into the lake, and this page reads it:</p><pre><code>cne sources probe --vantage cn</code></pre><p>This page only shows existing reports, and opening it does not contact third-party hosts. To probe, start it from the operations page, or run <code>cne sources probe</code>.</p>"
                ),
                status_code=404,
            )
        return HTMLResponse(render_page(reports))

    @app.get("/api/health", response_model=Health)
    def health(view: View) -> Health:
        # The overview page loads this first, so it is where the cache gets its
        # chance to notice the lake moved. A partial refresh usually finishes
        # within the wait; a long one carries on and the page says so.
        view.refresh_stats_in_background(wait=3.0)
        return Health(**view.health())

    @app.get("/api/tiers", response_model=list[Tier])
    def tiers(view: View) -> list[Tier]:
        return [Tier(**row) for row in view.tiers()]

    @app.get("/api/datasets", response_model=list[Dataset])
    def datasets(
        view: View,
        tier: Annotated[str | None, Query(description="Restrict to one L0–L9 tier.")] = None,
    ) -> list[Dataset]:
        return [Dataset(**row) for row in view.datasets(tier=tier)]

    def _known(dataset: str) -> None:
        from cnequity.domain.datasets import DATASETS

        if dataset not in DATASETS:
            raise HTTPException(404, f"unknown dataset {dataset!r}")

    @app.get("/api/read/{dataset}", response_model=ReadReceiptPage)
    def read_with_receipt(
        dataset: str,
        view: View,
        start: date,
        end: date,
        symbol: str | None = None,
        as_of: date | None = None,
        adjust: Annotated[str | None, Query(pattern="^(hfq|qfq|total_return)$")] = None,
        pit_mode: Annotated[str | None, Query(pattern="^(strict|best_effort)$")] = None,
    ) -> ReadReceiptPage:
        """Read a bounded window with retained revision and row provenance."""
        from cnequity.domain.datasets import DATASETS
        from cnequity.query import ReadReceiptError, load_with_receipt
        from cnequity.query.reader import ReaderError

        _known(dataset)
        spec = DATASETS[dataset]
        if spec.query_date_col is None:
            raise HTTPException(422, "dataset has no date-range query axis; use the Python API")
        if end < start or (end - start).days > 366:
            raise HTTPException(422, "read window must be 0–366 calendar days")
        if symbol is None and end != start:
            raise HTTPException(422, "multi-day HTTP reads require a symbol")
        if spec.pit and as_of is None:
            raise HTTPException(422, "PIT dataset requires as_of")
        try:
            result = load_with_receipt(
                dataset,
                config=view.config,
                start=start,
                end=end,
                symbols=[symbol] if symbol else None,
                as_of=as_of,
                adjust=adjust,
                pit_mode=pit_mode or ("strict" if spec.pit else None),
                require_replayable=True,
            )
        except (ReaderError, ReadReceiptError, ValueError) as exc:
            raise HTTPException(422, str(exc)) from exc
        if result.frame.height > 1000:
            raise HTTPException(413, "read exceeds 1000 rows; narrow the window or use Python")
        return ReadReceiptPage(rows=result.frame.to_dicts(), receipt=result.receipt)

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
        pit_mode: Annotated[
            str | None,
            Query(
                pattern="^(strict|best_effort)$",
                description="With as_of; best_effort (default) keeps backfilled rows.",
            ),
        ] = None,
        adjust: Annotated[str | None, Query(pattern="^(hfq|qfq|total_return)$")] = None,
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
                    pit_mode=pit_mode,
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

    @app.get("/api/runs", response_model=RunPage)
    def runs(
        view: View,
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
        offset: Annotated[int, Query(ge=0)] = 0,
        status: Annotated[
            str,
            Query(description="Comma-separated run statuses. Omit to list every run."),
        ] = "",
        open_runs: Annotated[
            bool,
            Query(
                alias="open",
                description="Only runs that still need attention. An older failure is left "
                "out once a later attempt of the same job succeeded, is running, or replaced "
                "it. A skipped session does not replace it. An init is left out once a later "
                "run completed the steps it missed.",
            ),
        ] = False,
    ) -> RunPage:
        chosen = tuple(dict.fromkeys(part.strip() for part in status.split(",") if part.strip()))
        unknown = [part for part in chosen if part not in RUN_STATUSES]
        if unknown:
            raise HTTPException(422, "unknown run status: " + ", ".join(unknown))
        total, rows, failed_daily = view.runs(
            limit=limit,
            offset=offset,
            statuses=chosen or None,
            open_only=open_runs,
        )
        return RunPage(
            total=total,
            offset=offset,
            limit=limit,
            runs=[RunSummary(**row) for row in rows],
            failed_daily_groups=failed_daily,
        )

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
