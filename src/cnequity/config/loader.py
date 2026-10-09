from __future__ import annotations

import logging
import math
import os
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Literal

from cnequity.domain.rate_limit import RateLimitSpec
from cnequity.domain.symbols import INGEST_UNIVERSES

logger = logging.getLogger(__name__)

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore


@dataclass
class WaveConfig:
    name: str
    parallel: bool
    steps: list[str]


@dataclass
class ScheduleGroup:
    at: str
    steps: list[str]
    # Groups use the same dependency DAG as configured waves.  The default is
    # parallel so a group invocation does not silently discard independent
    # source lanes; callers may opt out for a particularly fragile source.
    parallel: bool = True
    # "daily" or "weekly" (see orchestrator/cadence.py): a weekly group fetches
    # its history datasets on the last session of each week up to ISO
    # ``weekday``; its snapshot datasets still run every session.
    cadence: str = "daily"
    weekday: int = 5


@dataclass
class FailoverDatasetSpec:
    name: str
    primary: str
    backup: str
    compare_fields: list[str] = field(default_factory=lambda: ["close"])
    price_tolerance_bps: float = 10.0
    # ``on_demand`` snapshots are written only when a route/gap-fill invokes
    # the backup source. Their absence for a clean primary day is informative,
    # not a primary-data failure. ``daily`` requires a same-day peer.
    snapshot_cadence: Literal["on_demand", "daily"] = "on_demand"
    # When true, a candidate compact is not published if the independent
    # snapshot exceeds the configured drift gate.  Missing/on-demand snapshots
    # remain informational and never create a false failure.
    revision_gate: bool = False
    # ``0`` means compare the full comparable universe; otherwise use a
    # deterministic stratified sample of this many keys.
    sample_limit: int = 500
    # Fraction of unique comparable keys allowed to exceed field tolerance.
    # The default 0.0 is intentionally fail-closed for enabled gates.
    max_drift_fraction: float = 0.0


@dataclass
class Config:
    data_root: Path
    # "demo" / "sample" for the tiny lakes `cne init --profile demo|sample`
    # writes. Whole-lake judgements read it: a five-symbol lake measured
    # against the 42-dataset registry reports forty absences and exits 1,
    # which tells a new user their first run failed when it did exactly what
    # it promised. Nothing else branches on it, and a production config has
    # no reason to set it.
    lake_profile: str | None = None
    workers: int = 8
    batch_size: int = 100
    max_retries: int = 3
    retry_backoff_seconds: int = 5
    batch_stale_seconds: int = 3600
    tdx_enabled: bool = True
    tdx_min_interval_ms: int = 100
    tdx_lock_timeout_sec: float = 15.0
    tdx_servers: str = "auto"
    tdx_connect_timeout_sec: int = 10
    # Preferred standard-market host pool ("ip:port"). When set and servers=auto,
    # these are probed (in parallel) before the bundled fallback list.
    tdx_host_pool: list[str] = field(default_factory=list)
    # Test/demo escape hatch only: lets TDX adapters return fabricated rows
    # (labeled source="mock") instead of failing the batch.
    tdx_allow_mock: bool = False
    # QMT is the preferred daily-bars source when the local BigQMT bridge is
    # reachable.  Missing rows still fall through to the existing TDX lane.
    qmt_bridge_enabled: bool = False
    qmt_bridge_tdx_gap_fill: bool = True
    qmt_bridge_src_path: str | None = None
    qmt_bridge_client_config_path: str | None = None
    qmt_bridge_account_id: str | None = field(default=None, repr=False)
    qmt_bridge_timeout_seconds: float | None = 30.0
    qmt_bridge_download_timeout_seconds: float = 180.0
    qmt_bridge_data_wait_seconds: float = 10.0
    qmt_bridge_chunk_size: int = 0
    # "auto" infers 股/手 from amount / close / volume; "share" and "lot"
    # pin the bridge's QMT deployment.
    qmt_bridge_volume_unit: str = "auto"
    sources: dict[str, bool] = field(default_factory=dict)
    source_intervals: dict[str, float] = field(default_factory=dict)
    # Optional HTTP(S) proxy for EastMoneyClient (e.g. mainland egress for push2his).
    # Env HTTPS_PROXY / HTTP_PROXY still work when this is unset.
    eastmoney_proxy: str | None = None
    # Per-request timeout for EastMoneyClient (connect+read). Keep modest so
    # overseas daily groups fail fast instead of 30s × max_retries hangs.
    eastmoney_timeout_sec: float = 15.0
    # baostock free-API pacing (full-market history sweeps).
    baostock_batch_size: int = 20
    baostock_batch_rest_seconds: float = 120.0
    # Bound direct historical ST step calls. Init uses current snapshots;
    # explicit `cne backfill trading_status` removes this bound and checkpoints
    # the full sweep while retaining the provider pacing above.
    st_history_symbols_per_run: int = 400
    # Optional Tushare Pro token for historical BJ ST evidence.  The token is
    # read from [sources.tushare].token or TUSHARE_TOKEN and is never written
    # to manifests, checkpoints, or provenance.
    tushare_token: str | None = field(default=None, repr=False)
    tushare_timeout_sec: float = 30.0
    # Optional 同花顺 official API key. Read from [sources.ths_official].api_key
    # or HITHINK_FINANCE_API_KEY, and never written to manifests, checkpoints,
    # or provenance. The whole source is optional: with no key every path that
    # uses it degrades to silence and the lake keeps its existing sources.
    ths_official_api_key: str | None = field(default=None, repr=False)
    ths_official_timeout_sec: float = 30.0
    # Verification (arbitration, cross-checks) is safe to run whenever a key is
    # present — it writes source snapshots and findings, never curated rows.
    # Backfilling content is not: it changes what the lake holds, so it stays
    # off until asked for explicitly. One flag for each, never one for both.
    ths_official_verify_enabled: bool = True
    ths_official_backfill_enabled: bool = False
    # Which instrument classes the *ingest* covers.  `instruments` lists every code TDX
    # returns, a quarter of which are ETF/LOF/fund codes that no research
    # profile selects and that no configured vendor reliably serves.  Fetching
    # them costs a quarter of every sweep, trips the EastMoney and Sina circuit
    # breakers on symbols nobody needs, and leaves their unfillable keys in the
    # interior-gap gate — which then refuses to checkpoint the whole day.
    # "all_a" keeps ST, suspended, CDR and delisted names: those are exactly
    # what this lake exists to retain.
    ingest_universe: str = "all_a"
    # An explicit extension of the daily-bar ingest scope: only ETFs with a
    # recent, replayable, positively eligible exchange-directory snapshot.
    # Keep this off by default so existing A-share lakes do not acquire a
    # fund-wide fetch obligation merely by upgrading.
    ingest_eligible_etfs: bool = False
    # How many trailing sessions the per-symbol Sina leg reconciles on a daily
    # run. TDX has no Beijing route, so all ~580 BJ symbols come through that
    # one-request-per-symbol endpoint; running daily_bars' full 5-session
    # lookback through it is ~2,900 requests a run, which is what earns HTTP
    # 456 and a vendor-wide cooldown. The tip itself now comes from the BSE
    # board snapshot (~30 paginated requests), so this only governs how deep
    # the per-symbol backstop reaches behind it.
    bj_history_lookback_days: int = 1
    daily_waves: list[WaveConfig] = field(default_factory=list)
    schedule_groups: dict[str, ScheduleGroup] = field(default_factory=dict)
    # Beijing wall-clock times the scheduled daily pipeline and its snapshot
    # catch-up run each session (orchestrator/run_window.py). The host's own
    # timezone and daylight saving never enter into it.
    daily_run_at: str = "17:30"
    stale_run_at: str = "21:00"
    # Continuous event streams. Same shape as a daily schedule group, but the
    # job they belong to runs on the natural calendar and takes its own
    # ingestion lock, so a disclosure or news sweep neither waits for the
    # evening batch nor is skipped because the market was closed.
    events_groups: dict[str, ScheduleGroup] = field(default_factory=dict)
    init_phases: list[str] = field(default_factory=list)
    on_demand_enabled: bool = True
    on_demand_datasets: list[str] = field(default_factory=list)
    duckdb_path: Path | None = None
    duckdb_memory_limit: str = "2GB"
    duckdb_threads: int = 4
    # Who owns `margin_trading` rows: "exchange" (SSE + SZSE, the bodies that
    # compile the balances) or "eastmoney". The exchange path matched EastMoney
    # exactly on every compared field and carries more securities, but SSE does
    # not publish 融券余额, so `short_balance` is null on SH rows there.
    margin_trading_source: str = "exchange"
    adj_factors_source: str = "sina"
    adj_factors_types: list[str] = field(default_factory=lambda: ["hfq"])
    # Recompute each factor step from curated corporate_actions and report the
    # disagreement. The tolerance is a materiality threshold, not an equality
    # test: the two vendors round differently and a sub-tolerance gap says
    # nothing useful. Advisory only — it never fails the derive.
    adj_factors_crosscheck_enabled: bool = True
    adj_factors_crosscheck_tolerance_bps: float = 50.0
    adj_factors_crosscheck_error_bps: float = 200.0
    sentiment_use_snownlp: bool = False
    sentiment_news_symbol_limit: int = 50
    # Intraday capture is off by default and scoped when on. Full market 1m is
    # ~1.3M rows and ~30MB a day (6-8GB a year, several times the whole daily
    # lake), so the default scope is an index rather than every symbol.
    minute_bars_enabled: bool = False
    minute_bars_scope: str = "index:000300.SH"
    minute_bars_symbols: list[str] = field(default_factory=list)
    # Which frequencies to capture. Each lands in its own registered dataset
    # (1m -> minute_bars, 5m -> minute_bars_5m) because their horizons differ:
    # the source keeps 95 trading days of 1m against 491 of 5m.
    minute_bars_frequencies: list[str] = field(default_factory=lambda: ["1m"])
    # Concurrent TDX connections for intraday capture. 1 = one connection,
    # today's behaviour. This does NOT raise the request rate — the limiter
    # is cross-process and paces every request regardless — it only stops a
    # single lane idling on network latency between calls.
    minute_bars_fetch_workers: int = 4
    # Transaction records (分笔) get their own block rather than riding on
    # [minute_bars]. Both are opt-in intraday capture, but they are different
    # decisions by an order of magnitude: enabling 1m for an index is ~2MB a
    # day, enabling ticks for the whole market is ~60MB a day and ~20 minutes
    # of wire time. One switch must not turn on both.
    # Coverage warning thresholds retained for existing configurations. They
    # measure unresolved scope; they do not forbid independent validated facts
    # from publishing or turn a partial scope into complete evidence.
    daily_bars_unresolved_tolerance: float = 0.01
    daily_bars_tip_unresolved_tolerance: float = 0.002
    trade_ticks_enabled: bool = False
    # No 'all'. The guard is `trade_ticks_max_symbols` below, and a scope that
    # cannot be counted before it is resolved would slip past it.
    trade_ticks_scope: str = "watchlist"
    trade_ticks_symbols: list[str] = field(default_factory=list)
    # Hard ceiling on the resolved scope. A CSI300 scope resolves to ~300 and
    # therefore fails here until the user raises this deliberately — the
    # friction is the point, because the cost is theirs to accept.
    trade_ticks_max_symbols: int = 200
    trade_ticks_fetch_workers: int = 4
    # Futures and options (ADR-0013). Off by default and outside `cne init`:
    # the equity lake does not pay for a family nobody asked for. `exchanges`
    # empty means every exchange this release can read.
    futures_enabled: bool = False
    futures_exchanges: list[str] = field(default_factory=list)
    futures_options: bool = True
    # DCE's own endpoints answer with an access challenge; "sina" fills its
    # futures from Sina, "off" leaves DCE out.
    futures_dce_route: str = "sina"
    # One-minute bars for a watchlist: `minute_products` takes each product's
    # main and second contract ("CU.SHF"), `minute_contracts` names contracts.
    futures_minute_enabled: bool = False
    futures_minute_products: list[str] = field(default_factory=list)
    futures_minute_contracts: list[str] = field(default_factory=list)
    futures_minute_max_contracts: int = 100
    # `[research] holdout_start`: evidence inventories (`cne decision-data`)
    # refuse any window reaching this date, keeping an out-of-sample period
    # unseen. Unset, every window is allowed.
    research_holdout_start: date | None = None
    failover_enabled: bool = True
    # Backfill source snapshots are an optional audit artifact. Keeping them
    # off the canonical backfill path prevents a slow backup vendor from
    # blocking the primary historical fetch.
    failover_backfill_snapshots: bool = False
    failover_datasets: list[FailoverDatasetSpec] = field(default_factory=list)
    # `daily_bars` against the exchanges' own published closes. Prices measured
    # exactly equal across the shared universe, so the price tolerance is tight
    # and a breach is an error. Turnover carries a real definitional gap — the
    # exchange daily total folds in trading a continuous-auction bar excludes —
    # so it is judged on the *share* of the universe that diverges, not on any
    # single symbol.
    exchange_audit_price_tolerance_bps: float = 10.0
    exchange_audit_turnover_tolerance_bps: float = 100.0
    exchange_audit_turnover_max_fraction: float = 0.15
    config_path: Path | None = None
    _backfill: bool = False
    _backfill_symbols: list[str] | None = None
    _corporate_actions_baostock_repair: bool = False
    _corporate_actions_ths_repair: bool = False
    _corporate_actions_eastmoney_bj_repair: bool = False
    _bse_tip_repair: bool = False
    _bj_amount_repair: bool = False
    _tdx_amount_repair: bool = False
    _tdx_volume_repair: bool = False
    _turnover_repair: bool = False
    _sector_bars_force: bool = False
    _rate_limiters: object | None = field(default=None, repr=False)
    # ``workers`` is the legacy/global scheduler budget.  Source-specific
    # pools below deliberately do not inherit platform-specific process
    # restrictions from this value; in particular, macOS daily bars use a
    # thread pool when ``tdx_daily_backend=auto``.
    #
    # These fields intentionally live after the historical dataclass fields so
    # positional Config(...) callers keep their old argument order.
    # ``None`` means "follow the legacy workers value" for daily bars and
    # derives (with the macOS HTTP exception documented in the method below).
    tdx_daily_workers: int | None = None
    tdx_daily_backend: str = "auto"
    adj_factor_workers: int | None = None
    derive_workers: int | None = None
    # Maximum in-flight requests per source.  The pacing limiter remains the
    # authoritative QPS guard; this map only bounds concurrent callers from
    # parallel DAG waves and derive pools.  ``http_workers`` is retained as a
    # spelling aliases for configs written during the migration.
    source_concurrency: dict[str, int] = field(default_factory=dict)
    http_workers: dict[str, int] = field(default_factory=dict)
    source_workers: dict[str, int] = field(default_factory=dict)
    # Immutable wire-payload archive for high-value/snapshot-only feeds.  The
    # archive lives under ``meta/raw`` and redacts request secrets before it
    # writes metadata; an empty dataset list means use the built-in critical
    # set (resolved by ``should_archive_raw``).
    raw_archive_enabled: bool = True
    raw_archive_datasets: list[str] = field(default_factory=list)
    raw_archive_compression: str = "gzip"
    raw_archive_max_payload_bytes: int | None = 32 * 1024 * 1024
    # How long a source-empty observation may suppress another expensive
    # per-symbol retry.  The evidence is still invalidated by instrument /
    # status identity changes; this TTL is only a bound on a quiet source.
    # Keep the option at the end so historical positional Config(...) callers
    # retain their argument order.
    negative_evidence_ttl_days: int = 7
    # ISO weekday (1=Mon..7=Sun) on which datasets that declare a shallow
    # reconciliation window walk their full one instead. 0 disables tiering.
    deep_reconciliation_dow: int = 6
    # Probe keys this deployment's network cannot reach at all. They are still
    # measured and reported, but under `not_applicable` rather than as SLO
    # failures: a source that is absent is not a source that is degraded, and a
    # gate held open on one never closes. An egress whose IP EastMoney push2 has
    # banned lists its probes here while the ban lasts — the same host behind a
    # proxy onto an unbanned IP does not, which is exactly why this is declared
    # rather than inferred.
    slo_unreachable_sources: tuple[str, ...] = ()
    # "off" | "shadow" | "block".  The audit runs after compact, so it has
    # never been able to stop bad data reaching curated; "shadow" records what
    # it would have blocked so the gate can be switched on with evidence
    # rather than optimism.  See [quality].audit_gate.
    audit_gate: str = "shadow"
    # Full offline candidate audit; independent of post-publication run status.
    publication_gate: str = "off"
    # Keep new compatibility options at the end for the same reason. If an
    # explicit proxy fails only for push2his, this permits one retry without
    # proxy/environment routing; default-off avoids bypassing mandatory proxy
    # policies in existing deployments.
    eastmoney_direct_fallback: bool = False
    # Refuse every push2 / push2his / push2delay request before it is sent.
    # push2 bans an egress IP that crawls it too hard, and a daily run that
    # keeps knocking may keep the ban alive; this lets the IP rest while the
    # affected datasets fail fast exactly as they do under the ban.
    eastmoney_push2_paused: bool = False
    # After the first push2 refusal (403/429/5xx, a dropped connection, a
    # timeout), refuse every push2 request, on every host, until local
    # midnight. A failover to the backup hosts is what spread the 2026-09 ban.
    eastmoney_push2_breaker: bool = True
    # Hard cap on push2 requests per local day, across processes; 0 = no cap.
    # Size a deployment from its own ledger, not from another lake's counts.
    eastmoney_push2_daily_budget: int = 150
    # Serve every full-market clist caller (instruments list_date, valuation,
    # fund_flow, the clist bar fallback) from one union-field sweep per
    # closed market session instead of one sweep each.
    eastmoney_push2_shared_snapshot: bool = True
    # datacenter-web: breaker trips after this many refusals in a row (403/429/
    # 5xx, a refused/reset/dropped connection; not a slow-report timeout), or at
    # once on "busy" through every backoff. It serves most EastMoney datasets.
    eastmoney_datacenter_breaker: bool = True
    eastmoney_datacenter_breaker_strikes: int = 3
    # Requests per local day; 0 counts them (meta/rate_limits/eastmoney_guard.json)
    # without a cap. Set a cap from measured usage, not a guess: backfills of the
    # large reports legitimately need thousands.
    eastmoney_datacenter_daily_budget: int = 0
    # Optional egress-wide cap across push2 and datacenter; 0 = observe only.
    eastmoney_daily_budget: int = 0
    # Deno binary for the 同花顺 hexin-v token (adapters/ths/hexin.py); None
    # finds it on PATH or in the usual install locations.
    ths_js_runtime: str | None = None

    def __post_init__(self) -> None:
        """Normalize path-like fields for programmatic configurations.

        ``load_config`` already constructs ``Path`` objects, but callers that
        instantiate ``Config(data_root="...")`` should get the same effective
        behaviour.  This is especially important for the cross-process source
        limiter, which derives its shared ledger below ``meta_root``.
        """
        # ``load_config`` resolves data roots before constructing Config.  Do
        # the same for programmatic callers so manifests, staging and shared
        # limiter ledgers never depend on the process working directory.
        # ``absolute`` removes cwd dependence while preserving a configured
        # symlink.  Storage boundaries intentionally inspect that lexical path
        # and reject symlinked lake roots instead of silently following them.
        self.data_root = _absolute(Path(self.data_root).expanduser())
        if self.duckdb_path is not None:
            self.duckdb_path = Path(self.duckdb_path).expanduser().resolve()
        if self.config_path is not None:
            self.config_path = Path(self.config_path).expanduser().resolve()

    def rate_limit(self, source: str) -> None:
        self._validate_source_limits()
        if self._rate_limiters is None:
            from cnequity.adapters.throttle import SourceRateLimiters

            self._rate_limiters = SourceRateLimiters(self)
        self._rate_limiters.wait(source)  # type: ignore[union-attr]

    def defer_source(self, source: str, seconds: float) -> None:
        """Share a source cooling-off deadline with all local processes."""
        self._validate_source_limits()
        if self._rate_limiters is None:
            from cnequity.adapters.throttle import SourceRateLimiters

            self._rate_limiters = SourceRateLimiters(self)
        limiters = self._rate_limiters
        if hasattr(limiters, "defer"):
            limiters.defer(source, seconds)  # type: ignore[union-attr]

    @contextmanager
    def source_slot(
        self, source: str, *, metrics: dict | None = None, timeout: float | None = None
    ):
        """Hold one in-flight slot for an upstream request, without pacing it."""
        self._validate_source_limits()
        if self._rate_limiters is None:
            from cnequity.adapters.throttle import SourceRateLimiters

            self._rate_limiters = SourceRateLimiters(self)
        limiters = self._rate_limiters
        if hasattr(limiters, "slot"):
            with limiters.slot(source, metrics=metrics, timeout=timeout):  # type: ignore[union-attr]
                yield
        else:
            # Keep custom limiter doubles working; the real implementation is
            # always SourceRateLimiters and therefore always has a slot.
            yield

    @contextmanager
    def source_request(
        self, source: str, *, metrics: dict | None = None, timeout: float | None = None
    ):
        """Pace and bound one actual request to *source*.

        The context is deliberately request-scoped: callers must enter it
        immediately around ``get``/``post``/wire calls and leave it in a
        ``finally`` path.  This makes a slow request count toward the same
        cap as concurrent DAG waves and separate worker processes.
        """
        self._validate_source_limits()
        if self._rate_limiters is None:
            from cnequity.adapters.throttle import SourceRateLimiters

            self._rate_limiters = SourceRateLimiters(self)
        limiters = self._rate_limiters
        if hasattr(limiters, "request"):
            with limiters.request(source, metrics=metrics, timeout=timeout):  # type: ignore[union-attr]
                yield
        else:
            self.rate_limit(source)
            with self.source_slot(source, metrics=metrics, timeout=timeout):
                yield

    def _validate_source_limits(self) -> None:
        """Reject invalid explicit source caps before a limiter is built."""
        errors = _interval_errors(self)
        if errors:
            raise ValueError("; ".join(errors))
        for field_name, limits in (
            ("source_concurrency", self.source_concurrency),
            ("http_workers", self.http_workers),
            ("source_workers", self.source_workers),
        ):
            if not isinstance(limits, Mapping):
                raise ValueError(
                    f"orchestrator source concurrency {field_name} must be a mapping; "
                    f"got {limits!r}"
                )
            for source, value in limits.items():
                if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                    raise ValueError(
                        f"orchestrator source concurrency {field_name}[{source!r}] "
                        f"must be a positive integer; got {value!r}"
                    )

    def tdx_daily_worker_count(self) -> int:
        """Return the effective daily-bars lane count.

        ``tdx_daily_workers`` is intentionally optional for backwards
        compatibility.  Old Linux configs therefore retain their previous
        process-pool width, while old macOS configs remain conservative (the
        platform backend below uses threads and can be raised explicitly).
        """
        value = self.workers if self.tdx_daily_workers is None else self.tdx_daily_workers
        return max(1, int(value))

    def tdx_daily_executor(self) -> str:
        """Resolve the daily-bars executor to ``thread`` or ``process``."""
        import sys

        mode = str(self.tdx_daily_backend or "auto").strip().lower()
        if mode in {"thread", "threads", "threadpool"}:
            return "thread"
        if mode in {"process", "processes", "processpool"}:
            # Keep the public API safe even when a caller bypasses
            # ``validate_config`` and constructs Config directly.  The wire
            # client starts a heartbeat thread and must never be forked on
            # macOS.
            return "thread" if sys.platform == "darwin" else "process"
        # The wire client owns a heartbeat thread and is not fork-safe.  A
        # thread pool is therefore the safe default on macOS/Windows; Linux
        # keeps the established process isolation unless explicitly changed.
        return "thread" if sys.platform in {"darwin", "win32"} else "process"

    def source_concurrency_for(self, source: str, default: int | None = None) -> int:
        """Return a positive in-flight cap for *source*.

        ``source_concurrency`` wins over the migration alias ``http_workers``.
        A caller may provide a pool-local fallback; without one the global
        derive/scheduler budget is the conservative fallback.
        """
        self._validate_source_limits()
        value = self.source_concurrency.get(source)
        if value is None:
            value = self.http_workers.get(source)
        if value is None:
            value = self.source_workers.get(source)
        if value is None:
            value = default if default is not None else self.workers
        # Never turn an invalid explicit limit into one usable worker.  That
        # silently defeats the operator's safety budget and can overload a
        # source; ``validate_config`` reports the same condition before a CLI
        # run starts, while programmatic callers get a fail-closed error here.
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(
                f"source concurrency for {source!r} must be a positive integer; got {value!r}"
            )
        return value

    # Public aliases used by callers that describe this setting as a worker
    # cap rather than a concurrency cap.  Keep one implementation so both
    # spellings have identical precedence and fallback behaviour.
    def source_worker_count(self, source: str, default: int | None = None) -> int:
        return self.source_concurrency_for(source, default)

    def http_worker_count(self, source: str, default: int | None = None) -> int:
        return self.source_concurrency_for(source, default)

    def should_archive_raw(self, dataset: str) -> bool:
        """Whether source payloads for *dataset* should be retained."""
        if not self.raw_archive_enabled:
            return False
        if self.raw_archive_datasets:
            return dataset in self.raw_archive_datasets
        # Keep the default focused: these payloads are either hard to replay,
        # point-in-time sensitive, or have no honest historical source.
        from cnequity.domain.datasets import DATASETS, history_mode_for

        # ``regulatory_events`` is absent on purpose: it is derived from
        # ``announcement_index`` rows, so the wire capture that backs it is the
        # announcement one. Archiving it again would store the same CNINFO
        # pages twice under two dataset names.
        return dataset in {
            "announcement_index",
            "financial_statement_items",
            "share_structure",
            "shareholder_counts",
            "top_holders",
            "corporate_actions",
        } or (dataset in DATASETS and history_mode_for(DATASETS[dataset]) == "snapshot_only")

    def adj_factor_worker_count(self) -> int:
        """Return the effective adjustment-factor worker budget."""
        import sys

        value = self.adj_factor_workers
        if value is None:
            if self.derive_workers is not None:
                value = self.derive_workers
            elif sys.platform == "darwin":
                # The legacy macOS default is ``workers = 1`` because it
                # guarded the TDX process pool.  Adjustment factors use
                # independent HTTP clients, so retaining that value here
                # needlessly serialized a safe network-bound derive.
                value = 4
            else:
                value = self.workers
        return max(1, int(value))

    def tdx_rate_limit_spec(self) -> RateLimitSpec | None:
        if not self.tdx_enabled:
            return None
        self._validate_source_limits()
        return RateLimitSpec(
            str(self.rate_limit_root),
            "tdx_protocol",
            max(
                self.tdx_min_interval_ms / 1000.0,
                self.source_intervals.get("tdx_protocol", 0.0),
            ),
            self.tdx_lock_timeout_sec,
            # Fall back to the TDX lane width, not the global ``workers``
            # budget.  This cap is a socket-level semaphore; when it is
            # tighter than the pool feeding it, every TDX lane serializes
            # behind it.  ``cne config create`` writes ``workers = 1`` on
            # macOS/Windows, so the old fallback silently collapsed the whole
            # L0/L1 fan-out to one in-flight request on a default install.
            self.source_concurrency_for("tdx_protocol", self.tdx_daily_worker_count()),
            str(self.rate_limit_root),
            self.tdx_lock_timeout_sec,
        )

    @property
    def rate_limit_root(self) -> Path:
        """One local ledger directory for HTTP and wire traffic on this egress."""
        value = os.environ.get("CNE_RATE_LIMIT_ROOT")
        return _absolute(Path(value).expanduser()) if value else self.meta_root / "rate_limits"

    @property
    def manifest_path(self) -> Path:
        return self.data_root / "meta" / "manifest.db"

    @property
    def staging_root(self) -> Path:
        return self.data_root / "staging"

    @property
    def curated_root(self) -> Path:
        return self.data_root / "curated"

    @property
    def derived_root(self) -> Path:
        return self.data_root / "derived"

    @property
    def meta_root(self) -> Path:
        return self.data_root / "meta"


def _expand(path_str: str, data_root: Path) -> Path:
    return Path(path_str.replace("{data.root}", str(data_root))).expanduser().resolve()


def _absolute(path: Path) -> Path:
    """Normalize a path to an absolute lexical path without following links."""
    return Path(os.path.abspath(os.fspath(path)))


def _parse_tdx_host_pool(hosts_raw: object) -> list[str]:
    """Parse ``[tdx_protocol.hosts]``: a flat list, or {standard, extended}.

    Only ``standard`` (A-share main sites) feed stock/index fetches; extended
    (HK/futures) hosts do not serve A-share bars, so they are ignored here.
    """
    if isinstance(hosts_raw, list):
        entries = hosts_raw
    elif isinstance(hosts_raw, dict):
        entries = hosts_raw.get("standard", [])
    else:
        entries = []
    pool: list[str] = []
    for entry in entries:
        text = str(entry).strip()
        if ":" in text:
            pool.append(text)
    return pool


def _optional_date(value) -> date | None:
    """A TOML date (bare `2025-01-01`) or its quoted string form."""
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def push2_paused_by_env() -> bool:
    """Whether ``CNE_PUSH2_PAUSED`` forces push2 off, ahead of the config file."""
    return os.environ.get("CNE_PUSH2_PAUSED", "").strip().lower() in {"1", "true", "yes", "on"}


def load_config(path: str | Path) -> Config:
    config_path = Path(path).expanduser().resolve()
    with open(config_path, "rb") as f:
        raw = tomllib.load(f)

    data_root = _absolute(Path(raw.get("data", {}).get("root", "./data/cnequity")).expanduser())
    lake_profile = raw.get("data", {}).get("profile") or None
    orch = raw.get("orchestrator", {})
    tdx = raw.get("tdx_protocol", {})
    qmt = raw.get("qmt_bridge", {})
    sources_raw = raw.get("sources", {})
    derive_raw = raw.get("derive", {})

    # Worker budgets are deliberately parsed independently.  ``workers`` is
    # the legacy/global budget; specialized pools may opt in without changing
    # the behaviour of an existing config.  Accept both ``http_workers`` and
    # ``source_concurrency`` while the latter becomes the canonical spelling.
    source_concurrency: dict[str, int] = {}
    http_workers: dict[str, int] = {}
    http_raw = raw.get("http", {})
    source_workers: dict[str, int] = {}
    for key, target in (
        ("source_concurrency", source_concurrency),
        ("http_workers", http_workers),
        ("source_workers", source_workers),
    ):
        candidate_maps = (
            derive_raw.get(key, {}) if isinstance(derive_raw, dict) else {},
            http_raw.get(key, {}) if isinstance(http_raw, dict) else {},
            raw.get(key, {}),
            orch.get(key, {}),
        )
        for raw_limits in candidate_maps:
            if not isinstance(raw_limits, dict):
                if raw_limits not in ({}, None):
                    # Preserve a malformed map as a validation sentinel.  If
                    # it were dropped here, the limiter would silently fall
                    # back to the global worker budget.
                    target[f"<invalid:{key}>"] = raw_limits  # type: ignore[assignment]
                continue
            for source, value in raw_limits.items():
                # Preserve malformed values so ``validate_config`` can report
                # the offending source/key instead of silently dropping the
                # safety limit and falling back to the global worker count.
                # TOML integer values remain ints; floats, strings and bools
                # are intentionally left untouched and fail validation.
                target[str(source)] = value  # type: ignore[assignment]

    sources: dict[str, bool] = {}
    source_intervals: dict[str, float] = {}
    eastmoney_proxy: str | None = None
    eastmoney_direct_fallback = False
    eastmoney_push2_paused = False
    eastmoney_push2_breaker = True
    eastmoney_push2_daily_budget = 150
    eastmoney_push2_shared_snapshot = True
    eastmoney_datacenter_breaker = True
    eastmoney_datacenter_breaker_strikes = 3
    eastmoney_datacenter_daily_budget = 0
    eastmoney_daily_budget = 0
    # datacenter's own lane (source "eastmoney_dc"). It ran at 0.5 s / 4 in
    # flight for months without a busy reply; this halves the peak rate.
    datacenter_interval = 1.0
    datacenter_concurrency: object = 2
    ths_js_runtime: str | None = None
    # push2's own pacing lane (source "eastmoney_push2"): one request in flight,
    # one every few seconds. A ~60-page sweep takes ~4 minutes this way.
    push2_interval = 4.0
    push2_concurrency: object = 1
    eastmoney_timeout_sec = 15.0
    baostock_batch_size = 20
    baostock_batch_rest_seconds = 120.0
    tushare_token: str | None = os.environ.get("TUSHARE_TOKEN") or None
    tushare_timeout_sec = 30.0
    ths_official_api_key: str | None = os.environ.get("HITHINK_FINANCE_API_KEY") or None
    ths_official_timeout_sec = 30.0
    ths_official_verify_enabled = True
    ths_official_backfill_enabled = False
    for name, val in sources_raw.items():
        if isinstance(val, dict):
            sources[name] = bool(val.get("enabled", True))
            if "min_interval_seconds" in val:
                source_intervals[name] = val["min_interval_seconds"]
            # Source-local caps are convenient for operators because the
            # interval and the concurrency contract live beside each other.
            # ``max_concurrency`` is canonical; the shorter spellings are
            # accepted for compatibility with early migration configs.
            for key in ("max_concurrency", "concurrency", "max_in_flight", "workers"):
                if key in val:
                    # Keep the raw value for fail-closed validation.  A
                    # malformed source-local value must not disappear and
                    # silently inherit the global worker budget.
                    source_concurrency[name] = val[key]  # type: ignore[assignment]
                    break
            if name == "eastmoney" and val.get("proxy"):
                eastmoney_proxy = str(val["proxy"]).strip() or None
            if name == "eastmoney" and val.get("direct_fallback") is not None:
                eastmoney_direct_fallback = bool(val["direct_fallback"])
            if name == "eastmoney" and val.get("push2_paused") is not None:
                eastmoney_push2_paused = bool(val["push2_paused"])
            if name == "eastmoney" and val.get("push2_breaker") is not None:
                eastmoney_push2_breaker = bool(val["push2_breaker"])
            if name == "eastmoney" and val.get("push2_daily_budget") is not None:
                eastmoney_push2_daily_budget = int(val["push2_daily_budget"])
            if name == "eastmoney" and val.get("push2_shared_snapshot") is not None:
                eastmoney_push2_shared_snapshot = bool(val["push2_shared_snapshot"])
            if name == "eastmoney" and val.get("push2_min_interval_seconds") is not None:
                push2_interval = val["push2_min_interval_seconds"]
            if name == "eastmoney" and val.get("push2_max_concurrency") is not None:
                push2_concurrency = val["push2_max_concurrency"]
            if name == "eastmoney" and val.get("datacenter_breaker") is not None:
                eastmoney_datacenter_breaker = bool(val["datacenter_breaker"])
            if name == "eastmoney" and val.get("datacenter_breaker_strikes") is not None:
                eastmoney_datacenter_breaker_strikes = int(val["datacenter_breaker_strikes"])
            if name == "eastmoney" and val.get("datacenter_daily_budget") is not None:
                eastmoney_datacenter_daily_budget = int(val["datacenter_daily_budget"])
            if name == "eastmoney" and val.get("daily_budget") is not None:
                eastmoney_daily_budget = int(val["daily_budget"])
            if name == "eastmoney" and val.get("datacenter_min_interval_seconds") is not None:
                datacenter_interval = val["datacenter_min_interval_seconds"]
            if name == "eastmoney" and val.get("datacenter_max_concurrency") is not None:
                datacenter_concurrency = val["datacenter_max_concurrency"]
            if name == "ths" and val.get("js_runtime"):
                ths_js_runtime = str(val["js_runtime"]).strip() or None
            if name == "eastmoney" and val.get("timeout_sec") is not None:
                eastmoney_timeout_sec = float(val["timeout_sec"])
            # No eastmoney batch_size / batch_rest_seconds: the batch cool-down
            # is a baostock mechanism. Parsing them here made them look wired up
            # while every EastMoney sweep ran on min_interval_seconds alone.
            # Unknown keys are ignored, so configs still carrying them load fine.
            if name == "baostock":
                if val.get("batch_size") is not None:
                    baostock_batch_size = int(val["batch_size"])
                if val.get("batch_rest_seconds") is not None:
                    baostock_batch_rest_seconds = float(val["batch_rest_seconds"])
            if name == "tushare":
                if val.get("token"):
                    tushare_token = str(val["token"]).strip() or None
                if val.get("timeout_sec") is not None:
                    tushare_timeout_sec = float(val["timeout_sec"])
            if name == "ths_official":
                if val.get("api_key"):
                    ths_official_api_key = str(val["api_key"]).strip() or None
                if val.get("timeout_sec") is not None:
                    ths_official_timeout_sec = float(val["timeout_sec"])
                if val.get("verify") is not None:
                    ths_official_verify_enabled = bool(val["verify"])
                if val.get("backfill") is not None:
                    ths_official_backfill_enabled = bool(val["backfill"])
        else:
            sources[name] = bool(val)
    # An explicit [sources.eastmoney_push2] or source_concurrency entry wins.
    source_intervals.setdefault("eastmoney_push2", push2_interval)
    source_concurrency.setdefault("eastmoney_push2", push2_concurrency)  # type: ignore[arg-type]
    source_intervals.setdefault("eastmoney_dc", datacenter_interval)
    source_concurrency.setdefault("eastmoney_dc", datacenter_concurrency)  # type: ignore[arg-type]
    # 同花顺 data pages (money-flow fallback): one page every 3 s. Its list pages
    # have returned 401 after ~20 quick requests; the lane shares the `ths`
    # in-flight cap, which ths_pages keeps at 1.
    source_intervals.setdefault("ths_data", 3.0)
    # Historical constituent files are also network requests. These lanes
    # were absent from the example and previously ran without any spacing.
    source_intervals.setdefault("sw", 1.0)
    source_intervals.setdefault("cni", 1.0)
    source_concurrency.setdefault("sw", 1)
    source_concurrency.setdefault("cni", 1)
    # The scheduler's late stale-only pass sets this so it never touches push2.
    if push2_paused_by_env():
        eastmoney_push2_paused = True

    daily_waves: list[WaveConfig] = []
    for wave in raw.get("job", {}).get("daily", {}).get("waves", []):
        daily_waves.append(
            WaveConfig(
                name=wave["name"],
                parallel=bool(wave.get("parallel", True)),
                steps=list(wave.get("steps", [])),
            )
        )

    schedule_groups: dict[str, ScheduleGroup] = {}
    daily_run_at = str(raw.get("job", {}).get("daily", {}).get("run_at", "17:30")).strip()
    stale_run_at = str(raw.get("job", {}).get("stale", {}).get("run_at", "21:00")).strip()
    groups_raw = raw.get("job", {}).get("daily", {}).get("groups", {})
    for name, group in groups_raw.items():
        schedule_groups[name] = ScheduleGroup(
            at=group.get("at", "16:00"),
            steps=list(group.get("steps", [])),
            parallel=bool(group.get("parallel", True)),
            cadence=str(group.get("cadence", "daily")).strip().lower(),
            weekday=int(group.get("weekday", 5)),
        )

    events_groups: dict[str, ScheduleGroup] = {}
    events_raw = raw.get("job", {}).get("events", {}).get("groups", {})
    for name, group in events_raw.items():
        events_groups[name] = ScheduleGroup(
            at=group.get("at", "08:00"),
            steps=list(group.get("steps", [])),
            parallel=bool(group.get("parallel", True)),
        )

    duckdb_raw = raw.get("duckdb", {})
    duckdb_path_str = duckdb_raw.get("path")
    duckdb_path = (
        _expand(duckdb_path_str, data_root)
        if duckdb_path_str
        else data_root / "duckdb" / "cnequity.duckdb"
    )

    on_demand = raw.get("on_demand", {})
    adj_raw = raw.get("adj_factors", {})
    margin_raw = raw.get("margin_trading", {})
    sentiment_raw = raw.get("sentiment", {})
    failover_raw = raw.get("failover", {})
    exchange_audit_raw = raw.get("exchange_audit", {})
    incremental_raw = raw.get("incremental", {})
    quality_raw = raw.get("quality", {})
    if not isinstance(quality_raw, dict):
        quality_raw = {}
    raw_archive_raw = raw.get("raw_archive", {})
    if not isinstance(raw_archive_raw, dict):
        raw_archive_raw = {}
    failover_datasets: list[FailoverDatasetSpec] = []
    for item in failover_raw.get("datasets", []):
        failover_datasets.append(
            FailoverDatasetSpec(
                name=str(item["name"]),
                primary=str(item.get("primary", "tdx_protocol")),
                backup=str(item.get("backup", "eastmoney")),
                compare_fields=list(item.get("compare_fields", ["close"])),
                price_tolerance_bps=float(item.get("price_tolerance_bps", 10.0)),
                snapshot_cadence=str(item.get("snapshot_cadence", "on_demand")),
                revision_gate=bool(item.get("revision_gate", False)),
                sample_limit=int(item.get("sample_limit", 500)),
                max_drift_fraction=float(item.get("max_drift_fraction", 0.0)),
            )
        )

    minute_raw = raw.get("minute_bars", {})
    ticks_raw = raw.get("trade_ticks", {})
    futures_raw = raw.get("futures", {})
    init_raw = raw.get("job", {}).get("init", {})
    phases_block = init_raw.get("phases", init_raw)
    init_phases = list(phases_block.get("names", init_raw.get("names", [])))
    adj_factor_workers_raw = orch.get(
        "adj_factor_workers",
        adj_raw.get("workers", adj_raw.get("fetch_workers")),
    )
    derive_workers_raw = orch.get("derive_workers", derive_raw.get("workers"))

    if "default" in raw.get("universe", {}):
        # Parsed into `universe_default` for a long time and read by nothing:
        # `load()` resolves its universe from the call and the profile, never
        # from config. Keep loading the file — an operator who wrote this down
        # is not wrong about wanting it, only about it working.
        logger.warning(
            "[universe].default is not read by anything and has no effect; "
            "pass universe= (or universe_profile=) to load() instead"
        )

    ingest_eligible_etfs = raw.get("universe", {}).get("ingest_eligible_etfs", False)
    if not isinstance(ingest_eligible_etfs, bool):
        raise ValueError("[universe].ingest_eligible_etfs must be true or false")

    def _resolve_qmt_path(value: object) -> str | None:
        if not value:
            return None
        path = Path(str(value)).expanduser()
        if not path.is_absolute():
            path = config_path.parent / path
        return str(path.resolve())

    cfg = Config(
        data_root=data_root,
        lake_profile=str(lake_profile) if lake_profile else None,
        workers=int(orch.get("workers", 8)),
        tdx_daily_workers=(
            int(orch["tdx_daily_workers"]) if orch.get("tdx_daily_workers") is not None else None
        ),
        tdx_daily_backend=str(
            orch.get("tdx_daily_backend", orch.get("tdx_daily_executor", "auto"))
        ),
        adj_factor_workers=(
            int(adj_factor_workers_raw) if adj_factor_workers_raw is not None else None
        ),
        derive_workers=(int(derive_workers_raw) if derive_workers_raw is not None else None),
        source_concurrency=source_concurrency,
        http_workers=http_workers,
        source_workers=source_workers,
        raw_archive_enabled=bool(raw_archive_raw.get("enabled", True)),
        raw_archive_datasets=[str(item) for item in raw_archive_raw.get("datasets", [])],
        raw_archive_compression=str(raw_archive_raw.get("compression", "gzip")),
        raw_archive_max_payload_bytes=(
            int(raw_archive_raw["max_payload_bytes"])
            if raw_archive_raw.get("max_payload_bytes") is not None
            else None
        ),
        batch_size=int(orch.get("batch_size", 100)),
        max_retries=int(orch.get("max_retries", 3)),
        retry_backoff_seconds=int(orch.get("retry_backoff_seconds", 5)),
        batch_stale_seconds=int(orch.get("batch_stale_seconds", 3600)),
        tdx_enabled=bool(tdx.get("enabled", True)),
        tdx_min_interval_ms=int(tdx.get("min_interval_ms", 100)),
        tdx_lock_timeout_sec=float(tdx.get("lock_timeout_sec", 15.0)),
        tdx_servers=str(tdx.get("servers", "auto")),
        tdx_connect_timeout_sec=int(tdx.get("connect_timeout_sec", 10)),
        tdx_host_pool=_parse_tdx_host_pool(tdx.get("hosts", {})),
        tdx_allow_mock=bool(tdx.get("allow_mock", False)),
        qmt_bridge_enabled=bool(qmt.get("enabled", False)),
        qmt_bridge_tdx_gap_fill=bool(qmt.get("tdx_gap_fill", True)),
        qmt_bridge_src_path=_resolve_qmt_path(qmt.get("bridge_src_path")),
        qmt_bridge_client_config_path=_resolve_qmt_path(qmt.get("client_config_path")),
        qmt_bridge_account_id=str(qmt["account_id"]) if qmt.get("account_id") else None,
        qmt_bridge_timeout_seconds=(
            float(qmt["timeout_seconds"]) if qmt.get("timeout_seconds") is not None else 30.0
        ),
        qmt_bridge_download_timeout_seconds=float(qmt.get("download_timeout_seconds", 180.0)),
        qmt_bridge_data_wait_seconds=float(qmt.get("data_wait_seconds", 10.0)),
        qmt_bridge_chunk_size=int(qmt.get("chunk_size", 0)),
        qmt_bridge_volume_unit=str(qmt.get("volume_unit", "auto")),
        sources=sources,
        source_intervals=source_intervals,
        eastmoney_proxy=eastmoney_proxy,
        eastmoney_direct_fallback=eastmoney_direct_fallback,
        eastmoney_push2_paused=eastmoney_push2_paused,
        eastmoney_push2_breaker=eastmoney_push2_breaker,
        eastmoney_push2_daily_budget=eastmoney_push2_daily_budget,
        eastmoney_push2_shared_snapshot=eastmoney_push2_shared_snapshot,
        eastmoney_datacenter_breaker=eastmoney_datacenter_breaker,
        eastmoney_datacenter_breaker_strikes=eastmoney_datacenter_breaker_strikes,
        eastmoney_datacenter_daily_budget=eastmoney_datacenter_daily_budget,
        eastmoney_daily_budget=eastmoney_daily_budget,
        ths_js_runtime=ths_js_runtime,
        eastmoney_timeout_sec=eastmoney_timeout_sec,
        baostock_batch_size=baostock_batch_size,
        baostock_batch_rest_seconds=baostock_batch_rest_seconds,
        tushare_token=tushare_token,
        tushare_timeout_sec=tushare_timeout_sec,
        ths_official_api_key=ths_official_api_key,
        ths_official_timeout_sec=ths_official_timeout_sec,
        ths_official_verify_enabled=ths_official_verify_enabled,
        ths_official_backfill_enabled=ths_official_backfill_enabled,
        ingest_universe=str(raw.get("universe", {}).get("ingest", "all_a")).strip().lower(),
        ingest_eligible_etfs=ingest_eligible_etfs,
        bj_history_lookback_days=int(
            raw.get("sources", {}).get("sina_bars", {}).get("reconciliation_lookback_days", 1)
        ),
        daily_waves=daily_waves,
        schedule_groups=schedule_groups,
        daily_run_at=daily_run_at,
        stale_run_at=stale_run_at,
        events_groups=events_groups,
        init_phases=init_phases,
        on_demand_enabled=bool(on_demand.get("enabled", True)),
        on_demand_datasets=list(on_demand.get("datasets", [])),
        duckdb_path=duckdb_path,
        duckdb_memory_limit=str(duckdb_raw.get("memory_limit", "2GB")),
        duckdb_threads=int(duckdb_raw.get("threads", 4)),
        margin_trading_source=str(margin_raw.get("source", "exchange")),
        adj_factors_source=str(adj_raw.get("source", "sina")),
        adj_factors_types=list(adj_raw.get("adjust_types", ["hfq"])),
        adj_factors_crosscheck_enabled=bool(adj_raw.get("crosscheck_enabled", True)),
        adj_factors_crosscheck_tolerance_bps=float(adj_raw.get("crosscheck_tolerance_bps", 50.0)),
        adj_factors_crosscheck_error_bps=float(adj_raw.get("crosscheck_error_bps", 200.0)),
        sentiment_use_snownlp=bool(sentiment_raw.get("use_snownlp", False)),
        sentiment_news_symbol_limit=int(sentiment_raw.get("news_symbol_limit", 50)),
        minute_bars_enabled=bool(minute_raw.get("enabled", False)),
        minute_bars_scope=str(minute_raw.get("scope", "index:000300.SH")),
        minute_bars_symbols=list(minute_raw.get("symbols", [])),
        minute_bars_frequencies=list(minute_raw.get("frequencies", ["1m"])),
        minute_bars_fetch_workers=int(minute_raw.get("fetch_workers", 4)),
        trade_ticks_enabled=bool(ticks_raw.get("enabled", False)),
        trade_ticks_scope=str(ticks_raw.get("scope", "watchlist")),
        trade_ticks_symbols=list(ticks_raw.get("symbols", [])),
        trade_ticks_max_symbols=int(ticks_raw.get("max_symbols", 200)),
        trade_ticks_fetch_workers=int(ticks_raw.get("fetch_workers", 4)),
        futures_enabled=bool(futures_raw.get("enabled", False)),
        futures_exchanges=[str(x).strip().upper() for x in futures_raw.get("exchanges", [])],
        futures_options=bool(futures_raw.get("options", True)),
        futures_dce_route=str(futures_raw.get("dce_route", "sina")).strip().lower(),
        futures_minute_enabled=bool(futures_raw.get("minute_enabled", False)),
        futures_minute_products=[
            str(x).strip().upper() for x in futures_raw.get("minute_products", [])
        ],
        futures_minute_contracts=[
            str(x).strip().upper() for x in futures_raw.get("minute_contracts", [])
        ],
        futures_minute_max_contracts=int(futures_raw.get("minute_max_contracts", 100)),
        research_holdout_start=_optional_date(raw.get("research", {}).get("holdout_start")),
        failover_enabled=bool(failover_raw.get("enabled", True)),
        failover_backfill_snapshots=bool(failover_raw.get("backfill_snapshots", False)),
        failover_datasets=failover_datasets,
        exchange_audit_price_tolerance_bps=float(
            exchange_audit_raw.get("price_tolerance_bps", 10.0)
        ),
        exchange_audit_turnover_tolerance_bps=float(
            exchange_audit_raw.get("turnover_tolerance_bps", 100.0)
        ),
        exchange_audit_turnover_max_fraction=float(
            exchange_audit_raw.get("turnover_max_fraction", 0.15)
        ),
        negative_evidence_ttl_days=int(incremental_raw.get("negative_evidence_ttl_days", 7)),
        deep_reconciliation_dow=int(incremental_raw.get("deep_reconciliation_dow", 6)),
        slo_unreachable_sources=tuple(
            str(value).strip()
            for value in (raw.get("sources", {}).get("slo", {}).get("unreachable", []) or [])
            if str(value).strip()
        ),
        audit_gate=str(quality_raw.get("audit_gate", "shadow")).strip().lower(),
        publication_gate=str(quality_raw.get("publication_gate", "off")).strip().lower(),
        config_path=config_path,
    )
    return cfg


def _interval_errors(cfg: Config) -> list[str]:
    values = {
        "tdx_protocol.min_interval_ms": cfg.tdx_min_interval_ms,
        **{
            f"sources.{key}.min_interval_seconds": value
            for key, value in cfg.source_intervals.items()
        },
    }
    return [
        f"{key} must be a finite number >= 0"
        for key, value in values.items()
        if isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ]


def validate_config(cfg: Config) -> list[str]:
    import sys

    import cnequity.steps  # noqa: F401 — register steps
    from cnequity.orchestrator.registry import STEP_REGISTRY

    errors: list[str] = _interval_errors(cfg)
    if cfg.workers < 1:
        errors.append("orchestrator.workers must be >= 1")
    if cfg.tdx_daily_workers is not None and cfg.tdx_daily_workers < 1:
        errors.append("orchestrator.tdx_daily_workers must be >= 1")
    if cfg.adj_factor_workers is not None and cfg.adj_factor_workers < 1:
        errors.append("orchestrator.adj_factor_workers must be >= 1")
    if cfg.derive_workers is not None and cfg.derive_workers < 1:
        errors.append("orchestrator.derive_workers must be >= 1")
    backend = str(cfg.tdx_daily_backend or "auto").strip().lower()
    if backend not in {
        "auto",
        "thread",
        "threads",
        "threadpool",
        "process",
        "processes",
        "processpool",
    }:
        errors.append("orchestrator.tdx_daily_backend must be 'auto', 'thread', or 'process'")
    import re as _re

    for key in ("daily_run_at", "stale_run_at"):
        value = str(getattr(cfg, key, ""))
        match = _re.fullmatch(r"(\d{1,2}):(\d{2})", value)
        if not match or int(match.group(1)) > 23 or int(match.group(2)) > 59:
            section = "job.daily" if key == "daily_run_at" else "job.stale"
            errors.append(f"{section}.run_at must be Beijing time HH:MM; got {value!r}")
    for name, group in (getattr(cfg, "schedule_groups", None) or {}).items():
        if getattr(group, "cadence", "daily") not in {"daily", "weekly"}:
            errors.append(
                f"job.daily.groups.{name}.cadence must be 'daily' or 'weekly'; "
                f"got {group.cadence!r}"
            )
        if not 1 <= int(getattr(group, "weekday", 5)) <= 7:
            errors.append(
                f"job.daily.groups.{name}.weekday must be an ISO weekday 1-7; got {group.weekday!r}"
            )
    for field_name, limits in (
        ("source_concurrency", cfg.source_concurrency),
        ("http_workers", cfg.http_workers),
        ("source_workers", cfg.source_workers),
    ):
        if not isinstance(limits, Mapping):
            errors.append(
                f"orchestrator source concurrency {field_name} must be a mapping; got {limits!r}"
            )
            continue
        for source, value in limits.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                errors.append(
                    f"orchestrator source concurrency {field_name}[{source!r}] "
                    f"must be a positive integer; "
                    f"got {value!r}"
                )
                continue
            if str(source).strip().lower() == "baostock" and value > 1:
                errors.append(
                    f"orchestrator source concurrency {field_name}[{source!r}] "
                    "must be 1: BaoStock allows one connection at a time"
                )
    # The TDX client is not fork-safe; ProcessPool on macOS is the OOM / BrokenProcessPool
    # footgun that wiped notes under load. Refuse the unsafe combo loudly.
    if sys.platform == "darwin" and cfg.workers > 1:
        errors.append(
            "orchestrator.workers must be 1 on macOS "
            "(TDX client + ProcessPool fork is unsafe; use workers = 1)"
        )
    if sys.platform == "darwin" and backend in {"process", "processes", "processpool"}:
        errors.append(
            "orchestrator.tdx_daily_backend must not be process on macOS "
            "(TDX client is not fork-safe; use thread or auto)"
        )
    if cfg.batch_size < 1:
        errors.append("orchestrator.batch_size must be >= 1")
    if not 0 <= cfg.deep_reconciliation_dow <= 7:
        errors.append("[incremental].deep_reconciliation_dow must be 0-7 (0 disables tiering)")
    if cfg.negative_evidence_ttl_days < 0:
        errors.append("[incremental].negative_evidence_ttl_days must be >= 0")
    if cfg.bj_history_lookback_days < 1:
        errors.append("[sources.sina_bars].reconciliation_lookback_days must be >= 1")
    if cfg.ingest_universe not in INGEST_UNIVERSES:
        known = ", ".join(sorted(INGEST_UNIVERSES))
        errors.append(f"[universe].ingest must be one of: {known}")
    if cfg.audit_gate not in {"off", "shadow", "block"}:
        errors.append("[quality].audit_gate must be one of: off, shadow, block")
    if cfg.publication_gate not in {"off", "shadow", "block"}:
        errors.append("[quality].publication_gate must be one of: off, shadow, block")
    if cfg.qmt_bridge_volume_unit not in {"auto", "share", "lot"}:
        errors.append("[qmt_bridge].volume_unit must be 'auto', 'share', or 'lot'")
    if cfg.qmt_bridge_timeout_seconds is not None and cfg.qmt_bridge_timeout_seconds <= 0:
        errors.append("[qmt_bridge].timeout_seconds must be > 0")
    if cfg.qmt_bridge_download_timeout_seconds <= 0:
        errors.append("[qmt_bridge].download_timeout_seconds must be > 0")
    if cfg.qmt_bridge_data_wait_seconds < 0:
        errors.append("[qmt_bridge].data_wait_seconds must be >= 0")
    if cfg.qmt_bridge_chunk_size < 0:
        errors.append("[qmt_bridge].chunk_size must be >= 0")
    if cfg.raw_archive_compression not in {"gzip", "none"}:
        errors.append("[raw_archive].compression must be 'gzip' or 'none'")
    if cfg.raw_archive_max_payload_bytes is not None and cfg.raw_archive_max_payload_bytes < 1:
        errors.append("[raw_archive].max_payload_bytes must be >= 1")
    if cfg.raw_archive_datasets:
        from cnequity.domain.datasets import DATASETS

        unknown_raw = sorted(set(cfg.raw_archive_datasets) - set(DATASETS))
        if unknown_raw:
            errors.append(
                "[raw_archive].datasets contains unknown dataset(s): " + ", ".join(unknown_raw)
            )
    servers = cfg.tdx_servers.strip()
    if servers.lower() != "auto" and ":" not in servers:
        errors.append("[tdx_protocol].servers must be 'auto' or host:port")
    if cfg.tdx_connect_timeout_sec < 1:
        errors.append("[tdx_protocol].connect_timeout_sec must be >= 1")
    if cfg.tdx_min_interval_ms < 0:
        errors.append("[tdx_protocol].min_interval_ms must be >= 0")
    if cfg.tdx_lock_timeout_sec <= 0:
        errors.append("[tdx_protocol].lock_timeout_sec must be > 0")
    # Demo/sample configs support reading a small lake and deliberately omit
    # production scheduling. `run daily` checks for waves before executing.
    if not cfg.daily_waves and cfg.lake_profile not in {"demo", "sample"}:
        errors.append("job.daily.waves must define at least one wave")

    from cnequity.domain.datasets import DATASETS, intraday_datasets

    known_sources: set[str] = set()
    for ds_spec in DATASETS.values():
        known_sources.add(ds_spec.primary_source)
        if ds_spec.backup_source:
            known_sources.add(ds_spec.backup_source)
        if ds_spec.backfill_source:
            known_sources.add(ds_spec.backfill_source)

    seen_failover_names: set[str] = set()
    for spec in cfg.failover_datasets:
        if spec.snapshot_cadence not in {"on_demand", "daily"}:
            errors.append(
                f"failover dataset {spec.name!r}: snapshot_cadence must be 'on_demand' or 'daily'"
            )
        if spec.sample_limit < 0:
            errors.append(f"failover dataset {spec.name!r}: sample_limit must be >= 0")
        if not 0.0 <= spec.max_drift_fraction <= 1.0:
            errors.append(
                f"failover dataset {spec.name!r}: max_drift_fraction must be between 0 and 1"
            )
        if spec.name in seen_failover_names:
            errors.append(f"[[failover.datasets]]: duplicate entry for {spec.name!r}")
        seen_failover_names.add(spec.name)
        if spec.name not in DATASETS:
            errors.append(
                f"[[failover.datasets]]: {spec.name!r} is not a registered dataset "
                f"(available: {', '.join(sorted(DATASETS))})"
            )
        if spec.primary not in known_sources:
            errors.append(
                f"failover dataset {spec.name!r}: unknown primary source {spec.primary!r} "
                f"(known sources: {', '.join(sorted(known_sources))})"
            )
        if spec.backup not in known_sources:
            errors.append(
                f"failover dataset {spec.name!r}: unknown backup source {spec.backup!r} "
                f"(known sources: {', '.join(sorted(known_sources))})"
            )

    # Each frequency must have a dataset to land in, or its rows would have
    # nowhere to go and its horizon nowhere to be declared.
    known_frequencies = intraday_datasets()
    for frequency in cfg.minute_bars_frequencies:
        if frequency not in known_frequencies:
            errors.append(
                f"[minute_bars].frequencies: {frequency!r} has no registered dataset "
                f"(available: {', '.join(sorted(known_frequencies))})"
            )
    if cfg.minute_bars_enabled and not cfg.minute_bars_frequencies:
        errors.append("[minute_bars].enabled = true but frequencies is empty")
    if cfg.minute_bars_fetch_workers < 1:
        errors.append("[minute_bars].fetch_workers must be >= 1")

    scope = (cfg.trade_ticks_scope or "").strip()
    if scope == "all":
        # Rejected here rather than at run time: a full-market tick sweep is
        # ~9,600 requests and ~60MB a day, and finding that out twenty minutes
        # into a run is finding out too late.
        errors.append(
            "[trade_ticks].scope = 'all' is not supported — a full-market tick sweep "
            "is ~9,600 requests and ~60MB per session. Use 'watchlist' or "
            "'index:<symbol>', and raise [trade_ticks].max_symbols deliberately."
        )
    elif scope and scope != "watchlist" and not scope.startswith("index:"):
        errors.append(
            f"[trade_ticks].scope {scope!r} is not understood "
            "(expected 'watchlist' or 'index:<symbol>')"
        )
    if cfg.trade_ticks_enabled and scope == "watchlist" and not cfg.trade_ticks_symbols:
        errors.append("[trade_ticks].scope = 'watchlist' but symbols is empty")
    if cfg.trade_ticks_max_symbols < 1:
        errors.append("[trade_ticks].max_symbols must be >= 1")
    if cfg.trade_ticks_fetch_workers < 1:
        errors.append("[trade_ticks].fetch_workers must be >= 1")

    from cnequity.adapters.futures_exchange import SUPPORTED_EXCHANGES

    for exchange in cfg.futures_exchanges:
        if exchange not in SUPPORTED_EXCHANGES:
            errors.append(
                f"[futures].exchanges: {exchange!r} is not readable by this release "
                f"(available: {', '.join(SUPPORTED_EXCHANGES)})"
            )
    if cfg.futures_minute_max_contracts < 1:
        errors.append("[futures].minute_max_contracts must be >= 1")
    if (
        cfg.futures_minute_enabled
        and not cfg.futures_minute_products
        and not cfg.futures_minute_contracts
    ):
        errors.append(
            "[futures].minute_enabled = true but minute_products and minute_contracts are empty"
        )
    if cfg.futures_dce_route not in {"sina", "official", "off"}:
        errors.append(
            f"[futures].dce_route: {cfg.futures_dce_route!r} is not understood "
            "(expected 'sina', 'official' or 'off')"
        )

    referenced: list[tuple[str, str]] = []
    for wave in cfg.daily_waves:
        if not wave.steps:
            errors.append(f"wave '{wave.name}' has no steps")
        for step in wave.steps:
            referenced.append((f"wave '{wave.name}'", step))

    for group_name, group in cfg.schedule_groups.items():
        for step in group.steps:
            referenced.append((f"group '{group_name}'", step))

    for group_name, group in cfg.events_groups.items():
        if not group.steps:
            errors.append(f"events group '{group_name}' has no steps")
        for step in group.steps:
            referenced.append((f"events group '{group_name}'", step))

    for location, step in referenced:
        if step not in STEP_REGISTRY:
            errors.append(f"{location}: unknown step '{step}' (not registered)")

    errors.extend(_events_group_errors(cfg, STEP_REGISTRY))
    return errors


def _events_group_errors(cfg: Config, step_registry: Mapping[str, object]) -> list[str]:
    """Check what may be scheduled as a 7x24 event stream.

    The events job is exempt from the trading-day gate, and that exemption is
    only honest for feeds whose date axis is the natural calendar. Scheduling a
    session dataset there would ask a source for a day the market never held —
    and, because the two jobs hold different locks, scheduling the same step in
    both would let the daily batch and an event sweep ingest one dataset at the
    same time. Both are configuration mistakes, caught before a run starts.
    """
    from cnequity.domain.datasets import calendar_scope_datasets
    from cnequity.orchestrator.registry import FINALIZE_STEP_GROUPS

    if not cfg.events_groups:
        return []

    calendar_steps = calendar_scope_datasets()
    daily_steps = {step for wave in cfg.daily_waves for step in wave.steps}
    daily_steps |= {step for group in cfg.schedule_groups.values() for step in group.steps}

    errors: list[str] = []
    for group_name, group in cfg.events_groups.items():
        for step in group.steps:
            entry = step_registry.get(step)
            if entry is None or getattr(entry, "group", "") in FINALIZE_STEP_GROUPS:
                # Unknown steps are reported by the caller; a finalize step
                # (compact) publishes whatever the group staged and belongs to
                # every job.
                continue
            if step not in calendar_steps:
                errors.append(
                    f"events group '{group_name}': '{step}' is a trading-session dataset. "
                    "The events job runs on natural days, so only calendar-scoped feeds "
                    f"belong here ({', '.join(sorted(calendar_steps))})"
                )
            elif step in daily_steps:
                errors.append(
                    f"events group '{group_name}': '{step}' is also scheduled in the daily "
                    "job. The two jobs hold different locks and would ingest it twice — "
                    "remove it from [job.daily]"
                )
    return errors
