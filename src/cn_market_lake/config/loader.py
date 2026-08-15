from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from cn_market_lake.domain.rate_limit import RateLimitSpec

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


@dataclass
class FailoverDatasetSpec:
    name: str
    primary: str
    backup: str
    compare_fields: list[str] = field(default_factory=lambda: ["close"])
    price_tolerance_bps: float = 10.0


@dataclass
class Config:
    data_root: Path
    workers: int = 8
    batch_size: int = 100
    max_retries: int = 3
    retry_backoff_seconds: int = 5
    batch_stale_seconds: int = 3600
    tdx_enabled: bool = True
    tdx_min_interval_ms: int = 50
    tdx_servers: str = "auto"
    tdx_connect_timeout_sec: int = 10
    # Preferred standard-market host pool ("ip:port"). When set and servers=auto,
    # these are probed (in parallel) before the bundled fallback list.
    tdx_host_pool: list[str] = field(default_factory=list)
    # Test/demo escape hatch only: lets TDX adapters return fabricated rows
    # (labeled source="mock") instead of failing the batch.
    tdx_allow_mock: bool = False
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
    universe_default: str = "all_a"
    daily_waves: list[WaveConfig] = field(default_factory=list)
    schedule_groups: dict[str, ScheduleGroup] = field(default_factory=dict)
    init_phases: list[str] = field(default_factory=list)
    on_demand_enabled: bool = True
    on_demand_datasets: list[str] = field(default_factory=list)
    duckdb_path: Path | None = None
    duckdb_memory_limit: str = "2GB"
    duckdb_threads: int = 4
    adj_factors_source: str = "sina"
    adj_factors_types: list[str] = field(default_factory=lambda: ["hfq"])
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
    failover_enabled: bool = True
    failover_datasets: list[FailoverDatasetSpec] = field(default_factory=list)
    config_path: Path | None = None
    _backfill: bool = False
    _sector_bars_force: bool = False
    _rate_limiters: object | None = field(default=None, repr=False)

    def rate_limit(self, source: str) -> None:
        if self._rate_limiters is None:
            from cn_market_lake.adapters.throttle import SourceRateLimiters

            self._rate_limiters = SourceRateLimiters(self)
        self._rate_limiters.wait(source)  # type: ignore[union-attr]

    def tdx_rate_limit_spec(self) -> RateLimitSpec | None:
        if not self.tdx_enabled:
            return None
        return RateLimitSpec(
            str(self.meta_root / "rate_limits"),
            "tdx_protocol",
            self.tdx_min_interval_ms / 1000.0,
        )

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


def load_config(path: str | Path) -> Config:
    config_path = Path(path).expanduser().resolve()
    with open(config_path, "rb") as f:
        raw = tomllib.load(f)

    data_root = (
        Path(raw.get("data", {}).get("root", "./data/cn-market-lake")).expanduser().resolve()
    )
    orch = raw.get("orchestrator", {})
    tdx = raw.get("tdx_protocol", {})
    sources_raw = raw.get("sources", {})

    sources: dict[str, bool] = {}
    source_intervals: dict[str, float] = {}
    eastmoney_proxy: str | None = None
    eastmoney_timeout_sec = 15.0
    baostock_batch_size = 20
    baostock_batch_rest_seconds = 120.0
    for name, val in sources_raw.items():
        if isinstance(val, dict):
            sources[name] = bool(val.get("enabled", True))
            if "min_interval_seconds" in val:
                source_intervals[name] = float(val["min_interval_seconds"])
            if name == "eastmoney" and val.get("proxy"):
                eastmoney_proxy = str(val["proxy"]).strip() or None
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
        else:
            sources[name] = bool(val)

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
    groups_raw = raw.get("job", {}).get("daily", {}).get("groups", {})
    for name, group in groups_raw.items():
        schedule_groups[name] = ScheduleGroup(
            at=group.get("at", "16:00"), steps=list(group.get("steps", []))
        )

    duckdb_raw = raw.get("duckdb", {})
    duckdb_path_str = duckdb_raw.get("path")
    duckdb_path = (
        _expand(duckdb_path_str, data_root)
        if duckdb_path_str
        else data_root / "duckdb" / "cn-market-lake.duckdb"
    )

    on_demand = raw.get("on_demand", {})
    adj_raw = raw.get("adj_factors", {})
    sentiment_raw = raw.get("sentiment", {})
    failover_raw = raw.get("failover", {})
    failover_datasets: list[FailoverDatasetSpec] = []
    for item in failover_raw.get("datasets", []):
        failover_datasets.append(
            FailoverDatasetSpec(
                name=str(item["name"]),
                primary=str(item.get("primary", "tdx_protocol")),
                backup=str(item.get("backup", "eastmoney")),
                compare_fields=list(item.get("compare_fields", ["close"])),
                price_tolerance_bps=float(item.get("price_tolerance_bps", 10.0)),
            )
        )

    minute_raw = raw.get("minute_bars", {})
    ticks_raw = raw.get("trade_ticks", {})
    init_raw = raw.get("job", {}).get("init", {})
    phases_block = init_raw.get("phases", init_raw)
    init_phases = list(phases_block.get("names", init_raw.get("names", [])))

    cfg = Config(
        data_root=data_root,
        workers=int(orch.get("workers", 8)),
        batch_size=int(orch.get("batch_size", 100)),
        max_retries=int(orch.get("max_retries", 3)),
        retry_backoff_seconds=int(orch.get("retry_backoff_seconds", 5)),
        batch_stale_seconds=int(orch.get("batch_stale_seconds", 3600)),
        tdx_enabled=bool(tdx.get("enabled", True)),
        tdx_min_interval_ms=int(tdx.get("min_interval_ms", 50)),
        tdx_servers=str(tdx.get("servers", "auto")),
        tdx_connect_timeout_sec=int(tdx.get("connect_timeout_sec", 10)),
        tdx_host_pool=_parse_tdx_host_pool(tdx.get("hosts", {})),
        tdx_allow_mock=bool(tdx.get("allow_mock", False)),
        sources=sources,
        source_intervals=source_intervals,
        eastmoney_proxy=eastmoney_proxy,
        eastmoney_timeout_sec=eastmoney_timeout_sec,
        baostock_batch_size=baostock_batch_size,
        baostock_batch_rest_seconds=baostock_batch_rest_seconds,
        universe_default=str(raw.get("universe", {}).get("default", "all_a")),
        daily_waves=daily_waves,
        schedule_groups=schedule_groups,
        init_phases=init_phases,
        on_demand_enabled=bool(on_demand.get("enabled", True)),
        on_demand_datasets=list(on_demand.get("datasets", [])),
        duckdb_path=duckdb_path,
        duckdb_memory_limit=str(duckdb_raw.get("memory_limit", "2GB")),
        duckdb_threads=int(duckdb_raw.get("threads", 4)),
        adj_factors_source=str(adj_raw.get("source", "sina")),
        adj_factors_types=list(adj_raw.get("adjust_types", ["hfq"])),
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
        failover_enabled=bool(failover_raw.get("enabled", True)),
        failover_datasets=failover_datasets,
        config_path=config_path,
    )
    return cfg


def validate_config(cfg: Config) -> list[str]:
    import sys

    import cn_market_lake.steps  # noqa: F401 — register steps
    from cn_market_lake.orchestrator.registry import STEP_REGISTRY

    errors: list[str] = []
    if cfg.workers < 1:
        errors.append("orchestrator.workers must be >= 1")
    # The TDX client is not fork-safe; ProcessPool on macOS is the OOM / BrokenProcessPool
    # footgun that wiped notes under load. Refuse the unsafe combo loudly.
    if sys.platform == "darwin" and cfg.workers > 1:
        errors.append(
            "orchestrator.workers must be 1 on macOS "
            "(TDX client + ProcessPool fork is unsafe; use workers = 1)"
        )
    if cfg.batch_size < 1:
        errors.append("orchestrator.batch_size must be >= 1")
    servers = cfg.tdx_servers.strip()
    if servers.lower() != "auto" and ":" not in servers:
        errors.append("[tdx_protocol].servers must be 'auto' or host:port")
    if cfg.tdx_connect_timeout_sec < 1:
        errors.append("[tdx_protocol].connect_timeout_sec must be >= 1")
    if not cfg.daily_waves:
        errors.append("job.daily.waves must define at least one wave")

    # Each frequency must have a dataset to land in, or its rows would have
    # nowhere to go and its horizon nowhere to be declared.
    from cn_market_lake.domain.datasets import intraday_datasets

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

    referenced: list[tuple[str, str]] = []
    for wave in cfg.daily_waves:
        if not wave.steps:
            errors.append(f"wave '{wave.name}' has no steps")
        for step in wave.steps:
            referenced.append((f"wave '{wave.name}'", step))

    for group_name, group in cfg.schedule_groups.items():
        for step in group.steps:
            referenced.append((f"group '{group_name}'", step))

    for location, step in referenced:
        if step not in STEP_REGISTRY:
            errors.append(f"{location}: unknown step '{step}' (not registered)")

    return errors
