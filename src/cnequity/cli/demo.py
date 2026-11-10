"""One-command mini demos: real TDX data or a deterministic offline sample.

Designed for first-run / star-seeker UX. Reached as ``cne init --profile
demo|sample``; it is not a substitute for the market profiles.
Writes into a separate data root so a later full-market init is not poisoned
by a 5-symbol instruments file.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

import click
import polars as pl

from cnequity.cli._shared import (
    DEMO_CONFIG,
    SAMPLE_CONFIG,
    _run_status_exit_code,
    star_hint_once,
)
from cnequity.config import Config, WaveConfig
from cnequity.domain.market_time import shanghai_today
from cnequity.domain.schemas import validate_dataframe, with_provenance
from cnequity.i18n import _
from cnequity.orchestrator.engine import JobEngine
from cnequity.run_scope import set_backfill
from cnequity.storage.atomic import replace_with_retry, write_parquet_atomic
from cnequity.storage.layout import init_data_layout

logger = logging.getLogger(__name__)

DEFAULT_SYMBOLS = (
    "600519.SH",  # 贵州茅台
    "000001.SZ",  # 平安银行
    "000858.SZ",  # 五粮液
    "300750.SZ",  # 宁德时代
    "601318.SH",  # 中国平安
)
DEFAULT_DAYS = 30
RESEARCH_MIN_DAYS = 756  # roughly three trading years; enough to cross corporate actions

MCP_GUIDE_URL = "https://rootsunc.github.io/CNEquity/reference/mcp/"


def _banner(step: str, title: str) -> None:
    click.echo(f"\n=== [{step}] {title} ===", err=False)
    sys.stdout.flush()


def _write_demo_config(path: Path, text: str, *, force: bool) -> None:
    """Write a demo config without silently repointing an existing file."""
    if path.exists():
        if not path.is_file():
            raise click.ClickException(
                _("Config output path is not a regular file: {path}").format(path=path)
            )
        try:
            current = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise click.ClickException(
                _("Cannot read existing config {path}: {exc}").format(path=path, exc=exc)
            ) from exc
        if current == text:
            return
        if not force:
            raise click.ClickException(
                _(
                    "A config already exists with different contents: {path}. The original is untouched; choose another --config-out, or add --force to overwrite after checking."
                ).format(path=path)
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        replace_with_retry(temporary, path)
    except OSError as exc:
        raise click.ClickException(
            _("Cannot write config {path}: {exc}").format(path=path, exc=exc)
        ) from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _write_demo_toml(path: Path, data_root: Path, *, force: bool = False) -> None:
    """Persist a tiny config so follow-up ``cne query --config …`` works."""
    from cnequity.config.bootstrap import path_for_toml

    # POSIX form + TOML escape: bare ``C:\Users\…`` is invalid TOML (``\U`` etc.).
    root = path_for_toml(data_root)
    text = f"""# Auto-written by `cne init --profile demo`. Safe to delete with the demo data_root.
[data]
root = "{root}"
# Says what this lake is, so whole-lake checks do not judge five symbols
# against the full dataset registry.
profile = "demo"

[orchestrator]
workers = 1
batch_size = 50

[tdx_protocol]
enabled = true
allow_mock = false
min_interval_ms = 100
servers = "auto"
"""
    _write_demo_config(path, text, force=force)


def _write_sample_toml(path: Path, data_root: Path, *, force: bool = False) -> None:
    """Persist a read-only config for the generated, explicitly synthetic lake."""
    from cnequity.config.bootstrap import path_for_toml

    root = path_for_toml(data_root)
    text = f"""# Auto-written by `cne init --profile sample`.
# The rows are synthetic and carry source=mock. Never use this lake for research.
[data]
root = "{root}"
profile = "sample"

[orchestrator]
workers = 1
batch_size = 50

[tdx_protocol]
enabled = false
allow_mock = false
"""
    _write_demo_config(path, text, force=force)


def _demo_config(
    data_root: Path, config_path: Path | None = None, *, research: bool = False
) -> Config:
    """Minimal real-source config (workers=1, no mock, TDX only).

    ``research`` also enables Sina, which serves the hfq factors the research
    view derives; without it ``--research`` died on "source disabled".
    """
    return Config(
        data_root=data_root.resolve(),
        lake_profile="demo",
        workers=1,
        batch_size=50,
        tdx_enabled=True,
        tdx_allow_mock=False,
        tdx_min_interval_ms=100,
        tdx_servers="auto",
        # Prefer known-good CN hosts (same pool as the example config).
        tdx_host_pool=[
            "120.76.1.198:7709",
            "123.125.108.101:7709",
            "114.141.177.118:7709",
            "27.151.2.113:7709",
            "182.118.8.9:7709",
        ],
        sources={
            "eastmoney": False,
            "sina": research,
            "cninfo": False,
            "baostock": False,
        },
        failover_enabled=False,
        daily_waves=[WaveConfig(name="demo", parallel=False, steps=["daily_bars", "compact"])],
        config_path=config_path,
    )


def _configure_logging() -> None:
    from cnequity.cli._shared import terminal_formatter

    handler = logging.StreamHandler()
    handler.setFormatter(terminal_formatter(handler.stream))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    # Keep httpx noise down; TDX/session logs stay visible.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def _probe_tdx(cfg: Config) -> None:
    from cnequity.adapters.tdx_protocol.client import _quotes_client
    from cnequity.adapters.tdx_protocol.session import close_quotes_client

    t0 = time.perf_counter()
    click.echo(_("Probing TDX hosts (the first server to connect wins)…"))
    sys.stdout.flush()
    client = _quotes_client(cfg)
    try:
        click.echo(_("TDX connection OK ({value:.1f}s)").format(value=time.perf_counter() - t0))
    finally:
        # The heartbeat thread is not a daemon, so an unclosed client keeps the
        # interpreter alive after the demo has printed everything — the run looks
        # like it hangs when in fact all six steps already finished.
        close_quotes_client(client)


def _write_demo_instruments(cfg: Config, symbols: list[str]) -> list[str]:
    from cnequity.adapters.tdx_protocol.client import fetch_instruments, normalize_with_source

    click.echo(
        _("Fetching the full symbol list, then keeping only the {value} demo symbols…").format(
            value=len(symbols)
        )
    )
    sys.stdout.flush()
    raw = fetch_instruments(
        rate_limit=cfg.tdx_rate_limit_spec(),
        allow_mock=False,
        config=cfg,
    )
    raw = normalize_with_source(raw, "tdx_protocol")
    wanted = set(symbols)
    kept = raw.filter(pl.col("symbol").is_in(list(wanted)))
    found = set(kept["symbol"].to_list())
    missing = [s for s in symbols if s not in found]
    if missing:
        click.echo(
            _("Warning: not in the TDX list (skipped): {value}").format(value=", ".join(missing)),
            err=True,
        )
    if kept.is_empty():
        raise click.ClickException(
            _(
                "TDX returned none of the demo symbols.\n  Check the environment first: `cne doctor` (needs no config and no network).\n  Then the connection: `cne sources probe --only tdx_protocol --config configs/cnequity.demo.toml`.\n  No network at all? Build an offline lake with `cne init --profile sample`."
            )
        )
    df = validate_dataframe(
        with_provenance(kept, source="tdx_protocol", data_version="v1"),
        "instruments",
    )
    out = cfg.curated_root / "instruments" / "part-merged.parquet"
    write_parquet_atomic(out, df, compression="zstd")
    click.echo(_("Wrote {height} instruments → {out}").format(height=df.height, out=out))
    return df["symbol"].to_list()


def _last_trading_day(cfg: Config, as_of: date) -> date:
    """The newest session the demo may ask for.

    Not merely the last *trading* day: during a session that is today, whose
    bar is still forming until 15:05. `cne init --profile demo` is the first
    command in the README, and run during market hours it died on the finality
    guard while reporting a TDX connectivity problem that did not exist.
    """
    from cnequity.steps.bars import _last_final_session
    from cnequity.steps.common import list_trading_dates

    as_of = min(as_of, _last_final_session())
    window = list_trading_dates(cfg, as_of - timedelta(days=21), as_of)
    return window[-1] if window else as_of


def _start_for_days(cfg: Config, end: date, days: int) -> date:
    from cnequity.steps.common import list_trading_dates

    # Pull a padded calendar window, then take the last `days` sessions.
    probe_start = end - timedelta(days=max(days * 3, 60))
    sessions = list_trading_dates(cfg, probe_start, end)
    if not sessions:
        return end - timedelta(days=days)
    if len(sessions) <= days:
        return sessions[0]
    return sessions[-days]


def _sample_query(cfg: Config, symbol: str) -> pl.DataFrame:
    from cnequity.query.reader import load

    return (
        load(
            "daily_bars",
            start=None,
            end=None,
            symbols=[symbol],
            config=cfg,
        )
        .sort("trade_date", descending=True)
        .head(8)
    )


def _return_summary(raw: pl.DataFrame, adjusted: pl.DataFrame) -> dict[str, object]:
    """Compare a raw and adjusted close series without hiding missing factors."""
    if raw.is_empty() or adjusted.is_empty():
        raise click.ClickException(_("The research demo got no daily bars"))
    raw = raw.sort("trade_date")
    adjusted = adjusted.sort("trade_date")
    first_raw = float(raw["close"][0])
    last_raw = float(raw["close"][-1])
    first_adj = float(adjusted["adj_close"][0])
    last_adj = float(adjusted["adj_close"][-1])
    if min(first_raw, first_adj) <= 0:
        raise click.ClickException(_("The research demo's starting close is not positive"))
    return {
        "start": raw["trade_date"][0].isoformat(),
        "end": raw["trade_date"][-1].isoformat(),
        "raw_return": last_raw / first_raw - 1.0,
        "adjusted_return": last_adj / first_adj - 1.0,
        "rows": adjusted.height,
        "exact": bool(adjusted["adj_is_exact"].all()),
    }


def _run_research_demo(
    cfg: Config,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, object]:
    """Derive a small exact hfq series and show why query-time adjustment matters."""
    from cnequity.derive.adj_factors import compute_adj_factors
    from cnequity.query.reader import MissingAdjustmentError, load

    click.echo(_("Deriving hfq adjustment factors for the demo symbols from Sina…"))
    result = compute_adj_factors(
        cfg,
        adjust_type="hfq",
        refresh_symbols=symbols,
        full=True,
    )
    # A vendor gap on one of five symbols is not a reason to abandon the whole
    # demonstration: Sina serves the factor series per symbol, and the point
    # here is to show one raw-vs-adjusted comparison. Name what was missed and
    # carry on with the symbols that answered.
    failed_symbols = {str(item).split(":", 1)[0].strip().upper() for item in result.failed}
    usable = [symbol for symbol in symbols if symbol.strip().upper() not in failed_symbols]
    if result.failed:
        click.echo(
            _("Warning: Sina returned no adjustment factors for: {value}").format(
                value=", ".join(sorted(result.failed))
            ),
            err=True,
        )
    errors = [
        finding
        for finding in result.findings
        if finding.get("severity") == "error" and finding.get("check") != "adj_factor_fetch_failed"
    ]
    if errors:
        raise click.ClickException(
            _("Sina adjustment factor check failed: ")
            + _("; ").join(str(finding.get("message", "unknown finding")) for finding in errors)
        )
    # This lake holds bars and factors but no corporate_actions, so the ex-date
    # cross-check has nothing to arbitrate against. Say so rather than letting
    # the printed return look like a verified one.
    click.echo(
        _(
            "Note: the demo lake has no corporate_actions, so these factors were not cross-checked against ex-dates. A full lake checks every jump."
        ),
        err=True,
    )

    for sample_symbol in usable:
        raw = load(
            "daily_bars",
            start=start,
            end=end,
            symbols=[sample_symbol],
            config=cfg,
        )
        if raw.is_empty():
            continue
        try:
            adjusted = load(
                "daily_bars",
                start=start,
                end=end,
                symbols=[sample_symbol],
                adjust="hfq",
                strict_adj=True,
                config=cfg,
            )
        except MissingAdjustmentError:
            continue
        if adjusted.is_empty():
            continue
        summary = _return_summary(raw, adjusted)
        click.echo(
            _(
                "{sample_symbol}: unadjusted return {raw_return:+.2%} → hfq adjusted return {adjusted_return:+.2%} ({rows} rows of exact factors, {start}..{end})"
            ).format(
                sample_symbol=sample_symbol,
                raw_return=summary["raw_return"],
                adjusted_return=summary["adjusted_return"],
                rows=summary["rows"],
                start=summary["start"],
                end=summary["end"],
            )
        )
        return {"symbol": sample_symbol, **summary}

    click.echo(
        _(
            "No exact adjustment example is available in this window; the research example was skipped, and the fetched daily bars stay readable."
        )
    )
    return {
        "status": "degraded",
        "execution_status": "completed",
        "coverage_status": "partial",
        "reason_code": "source_unavailable",
        "rows": 0,
        "fallback": "Use the verified unadjusted daily bars for now; once the source recovers, run cne derive adj_factors.",
    }


def _intraday_hint(summary: dict | None, cfg: Config, symbol: str) -> str:
    """Follow-up snippet for the intraday leg, or nothing when it did not run."""
    if summary is None:
        return ""
    return f"""
  minutes = load("minute_bars", symbols=["{symbol}"], data_root="{cfg.data_root}")

Intraday capture is opt-in in a real lake — see `[minute_bars]` in the config.
The source keeps ~95 trading days of 1m and ~491 of 5m (minute_bars_5m), so
there is no deep intraday history to backfill.
"""


def _run_intraday_demo(cfg: Config, engine, symbols: list[str], end: date, days: int) -> dict:
    """Capture a few sessions of 1m bars and show the session shape.

    Deliberately narrow: the point is to make the bar-time convention and the
    240-bar session visible, not to seed anything. The source only keeps ~95
    trading days of 1m, so the window is clamped to what the demo asks for.
    """
    from cnequity.adapters.tdx_protocol.minute_bars import bars_per_session

    intraday_days = min(days, 5)
    start = _start_for_days(cfg, end, intraday_days)
    cfg.minute_bars_enabled = True
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = list(symbols)
    cfg.minute_bars_frequencies = ["1m"]
    set_backfill(cfg, active=True, start=start, end=end)

    result = engine.run_job(
        "demo:intraday",
        trade_date=end,
        waves=[WaveConfig(name="intraday", parallel=False, steps=["minute_bars", "compact"])],
        backfill=True,
    )
    if _run_status_exit_code(result.get("status")):
        raise click.ClickException(_("minute_bars failed: {result}").format(result=result))

    from cnequity.query.reader import load

    bars = load("minute_bars", symbols=symbols, config=cfg)
    if bars.is_empty():
        if result.get("status") in {"warning", "degraded"}:
            click.echo(
                _("The minute-bar source is limited; the data available in the demo lake was kept.")
            )
            click.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))
            return {**result, "rows": 0, "symbol_days": 0, "full_sessions": 0}
        raise click.ClickException(_("minute_bars returned no rows inside the demo window"))

    expected = bars_per_session("1m")
    per_day = (
        bars.group_by("symbol", "trade_date")
        .agg(pl.len().alias("bars"))
        .sort("symbol", "trade_date")
    )
    full = per_day.filter(pl.col("bars") == expected).height
    click.echo(
        _(
            "{height} 1-minute bars covering {height2} symbol×session pairs; {full}/{height2} are complete at {expected} bars per day"
        ).format(height=bars.height, height2=per_day.height, full=full, expected=expected)
    )
    sample_symbol = symbols[0]
    if sample_symbol not in set(bars.get_column("symbol")):
        sample_symbol = bars.get_column("symbol")[0]
    one = bars.filter(pl.col("symbol") == sample_symbol).sort("bar_time")
    session = one.filter(pl.col("trade_date") == one["trade_date"].max())
    with pl.Config(tbl_rows=6, tbl_cols=-1, fmt_str_lengths=24):
        click.echo(
            _(
                "\n{sample_symbol}: the first and last bars of {value} (bar_time is the minute's **closing** time):\n"
            ).format(sample_symbol=sample_symbol, value=session["trade_date"][0])
        )
        cols = ["symbol", "bar_time", "open", "high", "low", "close", "volume"]
        click.echo(pl.concat([session.head(3), session.tail(3)]).select(cols))
    return {
        "rows": bars.height,
        "symbol_days": per_day.height,
        "full_sessions": full,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }


def _ai_hint(config_out: Path) -> str:
    """The one command that hands this lake to an agent, with the real path.

    MCP clients spawn the server from a directory of their own, so the config
    must be absolute; printing it here saves the user from building that path.
    """
    return _(
        "\nConnect an AI (read-only MCP, Claude Code as the example):\n  claude mcp add cnequity -- cne mcp --config {value}\nChatGPT, Codex, Cursor, VS Code and others: {MCP_GUIDE_URL}\n"
    ).format(value=config_out.resolve(), MCP_GUIDE_URL=MCP_GUIDE_URL)


def _rebuild_demo_stats(cfg: Config) -> None:
    """Build the stats tables now, so `cne serve` opens on real row counts.

    Without them the panel's first view showed 0 rows and a pending rebuild.
    A failure here costs the demo nothing it promised, so it only warns.
    """
    from cnequity.storage.stats import rebuild_stats

    try:
        rebuild_stats(cfg)
    except Exception as exc:  # noqa: BLE001 — never fail a finished demo on stats
        click.echo(
            _(
                "The metrics tables could not be generated ({exc}); `cne serve` rebuilds them in the background when it opens."
            ).format(exc=exc),
            err=True,
        )


def _sample_sessions(end: date, days: int) -> list[date]:
    sessions: list[date] = []
    cursor = end
    while len(sessions) < days:
        if cursor.weekday() < 5:
            sessions.append(cursor)
        cursor -= timedelta(days=1)
    return sorted(sessions)


def _sample_frames(symbols: list[str], sessions: list[date]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Create stable sample rows that exercise the real schemas and query layer."""
    from cnequity.domain.schemas import MOCK_SOURCE, data_version_for

    names = {
        "600519.SH": "贵州茅台（样例）",  # i18n-ignore: sample security names are data
        "000001.SZ": "平安银行（样例）",  # i18n-ignore: sample security names are data
        "000858.SZ": "五粮液（样例）",  # i18n-ignore: sample security names are data
        "300750.SZ": "宁德时代（样例）",  # i18n-ignore: sample security names are data
        "601318.SH": "中国平安（样例）",  # i18n-ignore: sample security names are data
    }
    instruments = pl.DataFrame(
        [
            {
                "symbol": symbol,
                "name": names.get(symbol, f"{symbol}（样例）"),  # i18n-ignore: data
                "exchange": symbol.rsplit(".", 1)[-1],
                "asset_type": "stock",
                "list_date": date(2010, 1, 1),
                "delist_date": None,
                "prev_symbol": None,
            }
            for symbol in symbols
        ]
    )
    instruments = validate_dataframe(
        with_provenance(instruments, source=MOCK_SOURCE, data_version="v1"),
        "instruments",
    )

    rows: list[dict[str, object]] = []
    for symbol_index, symbol in enumerate(symbols):
        base = 10.0 + symbol_index * 23.0
        for day_index, session in enumerate(sessions):
            drift = day_index * (0.06 + symbol_index * 0.01)
            wave = ((day_index % 5) - 2) * 0.03
            open_price = round(base + drift + wave, 2)
            close = round(open_price + ((day_index % 3) - 1) * 0.08, 2)
            high = round(max(open_price, close) + 0.18, 2)
            low = round(min(open_price, close) - 0.16, 2)
            volume = 1_000_000 + symbol_index * 100_000 + day_index * 10_000
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": session,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "amount": round(close * volume, 2),
                }
            )
    bars = validate_dataframe(
        with_provenance(
            pl.DataFrame(rows),
            source=MOCK_SOURCE,
            data_version=data_version_for("daily_bars"),
        ),
        "daily_bars",
    )
    return instruments, bars


def run_sample_demo(
    *,
    symbols: list[str],
    days: int,
    data_root: Path,
    trade_date: date | None = None,
    config_out: Path | None = None,
    intraday: bool = False,
    research: bool = False,
    force: bool = False,
) -> dict:
    """Write a deterministic no-network lake for installation and query checks."""
    from cnequity.domain.schemas import MOCK_SOURCE
    from cnequity.query.views import ensure_duckdb_views

    if intraday or research:
        raise click.ClickException(_("--sample cannot be combined with --intraday or --research"))
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    if not symbols:
        raise click.ClickException(_("--symbols needs at least one symbol"))
    if days < 1 or days > 366:
        raise click.ClickException(_("In sample mode --days must be between 1 and 366"))
    if any(data_root.rglob("*.parquet")):
        raise click.ClickException(
            _(
                "The sample target directory already has Parquet files: {data_root}. Use an empty --data-root, so synthetic and real data never mix."
            ).format(data_root=data_root)
        )

    config_out = config_out or Path(SAMPLE_CONFIG)
    end = trade_date or date(2024, 6, 28)
    sessions = _sample_sessions(end, days)

    _banner("1/3", _("Preparing an offline sample lake at {data_root}").format(data_root=data_root))
    click.echo(_("Offline sample: the prices are synthetic, not market data."))
    _write_sample_toml(config_out, data_root, force=force)
    cfg = _demo_config(data_root, config_path=config_out.resolve())
    cfg.lake_profile = "sample"
    cfg.tdx_enabled = False
    init_data_layout(cfg)

    _banner(
        "2/3",
        _("Writing {value} symbols × {value2} sessions").format(
            value=len(symbols), value2=len(sessions)
        ),
    )
    instruments, bars = _sample_frames(symbols, sessions)
    write_parquet_atomic(
        cfg.curated_root / "instruments" / "part-sample.parquet",
        instruments,
        compression="zstd",
    )
    # Same period as every other writer (month for daily_bars), so a fresh
    # sample lake does not start out with a layout the audit calls mixed.
    from cnequity.domain.datasets import DATASETS

    spec = DATASETS["daily_bars"]
    periods = bars.with_columns(
        pl.col("trade_date").map_elements(spec.partition_for, return_dtype=pl.Utf8).alias("_p")
    )
    for (period,), frame in periods.partition_by("_p", as_dict=True).items():
        out = cfg.curated_root / "daily_bars" / f"{spec.partition_col}={period}"
        write_parquet_atomic(out / "part-sample.parquet", frame.drop("_p"), compression="zstd")
    ensure_duckdb_views(cfg)
    _rebuild_demo_stats(cfg)

    _banner("3/3", _("Querying the sample through the public API"))
    sample_symbol = symbols[0]
    sample = _sample_query(cfg, sample_symbol)
    with pl.Config(tbl_rows=10, tbl_cols=-1, fmt_str_lengths=24):
        click.echo(sample.select("symbol", "trade_date", "close", "volume", "source"))
    click.echo(
        _(
            '\nOffline sample ready: {data_root}\nConfig written to:    {config_out}\nEvery row has source={MOCK_SOURCE}; never use it for research or production.\n\nNext steps:\n  cne query --config {config_out} --sql "\n    SELECT symbol, trade_date, close, volume, source\n    FROM daily_bars\n    ORDER BY trade_date DESC\n    LIMIT 10\n  "\n\nWith network access, run `cne init --profile demo` to fetch real TDX data.\n{value}'
        ).format(
            data_root=cfg.data_root,
            config_out=config_out,
            MOCK_SOURCE=MOCK_SOURCE,
            value=_ai_hint(config_out),
        )
    )
    star_hint_once(cfg)
    return {
        "data_root": str(cfg.data_root),
        "config": str(config_out),
        "symbols": symbols,
        "start": sessions[0].isoformat(),
        "end": sessions[-1].isoformat(),
        "sample_symbol": sample_symbol,
        "sample_rows": sample.height,
        "source": MOCK_SOURCE,
    }


def run_demo(
    *,
    symbols: list[str],
    days: int,
    data_root: Path,
    trade_date: date | None = None,
    config_out: Path | None = None,
    intraday: bool = False,
    research: bool = False,
    force: bool = False,
) -> dict:
    """Run the mini real-source demo. Returns a small summary dict."""
    _configure_logging()
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    if not symbols:
        raise click.ClickException(_("--symbols needs at least one symbol"))
    if days < 1:
        raise click.ClickException(_("--days must be >= 1"))

    if any((data_root / "curated").rglob("part-sample.parquet")):
        raise click.ClickException(
            _(
                "{data_root} holds the synthetic sample lake (source=mock). Real data is not written over it: pass another --data-root, or delete that directory first."
            ).format(data_root=data_root)
        )
    config_out = config_out or Path(DEMO_CONFIG)
    steps = 8 if research and intraday else 7 if (research or intraday) else 6
    _banner(f"1/{steps}", _("Preparing a demo lake at {data_root}").format(data_root=data_root))
    _write_demo_toml(config_out, data_root, force=force)
    cfg = _demo_config(data_root, config_path=config_out.resolve(), research=research)
    init_data_layout(cfg)
    click.echo(f"data_root = {cfg.data_root}")
    click.echo(f"config    = {config_out}")
    click.echo(
        _(
            "Note: this lake is completely separate from `cne init --profile quick|full`; you can delete it at any time."
        )
    )

    _banner(f"2/{steps}", _("Probing TDX"))
    try:
        _probe_tdx(cfg)
    except Exception as exc:
        raise click.ClickException(
            _(
                "Cannot connect to TDX: {exc}\nTry again from a mainland network or VPN exit, or check `[tdx_protocol.hosts]` in the example config."
            ).format(exc=exc)
        ) from exc

    _banner(f"3/{steps}", _("Instruments (demo symbol scope)"))
    kept = _write_demo_instruments(cfg, symbols)

    _banner(f"4/{steps}", _("Trading calendar"))
    engine = JobEngine(cfg)
    as_of = trade_date or shanghai_today()
    # Seed calendar covers a wide range; backfill window is cheap (CSV/seed).
    set_backfill(cfg, active=True, start=date(2020, 1, 1), end=as_of)
    cal = engine.run_job(
        "demo:calendar",
        trade_date=as_of,
        # With `compact`, and sequential so it runs after the fetch. Without it
        # the calendar stayed in staging: the step logged 2,818 rows and
        # `cne status --datasets` still reported trading_calendar empty, which
        # is the demo telling a new user it succeeded at nothing.
        waves=[WaveConfig(name="calendar", parallel=False, steps=["trading_calendar", "compact"])],
        backfill=True,
    )
    if _run_status_exit_code(cal.get("status")):
        raise click.ClickException(_("trading_calendar failed: {cal}").format(cal=cal))
    end = _last_trading_day(cfg, as_of)
    window_days = max(days, RESEARCH_MIN_DAYS) if research else days
    start = _start_for_days(cfg, end, window_days)
    click.echo(
        _("Demo window: {value} → {value2} (target {window_days} sessions)").format(
            value=start.isoformat(), value2=end.isoformat(), window_days=window_days
        )
    )
    if research and window_days != days:
        click.echo(
            _(
                "Research mode widens the window from {days} to {window_days} sessions, so the adjustment difference from ex-dividend dates becomes visible."
            ).format(days=days, window_days=window_days)
        )

    _banner(f"5/{steps}", _("daily_bars ({value} symbols)").format(value=len(kept)))
    set_backfill(cfg, active=True, start=start, end=end)
    # Name the symbols rather than leaving the step to infer them from
    # `instruments`. TDX publishes no list_date, so an undated demo symbol with
    # no bar yet in the lake is classified as a pre-listing placeholder and
    # skipped — which made a second `--profile demo` run on an existing demo
    # lake fetch nothing and then blame TDX for the empty result.
    set_backfill(cfg, symbols=list(kept))
    bars = engine.run_job(
        "demo:bars",
        trade_date=end,
        waves=[WaveConfig(name="bars", parallel=False, steps=["daily_bars", "compact"])],
        backfill=True,
    )
    if _run_status_exit_code(bars.get("status")):
        # Name the step's own reason rather than guessing one. This blamed TDX
        # connectivity for every failure, sending people to debug a network
        # that was fine.
        reasons = [
            str(r.get("error")) for r in bars.get("results", []) if r.get("status") == "failed"
        ]
        detail = "\n".join(f"  {r}" for r in reasons) or f"  {bars}"
        raise click.ClickException(
            _(
                "daily_bars failed:\n{detail}\nRe-run `cne init --profile demo` once it is fixed."
            ).format(detail=detail)
        )
    # The run total also counts compact re-publishing the same rows, so the
    # fetched figure is the daily_bars step's own.
    fetched = next(
        (r.get("rows_written") for r in bars.get("results", []) if r.get("step") == "daily_bars"),
        bars.get("rows_written", "?"),
    )
    click.echo(
        _("Daily bars run {value}: status={value2} rows_written≈{value3}").format(
            value=bars.get("run_id"),
            value2=bars.get("status"),
            value3=fetched,
        )
    )

    _banner(f"6/{steps}", _("Sample results"))
    sample_symbol = kept[0]
    try:
        sample = _sample_query(cfg, sample_symbol)
    except Exception as exc:
        raise click.ClickException(
            _("Querying after the demo was written failed: {exc}").format(exc=exc)
        ) from exc
    if sample.is_empty():
        if bars.get("status") in {"warning", "degraded"}:
            click.echo(
                _(
                    "The daily-bar source is limited; the sample symbol has no readable rows, and other available data was kept."
                )
            )
            click.echo(json.dumps(bars, ensure_ascii=False, indent=2, default=str))
        else:
            raise click.ClickException(
                _(
                    "{sample_symbol} has no daily_bars rows in {value}..{value2}. The step reported success, so the source's window was empty rather than failing: see this run's own findings with `cne status --run {value3} --config {config_out}`."
                ).format(
                    sample_symbol=sample_symbol,
                    value=start.isoformat(),
                    value2=end.isoformat(),
                    value3=bars.get("run_id"),
                    config_out=config_out,
                )
            )
    with pl.Config(tbl_rows=10, tbl_cols=-1, fmt_str_lengths=24):
        click.echo(_("\n{sample_symbol}: the latest rows:\n").format(sample_symbol=sample_symbol))
        click.echo(
            sample.select(
                [
                    c
                    for c in (
                        "symbol",
                        "trade_date",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "source",
                    )
                    if c in sample.columns
                ]
            )
        )

    research_summary = None
    if research:
        _banner(f"7/{steps}", _("Research view: unadjusted vs hfq returns"))
        from cnequity.query.parquet_scan import dataset_has_parquet
        from cnequity.storage.read_context import read_root

        if dataset_has_parquet(read_root(cfg, "daily_bars")):
            research_summary = _run_research_demo(cfg, kept, start, end)
        else:
            click.echo(
                _(
                    "Daily-bar input is missing; the research example was skipped, and the lake and fetch fallback were kept."
                )
            )

    intraday_summary = None
    if intraday:
        step = 8 if research else 7
        _banner(
            f"{step}/{steps}",
            _("minute_bars 1-minute bars ({value} symbols)").format(value=len(kept)),
        )
        intraday_summary = _run_intraday_demo(cfg, engine, kept, end, days)

    _rebuild_demo_stats(cfg)
    click.echo(
        _(
            '\nDemo lake ready: {data_root}\nConfig written to: {config_out}\n\nNext steps:\n  cne query --config {config_out} --sql "\n    SELECT symbol, trade_date, close, volume, source\n    FROM daily_bars\n    WHERE symbol = \'{sample_symbol}\'\n    ORDER BY trade_date DESC\n    LIMIT 10\n  "\n\nPython:\n  from cnequity.query import load\n  bars = load("daily_bars", symbols=["{sample_symbol}"], data_root="{data_root}")\n{value}{value2}\nA whole-market init (hours to days) is a different thing: run `cne init` directly.\nDo not run production against this demo\'s data_root.\n'
        ).format(
            data_root=cfg.data_root,
            config_out=config_out,
            sample_symbol=sample_symbol,
            value=_intraday_hint(intraday_summary, cfg, sample_symbol),
            value2=_ai_hint(config_out),
        )
    )
    star_hint_once(cfg)
    return {
        "data_root": str(cfg.data_root),
        "config": str(config_out),
        "symbols": kept,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sample_symbol": sample_symbol,
        "sample_rows": sample.height,
        "bars_run_id": bars.get("run_id"),
        "research": research_summary,
        "intraday": intraday_summary,
    }
