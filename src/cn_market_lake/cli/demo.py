"""One-command mini demo: real TDX fetch for a handful of liquid names.

Designed for first-run / star-seeker UX — not a substitute for ``cml init``.
Writes into a separate data root so a later full-market init is not poisoned
by a 5-symbol instruments file.
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import click
import polars as pl

from cn_market_lake.config import Config, WaveConfig
from cn_market_lake.domain.schemas import validate_dataframe, with_provenance
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.storage.atomic import write_parquet_atomic
from cn_market_lake.storage.layout import init_data_layout

logger = logging.getLogger(__name__)

DEFAULT_SYMBOLS = (
    "600519.SH",  # 贵州茅台
    "000001.SZ",  # 平安银行
    "000858.SZ",  # 五粮液
    "300750.SZ",  # 宁德时代
    "601318.SH",  # 中国平安
)
DEFAULT_DAYS = 30
DEFAULT_DATA_ROOT = Path("data/cn-market-lake-demo")
RESEARCH_MIN_DAYS = 756  # roughly three trading years; enough to cross corporate actions


def _banner(step: str, title: str) -> None:
    click.echo(f"\n=== [{step}] {title} ===", err=False)
    sys.stdout.flush()


def _write_demo_toml(path: Path, data_root: Path) -> None:
    """Persist a tiny config so follow-up ``cml query --config …`` works."""
    from cn_market_lake.config.bootstrap import path_for_toml

    path.parent.mkdir(parents=True, exist_ok=True)
    # POSIX form + TOML escape: bare ``C:\Users\…`` is invalid TOML (``\U`` etc.).
    root = path_for_toml(data_root)
    path.write_text(
        f"""# Auto-written by `cml demo`. Safe to delete with the demo data_root.
[data]
root = "{root}"

[orchestrator]
workers = 1
batch_size = 50

[tdx_protocol]
enabled = true
allow_mock = false
min_interval_ms = 100
servers = "auto"
""",
        encoding="utf-8",
    )


def _demo_config(data_root: Path, config_path: Path | None = None) -> Config:
    """Minimal real-source config (workers=1, no mock, TDX only)."""
    return Config(
        data_root=data_root.resolve(),
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
            "sina": False,
            "cninfo": False,
            "baostock": False,
        },
        failover_enabled=False,
        daily_waves=[WaveConfig(name="demo", parallel=False, steps=["daily_bars", "compact"])],
        config_path=config_path,
    )


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
    # Keep httpx noise down; TDX/session logs stay visible.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def _probe_tdx(cfg: Config) -> None:
    from cn_market_lake.adapters.tdx_protocol.client import _quotes_client
    from cn_market_lake.adapters.tdx_protocol.session import close_quotes_client

    t0 = time.perf_counter()
    click.echo("Probing TDX hosts (first successful server wins)…")
    sys.stdout.flush()
    client = _quotes_client(cfg)
    try:
        click.echo(f"TDX connection OK ({time.perf_counter() - t0:.1f}s)")
    finally:
        # The heartbeat thread is not a daemon, so an unclosed client keeps the
        # interpreter alive after the demo has printed everything — the run looks
        # like it hangs when in fact all six steps already finished.
        close_quotes_client(client)


def _write_demo_instruments(cfg: Config, symbols: list[str]) -> list[str]:
    from cn_market_lake.adapters.tdx_protocol.client import fetch_instruments, normalize_with_source

    click.echo(f"Fetching full instrument list, then keeping {len(symbols)} demo symbols…")
    sys.stdout.flush()
    raw = fetch_instruments(
        rate_limit=cfg.tdx_rate_limit_spec(),
        allow_mock=False,
        config=cfg,
    )
    raw = normalize_with_source(raw)
    wanted = set(symbols)
    kept = raw.filter(pl.col("symbol").is_in(list(wanted)))
    found = set(kept["symbol"].to_list())
    missing = [s for s in symbols if s not in found]
    if missing:
        click.echo(
            f"Warning: not in TDX list (skipped): {', '.join(missing)}",
            err=True,
        )
    if kept.is_empty():
        raise click.ClickException(
            "None of the demo symbols were returned by TDX. "
            "Check connectivity with `cml servers test` or pass --symbols."
        )
    df = validate_dataframe(
        with_provenance(kept, source="tdx_protocol", data_version="v1"),
        "instruments",
    )
    out = cfg.curated_root / "instruments" / "part-merged.parquet"
    write_parquet_atomic(out, df, compression="zstd")
    click.echo(f"Wrote {df.height} instruments → {out}")
    return df["symbol"].to_list()


def _last_trading_day(cfg: Config, as_of: date) -> date:
    from cn_market_lake.steps.common import list_trading_dates

    window = list_trading_dates(cfg, as_of - timedelta(days=21), as_of)
    return window[-1] if window else as_of


def _start_for_days(cfg: Config, end: date, days: int) -> date:
    from cn_market_lake.steps.common import list_trading_dates

    # Pull a padded calendar window, then take the last `days` sessions.
    probe_start = end - timedelta(days=max(days * 3, 60))
    sessions = list_trading_dates(cfg, probe_start, end)
    if not sessions:
        return end - timedelta(days=days)
    if len(sessions) <= days:
        return sessions[0]
    return sessions[-days]


def _sample_query(cfg: Config, symbol: str) -> pl.DataFrame:
    from cn_market_lake.query.reader import load

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
        raise click.ClickException("research demo returned no daily bars")
    raw = raw.sort("trade_date")
    adjusted = adjusted.sort("trade_date")
    first_raw = float(raw["close"][0])
    last_raw = float(raw["close"][-1])
    first_adj = float(adjusted["adj_close"][0])
    last_adj = float(adjusted["adj_close"][-1])
    if min(first_raw, first_adj) <= 0:
        raise click.ClickException("research demo returned a non-positive starting close")
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
    from cn_market_lake.derive.adj_factors import compute_adj_factors
    from cn_market_lake.query.reader import load

    click.echo("Deriving hfq factors from Sina for the demo symbols…")
    result = compute_adj_factors(
        cfg,
        adjust_type="hfq",
        refresh_symbols=symbols,
        full=True,
    )
    if result.failed:
        failed = ", ".join(result.failed)
        raise click.ClickException(
            f"Sina returned no adjustment factor for: {failed}. "
            "Run `cml demo` without --research to test TDX only."
        )
    errors = [finding for finding in result.findings if finding.get("severity") == "error"]
    if errors:
        raise click.ClickException(
            "Sina adjustment validation failed: "
            + "; ".join(str(finding.get("message", "unknown finding")) for finding in errors)
        )

    sample_symbol = symbols[0]
    raw = load(
        "daily_bars",
        start=start,
        end=end,
        symbols=[sample_symbol],
        config=cfg,
    )
    adjusted = load(
        "daily_bars",
        start=start,
        end=end,
        symbols=[sample_symbol],
        adjust="hfq",
        strict_adj=True,
        config=cfg,
    )
    summary = _return_summary(raw, adjusted)
    click.echo(
        f"{sample_symbol}: raw return {summary['raw_return']:+.2%} → "
        f"hfq return {summary['adjusted_return']:+.2%} "
        f"({summary['rows']} exact rows, {summary['start']}..{summary['end']})"
    )
    return {"symbol": sample_symbol, **summary}


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
    from cn_market_lake.adapters.tdx_protocol.minute_bars import bars_per_session

    intraday_days = min(days, 5)
    start = _start_for_days(cfg, end, intraday_days)
    cfg.minute_bars_enabled = True
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = list(symbols)
    cfg.minute_bars_frequencies = ["1m"]
    cfg._backfill = True
    cfg._backfill_start = start
    cfg._backfill_end = end

    result = engine.run_job(
        "demo:intraday",
        trade_date=end,
        waves=[WaveConfig(name="intraday", parallel=False, steps=["minute_bars", "compact"])],
        backfill=True,
    )
    if result.get("status") not in ("success", "warning"):
        raise click.ClickException(f"minute_bars failed: {result}")

    from cn_market_lake.query.reader import load

    bars = load("minute_bars", symbols=symbols, config=cfg)
    if bars.is_empty():
        raise click.ClickException("minute_bars returned no rows for the demo window")

    expected = bars_per_session("1m")
    per_day = (
        bars.group_by("symbol", "trade_date")
        .agg(pl.len().alias("bars"))
        .sort("symbol", "trade_date")
    )
    full = per_day.filter(pl.col("bars") == expected).height
    click.echo(
        f"{bars.height} 1m bars over {per_day.height} symbol-day(s); "
        f"{full}/{per_day.height} hold a full {expected}-bar session"
    )
    one = bars.filter(pl.col("symbol") == symbols[0]).sort("bar_time")
    session = one.filter(pl.col("trade_date") == one["trade_date"].max())
    with pl.Config(tbl_rows=6, tbl_cols=-1, fmt_str_lengths=24):
        click.echo(
            f"\n{symbols[0]} — first and last bars of {session['trade_date'][0]} "
            "(bar_time is the CLOSING minute):\n"
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


def run_demo(
    *,
    symbols: list[str],
    days: int,
    data_root: Path,
    trade_date: date | None = None,
    config_out: Path | None = None,
    intraday: bool = False,
    research: bool = False,
) -> dict:
    """Run the mini real-source demo. Returns a small summary dict."""
    _configure_logging()
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    if not symbols:
        raise click.ClickException("--symbols must list at least one symbol")
    if days < 1:
        raise click.ClickException("--days must be >= 1")

    config_out = config_out or Path("configs/cn-market-lake.demo.toml")
    steps = 8 if research and intraday else 7 if (research or intraday) else 6
    _banner(f"1/{steps}", f"Prepare demo lake at {data_root}")
    _write_demo_toml(config_out, data_root)
    cfg = _demo_config(data_root, config_path=config_out.resolve())
    init_data_layout(cfg)
    click.echo(f"data_root = {cfg.data_root}")
    click.echo(f"config    = {config_out}")
    click.echo("Note: this is a SEPARATE lake from a full `cml init` — safe to wipe.")

    _banner(f"2/{steps}", "Probe TDX")
    try:
        _probe_tdx(cfg)
    except Exception as exc:
        raise click.ClickException(
            f"TDX unreachable: {exc}\n"
            "Tips: try from a mainland network / VPN egress, or check "
            "`[tdx_protocol.hosts]` in the example config."
        ) from exc

    _banner(f"3/{steps}", "Instruments (demo universe)")
    kept = _write_demo_instruments(cfg, symbols)

    _banner(f"4/{steps}", "Trading calendar")
    engine = JobEngine(cfg)
    as_of = trade_date or date.today()
    # Seed calendar covers a wide range; backfill window is cheap (CSV/seed).
    cfg._backfill = True
    cfg._backfill_start = date(2020, 1, 1)
    cfg._backfill_end = as_of
    cal = engine.run_job(
        "demo:calendar",
        trade_date=as_of,
        steps=["trading_calendar"],
        backfill=True,
    )
    if cal.get("status") not in ("success", "warning"):
        raise click.ClickException(f"trading_calendar failed: {cal}")
    end = _last_trading_day(cfg, as_of)
    window_days = max(days, RESEARCH_MIN_DAYS) if research else days
    start = _start_for_days(cfg, end, window_days)
    click.echo(
        f"Demo window: {start.isoformat()} → {end.isoformat()} ({window_days} trading days target)"
    )
    if research and window_days != days:
        click.echo(
            f"Research mode expanded the window from {days} to {window_days} sessions "
            "so corporate-action adjustments can be observed."
        )

    _banner(f"5/{steps}", f"daily_bars for {len(kept)} symbols")
    cfg._backfill = True
    cfg._backfill_start = start
    cfg._backfill_end = end
    bars = engine.run_job(
        "demo:bars",
        trade_date=end,
        waves=[WaveConfig(name="bars", parallel=False, steps=["daily_bars", "compact"])],
        backfill=True,
    )
    if bars.get("status") not in ("success", "warning"):
        raise click.ClickException(
            f"daily_bars failed: {bars}\nRe-run `cml demo` after fixing TDX connectivity."
        )
    click.echo(
        f"Bars run {bars.get('run_id')}: status={bars.get('status')} "
        f"rows_written≈{bars.get('rows_written', '?')}"
    )

    _banner(f"6/{steps}", "Sample result")
    sample_symbol = kept[0]
    try:
        sample = _sample_query(cfg, sample_symbol)
    except Exception as exc:
        raise click.ClickException(f"query failed after demo write: {exc}") from exc
    if sample.is_empty():
        raise click.ClickException(
            f"No daily_bars rows for {sample_symbol}. TDX may have returned an empty window."
        )
    with pl.Config(tbl_rows=10, tbl_cols=-1, fmt_str_lengths=24):
        click.echo(f"\n{sample_symbol} — latest rows:\n")
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
        _banner(f"7/{steps}", "Research: raw vs hfq return")
        research_summary = _run_research_demo(cfg, kept, start, end)

    intraday_summary = None
    if intraday:
        step = 8 if research else 7
        _banner(f"{step}/{steps}", f"minute_bars (1m) for {len(kept)} symbols")
        intraday_summary = _run_intraday_demo(cfg, engine, kept, end, days)

    click.echo(
        f"""
Demo lake ready under: {cfg.data_root}
Config written to:     {config_out}

Next:
  cml query --config {config_out} --sql "
    SELECT symbol, trade_date, close, volume, source
    FROM daily_bars
    WHERE symbol = '{sample_symbol}'
    ORDER BY trade_date DESC
    LIMIT 10
  "

Python:
  from cn_market_lake.query import load
  bars = load("daily_bars", symbols=["{sample_symbol}"], data_root="{cfg.data_root}")
{_intraday_hint(intraday_summary, cfg, sample_symbol)}
Full-market backfill (hours/days) is separate: `cml config init` then `cml init`.
Do not reuse this demo data_root for production.
"""
    )
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
