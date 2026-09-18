"""One-command mini demos: real TDX data or a deterministic offline sample.

Designed for first-run / star-seeker UX. Reached as ``cne init --profile
demo|sample``; it is not a substitute for the market profiles.
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

from cnequity.config import Config, WaveConfig
from cnequity.domain.market_time import shanghai_today
from cnequity.domain.schemas import validate_dataframe, with_provenance
from cnequity.orchestrator.engine import JobEngine
from cnequity.storage.atomic import write_parquet_atomic
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
DEFAULT_DATA_ROOT = Path("data/cnequity-demo")
RESEARCH_MIN_DAYS = 756  # roughly three trading years; enough to cross corporate actions


def _banner(step: str, title: str) -> None:
    click.echo(f"\n=== [{step}] {title} ===", err=False)
    sys.stdout.flush()


def _write_demo_toml(path: Path, data_root: Path) -> None:
    """Persist a tiny config so follow-up ``cne query --config …`` works."""
    from cnequity.config.bootstrap import path_for_toml

    path.parent.mkdir(parents=True, exist_ok=True)
    # POSIX form + TOML escape: bare ``C:\Users\…`` is invalid TOML (``\U`` etc.).
    root = path_for_toml(data_root)
    path.write_text(
        f"""# Auto-written by `cne init --profile demo`. Safe to delete with the demo data_root.
[data]
root = "{root}"
# Says what this lake is, so whole-lake checks do not judge five symbols
# against the full 42-dataset registry.
profile = "demo"

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


def _write_sample_toml(path: Path, data_root: Path) -> None:
    """Persist a read-only config for the generated, explicitly synthetic lake."""
    from cnequity.config.bootstrap import path_for_toml

    path.parent.mkdir(parents=True, exist_ok=True)
    root = path_for_toml(data_root)
    path.write_text(
        f"""# Auto-written by `cne init --profile sample`.
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
""",
        encoding="utf-8",
    )


def _demo_config(data_root: Path, config_path: Path | None = None) -> Config:
    """Minimal real-source config (workers=1, no mock, TDX only)."""
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
    from cnequity.adapters.tdx_protocol.client import _quotes_client
    from cnequity.adapters.tdx_protocol.session import close_quotes_client

    t0 = time.perf_counter()
    click.echo("正在探测 TDX 主机（第一个连通的服务器胜出）…")
    sys.stdout.flush()
    client = _quotes_client(cfg)
    try:
        click.echo(f"TDX 连接正常（{time.perf_counter() - t0:.1f}s）")
    finally:
        # The heartbeat thread is not a daemon, so an unclosed client keeps the
        # interpreter alive after the demo has printed everything — the run looks
        # like it hangs when in fact all six steps already finished.
        close_quotes_client(client)


def _write_demo_instruments(cfg: Config, symbols: list[str]) -> list[str]:
    from cnequity.adapters.tdx_protocol.client import fetch_instruments, normalize_with_source

    click.echo(f"拉取完整标的清单，然后只保留 {len(symbols)} 只 demo 标的…")
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
            f"警告：不在 TDX 清单里（已跳过）：{', '.join(missing)}",
            err=True,
        )
    if kept.is_empty():
        raise click.ClickException(
            "TDX 一只 demo 标的都没有返回。\n"
            "  先查环境：`cne doctor`（不需要配置也不需要网络）。\n"
            "  再查链路：`cne sources probe --only tdx_protocol "
            "--config configs/cnequity.demo.toml`。\n"
            "  完全没有网络？用 `cne init --profile sample` 建一个离线湖。"
        )
    df = validate_dataframe(
        with_provenance(kept, source="tdx_protocol", data_version="v1"),
        "instruments",
    )
    out = cfg.curated_root / "instruments" / "part-merged.parquet"
    write_parquet_atomic(out, df, compression="zstd")
    click.echo(f"已写入 {df.height} 条 instruments → {out}")
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
        raise click.ClickException("research demo 没有取到任何日线")
    raw = raw.sort("trade_date")
    adjusted = adjusted.sort("trade_date")
    first_raw = float(raw["close"][0])
    last_raw = float(raw["close"][-1])
    first_adj = float(adjusted["adj_close"][0])
    last_adj = float(adjusted["adj_close"][-1])
    if min(first_raw, first_adj) <= 0:
        raise click.ClickException("research demo 的起始收盘价不是正数")
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
    from cnequity.query.reader import load

    click.echo("正在用 Sina 为 demo 标的派生 hfq 复权因子…")
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
            f"警告：Sina 没有返回这些的复权因子：{', '.join(sorted(result.failed))}",
            err=True,
        )
    if not usable:
        raise click.ClickException(
            f"Sina 没有为任何一只 demo 标的返回复权因子：{', '.join(result.failed)}。"
            "去掉 --research 跑 `cne init --profile demo`，可以只验证 TDX。"
        )
    errors = [finding for finding in result.findings if finding.get("severity") == "error"]
    if errors:
        raise click.ClickException(
            "Sina 复权因子校验失败："
            + "；".join(str(finding.get("message", "unknown finding")) for finding in errors)
        )
    # This lake holds bars and factors but no corporate_actions, so the ex-date
    # cross-check has nothing to arbitrate against. Say so rather than letting
    # the printed return look like a verified one.
    click.echo(
        "提示：demo 湖里没有 corporate_actions，所以这些因子没有和除权日交叉校验过。"
        "完整的湖会校验每一次跳变。",
        err=True,
    )

    sample_symbol = usable[0]
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
        f"{sample_symbol}：未复权收益 {summary['raw_return']:+.2%} → "
        f"hfq 复权收益 {summary['adjusted_return']:+.2%}"
        f"（{summary['rows']} 行精确因子，{summary['start']}..{summary['end']}）"
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
    from cnequity.adapters.tdx_protocol.minute_bars import bars_per_session

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
        raise click.ClickException(f"minute_bars 失败：{result}")

    from cnequity.query.reader import load

    bars = load("minute_bars", symbols=symbols, config=cfg)
    if bars.is_empty():
        raise click.ClickException("minute_bars 在 demo 窗口内没有返回任何行")

    expected = bars_per_session("1m")
    per_day = (
        bars.group_by("symbol", "trade_date")
        .agg(pl.len().alias("bars"))
        .sort("symbol", "trade_date")
    )
    full = per_day.filter(pl.col("bars") == expected).height
    click.echo(
        f"{bars.height} 根 1 分钟线，覆盖 {per_day.height} 个 标的×交易日；"
        f"其中 {full}/{per_day.height} 是完整的 {expected} 根/日"
    )
    one = bars.filter(pl.col("symbol") == symbols[0]).sort("bar_time")
    session = one.filter(pl.col("trade_date") == one["trade_date"].max())
    with pl.Config(tbl_rows=6, tbl_cols=-1, fmt_str_lengths=24):
        click.echo(
            f"\n{symbols[0]} —— {session['trade_date'][0]} 的头尾几根 K 线"
            "（bar_time 是这一分钟的**收盘**时刻）：\n"
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
        "600519.SH": "贵州茅台（样例）",
        "000001.SZ": "平安银行（样例）",
        "000858.SZ": "五粮液（样例）",
        "300750.SZ": "宁德时代（样例）",
        "601318.SH": "中国平安（样例）",
    }
    instruments = pl.DataFrame(
        [
            {
                "symbol": symbol,
                "name": names.get(symbol, f"{symbol}（样例）"),
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
) -> dict:
    """Write a deterministic no-network lake for installation and query checks."""
    from cnequity.domain.schemas import MOCK_SOURCE
    from cnequity.query.views import ensure_duckdb_views

    if intraday or research:
        raise click.ClickException("--sample 不能和 --intraday 或 --research 一起用")
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    if not symbols:
        raise click.ClickException("--symbols 至少要给一只标的")
    if days < 1 or days > 366:
        raise click.ClickException("sample 模式下 --days 必须在 1 到 366 之间")
    if any(data_root.rglob("*.parquet")):
        raise click.ClickException(
            f"样例目标目录里已经有 Parquet 文件：{data_root}。"
            "请换一个空的 --data-root，避免把合成数据和真实数据混在一起。"
        )

    config_out = config_out or Path("configs/cnequity.demo.toml")
    end = trade_date or date(2024, 6, 28)
    sessions = _sample_sessions(end, days)

    _banner("1/3", f"在 {data_root} 准备离线样例湖")
    click.echo("离线样例：生成的是合成价格，不是市场数据。")
    _write_sample_toml(config_out, data_root)
    cfg = _demo_config(data_root, config_path=config_out.resolve())
    cfg.tdx_enabled = False
    init_data_layout(cfg)

    _banner("2/3", f"写入 {len(symbols)} 只标的 × {len(sessions)} 个交易日")
    instruments, bars = _sample_frames(symbols, sessions)
    write_parquet_atomic(
        cfg.curated_root / "instruments" / "part-sample.parquet",
        instruments,
        compression="zstd",
    )
    for (session,), frame in bars.partition_by("trade_date", as_dict=True).items():
        out = cfg.curated_root / "daily_bars" / f"trade_date={session.isoformat()}"
        write_parquet_atomic(out / "part-sample.parquet", frame, compression="zstd")
    ensure_duckdb_views(cfg)

    _banner("3/3", "通过公开 API 查询样例")
    sample_symbol = symbols[0]
    sample = _sample_query(cfg, sample_symbol)
    with pl.Config(tbl_rows=10, tbl_cols=-1, fmt_str_lengths=24):
        click.echo(sample.select("symbol", "trade_date", "close", "volume", "source"))
    click.echo(
        f"""
离线样例已就绪：{cfg.data_root}
配置写到：    {config_out}
所有行的 source={MOCK_SOURCE}；绝不可用于研究或生产。

下一步：
  cne query --config {config_out} --sql "
    SELECT symbol, trade_date, close, volume, source
    FROM daily_bars
    ORDER BY trade_date DESC
    LIMIT 10
  "

网络可用时，跑 `cne init --profile demo` 取真实的 TDX 数据。
"""
    )
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
) -> dict:
    """Run the mini real-source demo. Returns a small summary dict."""
    _configure_logging()
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    if not symbols:
        raise click.ClickException("--symbols 至少要给一只标的")
    if days < 1:
        raise click.ClickException("--days 必须 >= 1")

    config_out = config_out or Path("configs/cnequity.demo.toml")
    steps = 8 if research and intraday else 7 if (research or intraday) else 6
    _banner(f"1/{steps}", f"在 {data_root} 准备 demo 湖")
    _write_demo_toml(config_out, data_root)
    cfg = _demo_config(data_root, config_path=config_out.resolve())
    init_data_layout(cfg)
    click.echo(f"data_root = {cfg.data_root}")
    click.echo(f"config    = {config_out}")
    click.echo("提示：这是和 `cne init --profile quick|full` 完全分开的湖 —— 可以随时删掉。")

    _banner(f"2/{steps}", "探测 TDX")
    try:
        _probe_tdx(cfg)
    except Exception as exc:
        raise click.ClickException(
            f"连不上 TDX：{exc}\n"
            "建议：换大陆网络 / VPN 出口再试，或者检查示例配置里的 "
            "`[tdx_protocol.hosts]`。"
        ) from exc

    _banner(f"3/{steps}", "Instruments（demo 标的范围）")
    kept = _write_demo_instruments(cfg, symbols)

    _banner(f"4/{steps}", "交易日历")
    engine = JobEngine(cfg)
    as_of = trade_date or shanghai_today()
    # Seed calendar covers a wide range; backfill window is cheap (CSV/seed).
    cfg._backfill = True
    cfg._backfill_start = date(2020, 1, 1)
    cfg._backfill_end = as_of
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
    if cal.get("status") not in ("success", "warning"):
        raise click.ClickException(f"trading_calendar 失败：{cal}")
    end = _last_trading_day(cfg, as_of)
    window_days = max(days, RESEARCH_MIN_DAYS) if research else days
    start = _start_for_days(cfg, end, window_days)
    click.echo(f"demo 窗口：{start.isoformat()} → {end.isoformat()}（目标 {window_days} 个交易日）")
    if research and window_days != days:
        click.echo(
            f"research 模式把窗口从 {days} 个交易日扩到 {window_days} 个，"
            "这样才看得到除权除息带来的复权差异。"
        )

    _banner(f"5/{steps}", f"daily_bars（{len(kept)} 只标的）")
    cfg._backfill = True
    cfg._backfill_start = start
    cfg._backfill_end = end
    # Name the symbols rather than leaving the step to infer them from
    # `instruments`. TDX publishes no list_date, so an undated demo symbol with
    # no bar yet in the lake is classified as a pre-listing placeholder and
    # skipped — which made a second `--profile demo` run on an existing demo
    # lake fetch nothing and then blame TDX for the empty result.
    cfg._backfill_symbols = list(kept)
    bars = engine.run_job(
        "demo:bars",
        trade_date=end,
        waves=[WaveConfig(name="bars", parallel=False, steps=["daily_bars", "compact"])],
        backfill=True,
    )
    if bars.get("status") not in ("success", "warning"):
        # Name the step's own reason rather than guessing one. This blamed TDX
        # connectivity for every failure, sending people to debug a network
        # that was fine.
        reasons = [
            str(r.get("error")) for r in bars.get("results", []) if r.get("status") == "failed"
        ]
        detail = "\n".join(f"  {r}" for r in reasons) or f"  {bars}"
        raise click.ClickException(
            f"daily_bars 失败：\n{detail}\n解决之后重跑 `cne init --profile demo`。"
        )
    click.echo(
        f"日线 run {bars.get('run_id')}：status={bars.get('status')} "
        f"rows_written≈{bars.get('rows_written', '?')}"
    )

    _banner(f"6/{steps}", "结果样例")
    sample_symbol = kept[0]
    try:
        sample = _sample_query(cfg, sample_symbol)
    except Exception as exc:
        raise click.ClickException(f"demo 写完之后查询失败：{exc}") from exc
    if sample.is_empty():
        raise click.ClickException(
            f"{sample_symbol} 在 {start.isoformat()}..{end.isoformat()} 内没有任何 daily_bars 行。"
            "这个 step 报的是成功，所以这是源端窗口为空、而不是失败："
            f"用 `cne status --run {bars.get('run_id')} --config {config_out}` "
            "看这次 run 自己的 findings。"
        )
    with pl.Config(tbl_rows=10, tbl_cols=-1, fmt_str_lengths=24):
        click.echo(f"\n{sample_symbol} —— 最近几行：\n")
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
        _banner(f"7/{steps}", "研究口径：未复权 vs hfq 收益")
        research_summary = _run_research_demo(cfg, kept, start, end)

    intraday_summary = None
    if intraday:
        step = 8 if research else 7
        _banner(f"{step}/{steps}", f"minute_bars 1 分钟线（{len(kept)} 只标的）")
        intraday_summary = _run_intraday_demo(cfg, engine, kept, end, days)

    click.echo(
        f"""
demo 湖已就绪：{cfg.data_root}
配置写到：     {config_out}

下一步：
  cne query --config {config_out} --sql "
    SELECT symbol, trade_date, close, volume, source
    FROM daily_bars
    WHERE symbol = '{sample_symbol}'
    ORDER BY trade_date DESC
    LIMIT 10
  "

Python：
  from cnequity.query import load
  bars = load("daily_bars", symbols=["{sample_symbol}"], data_root="{cfg.data_root}")
{_intraday_hint(intraday_summary, cfg, sample_symbol)}
全市场回填（数小时到数天）是另一回事：先 `cne config create`，再
`cne init --profile quick`。
不要把这个 demo 的 data_root 拿去跑生产。
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
