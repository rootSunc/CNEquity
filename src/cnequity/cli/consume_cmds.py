"""Reading the lake: `query`, `serve`, `mcp`.

Three front ends over the same data — SQL, a read-only dashboard, and an agent
protocol — none of which may write.
"""

from __future__ import annotations

import json
import logging
import sys

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    config_option,
    resolve_config_path,
)
from cnequity.query.on_demand import OnDemandService
from cnequity.query.views import ensure_duckdb_views

_LOOPBACK = {"127.0.0.1", "::1", "localhost"}


@cli.command()
@config_option
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8787, show_default=True)
@click.option(
    "--token",
    default=None,
    help="要求这个 bearer token（或 ?token=）。--host 不是回环地址时必须设置。",
)
def serve(config_path: str, host: str, port: int, token: str | None):
    """启动只读的数据湖面板。

    \b
    展示覆盖区间、新鲜度和来源构成。这里没有任何东西会写湖 ——
    跑批、重试和清理仍然只在 CLI 上。
    """
    import uvicorn

    from cnequity.serve.app import create_app

    # Checked before the config is even loaded: a typo in --config must not
    # mask the bind guard by failing first. The service has no other access
    # control, and a lake holds a full market history plus the paths and
    # sources that built it.
    if host not in _LOOPBACK and not token:
        raise click.ClickException(
            f"--host {host} 会把面板暴露到本机之外；"
            "请用 --token 要求令牌，或者把 --host 留在 127.0.0.1。"
        )

    cfg = _cfg(config_path)
    click.echo(f"数据湖：  {cfg.data_root}")
    click.echo(f"面板：    http://{host}:{port}/" + (f"?token={token}" if token else ""))
    click.echo(f"API 文档：http://{host}:{port}/api/docs")
    click.echo(
        f"源健康：  http://{host}:{port}/source-health" + (f"?token={token}" if token else "")
    )
    uvicorn.run(create_app(cfg, token=token), host=host, port=port, log_level="info")


@cli.command()
@config_option
@click.option("--sql", default="SELECT COUNT(*) AS n FROM daily_bars")
@click.option("--dataset", default=None, help="按需抓取的数据集名")
@click.option("--symbol", default=None, help="按需抓取的标的代码")
@click.option(
    "--refresh",
    is_flag=True,
    help="抓取前先刷新按需缓存（需要同时给 --dataset 和 --symbol）。",
)
def query(
    config_path: str,
    sql: str,
    dataset: str | None,
    symbol: str | None,
    refresh: bool,
):
    """跑 DuckDB SQL，或按需抓取单个数据集。"""
    cfg = _cfg(config_path)
    if (dataset is None) != (symbol is None):
        raise click.UsageError("--dataset 和 --symbol 必须一起给")
    if refresh and dataset is None:
        raise click.UsageError("--refresh 需要同时给 --dataset 和 --symbol")
    if dataset and symbol:
        svc = OnDemandService(cfg)
        fetch_kwargs = {"refresh": True} if refresh else {}
        data = svc.fetch(dataset, symbol, **fetch_kwargs)
        click.echo(json.dumps(data, indent=2, ensure_ascii=False, default=str))
        return
    db_path = ensure_duckdb_views(cfg)
    import duckdb

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # A typo in the SQL is the most ordinary thing that happens here, and
        # DuckDB's own message already names the line, the column and the near
        # miss. Keep that text and drop the Python traceback wrapped around it.
        try:
            df = con.execute(sql).pl()
        except duckdb.Error as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(df)
    finally:
        con.close()


@cli.command("mcp")
@config_option
@click.option(
    "--live",
    is_flag=True,
    help=(
        "湖里没有的数据就按需向源头取，并且不落盘。只支持标的查找和未复权日线；其它工具宁可拒绝，也不会在没有复权、universe "
        "和 PIT 的情况下作答。"
    ),
)
def mcp_cmd(config_path: str, live: bool):
    """通过 MCP（stdio）把这个湖开放给 AI agent。

    \b
    它不是拿来手敲的：任何兼容 MCP 的客户端会拉起这个进程，并在管道上讲 JSON-RPC。
    各家客户端的注册界面不一样，但可移植的命令和参数就是：

    \b
      cne mcp --config /path/to/cnequity.toml

    \b
    把它填进客户端 MCP 配置里的 `command` / `args`。这里用的是标准 stdio 传输，
    不是某一家厂商专有的 Claude 集成。

    \b
    和 `cne serve` 一样只读。这些工具只查询湖；采集仍然留在 CLI 上，由人来跑。
    """

    from cnequity.mcp_server import serve_stdio

    cfg = _cfg(config_path)
    # Opt-in, never inferred. A lake user whose lake is broken must get "no
    # parquet data" and go fix it, not a quietly different answer from a vendor.
    cfg._mcp_live = live
    if not live:
        _guard_mcp_data_root(cfg, config_path)

    # stdout is the JSON-RPC wire. Anything else written there is a parse error
    # on the client with no indication of where it came from, so every log
    # record — ours and every library's — goes to stderr, which MCP clients
    # capture as the server's log.
    # `force=True` because the CLI root configures INFO logging for every
    # command; without it this call would be a no-op and the wire's log
    # would carry every INFO record the pipeline emits.
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING, force=True)
    serve_stdio(cfg)


def _guard_mcp_data_root(cfg, config_path: str) -> None:
    """Refuse to serve a lake with nothing in it, and say why it is empty.

    A relative ``data.root`` resolves against the working directory, and this is
    the one entry point where the working directory belongs to somebody else —
    an MCP client spawns the process from wherever it happens to be. The lake
    then resolves to a path that does not exist, every tool answers "no parquet
    data", and the agent reports that the data is missing. Which is true of that
    path and false of the user's lake.

    Cheap enough to do on every start: one directory walk that stops at the
    first file.
    """
    curated = cfg.curated_root
    if curated.exists() and next(curated.rglob("*.parquet"), None) is not None:
        return
    raise click.ClickException(
        f"{curated} 下没有任何 curated 数据。\n"
        f"  配置：     {resolve_config_path(config_path).resolve()}\n"
        f"  data.root：{cfg.data_root}\n"
        "如果这不是你的湖：`data.root` 是相对路径，会相对客户端拉起这个进程时的工作目录解析。"
        "请把 `--config` 和 `[data].root` 都写成绝对路径。\n"
        "如果这确实是你的湖、而且它真的是空的：`cne init` 会建一个，"
        "`cne init --profile demo` 三十秒内做出一个 5 只票的样例，"
        "而 `--live` 不需要湖也能直接从源头提供标的查找和未复权日线。"
    )
