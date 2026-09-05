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
    help="Require this bearer token (or ?token=). Mandatory for a non-loopback --host.",
)
def serve(config_path: str, host: str, port: int, token: str | None):
    """Serve the read-only lake dashboard.

    Shows coverage, freshness and source mix. Nothing here writes to the lake —
    running, retrying and cleaning stay with the CLI.
    """
    import uvicorn

    from cnequity.serve.app import create_app

    # Checked before the config is even loaded: a typo in --config must not
    # mask the bind guard by failing first. The service has no other access
    # control, and a lake holds a full market history plus the paths and
    # sources that built it.
    if host not in _LOOPBACK and not token:
        raise click.ClickException(
            f"--host {host} would expose the dashboard beyond this machine; "
            "pass --token to require one, or leave --host at 127.0.0.1."
        )

    cfg = _cfg(config_path)
    click.echo(f"lake:      {cfg.data_root}")
    click.echo(f"dashboard: http://{host}:{port}/" + (f"?token={token}" if token else ""))
    click.echo(f"api docs:  http://{host}:{port}/api/docs")
    click.echo(
        f"sources:   http://{host}:{port}/source-health" + (f"?token={token}" if token else "")
    )
    uvicorn.run(create_app(cfg, token=token), host=host, port=port, log_level="info")


@cli.command()
@config_option
@click.option("--sql", default="SELECT COUNT(*) AS n FROM daily_bars")
@click.option("--dataset", default=None, help="On-demand dataset name")
@click.option("--symbol", default=None, help="Symbol for on-demand fetch")
@click.option(
    "--refresh",
    is_flag=True,
    help="Refresh the on-demand cache before fetching (requires --dataset and --symbol).",
)
def query(
    config_path: str,
    sql: str,
    dataset: str | None,
    symbol: str | None,
    refresh: bool,
):
    """Run DuckDB SQL or on-demand dataset fetch."""
    cfg = _cfg(config_path)
    if (dataset is None) != (symbol is None):
        raise click.UsageError("--dataset and --symbol must be provided together")
    if refresh and dataset is None:
        raise click.UsageError("--refresh requires --dataset and --symbol")
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
    help="Where the lake holds nothing, fetch from the vendor on demand and do "
    "not store it. Serves symbol lookup and unadjusted daily bars only; every "
    "other tool refuses rather than answer without adjustment, universe or PIT.",
)
def mcp_cmd(config_path: str, live: bool):
    """Serve this lake to an AI agent over MCP (stdio).

    Not meant to be typed interactively: any MCP-compatible client spawns it
    and talks JSON-RPC on the pipe. The client-specific registration UI varies;
    the portable command and arguments are simply::

      cne mcp --config /path/to/cnequity.toml

    Use that command as the ``command``/``args`` entry in the client's MCP
    configuration. This implementation uses the standard stdio transport, not
    a vendor-specific Claude integration.

    Read-only, like `cne serve`. The tools query the lake; ingestion stays on
    the CLI, where a person runs it.
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
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
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
        f"No curated data under {curated}.\n"
        f"  config:    {resolve_config_path(config_path).resolve()}\n"
        f"  data.root: {cfg.data_root}\n"
        "If that is not your lake, `data.root` is relative and resolved against "
        "the working directory the client started this process in. Make both "
        "`--config` and `[data].root` absolute paths.\n"
        "If it is your lake and it is genuinely empty: `cne init` builds one, "
        "`cne demo` makes a 5-symbol sample in 30 seconds, and `--live` serves "
        "symbol lookup and raw daily bars straight from the vendor without one."
    )
