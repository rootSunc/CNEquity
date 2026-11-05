"""Reading the lake: `query`, `serve`, `mcp`.

SQL and services read the lake; explicit on-demand queries may fetch and cache
remote data, and MCP live mode is separately opt-in.
"""

from __future__ import annotations

import errno
import json
import logging
import re
import socket
import sys
from pathlib import Path

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    config_option,
    resolve_config_path,
)
from cnequity.i18n import N_, _
from cnequity.query.on_demand import OnDemandService
from cnequity.query.views import views_connection
from cnequity.run_scope import set_run_flags

_LOOPBACK = {"127.0.0.1", "::1", "localhost"}
# macOS resolves localhost to ::1 before 127.0.0.1, and a browser does not
# fall back when that first address refuses the connection. Binding only the
# IPv4 loopback makes http://localhost:<port>/ look like the panel is down.
_LOOPBACK_FAMILIES = (
    (socket.AF_INET, "127.0.0.1"),
    (socket.AF_INET6, "::1"),
)


def _address_in_use(exc: OSError) -> bool:
    if exc.errno == errno.EADDRINUSE:
        return True
    # 10048 is WSAEADDRINUSE. 10013 is WSAEACCES from SO_EXCLUSIVEADDRUSE.
    return getattr(exc, "winerror", None) in {10048, 10013}


def bind_loopback(port: int) -> list[socket.socket]:
    """Listen on every loopback family this machine has.

    The sockets are already listening, so a browser can connect while the
    dashboard app is still being built. The kernel queues that connection
    until uvicorn accepts it, instead of answering with connection refused.
    """
    bound: list[socket.socket] = []
    chosen = port
    try:
        for family, address in _LOOPBACK_FAMILIES:
            sock = socket.socket(family, socket.SOCK_STREAM)
            try:
                if family is socket.AF_INET6:
                    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                # Windows SO_REUSEADDR lets a second socket bind a port that is
                # already listening, so a busy port would look free. Exclusive
                # use makes that bind fail. Other platforms still reuse the
                # address so a restart during TIME_WAIT can bind again.
                if sys.platform == "win32":
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                else:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((address, chosen))
                sock.listen(2048)
                sock.set_inheritable(True)
            except OSError as exc:
                sock.close()
                if _address_in_use(exc):
                    raise click.ClickException(
                        _(
                            "Port {value} is already in use, so the dashboard did not start. Stop the process using this port, then run cne serve again."
                        ).format(value=chosen or port)
                    ) from exc
                continue
            chosen = sock.getsockname()[1]
            bound.append(sock)
    except Exception:
        for sock in bound:
            sock.close()
        raise
    if not bound:
        raise click.ClickException(
            _("This machine has no usable loopback address, so the dashboard did not start.")
        )
    return bound


def _panel_url(sockets: list[socket.socket], token: str | None) -> str:
    """One loopback URL. localhost is the name browsers try first."""
    suffix = f"?token={token}" if token else ""
    port = sockets[0].getsockname()[1]
    names = [
        "localhost" if sock.getsockname()[0] == "::1" else sock.getsockname()[0] for sock in sockets
    ]
    name = "localhost" if "localhost" in names else names[0]
    return f"http://{name}:{port}/{suffix}"


_TOKEN_PATH = re.compile(r"(/mcp/)[^/?#\s]+")
_TOKEN_QUERY = re.compile(r"([?&]token=)[^&#\s]*", re.IGNORECASE)
_REDACTED = "<redacted>"


def redact_url_secrets(path: str) -> str:
    """Hide the access tokens a URL can carry: ``/mcp/<token>`` and ``?token=``.

    Both the dashboard (``?token=``) and ``cne mcp --http`` (``/mcp/<token>``,
    ``?token=``) accept a token in the URL, because some clients can only be
    given a URL. uvicorn's access log records the full path with its query
    string, so without this the secret lands in every log line, and in any
    tunnel or proxy log that captures stdout.
    """
    return _TOKEN_QUERY.sub(rf"\1{_REDACTED}", _TOKEN_PATH.sub(rf"\1{_REDACTED}", path))


class _AccessLogRedactor(logging.Filter):
    """Rewrite the request path of a ``uvicorn.access`` record before it is emitted."""

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        # uvicorn: (client_addr, method, full_path, http_version, status_code)
        if isinstance(args, tuple) and len(args) >= 3 and isinstance(args[2], str):
            record.args = (*args[:2], redact_url_secrets(args[2]), *args[3:])
        elif isinstance(record.msg, str):
            record.msg = redact_url_secrets(record.msg)
        return True


def _install_access_log_redaction() -> None:
    access = logging.getLogger("uvicorn.access")
    if not any(isinstance(f, _AccessLogRedactor) for f in access.filters):
        access.addFilter(_AccessLogRedactor())


def _run_server(app, host: str, port: int, sockets: list[socket.socket] | None) -> None:
    import uvicorn

    # uvicorn.Config re-applies its logging dictConfig on construction; that
    # replaces the logger's handlers but keeps its filters, so installing the
    # filter first is enough.
    _install_access_log_redaction()
    if sockets is not None:
        uvicorn.Server(uvicorn.Config(app, log_level="info")).run(sockets=sockets)
        return
    uvicorn.run(app, host=host, port=port, log_level="info")


@cli.command()
@config_option
@click.option(
    "--host",
    default="127.0.0.1",
    show_default=True,
    help=N_(
        "A loopback address listens on both 127.0.0.1 and localhost. A non-loopback address requires --token."
    ),
)
@click.option("--port", default=8787, show_default=True)
@click.option(
    "--token",
    default=None,
    help=N_(
        "Require this bearer token (or ?token=). Required when --host is not a loopback address."
    ),
)
@click.option(
    "--read-only",
    is_flag=True,
    help=N_(
        "Browse only. Do not register the write endpoints of the operations page and storage cleanup."
    ),
)
@click.option(
    "--allow-remote-ops",
    is_flag=True,
    help=N_(
        "Allow starting fetches from the dashboard on non-loopback addresses too. By default remote clients can only browse and run storage cleanup; the token is in the URL and the LAN is plaintext."
    ),
)
def serve(
    config_path: str,
    host: str,
    port: int,
    token: str | None,
    read_only: bool,
    allow_remote_ops: bool,
):
    """Start the lake dashboard. On a loopback address the page can start fetches and daily updates.

    \b
    View coverage, freshness and sources; the operations page starts init, daily updates,
    backfills and checks from an allowlist; fetch settings are previewed and written back to
    the config without starting a fetch; the storage page reviews and confirms expired
    version and experiment cleanup. ``--read-only`` turns off these write paths.
    A non-loopback address requires ``--token``. Remote browsers cannot start fetches unless
    ``--allow-remote-ops`` is also given. Loopback accepts both 127.0.0.1 and localhost.

    """
    from cnequity.serve.app import create_app

    # Checked before the config is even loaded: a typo in --config must not
    # mask the bind guard by failing first. The service has no other access
    # control, and a lake holds a full market history plus the paths and
    # sources that built it.
    if host not in _LOOPBACK and not token:
        raise click.ClickException(
            _(
                "--host {host} exposes the dashboard beyond this machine; require a token with --token, or keep --host at 127.0.0.1."
            ).format(host=host)
        )
    if allow_remote_ops and host in _LOOPBACK:
        allow_remote_ops = False

    ctx = click.get_current_context()
    config_was_default = (
        ctx.get_parameter_source("config_path") is click.core.ParameterSource.DEFAULT
    )
    config_file = Path(config_path).expanduser()
    setup = config_was_default and not config_file.exists()
    if setup and host not in _LOOPBACK:
        raise click.ClickException(
            _("First-time setup can only be opened on this machine; keep --host at 127.0.0.1.")
        )

    cfg = None if setup else _cfg(config_path)
    if read_only:
        mode = _("read-only (--read-only)")
    elif setup:
        mode = _("first-time setup")
    elif host not in _LOOPBACK and not allow_remote_ops:
        mode = _("remote browsing (fetching off; storage cleanup still available)")
    else:
        mode = _("fetches and daily updates can be started from the dashboard")
    sockets: list[socket.socket] | None = None
    try:
        if host in _LOOPBACK:
            sockets = bind_loopback(port)
            port = sockets[0].getsockname()[1]
        if cfg is not None:
            click.echo(_("Lake:      {data_root}").format(data_root=cfg.data_root))
        else:
            click.echo(
                _(
                    "No config file at {config_file} yet; the dashboard opens first-time setup."
                ).format(config_file=config_file)
            )
        if sockets is not None:
            url = _panel_url(sockets, token)
            click.echo(_("Dashboard: {url}").format(url=url))
            primary = url.split("?", 1)[0].rstrip("/")
        else:
            suffix = f"?token={token}" if token else ""
            primary = f"http://{host}:{port}"
            click.echo(_("Dashboard: {primary}/{suffix}").format(primary=primary, suffix=suffix))
        click.echo(_("API docs:  {primary}/api/docs").format(primary=primary))
        if not setup:
            query = f"?token={token}" if token else ""
            click.echo(
                _("Source health: {primary}/source-health{query}").format(
                    primary=primary, query=query
                )
            )
        click.echo(_("Mode:      {mode}").format(mode=mode))
        _run_server(
            create_app(
                cfg,
                token=token,
                read_only=read_only,
                allow_remote_ops=allow_remote_ops,
                setup=setup,
                config_path=config_file,
            ),
            host,
            port,
            sockets,
        )
    finally:
        for sock in sockets or []:
            try:
                sock.close()
            except OSError:
                pass


@cli.command()
@config_option
@click.option(
    "--sql",
    default="SELECT COUNT(*) AS n FROM daily_bars",
    show_default=True,
    help=N_("Read-only DuckDB SQL; each dataset is a view named as in `cne status --datasets`."),
)
@click.option("--dataset", default=None, help=N_("Dataset name to fetch on demand"))
@click.option("--symbol", default=None, help=N_("Symbol to fetch on demand"))
@click.option(
    "--refresh",
    is_flag=True,
    help=N_("Refresh the on-demand cache before fetching (requires both --dataset and --symbol)."),
)
def query(
    config_path: str,
    sql: str,
    dataset: str | None,
    symbol: str | None,
    refresh: bool,
):
    """Run DuckDB SQL, or fetch a single dataset on demand."""
    cfg = _cfg(config_path)
    if (dataset is None) != (symbol is None):
        raise click.UsageError(_("--dataset and --symbol must be given together"))
    if refresh and dataset is None:
        raise click.UsageError(_("--refresh needs both --dataset and --symbol"))
    if dataset and symbol:
        svc = OnDemandService(cfg)
        fetch_kwargs = {"refresh": True} if refresh else {}
        try:
            data = svc.fetch(dataset, symbol, **fetch_kwargs)
        except (ValueError, NotImplementedError, RuntimeError) as exc:
            raise click.ClickException(str(exc)) from None
        click.echo(json.dumps(data, indent=2, ensure_ascii=False, default=str))
        if data.get("error"):
            raise SystemExit(1)
        return
    import duckdb
    import polars as pl

    con = views_connection(cfg)
    try:
        # A typo in the SQL is the most ordinary thing that happens here, and
        # DuckDB's own message already names the line, the column and the near
        # miss. Keep that text and drop the Python traceback wrapped around it.
        try:
            df = pl.from_arrow(con.sql(sql).to_arrow_table())
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
        N_(
            "Fetch data missing from the lake on demand from the source, without writing it to disk. Supports only symbol lookup and unadjusted daily bars; other tools refuse rather than answer without price adjustment, universe and PIT."
        )
    ),
)
@click.option(
    "--http",
    "use_http",
    is_flag=True,
    help=N_(
        "Use Streamable HTTP instead, listening on /mcp. For clients that only accept a remote URL (such as ChatGPT), used with a tunnel."
    ),
)
@click.option(
    "--host",
    default="127.0.0.1",
    show_default=True,
    help=N_("Listen address for --http. A non-loopback address requires --token."),
)
@click.option("--port", default=8788, show_default=True, help=N_("Port for --http."))
@click.option(
    "--token",
    default=None,
    help=N_(
        "--http: every request must carry this token (Bearer header, /mcp/<token> path or ?token=). Required when exposed publicly through a tunnel."
    ),
)
def mcp_cmd(config_path: str, live: bool, use_http: bool, host: str, port: int, token: str | None):
    """Open this lake to AI agents over MCP (stdio or HTTP).

    \b
    It is not meant to be typed by hand: any MCP-compatible client launches this process
    and speaks JSON-RPC over the pipe. Clients register servers differently, but the
    portable command and arguments are:

    \b
      cne mcp --config /path/to/cnequity.toml

    \b
    Put that into the `command` / `args` of the client's MCP config. This is the standard
    stdio transport, not a vendor-specific Claude integration.

    \b
    \b
    Clients that only take a remote URL (such as ChatGPT) use `--http`, with a tunnel exposing /mcp over HTTPS, and must set `--token`:

    \b
      cne mcp --config /path/to/cnequity.toml --http --token <random-string>

    The MCP tools are read-only and do not offer serve's operations page or web-confirmed cleanup. They only query the lake; fetching starts from the CLI or serve's operations page.

    """

    from cnequity.mcp_server import serve_stdio

    ctx = click.get_current_context()
    if not use_http:
        given = [
            f"--{name}"
            for name in ("host", "port", "token")
            if ctx.get_parameter_source(name) is not click.core.ParameterSource.DEFAULT
        ]
        if given:
            raise click.UsageError(
                _("{value} only applies with --http").format(value=", ".join(given))
            )
    elif host not in _LOOPBACK and not token:
        raise click.ClickException(
            _(
                "--host {host} exposes MCP beyond this machine; require a token with --token, or keep --host at 127.0.0.1."
            ).format(host=host)
        )

    cfg = _cfg(config_path)
    # Opt-in, never inferred. A lake user whose lake is broken must get "no
    # parquet data" and go fix it, not a quietly different answer from a vendor.
    set_run_flags(cfg, mcp_live=live)
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
    if use_http:
        _serve_mcp_http(cfg, host, port, token)
        return
    serve_stdio(cfg)


def _serve_mcp_http(cfg, host: str, port: int, token: str | None) -> None:
    from cnequity.mcp_server.http import create_app

    sockets: list[socket.socket] | None = None
    try:
        if host in _LOOPBACK:
            sockets = bind_loopback(port)
            port = sockets[0].getsockname()[1]
            host = "127.0.0.1"
        base = f"http://{host}:{port}/mcp"
        click.echo(_("Lake:         {data_root}").format(data_root=cfg.data_root))
        click.echo(_("MCP endpoint: {base}").format(base=base))
        if token:
            click.echo(
                _(
                    "With token:   {base}/{token} (once tunnelled, replace the host with the tunnel's HTTPS domain)"
                ).format(base=base, token=token)
            )
        else:
            click.echo(
                _(
                    "No token set: only direct connections from this machine are accepted; requests through a tunnel or proxy are refused."
                )
            )
        _run_server(create_app(cfg, token=token), host, port, sockets)
    finally:
        for sock in sockets or []:
            try:
                sock.close()
            except OSError:
                pass


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
        _(
            "There is no curated data under {curated}.\n  config:    {value}\n  data.root: {data_root}\nIf this is not your lake: `data.root` is a relative path, resolved against the working directory the client launched this process from. Write both `--config` and `[data].root` as absolute paths.\nIf this really is your lake and it really is empty: `cne init` builds one, `cne init --profile demo` makes a 5-symbol sample in thirty seconds, and `--live` serves symbol lookup and unadjusted daily bars straight from the source with no lake at all."
        ).format(
            curated=curated,
            value=resolve_config_path(config_path).resolve(),
            data_root=cfg.data_root,
        )
    )
