"""Config resolution and the small pieces every command group needs.

`config_option` exists because `--config` was declared 34 separate times, each
free to drift in default or help text. One decorator makes the contract single.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path

import click

from cnequity.config import load_config
from cnequity.config.errors import ConfigError
from cnequity.config.locate import (
    CONFIG_ENV_VAR,
    PROJECT_CONFIG,
    default_config_path,
    user_config_path,
)
from cnequity.config.logging import DEFAULT_LOGGING, logging_settings
from cnequity.i18n import N_, _
from cnequity.terminal_log import LOG_FORMAT as LOG_FORMAT
from cnequity.terminal_log import RedactedFormatter, configured_log_secrets, diagnostic_formatter

USER_CONFIG = PROJECT_CONFIG.as_posix()
EXAMPLE_CONFIG = "configs/cnequity.example.toml"
DEMO_CONFIG = "configs/cnequity.demo.toml"
DEMO_DATA_ROOT = "data/cnequity-demo"
SAMPLE_CONFIG = "configs/cnequity.sample.toml"
SAMPLE_DATA_ROOT = "data/cnequity-sample"
DEFAULT_CONFIG = USER_CONFIG


def ingest_scope_label(universe: str) -> str:
    """Human-readable ingest scope without overstating configured coverage."""
    return {
        "all_a": _("all A shares in Shanghai, Shenzhen and Beijing"),
        "all_a_sh_sz": _("Shanghai and Shenzhen A shares (config excludes Beijing)"),
        "all_instruments": _("every exchange-traded instrument in instruments (including ETF/LOF)"),
    }.get(universe, _("configured scope {universe}").format(universe=universe))


def config_option(func):
    """Attach the standard `--config` option.

    Equivalent to the hand-written option it replaces, including the parameter
    name (`config_path`) every command body already reads.
    """
    return click.option(
        "--config",
        "config_path",
        # Resolved in the callback, not here: the default depends on which
        # files exist when the command runs (see `cnequity.config.locate`).
        # The parameter source stays DEFAULT, which is what lets `cne init`
        # tell an implicit config it may create from a mistyped explicit one.
        default=None,
        callback=_implicit_config,
        # Static on purpose: the generated CLI reference must not carry the
        # path of whichever machine rendered it. Windows uses %APPDATA%.
        show_default=f"{USER_CONFIG}, else ~/.config/cnequity/config.toml",
        help=N_("Path to the config file."),
        # The shell pipelines already read `CNE_CONFIG` and passed it through
        # as `--config`; the CLI itself did not, so a cron line that forgot its
        # `cd` failed with "Config not found" instead of using the config the
        # environment already named.
        envvar=CONFIG_ENV_VAR,
        show_envvar=True,
    )(func)


def _implicit_config(ctx: click.Context, param: click.Parameter, value: str | None) -> str:
    return str(default_config_path()) if value is None else value


def config_is_implicit() -> bool:
    """Whether the running command got its config from neither --config nor CNE_CONFIG."""
    ctx = click.get_current_context(silent=True)
    return ctx is not None and (
        ctx.get_parameter_source("config_path") is click.core.ParameterSource.DEFAULT
    )


def resolve_config_path(config_path: str):
    path = Path(config_path).expanduser()
    if config_is_implicit() and not path.exists():
        # `cne init --profile demo` writes the demo config, not the user one, so every
        # command it points at afterwards ("if this fails, run `cne sources
        # probe ...`") used to die here on a second, unrelated error — at
        # exactly the moment the user was already trying to recover. Name the
        # config that does exist rather than falling back to it silently:
        # `cne init` against a demo data_root is destructive enough that the
        # choice has to stay the user's.
        hint = ""
        for small, profile in ((DEMO_CONFIG, "demo"), (SAMPLE_CONFIG, "sample")):
            if Path(small).exists():
                hint = _(
                    "\nFound {config}, written by `cne init --profile {profile}`. To work on that lake, add `--config {config}`."
                ).format(config=small, profile=profile)
                break
        raise click.ClickException(
            _(
                "No config found: looked for {project} in the current directory and {user}. On first use, just run `cne init`: it writes {user} and starts initializing. To only write the config, use `cne config create`. A config elsewhere is named with --config or CNE_CONFIG.{hint}"
            ).format(project=USER_CONFIG, user=user_config_path(), hint=hint)
        )
    if not path.exists():
        raise click.ClickException(_("Config not found: {path}").format(path=path))
    return path


# tomllib ends every decode error with "(at line N, column M)".
_TOML_POSITION = re.compile(r"\(at line (\d+), column (\d+)\)")


def _cfg(config: str):
    path = resolve_config_path(config)
    try:
        cfg = load_config(path)
    except ConfigError as exc:
        raise click.ClickException(
            _("Could not load config {path}: {reason}").format(path=path, reason=exc)
        ) from None
    except (ValueError, TypeError, OSError) as exc:
        # Parser messages may contain a literal token/proxy from the TOML.
        # Keep the path and error type, never echo the configuration text —
        # but do say where: "TOMLDecodeError" alone sent people reading the
        # whole file for one missing quote.
        at = _TOML_POSITION.search(str(exc))
        where = (
            _(" at line {line}, column {column}").format(line=at.group(1), column=at.group(2))
            if at
            else ""
        )
        raise click.ClickException(
            _(
                "Could not load config {path} ({name}{where}); check the TOML syntax, value types, and that rate limits are finite and non-negative."
            ).format(path=path, name=type(exc).__name__, where=where)
        ) from None
    try:
        cfg._validate_source_limits()
    except ValueError as exc:
        raise click.ClickException(str(exc).split("; got", 1)[0]) from None
    _apply_logging_settings(cfg)
    return cfg


def _join_repeats(
    ctx: click.Context, param: click.Parameter, values: tuple[str, ...]
) -> str | None:
    return ",".join(values) if values else None


def comma_option(*param_decls: str, **attrs):
    """A comma-separated option that also accepts repeats: `--only a --only b`.

    A plain option keeps only its last occurrence, so a repeated flag used to
    narrow a scope to the final value without a word; here the occurrences are
    joined and every comma-separated form means the same thing.
    """
    default = attrs.pop("default", None)
    attrs["default"] = (default,) if default is not None else None
    return click.option(*param_decls, multiple=True, callback=_join_repeats, **attrs)


def comma_values(value: str | None, flag: str) -> list[str] | None:
    """An explicit empty scope must never become an unscoped network sweep."""
    if value is None:
        return None
    parts = list(dict.fromkeys(part.strip() for part in value.split(",") if part.strip()))
    if not parts:
        raise click.BadParameter(
            _("Give at least one item; an empty string or only commas is not allowed"),
            param_hint=flag,
        )
    return parts


def terminal_formatter(stream) -> RedactedFormatter:
    """The short, translated view for a person at a terminal; the log format otherwise.

    Output that is redirected — a scheduler capturing stderr, CI, a pipe into
    `tee` — is a log someone reads later, and keeps dates, levels and logger
    names in English like the log file.
    """
    isatty = getattr(stream, "isatty", None)
    try:
        interactive = bool(isatty and isatty())
    except (OSError, ValueError):
        interactive = False
    if interactive:
        from cnequity.terminal_log import TerminalFormatter

        formatter = TerminalFormatter()
        formatter.secrets = configured_log_secrets()
        return formatter
    return diagnostic_formatter()


_CONSOLE = "cne_console"
_FILE = "cne_file"
_QUIET_LOGGERS = ("httpx", "httpcore", "urllib3", "curl_cffi")


def _apply_logging_settings(config) -> None:
    ctx = click.get_current_context(silent=True)
    if ctx is None:
        return
    settings = logging_settings(config)
    ctx.meta["cne.logging.settings"] = settings
    secrets = configured_log_secrets(config)
    ctx.meta["cne.logging.secrets"] = secrets
    root = logging.getLogger()
    owned = [h for h in root.handlers if getattr(h, _CONSOLE, False) or getattr(h, _FILE, False)]
    if not owned:
        return
    quiet = ctx.meta.get("cne.logging.quiet", False)
    for handler in owned:
        if isinstance(handler.formatter, RedactedFormatter):
            handler.formatter.secrets = secrets
        if getattr(handler, _CONSOLE, False):
            handler.setLevel(settings.console_threshold(quiet=quiet))
    _enable_package_logging(owned, settings.heartbeat_seconds)


def _enable_package_logging(handlers, interval: int) -> None:
    from cnequity.progress import start_heartbeat

    threshold = min(h.level for h in handlers)
    logging.getLogger("cnequity").setLevel(threshold)
    start_heartbeat(interval if threshold <= logging.INFO else 0)


def close_command_logging(ctx: click.Context) -> None:
    """Scope our handlers and heartbeat to a CLI invocation, including when embedded."""
    root = logging.getLogger()
    package = logging.getLogger("cnequity")
    original = tuple(root.handlers)
    root_level, package_level = root.level, package.level
    external_levels = {name: logging.getLogger(name).level for name in _QUIET_LOGGERS}

    def close():
        from cnequity.progress import stop_heartbeat

        if ctx.meta.get("cne.logging.owned"):
            stop_heartbeat()
        for handler in root.handlers[:]:
            if handler not in original and (
                getattr(handler, _CONSOLE, False) or getattr(handler, _FILE, False)
            ):
                root.removeHandler(handler)
                handler.close()
        if ctx.meta.get("cne.logging.owned"):
            root.setLevel(root_level)
            package.setLevel(package_level)
            for name, level in external_levels.items():
                logging.getLogger(name).setLevel(level)

    ctx.call_on_close(close)


def _progress_logging(quiet: bool = False, *, take_over: bool = True) -> bool:
    """Install the command's stderr handler without replacing an embedding app's handlers."""
    root = logging.getLogger()
    if not take_over and root.handlers:
        return False
    ctx = click.get_current_context(silent=True)
    settings = ctx.meta.get("cne.logging.settings", DEFAULT_LOGGING) if ctx else DEFAULT_LOGGING
    threshold = settings.console_threshold(quiet=quiet)
    handler = logging.StreamHandler()
    setattr(handler, _CONSOLE, True)
    handler.setLevel(threshold)
    formatter = terminal_formatter(handler.stream)
    if ctx is not None:
        formatter.secrets = ctx.meta.get("cne.logging.secrets", formatter.secrets)
    handler.setFormatter(formatter)
    handler.addFilter(
        lambda record: (
            not (getattr(record, "echoed_by_click", False) or getattr(record, "file_only", False))
        )
    )
    if ctx is None:
        logging.basicConfig(level=threshold, handlers=[handler], force=True)
    else:
        ctx.meta["cne.logging.owned"] = True
        ctx.meta["cne.logging.quiet"] = quiet
        for previous in root.handlers[:]:
            if getattr(previous, _CONSOLE, False):
                root.removeHandler(previous)
                previous.close()
        root.addHandler(handler)
        if not any(h is not handler and not getattr(h, _FILE, False) for h in root.handlers):
            root.setLevel(threshold)
    for noisy in _QUIET_LOGGERS:
        logging.getLogger(noisy).setLevel(logging.WARNING)
    from cnequity.progress import enable_stack_dumps

    enable_stack_dumps()
    _enable_package_logging(
        [handler, *(h for h in root.handlers if getattr(h, _FILE, False))],
        settings.heartbeat_seconds,
    )
    return True


def attach_log_file(cfg, command: str) -> Path | None:
    """Tee this run's log into the lake, and say where.

    Progress on the terminal only helps someone watching it. A run that took
    hours and then failed left nothing to read afterwards and nothing to attach
    to a bug report: `CNE_LOG_DIR` was read by the pipeline shell scripts and by
    nothing in the CLI, so `cne init` run by hand wrote no file at all.

    Failing to open the file is never worth failing the run over — `cne init`
    in particular is what creates the directory tree this would live in.
    """
    ctx = click.get_current_context(silent=True)
    if ctx is not None and "cne.logging.file_path" in ctx.meta:
        return ctx.meta["cne.logging.file_path"]
    env_dir = os.environ.get("CNE_LOG_DIR")
    data_root = getattr(cfg, "data_root", None)
    if not env_dir and data_root is None:
        # Nothing to write into and nothing worth failing over: a caller with
        # no lake root is not running the kind of job this file is for.
        return None
    log_dir = Path(env_dir) if env_dir else Path(data_root) / "logs"
    path = (
        log_dir / f"cne-{command}-{datetime.now(timezone.utc):%Y%m%d-%H%M%S-%f}-{os.getpid()}.log"
    )
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path, encoding="utf-8")
    except OSError as exc:
        logging.getLogger(__name__).warning("no log file at %s: %s", path, exc)
        return None
    settings = logging_settings(cfg)
    handler.setFormatter(diagnostic_formatter(cfg))
    handler.setLevel(settings.file_threshold)
    setattr(handler, _FILE, True)
    root = logging.getLogger()
    root.addHandler(handler)
    _enable_package_logging(
        [handler, *(h for h in root.handlers if getattr(h, _CONSOLE, False))],
        settings.heartbeat_seconds,
    )
    if ctx is not None:
        ctx.meta["cne.logging.file_path"] = path
        ctx.meta["cne.logging.owned"] = True
        from cnequity.progress import current_step, step_scope

        if current_step() is None:
            ctx.find_root().with_resource(step_scope(ctx.command_path))
    launch = os.environ.get("CNE_LAUNCH_ID", "")
    logger = logging.getLogger("cnequity.command")
    logger.info(
        "started %s (task=%s, config=%s)",
        command,
        launch if re.fullmatch(r"[0-9a-f]{32}", launch) else "direct",
        getattr(cfg, "config_path", None) or "built-in",
        extra={"file_only": True},
    )
    click.echo(_("Log written to {path}").format(path=path), err=True)
    return path


def finish_command_logging(ctx: click.Context, elapsed: float, exc: BaseException | None) -> None:
    if "cne.logging.file_path" not in ctx.meta:
        return
    code = (
        exc.code if isinstance(exc, SystemExit) else getattr(exc, "exit_code", int(exc is not None))
    )
    if not isinstance(code, int):
        code = int(code is not None)
    logging.getLogger("cnequity.command").log(
        logging.WARNING if code else logging.INFO,
        "finished (exit=%d, elapsed=%.1fs)",
        code,
        elapsed,
        extra={"file_only": True},
    )


def parse_date_option(value: str | None, flag: str) -> date | None:
    """Parse an ISO date option the way Click reports every other bad input.

    `date.fromisoformat` raises a bare ``ValueError``, and the thirteen call
    sites that used it directly let that reach the operator as a Python
    traceback — for something as ordinary as a typo in ``--start``.
    """
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise click.BadParameter(
            _("{value!r} is not an ISO date (YYYY-MM-DD)").format(value=value), param_hint=flag
        ) from None


def attach_revision_retention(config, payload: dict) -> None:
    """Delete generations beyond ``[storage.retention].keep_generations`` after a publish.

    On by default (keep 5); 0 turns it off. A failure is written onto *payload*
    and stderr; it does not change the publish that already completed.
    """
    from cnequity.storage.revisions import apply_configured_revision_retention

    report = apply_configured_revision_retention(config)
    if report is None:
        return
    payload["revision_retention"] = report
    if report["status"] != "ok":
        click.echo(
            _(
                "Revision retention did not finish: {error}. Published data is unchanged; the next backfill or daily run tries again."
            ).format(error=report["error"]),
            err=True,
        )
        return
    if report["removed"]:
        click.echo(
            _(
                "Kept the latest {keep} generations per dataset, removed {removed} older ones, and released {freed_bytes} bytes that no kept generation shares."
            ).format(
                keep=report["keep"],
                removed=report["removed"],
                freed_bytes=report["freed_bytes"],
            ),
            err=True,
        )


def _run_status_exit_code(status: str, reason: str | None = None) -> int:
    """Coverage limitations remain visible without failing usable execution."""
    from cnequity.orchestrator.outcomes import execution_exit_code

    return execution_exit_code(status, reason)


STAR_URL = "https://github.com/rootSunc/CNEquity"


def star_hint_once(cfg) -> None:
    """After a lake's first successful build, ask for a star — once, on a terminal.

    A marker under ``meta/state`` keeps it to one line per lake; logs, pipes
    and scheduled runs never see it.
    """
    import sys

    if not sys.stderr.isatty():
        return
    marker = Path(cfg.meta_root) / "star-hint-shown"
    if marker.exists():
        return
    try:
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()
    except OSError:
        return
    click.echo(
        _(
            "\nFind it useful? A Star at {STAR_URL} helps, or share it with others maintaining A-share data."
        ).format(STAR_URL=STAR_URL),
        err=True,
    )


def emit_json(payload, **dumps_options) -> None:
    """Print a command's JSON result, and hand it to the dashboard job too.

    Prints ``json.dumps(payload, **dumps_options)`` exactly as the command did.
    A dashboard job sets ``CNE_RESULT_PATH``; the same payload is then written
    there, so the job reads the result instead of scraping the last JSON object
    out of everything the command printed. The last call wins, as the last
    printed object did.
    """
    import json
    import os

    click.echo(json.dumps(payload, **dumps_options))
    target = os.environ.get("CNE_RESULT_PATH")
    if target:
        from cnequity.storage.atomic import write_json_atomic

        write_json_atomic(Path(target), payload, default=str, ensure_ascii=False)


#: ``--apply`` for commands that preview by default (the repairs, ``import``).
apply_option = click.option(
    "--apply", is_flag=True, help=N_("Run it; without this only the plan is printed.")
)
