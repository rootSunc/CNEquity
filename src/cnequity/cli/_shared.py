"""Config resolution and the small pieces every command group needs.

`config_option` exists because `--config` was declared 34 separate times, each
free to drift in default or help text. One decorator makes the contract single.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from pathlib import Path

import click

from cnequity.config import load_config

USER_CONFIG = "configs/cnequity.toml"
EXAMPLE_CONFIG = "configs/cnequity.example.toml"
DEMO_CONFIG = "configs/cnequity.demo.toml"
DEFAULT_CONFIG = USER_CONFIG


def config_option(func):
    """Attach the standard `--config` option.

    Equivalent to the hand-written option it replaces, including the parameter
    name (`config_path`) every command body already reads.
    """
    return click.option(
        "--config",
        "config_path",
        default=DEFAULT_CONFIG,
        show_default=True,
    )(func)


def resolve_config_path(config_path: str):
    path = Path(config_path)
    if config_path == USER_CONFIG and not path.exists():
        # `cne init --profile demo` writes the demo config, not the user one, so every
        # command it points at afterwards ("if this fails, run `cne sources
        # probe ...`") used to die here on a second, unrelated error — at
        # exactly the moment the user was already trying to recover. Name the
        # config that does exist rather than falling back to it silently:
        # `cne init` against a demo data_root is destructive enough that the
        # choice has to stay the user's.
        hint = ""
        if Path(DEMO_CONFIG).exists():
            hint = (
                f"\nFound {DEMO_CONFIG} from `cne init --profile demo` — to act on "
                "the demo lake, "
                f"add `--config {DEMO_CONFIG}`."
            )
        raise click.ClickException(
            f"Config not found: {USER_CONFIG}. "
            "Run `cne config create` to write one from the packaged example "
            f"(or copy {EXAMPLE_CONFIG} if you have the repo checkout)."
            f"{hint}"
        )
    if not path.exists():
        raise click.ClickException(f"Config not found: {path}")
    return path


def _cfg(config: str):
    return load_config(resolve_config_path(config))


def _progress_logging(quiet: bool = False) -> None:
    """Send the pipeline's own INFO records to the terminal.

    Long fetches were silent until they finished: `cne init` runs for hours and
    printed nothing until the closing JSON, which is indistinguishable from
    hung — and a process that looks hung gets killed. The steps and the worker
    pool already log their progress; nothing was listening.

    Third-party loggers stay at WARNING. httpx logs a line per request, which
    on a full-market sweep is hundreds of thousands of lines and buries exactly
    the progress this exists to surface.

    Per-step progress still leaves gaps — a step reports a batch only once the
    whole batch lands — so a heartbeat names whatever is running whenever the
    log goes quiet. It is started here, after `force=True` has replaced the
    root handlers the heartbeat needs to watch.
    """
    logging.basicConfig(
        level=logging.WARNING if quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    for noisy in ("httpx", "httpcore", "urllib3", "curl_cffi"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    if not quiet:
        from cnequity.progress import start_heartbeat

        start_heartbeat()


def attach_log_file(cfg, command: str, *, quiet: bool = False) -> Path | None:
    """Tee this run's log into the lake, and say where.

    Progress on the terminal only helps someone watching it. A run that took
    hours and then failed left nothing to read afterwards and nothing to attach
    to a bug report: `CNE_LOG_DIR` was read by the pipeline shell scripts and by
    nothing in the CLI, so `cne init` run by hand wrote no file at all.

    Failing to open the file is never worth failing the run over — `cne init`
    in particular is what creates the directory tree this would live in.
    """
    env_dir = os.environ.get("CNE_LOG_DIR")
    data_root = getattr(cfg, "data_root", None)
    if not env_dir and data_root is None:
        # Nothing to write into and nothing worth failing over: a caller with
        # no lake root is not running the kind of job this file is for.
        return None
    log_dir = Path(env_dir) if env_dir else Path(data_root) / "logs"
    path = log_dir / f"cne-{command}-{datetime.now():%Y%m%d-%H%M%S}.log"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path, encoding="utf-8")
    except OSError as exc:
        logging.getLogger(__name__).warning("no log file at %s: %s", path, exc)
        return None
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    handler.setLevel(logging.WARNING if quiet else logging.INFO)
    logging.getLogger().addHandler(handler)
    if not quiet:
        # The heartbeat watches the root handlers; this is a new one.
        from cnequity.progress import start_heartbeat

        start_heartbeat()
    click.echo(f"Logging to {path}", err=True)
    return path


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
            f"{value!r} is not an ISO date (YYYY-MM-DD)", param_hint=flag
        ) from None


def _run_status_exit_code(status: str) -> int:
    """Map the run contract to scheduler-friendly exit codes.

    0 means all requested work succeeded; 2 means the core spine completed
    but research/advisory work degraded; 1 means a core failure (or another
    terminal failure without a usable result).
    """
    if status in {"success", "skipped_non_trading_day"}:
        return 0
    if status in {"degraded", "warning"}:
        return 2
    return 1
