"""Config resolution and the small pieces every command group needs.

`config_option` exists because `--config` was declared 34 separate times, each
free to drift in default or help text. One decorator makes the contract single.
"""

from __future__ import annotations

import logging
from datetime import date

import click

from cnequity.config import load_config

USER_CONFIG = "configs/cnequity.toml"
EXAMPLE_CONFIG = "configs/cnequity.example.toml"
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
    from pathlib import Path

    path = Path(config_path)
    if config_path == USER_CONFIG and not path.exists():
        raise click.ClickException(
            f"Config not found: {USER_CONFIG}. "
            "Run `cne config init` to write one from the packaged example "
            f"(or copy {EXAMPLE_CONFIG} if you have the repo checkout)."
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
    """
    logging.basicConfig(
        level=logging.WARNING if quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    for noisy in ("httpx", "httpcore", "urllib3", "curl_cffi"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


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
