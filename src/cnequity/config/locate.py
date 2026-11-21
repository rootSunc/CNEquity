"""Where a lake's config lives when no path is given.

Every entry point — the CLI, ``query.load()``, the MCP server and the OS timer —
asks this module, so they cannot disagree about which lake a bare command means.
The order is:

1. ``--config`` (or ``config=`` in Python), handled by the caller;
2. ``CNE_CONFIG``, likewise handled by the caller;
3. ``configs/cnequity.toml`` under the working directory — a repository
   checkout, or a project that keeps its lake config beside its code;
4. the per-user config: ``$XDG_CONFIG_HOME/cnequity/config.toml`` (default
   ``~/.config/cnequity/config.toml``), or ``%APPDATA%\\cnequity\\config.toml``
   on Windows.

The working-directory candidate used to be the only one, so a cron line without
its ``cd``, an MCP client started from ``/`` or a shell in another directory all
failed with "no config found" while the lake sat one directory away. The per-user
file is what ``cne init`` writes when neither exists, so a pip install works from
anywhere.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

#: Project-local config, relative to the working directory.
PROJECT_CONFIG = Path("configs") / "cnequity.toml"
CONFIG_ENV_VAR = "CNE_CONFIG"


def config_from_environment() -> Path | None:
    """The config named by the process, with normal home-directory expansion."""
    value = os.environ.get(CONFIG_ENV_VAR)
    return Path(value).expanduser() if value else None


def user_config_path() -> Path:
    """The per-user config file, whether or not it exists."""
    if sys.platform == "win32":
        base = os.environ.get("APPDATA")
        root = Path(base) if base else Path.home() / "AppData" / "Roaming"
    else:
        base = os.environ.get("XDG_CONFIG_HOME")
        # XDG says a relative value is invalid and must be ignored.
        root = Path(base) if base and Path(base).is_absolute() else Path.home() / ".config"
    return root / "cnequity" / "config.toml"


def config_candidates() -> tuple[Path, ...]:
    """The implicit locations, in lookup order."""
    return (PROJECT_CONFIG, user_config_path())


def find_config() -> Path | None:
    """The first implicit config that exists, or ``None``."""
    for path in config_candidates():
        if path.is_file():
            return path
    return None


def default_config_path() -> Path:
    """The config a command without ``--config`` uses: found, or where ``cne init`` writes one."""
    return find_config() or user_config_path()


def describe_source(path: Path) -> str:
    """Which implicit candidate *path* is: ``"project"``, ``"user"`` or ``""``."""
    if path == PROJECT_CONFIG:
        return "project"
    if path == user_config_path():
        return "user"
    return ""
