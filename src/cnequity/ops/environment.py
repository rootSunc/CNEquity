"""The dashboard's UTF-8 log contract must not depend on the host locale."""

from __future__ import annotations

import os


def command_environment() -> dict[str, str]:
    env = os.environ.copy()
    env.update(PYTHONUTF8="1", PYTHONIOENCODING="utf-8", PYTHONUNBUFFERED="1")
    return env
