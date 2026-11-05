"""The record of Shenwan-classed securities that never listed.

``storage.repairs.sw_never_listed`` writes it when it drops their classes from
``industry_members``; ``cne verify --industry`` reads it so they stay reported.
Reading it needs no repair machinery, so the reader lives here, below both.
"""

from __future__ import annotations

import json
from pathlib import Path

from cnequity.config import Config

RECORD = "sw_never_listed.json"


def _record_path(config: Config) -> Path:
    return config.meta_root / RECORD


def load_never_listed(config: Config) -> list[dict]:
    """The recorded never-listed securities, or ``[]`` before any were found."""
    path = _record_path(config)
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8")).get("securities", [])
