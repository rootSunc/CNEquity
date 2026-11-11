"""Open an SQLite file read-only, whatever characters its path holds.

Read-only access needs a URI (``file:...?mode=ro``), and a path pasted into one
unquoted breaks on ``#`` and ``?``: SQLite reads the rest as a fragment or a
query and opens a different, empty database instead of failing. A lake under
such a directory showed no runs and no history. Every read-only open of the
manifest and the other lake databases goes through here.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


def connect_readonly(path: str | Path, **kwargs) -> sqlite3.Connection:
    """``sqlite3.connect`` on *path* in read-only mode; *kwargs* pass through."""
    return sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, **kwargs)
