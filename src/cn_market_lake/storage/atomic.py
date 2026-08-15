"""Atomic parquet writes for curated/derived paths."""

from __future__ import annotations

import os
import time
from pathlib import Path

import polars as pl

# Windows refuses ``os.replace`` while another handle (DuckDB, Explorer preview,
# AV) holds the destination. A short backoff covers the common "query just
# closed" race; persistent locks still surface as PermissionError.
_REPLACE_ATTEMPTS = 5
_REPLACE_BACKOFF_SEC = 0.05


def write_parquet_atomic(path: Path, df: pl.DataFrame, **kwargs) -> Path:
    """Write *df* to *path* via a same-directory temp file and ``os.replace``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    try:
        df.write_parquet(tmp, **kwargs)
        last_exc: BaseException | None = None
        for attempt in range(_REPLACE_ATTEMPTS):
            try:
                os.replace(tmp, path)
                return path
            except PermissionError as exc:
                last_exc = exc
                if attempt + 1 >= _REPLACE_ATTEMPTS:
                    break
                time.sleep(_REPLACE_BACKOFF_SEC * (2**attempt))
        assert last_exc is not None
        raise last_exc
    except Exception:
        if tmp.exists():
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
        raise
