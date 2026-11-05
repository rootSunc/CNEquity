"""Ctrl-C mid-write must not leave hidden temp files beside lake files."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.storage import atomic as atomic_mod
from cnequity.storage import state as state_mod


def _interrupt(*_args, **_kwargs):
    raise KeyboardInterrupt


@pytest.mark.parametrize("writer", ["parquet", "json", "state"])
def test_interrupted_write_removes_temp_and_keeps_original(tmp_path, monkeypatch, writer):
    if writer == "state":
        store = state_mod.StateStore(tmp_path)
        store.set_date("bars", date(2025, 9, 30))
        before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
        monkeypatch.setattr(state_mod, "replace_with_retry", _interrupt)
        with pytest.raises(KeyboardInterrupt):
            store.set_date("bars", date(2025, 10, 9))
    else:
        target = tmp_path / ("x.parquet" if writer == "parquet" else "x.json")
        write = (
            (lambda: atomic_mod.write_parquet_atomic(target, pl.DataFrame({"a": [1]})))
            if writer == "parquet"
            else (lambda: atomic_mod.write_json_atomic(target, {"a": 1}))
        )
        write()
        before = {target: target.read_bytes()}
        monkeypatch.setattr(atomic_mod, "replace_with_retry", _interrupt)
        with pytest.raises(KeyboardInterrupt):
            write()

    assert not [p for p in tmp_path.rglob("*.tmp")]
    assert {p: p.read_bytes() for p in before} == before
