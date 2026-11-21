"""A planner-only backfill resumes from units recorded in the lake, not in ``meta/state``."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.run_scope import set_backfill
from cnequity.steps import fundamentals as fund
from cnequity.storage import resume_units
from cnequity.storage.state import StateStore


@pytest.fixture
def cfg(tmp_path):
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core.catalog import Catalog

    c = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    c.staging_root.mkdir(parents=True)
    with Catalog(lake_root(c), create=True):
        pass
    return replace(c, planner_only=frozenset({"top_holders", "financial_statement_items"}))


def _sweep(cfg, monkeypatch, run_id):
    calls = []

    def fetch(start, end, **_kwargs):
        calls.append((start, end))
        return pl.DataFrame({"record_date": [start], "symbol": ["600519.SH"]})

    def write(config, run_id, dataset, frame, *, batch_id, **_kwargs):
        path = config.staging_root / dataset / f"run_id={run_id}" / f"part-{batch_id}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)
        return {"rows_read": frame.height, "rows_written": frame.height}

    monkeypatch.setattr(fund, "write_fetched", write)
    result = fund._run_shareholder_step(
        cfg,
        date(2024, 12, 31),
        run_id,
        "top_holders",
        fetch,
        daily_by="record_date",
        daily_lookback_days=240,
    )
    return result, calls


def test_a_backfill_names_its_units_and_skips_them_once_compact_records_them(cfg, monkeypatch):
    set_backfill(cfg, active=True, start=date(2024, 1, 1), end=date(2024, 12, 31))
    first, calls = _sweep(cfg, monkeypatch, "run-1")
    assert len(calls) == 4
    named = [
        item
        for item in first["context_updates"]["audit_findings"]
        if item["check"] == resume_units.CHECK
    ]
    assert len(named) == 1 and len(named[0]["units"]) == 4
    # Nothing in meta/state.
    assert not StateStore(cfg.meta_root).get_payload("top_holders").get("staged_units")
    # Before compact records them, a new run asks every window again.
    assert len(_sweep(cfg, monkeypatch, "run-2")[1]) == 4
    assert resume_units.record(cfg, "run-1", named, {"top_holders"}) == 4
    assert resume_units.record(cfg, "run-1", named, {"top_holders"}) == 0
    again, calls = _sweep(cfg, monkeypatch, "run-3")
    assert calls == [] and again["windows_reused"] == 4


def test_units_of_a_dataset_compact_did_not_publish_whole_are_not_recorded(cfg):
    item = resume_units.finding("top_holders", ["u1"], "v1:market")
    assert resume_units.record(cfg, "r1", [item], {"financial_statement_items"}) == 0
    assert resume_units.published(cfg, "top_holders", "v1:market") == set()


def test_a_unit_older_than_a_week_is_asked_again(cfg):
    item = resume_units.finding("top_holders", ["u1"], "v1:market")
    resume_units.record(cfg, "r1", [item], {"top_holders"})
    assert resume_units.published(cfg, "top_holders", "v1:market") == {"u1"}
    later = datetime.now(UTC) + timedelta(days=resume_units.TTL_DAYS + 1)
    assert resume_units.published(cfg, "top_holders", "v1:market", now=later) == set()
    # Another scope (a scoped repair) never stands in for it.
    assert resume_units.published(cfg, "top_holders", "v1:symbols=600519.SH") == set()


def test_a_staged_statement_file_names_its_unit():
    name = "part-fsi-2024-03-31-RPT_LICO_FN_CPD-" + "a" * 32 + ".parquet"
    assert fund._fsi_unit_of(name, "fsi-") == "2024-03-31|RPT_LICO_FN_CPD"
    assert fund._fsi_unit_of("part-window-2024-01-01-x.parquet", "fsi-") is None
