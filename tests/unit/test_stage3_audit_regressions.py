"""Regression tests for Stage 3 audit findings (P0 & P1 fixes)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import polars as pl
import pytest
from click.testing import CliRunner
from tests.catalog_seed import publish_rows

from cnequity.cli.main import cli
from cnequity.config import Config
from cnequity.domain.schemas import validate_dataframe, with_provenance
from cnequity.ops import preflight
from cnequity.quality import planned_coverage
from cnequity.storage import revision_records
from cnequity.storage.catalog_bridge import apply_retention, lake_root
from cnequity.storage.catalog_migration import require_catalog_lake, upgrade_in_progress
from cnequity.storage.lake_record import LakeMigrationRequired
from cnequity.storage.layout import init_data_layout
from cnequity.storage.lifecycle import LifecycleStore
from cnequity.storage.revisions import apply_configured_revision_retention
from cnequity.storage.snapshots import SnapshotStore
from cnequity.storage.state import StateStore
from equitylake_core import coverage as cov
from equitylake_core.catalog import Catalog, copy_catalog
from equitylake_core.coverage import Coverage
from equitylake_core.plan import Declared, Scope, plan_all
from equitylake_core.raw import RawArchive
from equitylake_core.source import Fetcher, Policy, Request, Response


def _bars(day: date, close: float = 100.0, symbol: str = "600519.SH") -> pl.DataFrame:
    return validate_dataframe(
        with_provenance(
            pl.DataFrame(
                {
                    "symbol": [symbol],
                    "trade_date": [day],
                    "open": [close],
                    "high": [close + 1.0],
                    "low": [close - 1.0],
                    "close": [close],
                    "volume": [1000],
                    "amount": [close * 1000.0],
                }
            ),
            source="tdx_protocol",
            data_version="v1",
        ),
        "daily_bars",
    )


def _instruments(symbol: str = "600519.SH", list_date: date = date(2001, 8, 27)) -> pl.DataFrame:
    return validate_dataframe(
        with_provenance(
            pl.DataFrame(
                {
                    "symbol": [symbol],
                    "name": ["贵州茅台"],
                    "exchange": ["SH"],
                    "asset_type": ["stock"],
                    "list_date": [list_date],
                    "delist_date": [None],
                    "prev_symbol": [None],
                }
            ),
            source="tdx_protocol",
            data_version="v1",
        ),
        "instruments",
    )


def test_get_missing_dates_expands_multiday_ranges(tmp_path):
    store = StateStore(tmp_path / "meta")
    path = tmp_path / "meta" / "state" / "fund_flow.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"missing_ranges": [{"start": "2026-10-12", "end": "2026-10-15"}]}),
        encoding="utf-8",
    )
    assert store.get_missing_dates("fund_flow") == {
        date(2026, 10, 12),
        date(2026, 10, 13),
        date(2026, 10, 14),
        date(2026, 10, 15),
    }


def test_entity_seeds_and_owed_cells_respect_lake_history_start(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    init_data_layout(cfg)
    publish_rows(cfg, "instruments", _instruments("600519.SH", date(2001, 8, 27)), run_id="seed-1")
    publish_rows(cfg, "daily_bars", _bars(date(2026, 10, 16)), run_id="seed-2")

    # Never mark uncollected 2001..2026-10-15 history as settled empty or owed.
    assert planned_coverage.entity_seeds(cfg, date(2026, 10, 16)) == {}
    assert planned_coverage.owed_cells(cfg, "daily_bars", None, date(2026, 10, 16)) == []

    # Explicit [datasets.history_from] overrides the stored floor as requested.
    cfg_explicit = Config(
        data_root=tmp_path / "lake",
        history_from={"daily_bars": date(2026, 10, 14)},
    )
    owed = planned_coverage.owed_cells(cfg_explicit, "daily_bars", None, date(2026, 10, 16))
    assert ("600519.SH", date(2026, 10, 14)) in owed
    assert ("600519.SH", date(2026, 10, 15)) in owed
    assert all(day >= date(2026, 10, 14) for _, day in owed)


def test_apply_retention_batched_leaves_no_snapshot_lock_files(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    init_data_layout(cfg)
    for i in range(40):
        publish_rows(cfg, "daily_bars", _bars(date(2024, 6, 28), close=100.0 + i), run_id=f"r-{i}")

    with Catalog(lake_root(cfg)) as catalog:
        result = apply_retention(catalog, keep=2, apply=True)
    assert result.applied is True
    assert result.snapshots_expired >= 35
    assert result.active_readers is False
    lock_dir = lake_root(cfg) / "snapshot-locks"
    assert not lock_dir.exists() or list(lock_dir.iterdir()) == []


def test_sample_lake_publishes_to_catalog_and_supports_storage_and_mcp(tmp_path):
    data_root = tmp_path / "sample-lake"
    config_out = tmp_path / "sample.toml"
    result = CliRunner().invoke(
        cli,
        [
            "init",
            "--profile",
            "sample",
            "--days",
            "3",
            "--data-root",
            str(data_root),
            "--config-out",
            str(config_out),
        ],
    )
    assert result.exit_code == 0, result.output
    cfg = Config(data_root=data_root)
    assert (data_root / "lake" / "catalog.sqlite").is_file()
    assert not preflight.lake_empty(cfg)

    verify = CliRunner().invoke(cli, ["storage", "verify-catalog", "--config", str(config_out)])
    assert verify.exit_code == 0, verify.output

    inspected = LifecycleStore(cfg.meta_root).inspect()
    datasets = {obj["dataset"] for obj in inspected["objects"]}
    assert datasets == {"instruments", "daily_bars"}


def test_empty_lake_records_answers_and_resume_units_without_crash(tmp_path):
    from cnequity.storage import resume_units

    cfg = Config(data_root=tmp_path / "empty-lake")
    init_data_layout(cfg)
    now = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
    finding = planned_coverage.answer_finding("fund_flow", [date(2026, 10, 16)], now)
    assert planned_coverage.record_answers(cfg, "r1", [finding]) == 1
    assert (
        planned_coverage.record_cell_answers(
            cfg, "daily_bars", [("2026-10-16", "600519.SH")], run="r2"
        )
        == 1
    )
    resume_finding = {
        "check": resume_units.CHECK,
        "dataset": "daily_bars",
        "scope": "2026-10-16",
        "units": ["600519.SH"],
    }
    assert resume_units.record(cfg, "r3", [resume_finding], ["daily_bars"]) == 1


def test_storage_hold_and_automatic_retention_on_catalog_lake(tmp_path):
    cfg = Config(data_root=tmp_path / "lake", revision_keep_generations=2)
    init_data_layout(cfg)
    revs = []
    for i in range(5):
        rev = publish_rows(
            cfg, "daily_bars", _bars(date(2024, 6, 28), close=100.0 + i), run_id=f"r-{i}"
        )
        assert rev is not None
        revs.append(rev.revision_id)

    # Hold works on a fresh catalog lake without importing a reference manifest first.
    held_oid = f"revision/daily_bars/{revs[0]}"
    store = LifecycleStore(cfg.meta_root)
    store.hold(held_oid, "pin baseline")

    inspected = store.inspect(keep=2)
    by_id = {obj["object_id"]: obj for obj in inspected["objects"]}
    assert held_oid in by_id
    assert "explicit_hold" in by_id[held_oid]["blocked_reasons"]

    report = apply_configured_revision_retention(cfg)
    assert report is not None
    assert report["status"] == "ok"
    assert report["catalog"]["snapshots_expired"] >= 2
    assert report["removed"] >= 2

    # Held revision remains readable after retention; unheld old revision is gone.
    after = {obj["object_id"] for obj in store.inspect(keep=2)["objects"]}
    assert held_oid in after
    assert f"revision/daily_bars/{revs[1]}" not in after


def test_snapshot_v2_preserves_attempts_ledger_and_does_not_create_meta_state(tmp_path):
    cfg = Config(data_root=tmp_path / "src-lake")
    init_data_layout(cfg)
    publish_rows(cfg, "daily_bars", _bars(date(2024, 6, 28)), run_id="seed")
    planned_coverage.record_cell_answers(
        cfg, "daily_bars", [("2024-06-27", "600519.SH")], run="step:source_empty"
    )

    snaps = SnapshotStore(cfg, tmp_path / "snapshots")
    snaps.create("snap1", ["daily_bars"])
    assert snaps.verify("snap1").passed is True

    restored_root = tmp_path / "restored-lake"
    snaps.restore("snap1", restored_root)
    restored_cfg = Config(data_root=restored_root)
    revision_records.forget()
    assert not (restored_cfg.meta_root / "state").exists()
    assert planned_coverage.answered_cells(restored_cfg, "daily_bars") == {
        ("2024-06-27", "600519.SH")
    }


def test_interrupted_upgrade_blocks_writes_and_preview_leaves_no_lake_tmp(tmp_path):
    from cnequity.ops import upgrade as upgrade_ops
    from cnequity.storage.lake_record import _write_lake, start_on_catalog

    cfg = Config(data_root=tmp_path / "old-lake")
    cfg.meta_root.mkdir(parents=True, exist_ok=True)
    _write_lake(
        cfg,
        {
            "created_with": "0.24.0",
            "migrations": {
                "lake-catalog": {"status": "applied", "affected": 1},
                "revision-records": {"status": "applied", "affected": 1},
            },
        },
    )
    start_on_catalog(cfg)

    # Preview does not create lake/tmp/.
    assert not (cfg.data_root / "lake" / "tmp").exists()
    upgrade_ops.plan(cfg)
    assert not (cfg.data_root / "lake" / "tmp").exists()

    # Interrupted upgrade refuses writes outside `cne upgrade`, permits them inside.
    with pytest.raises(LakeMigrationRequired, match="attempts-ledger"):
        require_catalog_lake(cfg)
    with upgrade_in_progress():
        require_catalog_lake(cfg)


def test_core_fixes_copy_catalog_plan_all_attempted_source_raw(tmp_path):
    # 1. copy_catalog with `#` and `?` in path
    weird = tmp_path / "lake#1?v=2" / "lake"
    with Catalog(weird, create=True) as cat:
        cov.prepare(cat)
    copied = tmp_path / "copied" / "lake"
    copy_catalog(weird, copied)
    with Catalog(copied, read_only=True) as cat2:
        assert cat2.has_table(cov.ATTEMPTS)

    # 2. plan_all entity-level derived dataset with a `by="write"` input
    class _Cal:
        def sessions(self, start, end):
            return [date(2024, 6, 28)]

    class _Uni:
        def members(self, dataset, start, end):
            return pl.DataFrame(
                {"entity": ["600519.SH"], "first": [date(2020, 1, 1)], "last": [None]},
                schema={"entity": pl.String, "first": pl.Date, "last": pl.Date},
            )

        def excused(self, dataset, start, end):
            return pl.DataFrame(schema={"period": pl.String, "entity": pl.String})

    with Catalog(tmp_path / "plan-lake", create=True) as cat:
        with cat.transaction():
            cat.execute("CREATE TABLE lake.main.instruments AS SELECT DATE '2024-01-02' AS d")
        declared = {
            "instruments": Declared(Coverage("session", by="write"), "d"),
            "derived_entity": Declared(
                Coverage("session", entity="symbol", inputs=("instruments",)), "trade_date"
            ),
        }
        plans = plan_all(
            cat,
            declared,
            Scope(date(2024, 6, 28), date(2024, 6, 28)),
            calendar=_Cal(),
            universe=_Uni(),
        )
        assert plans["derived_entity"].cells.height == 1

    # 3. Fetcher.fetch appends `&` when URL already has `?`
    seen_urls: list[str] = []

    def transport(url, headers, timeout):
        seen_urls.append(url)
        return Response(200, b"ok")

    fetcher = Fetcher({"s": Policy("s")}, transport=transport, sleep=lambda _: None)
    fetcher.fetch(Request("s", "k", "https://example.test/api?foo=1", {"bar": "2"}))
    assert seen_urls == ["https://example.test/api?foo=1&bar=2"]

    # 4. RawArchive writes atomically and appends index under lock
    archive = RawArchive(tmp_path / "raw")
    stored = archive.put("s", "k1", "https://example.test/a", {}, 200, b"hello")
    assert stored.is_file()
    entries = list(archive.entries("s"))
    assert len(entries) == 1 and entries[0]["key"] == "k1"


def test_capture_derive_inputs_uses_explicit_trade_date(tmp_path, monkeypatch):
    from cnequity.steps import finalize

    cfg = Config(data_root=tmp_path / "lake")
    init_data_layout(cfg)
    seen: list[date | None] = []

    def fake_owes(config, dataset, trade_date=None):
        seen.append(trade_date)
        return False

    monkeypatch.setattr(finalize, "_planner_owes", fake_owes)
    target = date(2024, 6, 28)
    finalize.capture_derive_inputs(cfg, "adj_factors", trade_date=target)
    assert seen and all(day == target for day in seen)


def test_core_registry_and_open_lake_unify_cnequity_onto_equitylake_core(tmp_path):
    from cnequity.domain.core_registry import core_registry, core_spec
    from cnequity.domain.datasets import DATASETS
    from cnequity.storage.catalog_bridge import open_lake
    from equitylake_core.spec import DatasetSpec

    reg = core_registry()
    assert set(reg.names()) == set(DATASETS)
    assert core_spec("daily_bars").partition == "month"
    assert core_spec("financial_statement_items").partition in ("quarter", "none")

    # Quarter partition label & Hive-style col=label parsing in equitylake_core.spec.
    q_spec = DatasetSpec(
        name="qtest",
        columns={"report_date": "DATE", "symbol": "VARCHAR"},
        key=("report_date", "symbol"),
        time="report_date",
        partition="quarter",
    )
    assert q_spec.label(date(2024, 5, 15)) == "2024Q2"
    assert q_spec.period("2024Q2") == (date(2024, 4, 1), date(2024, 6, 30))
    assert q_spec.period("report_date=2024Q2") == (date(2024, 4, 1), date(2024, 6, 30))
    assert core_spec("daily_bars").period("trade_date=2024-06") == (
        date(2024, 6, 1),
        date(2024, 6, 30),
    )

    cfg = Config(data_root=tmp_path / "unified-lake")
    init_data_layout(cfg)
    publish_rows(cfg, "daily_bars", _bars(date(2024, 6, 28)), run_id="u1")

    lake = open_lake(cfg)
    loaded = lake.load("daily_bars", start=date(2024, 6, 1), end=date(2024, 6, 30))
    assert loaded.height == 1
    assert loaded["symbol"].to_list() == ["600519.SH"]
    revs = lake.revisions("daily_bars")
    assert len(revs) == 1 and revs[0]["run"] == "u1"
    summaries = {item.dataset: item for item in lake.summary()}
    assert summaries["daily_bars"].rows == 1


def test_redo_all_overrides_existing_dates_fn_in_walk_day_backfill(tmp_path):
    from cnequity.run_scope import set_backfill
    from cnequity.steps.common import walk_day_backfill

    cfg = Config(data_root=tmp_path / "redo-lake")
    init_data_layout(cfg)
    day = date(2024, 6, 28)

    asked_gaps: list[date] = []
    set_backfill(cfg, active=True, start=day, end=day, redo="gaps")
    walk_day_backfill(
        cfg,
        day,
        "r-gaps",
        "market_breadth",
        lambda d: (asked_gaps.append(d), pl.DataFrame())[1],
        source="eastmoney",
        date_col="trade_date",
        existing_dates_fn=lambda days: set(days),
    )
    assert asked_gaps == []

    asked_all: list[date] = []
    set_backfill(cfg, active=True, start=day, end=day, redo="all")
    walk_day_backfill(
        cfg,
        day,
        "r-all",
        "market_breadth",
        lambda d: (asked_all.append(d), pl.DataFrame())[1],
        source="eastmoney",
        date_col="trade_date",
        allow_empty_days=True,
        existing_dates_fn=lambda days: set(days),
    )
    assert asked_all == [day]


def test_redo_modes_respected_by_daily_bars_sector_bars_and_trading_status(tmp_path, monkeypatch):
    from cnequity.run_scope import set_backfill
    from cnequity.steps import reference, rotation

    cfg = Config(data_root=tmp_path / "redo-steps")
    init_data_layout(cfg)
    day = date(2024, 6, 28)

    # 1. sector_bars respects redo="all" by clearing completed skip set
    seen_skip: list[set[str]] = []
    monkeypatch.setattr(rotation, "_sector_bars_completed", lambda *a, **k: {"BK0001"})
    monkeypatch.setattr(rotation, "clear_sector_bars_backfill_state", lambda *a, **k: None)
    monkeypatch.setattr(
        rotation,
        "_sweep_sector_bars",
        lambda c, run_id, *, start, end, skip_sectors, mark_completed: (
            seen_skip.append(set(skip_sectors)),
            {"rows_read": 0, "rows_written": 0},
        )[1],
    )
    set_backfill(cfg, active=True, start=day, end=day, redo="gaps")
    rotation._backfill_sector_bars(cfg, day, "r1")
    set_backfill(cfg, active=True, start=day, end=day, redo="all")
    rotation._backfill_sector_bars(cfg, day, "r2")
    assert seen_skip == [{"BK0001"}, set()]

    # 2. trading_status ST backfill respects redo="empty" and redo="all"
    fetched_batches: list[list[str]] = []
    monkeypatch.setattr(
        reference,
        "load_st_checkpoint",
        lambda c, scope: {
            "completed_symbols": ["600000.SH", "600001.SH"],
            "evidence_rows_by_symbol": {"600000.SH": 5, "600001.SH": 0},
            "unresolved_symbols": [],
        },
    )
    monkeypatch.setattr(
        reference,
        "reusable_st_checkpoint_symbols",
        lambda c, ckpt, run_id: {"600000.SH", "600001.SH"},
    )
    monkeypatch.setattr(reference, "write_st_checkpoint", lambda *a, **k: None)
    monkeypatch.setattr(
        "cnequity.adapters.baostock.st_history.fetch_st_history",
        lambda batch, start, end, **kw: (fetched_batches.append(list(batch)), (pl.DataFrame(), []))[
            1
        ],
    )
    set_backfill(cfg, active=True, start=day, end=day, redo="empty")
    reference._backfill_trading_status_st_source(
        cfg,
        day,
        "r3",
        universe=["600000.SH", "600001.SH"],
        universe_name="all_a",
        source="baostock",
    )
    set_backfill(cfg, active=True, start=day, end=day, redo="all")
    reference._backfill_trading_status_st_source(
        cfg,
        day,
        "r4",
        universe=["600000.SH", "600001.SH"],
        universe_name="all_a",
        source="baostock",
    )
    assert fetched_batches == [["600001.SH"], ["600000.SH", "600001.SH"]]
