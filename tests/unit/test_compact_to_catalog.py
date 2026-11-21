"""Compaction publishing into the lake catalog matches compaction into generations."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import polars as pl
import pytest
from tests.catalog_seed import record_revisions
from tests.legacy_lake import publish_generation

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.steps.finalize import step_compact
from cnequity.storage import StagingWriter
from cnequity.storage.catalog_bridge import lake_root, sync_all
from cnequity.storage.revisions import RevisionStore
from equitylake_core.catalog import Catalog
from equitylake_core.extensions import ExtensionUnavailableError

FIRST, SECOND = date(2024, 6, 27), date(2024, 7, 1)
_PROV = {"source": "eastmoney", "data_version": "v1", "fetched_at": "2024-07-01T00:00:00+00:00"}


def _flows(day: date, inflows: dict[str, float]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "symbol": symbol,
                "trade_date": day,
                "main_net_inflow": value,
                "super_large_net_inflow": 0.0,
                "large_net_inflow": 0.0,
                "medium_net_inflow": 0.0,
                "small_net_inflow": 0.0,
                **_PROV,
            }
            for symbol, value in inflows.items()
        ]
    )


def _margins(day: date, balances: dict[str, float]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "symbol": symbol,
                "trade_date": day,
                "margin_balance": value,
                "margin_buy": 1.0,
                "short_balance": 0.0,
                "short_sell_volume": 0.0,
                **_PROV,
            }
            for symbol, value in balances.items()
        ]
    )


def _run(cfg: Config, run_id: str, day: date, flows, margins) -> dict:
    writer = StagingWriter(cfg.staging_root)
    writer.write_batch("fund_flow", run_id, "batch-0", flows)
    writer.write_batch("margin_trading", run_id, "batch-0", margins)
    return step_compact(cfg, day, run_id, {})


def _legacy_run(cfg: Config, run_id: str, day: date, flows, margins) -> None:
    """What a compaction published before 0.25.0: a generation of each dataset."""
    del day
    publish_generation(cfg, "fund_flow", flows, run_id=run_id)
    publish_generation(cfg, "margin_trading", margins, run_id=run_id)


def _lake(root, mode: str) -> Config:
    """A lake that published its first run on the catalog (``fresh``) or before (``catalog``)."""
    cfg = Config(data_root=root / mode)
    (_legacy_run if mode == "catalog" else _run)(
        cfg,
        "r1",
        FIRST,
        _flows(FIRST, {"600519.SH": 1.0, "000001.SZ": 2.0}),
        _margins(FIRST, {"600519.SH": 10.0}),
    )
    if mode == "catalog":
        try:
            with Catalog(lake_root(cfg), create=True) as lake:
                sync_all(cfg, lake)
            record_revisions(cfg)
        except ExtensionUnavailableError as exc:
            pytest.skip(str(exc))
        cfg = replace(cfg, storage_publish="catalog")
    # A new month for both, and a row restated for a day the first run published.
    _run(
        cfg,
        "r2",
        SECOND,
        pl.concat(
            [
                _flows(SECOND, {"600519.SH": 3.0}),
                _flows(FIRST, {"000001.SZ": 2.5}),
            ]
        ),
        _margins(SECOND, {"600519.SH": 11.0, "000001.SZ": 5.0}),
    )
    return cfg


def test_an_upgraded_lake_compacts_as_a_new_one_does(tmp_path, monkeypatch):
    from cnequity.query import catalog_read, load

    generation = _lake(tmp_path, "fresh")
    catalog = _lake(tmp_path, "catalog")
    catalog_read.close_all()

    for dataset in ("fund_flow", "margin_trading"):
        expected = load(dataset, config=generation).drop("fetched_at")
        got = load(dataset, config=catalog).drop("fetched_at")
        assert got.equals(expected), dataset
        pointer = RevisionStore(catalog.meta_root, catalog.curated_root).current_pointer(dataset)
        assert pointer["storage"] == "catalog" and pointer["revision"] == 2
        # Nothing was written to the working tree or a generation.
        assert not (catalog.meta_root / pointer["generation_path"]).exists()
    with Catalog(lake_root(catalog), read_only=True) as lake:
        snapshots = lake.execute("SELECT DISTINCT snapshot_id FROM _revisions").fetchall()
    assert len(snapshots) == 1, "both datasets of the run publish in one snapshot"
    # The run's checkout is gone once it published.
    assert not (catalog.staging_root / "_checkout" / "r2").exists()


def _gated_lake(root) -> Config:
    """A catalog lake holding the first run, with the publication gate blocking."""
    cfg = Config(data_root=root / "gated")
    _legacy_run(
        cfg,
        "r1",
        FIRST,
        _flows(FIRST, {"600519.SH": 1.0, "000001.SZ": 2.0}),
        _margins(FIRST, {"600519.SH": 10.0}),
    )
    try:
        with Catalog(lake_root(cfg), create=True) as lake:
            sync_all(cfg, lake)
        record_revisions(cfg)
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))
    return replace(cfg, storage_publish="catalog", publication_gate="block")


def test_the_publication_gate_audits_a_rehearsed_catalog_publish(tmp_path, monkeypatch):
    from cnequity.query import catalog_read, load

    cfg = _gated_lake(tmp_path)
    audited = []

    def errors(view, day, scope=None):
        # The audit reads the view like any lake: no special path for candidates.
        flows = load("fund_flow", config=view)
        margins = load("margin_trading", config=view)
        audited.append((flows.height, margins.height))
        if (flows["main_net_inflow"] == -999.0).any():
            return [{"dataset": "fund_flow", "severity": "error", "check": "bad_flow"}]
        return []

    monkeypatch.setattr("cnequity.quality.publication._errors", errors)
    with Catalog(lake_root(cfg), read_only=True) as lake:
        before = lake.snapshot()
    result = _run(
        cfg,
        "r2",
        SECOND,
        _flows(SECOND, {"600519.SH": -999.0}),
        _margins(SECOND, {"600519.SH": 11.0}),
    )
    catalog_read.close_all()

    # Baseline, both candidates, then each candidate removed in turn.
    assert audited == [(2, 1), (3, 2), (2, 2), (3, 1)]
    skipped = result["context_updates"]["compact_skipped_datasets"]
    assert [item["dataset"] for item in skipped] == ["fund_flow"]
    # The blocked dataset is as it was; the other one published.
    assert load("fund_flow", config=cfg).height == 2
    assert load("margin_trading", config=cfg).height == 2
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    assert store.current_pointer("fund_flow").get("storage") != "catalog"
    assert store.current_pointer("margin_trading")["storage"] == "catalog"
    # The rehearsals left nothing in the real catalog: one publish, one snapshot.
    with Catalog(lake_root(cfg), read_only=True) as lake:
        assert lake.snapshot() == before + 1
        assert lake.execute("SELECT dataset FROM _revisions").fetchall() == [("margin_trading",)]


def test_a_clean_rehearsal_lets_the_catalog_publish_through(tmp_path, monkeypatch):
    from cnequity.query import catalog_read, load

    cfg = _gated_lake(tmp_path)
    _run(
        cfg,
        "r2",
        SECOND,
        _flows(SECOND, {"600519.SH": 3.0}),
        _margins(SECOND, {"600519.SH": 11.0}),
    )
    catalog_read.close_all()
    assert load("fund_flow", config=cfg).height == 3
    report = next((cfg.meta_root / "quality" / "publication").glob("r2.json")).read_text()
    assert '"blocked": false' in report and "candidate_audit_failed" not in report


def test_compact_publishes_coverage_and_resume_units_in_the_same_snapshot(tmp_path):
    from datetime import datetime, timezone

    from cnequity.quality.planned_coverage import answer_finding
    from cnequity.storage.resume_units import finding

    cfg = Config(data_root=tmp_path / "lake")
    writer = StagingWriter(cfg.staging_root)
    writer.write_batch("fund_flow", "records", "batch-0", _flows(FIRST, {"600519.SH": 1.0}))
    findings = [
        answer_finding("fund_flow", [SECOND], datetime(2024, 7, 1, tzinfo=timezone.utc)),
        finding("fund_flow", ["one"], "test-scope"),
    ]
    step_compact(cfg, FIRST, "records", {"audit_findings": findings})
    with Catalog(lake_root(cfg), read_only=True) as lake:
        revision = lake.execute("SELECT snapshot_id FROM _revisions").fetchone()
        assert lake.execute("SELECT snapshot_id FROM _attempts").fetchone() == revision
        assert lake.execute("SELECT snapshot_id FROM _resume_units").fetchone() == revision


def test_compact_can_prepare_alongside_a_writer_and_rejects_a_moved_base(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    from tests.catalog_seed import publish_rows

    from cnequity.storage.lake_write import InputChanged
    from equitylake_core.lock import holds_lake_lock

    cfg = Config(data_root=tmp_path)
    publish_rows(cfg, "fund_flow", _flows(FIRST, {"600519.SH": 1.0}), run_id="seed")
    writer = StagingWriter(cfg.staging_root)
    writer.write_batch("fund_flow", "slow", "batch-0", _flows(SECOND, {"600519.SH": 2.0}))

    def compare_while_another_writer_commits(*args, **kwargs):
        assert not holds_lake_lock(lake_root(cfg))
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                publish_rows, cfg, "fund_flow", _flows(FIRST, {"600519.SH": 3.0}), run_id="fast"
            )
            assert future.result(timeout=20) is not None
        return {"blocked": False, "new_errors": []}

    monkeypatch.setattr(
        "cnequity.quality.publication.evaluate_publication", compare_while_another_writer_commits
    )
    with pytest.raises(InputChanged, match="fund_flow"):
        step_compact(cfg, SECOND, "slow", {})
    with Catalog(lake_root(cfg), read_only=True) as catalog:
        assert catalog.execute("SELECT main_net_inflow FROM fund_flow").fetchall() == [(3.0,)]
