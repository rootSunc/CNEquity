import shutil
import sqlite3
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from tests.catalog_seed import commit_tree

from cnequity.config import Config
from cnequity.quality.publication import check_repair_publication, evaluate_publication
from cnequity.query.reader import load
from cnequity.steps.finalize import step_compact
from cnequity.storage.parquet import StagingWriter
from cnequity.storage.revisions import RevisionStore


def _gate(cfg, run_id: str, day: date) -> dict:
    """Run the gate on a checkout of daily_bars that changes nothing."""
    from cnequity.storage.lake_write import LakeWrite

    with LakeWrite(cfg, run_id) as write:
        checkout = write.checkout("daily_bars")
        return evaluate_publication(
            cfg, run_id, day, {"daily_bars": checkout.root}, lake_write=write
        )


@pytest.mark.parametrize("backup_fails", [False, True])
def test_manifest_backup_closes_connections(tmp_path, monkeypatch, backup_fails):
    cfg = Config(data_root=tmp_path / "lake", publication_gate="block")
    cfg.meta_root.mkdir(parents=True)
    connection = sqlite3.connect(cfg.manifest_path)
    connection.execute("CREATE TABLE receipt (value INTEGER)")
    connection.execute("INSERT INTO receipt VALUES (42)")
    connection.commit()
    connection.close()
    connections = []
    connect = sqlite3.connect

    class BackupConnection(sqlite3.Connection):
        def backup(self, target, **kwargs):
            if backup_fails:
                raise sqlite3.OperationalError("backup unavailable")
            return super().backup(target, **kwargs)

    def tracked_connect(*args, **kwargs):
        connection = connect(*args, **kwargs, factory=BackupConnection)
        connections.append(connection)
        return connection

    def errors(view, day, scope=None):
        copied = connect(view.manifest_path)
        try:
            assert copied.execute("SELECT value FROM receipt").fetchone() == (42,)
        finally:
            copied.close()
        return []

    monkeypatch.setattr("cnequity.quality.publication.sqlite3.connect", tracked_connect)
    monkeypatch.setattr("cnequity.quality.publication._errors", errors)
    try:
        report = _gate(cfg, "backup", date(2024, 6, 28))
        assert report["blocked"] is backup_fails
        if backup_fails:
            assert report["new_errors"][0]["message"] == "backup unavailable"
        # The manifest's two, and the ones that copy the lake catalog for the view.
        assert len(connections) >= 2
        for connection in connections:
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                connection.execute("SELECT 1")
    finally:
        for connection in connections:
            connection.close()


def _bar(close: float, stamp: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [date(2024, 6, 28)],
            "open": [close],
            "high": [close],
            "low": [close],
            "close": [close],
            "volume": [1000],
            "amount": [close * 1000],
            "source": ["tdx_protocol"],
            "data_version": ["v2"],
            "fetched_at": [stamp],
        }
    )


@pytest.mark.parametrize("mode,expected", [("block", 10.0), ("shadow", 20.0)])
def test_candidate_audit_precedes_pointer_switch(tmp_path, monkeypatch, mode, expected):
    cfg = Config(data_root=tmp_path, publication_gate=mode)
    path = cfg.curated_root / "daily_bars/trade_date=2024-06-28/part.parquet"
    path.parent.mkdir(parents=True)
    _bar(10.0, "2024-06-28T00:00:00Z").write_parquet(path)
    revisions = RevisionStore(cfg.meta_root, cfg.curated_root)
    commit_tree(
        revisions,
        "daily_bars",
        run_id="initial",
        changed_files=[path],
        schema_version=2,
        contract_fingerprint="test",
    )
    old_id = revisions.latest("daily_bars").revision_id
    calls = []

    def errors(view, day, scope=None):
        # Both audits happen before the original lake has been published.
        assert revisions.latest("daily_bars").revision_id == old_id
        close = load("daily_bars", config=view)["close"][0]
        calls.append(close)
        common = [{"dataset": "legacy", "severity": "error", "check": "known_gap"}]
        return common + (
            [{"dataset": "daily_bars", "severity": "error", "check": "bad_correction"}]
            if close == 20
            else []
        )

    monkeypatch.setattr("cnequity.quality.publication._errors", errors)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", "candidate", "one", _bar(20.0, "2024-06-28T01:00:00Z")
    )
    result = step_compact(cfg, date(2024, 6, 28), "candidate", {})
    # One candidate: the finding is attributed to it without a third audit.
    assert calls == [10.0, 20.0]
    assert load("daily_bars", config=cfg)["close"].to_list() == [expected]
    assert Path(result["publication_audit"]).exists()
    if mode == "block":
        assert revisions.latest("daily_bars").revision_id == old_id
        assert (
            result["context_updates"]["compact_skipped_datasets"][0]["reason"] == "publication_gate"
        )
        assert pl.read_parquet(path)["close"].to_list() == [10.0]


def test_a_candidate_that_is_not_a_checkout_is_blocked(tmp_path):
    cfg = Config(data_root=tmp_path / "lake", publication_gate="block", lake_profile="sample")
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    report = evaluate_publication(cfg, "tree", date(2024, 6, 28), {"daily_bars": candidate})
    assert report["blocked"]
    assert report["new_errors"][0]["check"] == "candidate_audit_failed"
    assert "checkout" in report["new_errors"][0]["message"]


def test_audit_failure_cannot_publish_in_block_mode(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path, publication_gate="block")

    def fail(*args):
        raise RuntimeError("audit unavailable")

    monkeypatch.setattr("cnequity.quality.publication._errors", fail)
    report = _gate(cfg, "failure", date(2024, 6, 28))
    assert report["blocked"]
    assert report["new_errors"][0]["check"] == "candidate_audit_failed"


def test_repair_reports_a_new_factor_action_contradiction_without_blocking(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    before, step = date(2024, 6, 27), date(2024, 6, 28)
    for day, factor in ((before, 1.0), (step, 2.0)):
        path = cfg.derived_root / f"adj_factors/trade_date={day}/part.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "trade_date": [day],
                "adjust_type": ["hfq"],
                "factor": [factor],
                "source": ["sina"],
                "data_version": ["v1"],
                "fetched_at": ["2024-06-29T00:00:00Z"],
            }
        ).write_parquet(path)
    action = cfg.curated_root / "corporate_actions/ex_date=2024/part.parquet"
    action.parent.mkdir(parents=True, exist_ok=True)
    original = pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "ex_date": [step],
            "action_type": ["bonus"],
            "source": ["tdx_protocol"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-29T00:00:00Z"],
        }
    )
    original.write_parquet(action)
    revisions = RevisionStore(cfg.meta_root, cfg.curated_root)
    commit_tree(
        revisions,
        "corporate_actions",
        run_id="initial",
        changed_files=[action],
        schema_version=1,
        contract_fingerprint="test",
    )
    old_id = revisions.latest("corporate_actions").revision_id
    monkeypatch.setattr("cnequity.quality.publication._errors", lambda *args: [])
    from cnequity.domain.datasets import granularity_for_dataset
    from cnequity.storage.lake_write import LakeWrite
    from cnequity.storage.parquet import _partition_values

    moved = original.with_columns(pl.lit(before).alias("ex_date"))
    with LakeWrite(cfg, "repair") as write:
        checkout = write.checkout("corporate_actions")
        for frame in (original, moved):
            value = _partition_values(
                frame, "ex_date", granularity_for_dataset("corporate_actions")
            ).item()
            checkout.rewrite(f"ex_date={value}", moved if frame is moved else frame.clear())
        files = [path for found in checkout.changed().values() for path in found]
        report = check_repair_publication(
            cfg,
            "corporate_actions",
            "repair",
            files,
            symbols=frozenset({"600000.SH"}),
            lake_write=write,
        )
    assert not report["blocked"]
    assert report["factor_diagnostics"]["candidate"][0]["severity"] == "warning"
    assert report["factor_diagnostics"]["gate_impact"] == "none"

    assert revisions.latest("corporate_actions").revision_id == old_id
    assert not list((cfg.data_root / "_quarantine").glob("corporate_actions-*"))

    # An empty action candidate still exposes the factor step it would orphan.
    from cnequity.quality.cross_checks import factor_action_contradictions

    without_actions = Config(data_root=tmp_path / "without-actions")
    shutil.copytree(cfg.derived_root / "adj_factors", without_actions.derived_root / "adj_factors")
    contradictions, _ = factor_action_contradictions(
        without_actions, symbols=frozenset({"600000.SH"})
    )
    assert contradictions.select("ex_date", "kind").to_dicts() == [
        {"ex_date": step, "kind": "step_without_action"}
    ]


def test_scoped_repair_detects_action_on_a_flat_factor_series(tmp_path):
    from cnequity.quality.cross_checks import factor_action_contradictions

    cfg = Config(data_root=tmp_path / "lake")
    days = [date(2024, 6, 27), date(2024, 6, 28)]
    for day in days:
        path = cfg.derived_root / f"adj_factors/trade_date={day}/part.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "trade_date": [day],
                "adjust_type": ["hfq"],
                "factor": [1.0],
                "source": ["sina"],
                "data_version": ["v1"],
                "fetched_at": ["2024-06-29T00:00:00Z"],
            }
        ).write_parquet(path)
    path = cfg.curated_root / "corporate_actions/ex_date=2024/part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"symbol": ["600000.SH"], "ex_date": [days[1]], "action_type": ["bonus"]}
    ).write_parquet(path)

    contradictions, _ = factor_action_contradictions(cfg, symbols=frozenset({"600000.SH"}))
    assert contradictions.select("ex_date", "kind").to_dicts() == [
        {"ex_date": days[1], "kind": "action_without_step"}
    ]


def test_publication_blocks_only_the_candidate_that_causes_the_error(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake", publication_gate="block")
    writer = StagingWriter(cfg.staging_root)
    writer.write_batch("daily_bars", "mixed", "bars", _bar(20.0, "2024-06-28T01:00:00Z"))
    writer.write_batch(
        "fund_flow",
        "mixed",
        "flow",
        pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "trade_date": [date(2024, 6, 28)],
                "main_net_inflow": [1.0],
                "super_large_net_inflow": [0.0],
                "large_net_inflow": [0.0],
                "medium_net_inflow": [0.0],
                "small_net_inflow": [0.0],
                "source": ["eastmoney"],
                "data_version": ["v1"],
                "fetched_at": ["2024-06-28T01:00:00Z"],
            }
        ),
    )

    def errors(view, day, scope=None):
        try:
            closes = load("daily_bars", config=view)["close"].to_list()
        except Exception:  # noqa: BLE001 - nothing published or staged yet
            closes = []
        if 20.0 in closes:
            return [{"dataset": "daily_bars", "severity": "error", "check": "bad_bar"}]
        return []

    monkeypatch.setattr("cnequity.quality.publication._errors", errors)
    result = step_compact(cfg, date(2024, 6, 28), "mixed", {})

    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    assert store.current_pointer("daily_bars") is None
    assert load("fund_flow", config=cfg).height == 1
    assert result["context_updates"]["compact_skipped_datasets"][0]["dataset"] == "daily_bars"


@pytest.mark.parametrize("new,blocked", [({"A": 1}, False), ({"C": 1}, True), ({"A": 3}, True)])
def test_partial_repair_requires_per_key_non_regression(tmp_path, monkeypatch, new, blocked):
    cfg = Config(data_root=tmp_path / "lake", publication_gate="block")

    def finding(violations):
        return {
            "dataset": "daily_bars",
            "check": "pk_unique",
            "severity": "error",
            "message": f"{sum(violations.values())} duplicates",
            "violations": violations,
            "violations_complete": True,
        }

    baseline = [finding({"A": 2, "B": 1})]
    answers = iter([baseline, [finding(new)], baseline])
    monkeypatch.setattr("cnequity.quality.publication._errors", lambda *args: next(answers))
    report = _gate(cfg, "repair", date(2026, 1, 1))
    assert report["blocked"] is blocked


def _bad_rows(count: int, **scope) -> dict:
    return {
        "dataset": "daily_bars",
        "check": "known_bad_rows",
        "severity": "error",
        "message": f"{count} invalid rows",
        "invalid_rows": count,
        **scope,
    }


@pytest.mark.parametrize(
    "candidate,blocked",
    [
        ([_bad_rows(1)], False),  # fewer bad rows: improved, not a new error
        ([_bad_rows(2)], False),  # same issue, reworded message only
        ([_bad_rows(3)], True),  # worse
        ([_bad_rows(2), _bad_rows(1, partition_value="2026-01-02")], True),  # new scope
        ([], False),  # resolved
    ],
)
def test_gate_compares_stable_issue_keys_not_whole_findings(
    tmp_path, monkeypatch, candidate, blocked
):
    cfg = Config(data_root=tmp_path / "lake", publication_gate="block")
    baseline = [{**_bad_rows(2), "message": "2 rows failed invariants", "sample": ["x", "y"]}]
    answers = iter([baseline, candidate, baseline])
    monkeypatch.setattr("cnequity.quality.publication._errors", lambda *args: next(answers))
    report = _gate(cfg, "keys", date(2026, 1, 1))
    assert report["blocked"] is blocked
    changes = report["issue_changes"]
    if candidate == [_bad_rows(1)]:
        assert [item["invalid_rows"] for item in changes["improved"]] == [1.0]
    if not candidate:
        assert len(changes["resolved"]) == 1


def test_incomplete_violation_lists_fall_back_to_counts():
    from cnequity.quality.publication import compare_issue

    old = {"duplicate_rows": 5, "violations": {"A": 5}, "violations_complete": False}
    new = {"duplicate_rows": 4, "violations": {"B": 4}, "violations_complete": False}
    assert compare_issue(old, new) == "improved"
    assert compare_issue(new, old) == "worsened"


def test_a_new_unregistered_source_label_is_a_regression_even_at_equal_count():
    from cnequity.quality.publication import compare_issue

    old = {"dataset": "sources", "check": "unregistered_source", "sources": {"bse": ["daily_bars"]}}
    swapped = {**old, "sources": {"xyz": ["daily_bars"]}}
    spread = {**old, "sources": {"bse": ["daily_bars", "trading_status"]}}
    assert compare_issue(old, swapped) == "worsened"
    assert compare_issue(old, spread) == "worsened"
    assert compare_issue(old, old) == "unchanged"
