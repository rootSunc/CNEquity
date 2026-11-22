"""Lifecycle checks use only tiny, isolated lakes and local evidence files."""

from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest
from click.testing import CliRunner
from tests.legacy_lake import commit_generation

from cnequity.cli.main import cli
from cnequity.config import Config
from cnequity.storage.lifecycle import LifecycleError, LifecycleStore
from cnequity.storage.revisions import (
    RevisionConsistencyError,
    RevisionStore,
    preview_revision_retention,
)


@pytest.fixture
def lake(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    writer = RevisionStore(cfg.meta_root, cfg.curated_root)
    source = cfg.curated_root / "daily_bars/part.parquet"
    source.parent.mkdir(parents=True)
    for i in range(7):
        pl.DataFrame({"trade_date": [date(2026, 1, 1)], "close": [float(i)]}).write_parquet(source)
        commit_generation(
            writer,
            "daily_bars",
            run_id=f"run-{i}",
            changed_files=[source],
            schema_version=1,
            contract_fingerprint="test",
        )
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    (evidence / "report.json").write_text("{}")
    store = LifecycleStore(cfg.meta_root)
    store.import_manifest({"schema_version": 1, "meta_root": str(store.meta), "holds": {}})
    return cfg, store, writer, evidence


def candidates(store, keep: int = 2) -> list[str]:
    return [o["object_id"] for o in store.inspect(keep=keep)["objects"] if not o["blocked_reasons"]]


def test_preview_deletes_nothing_and_a_hold_keeps_a_version(lake):
    cfg, store, writer, _ = lake
    pointer = writer.current_pointer("daily_bars")
    before = {p: p.read_bytes() for p in cfg.meta_root.rglob("*.parquet")}
    preview = preview_revision_retention(cfg.meta_root, keep=2)
    assert sum(len(item.candidate_revision_ids) for item in preview) == 5
    assert {p: p.read_bytes() for p in before} == before
    assert writer.current_pointer("daily_bars") == pointer
    oid = candidates(store)[0]
    store.hold(oid, "Replay input")
    assert oid not in candidates(store)
    assert writer.current_root("daily_bars", revision=oid.rsplit("/", 1)[-1]).exists()


def test_a_merge_that_changed_no_row_does_not_use_up_a_place(lake):
    """`storage merge-files` publishes a generation whose rows did not change."""
    cfg, store, writer, _ = lake
    source = cfg.curated_root / "daily_bars/part.parquet"
    for content_changed in (False, False):
        commit_generation(
            writer,
            "daily_bars",
            run_id="merge",
            changed_files=[source],
            schema_version=1,
            contract_fingerprint="test",
            metadata={"reason": "merge_files", "content_changed": content_changed},
        )
    report = store.inspect(keep=2)
    kept = sorted(o["revision"] for o in report["objects"] if o["blocked_reasons"])
    # The two newest content generations (6 and 7) and both merges after them.
    assert kept[-4:] == [6, 7, 8, 9]
    assert len([o for o in report["objects"] if not o["blocked_reasons"]]) == 5


@pytest.mark.parametrize("change", ["hold", "pointer", "content"])
def test_changes_invalidate_saved_plan(lake, change):
    cfg, store, writer, evidence = lake
    oid = candidates(store)[0]
    plan = store.plan(selected_ids=[oid], keep=2)
    if change == "hold":
        store.hold(oid, "New dependency")
    elif change == "pointer":
        source = cfg.curated_root / "daily_bars/part.parquet"
        pl.DataFrame({"close": [99.0]}).write_parquet(source)
        commit_generation(
            writer,
            "daily_bars",
            run_id="new",
            changed_files=[source],
            schema_version=1,
            contract_fingerprint="test",
        )
    else:
        rid = oid.rsplit("/", 1)[-1]
        path = cfg.meta_root / "revisions/data/daily_bars" / rid / "part.parquet"
        path.write_bytes(b"changed")
    with pytest.raises(LifecycleError):
        store.purge(plan["plan_id"], maintenance=True, manifest=cfg.manifest_path)
    rid = oid.rsplit("/", 1)[-1]
    assert (cfg.meta_root / "revisions/data/daily_bars" / rid).exists()


def test_import_never_releases_and_a_file_citing_an_id_holds_nothing(lake):
    _, store, _, evidence = lake
    oid = candidates(store)[0]
    store.hold(oid, "Explicit hold")
    registry = store.registry()
    store.import_manifest({**registry, "holds": {}})
    assert oid in store.registry()["holds"]
    cited = candidates(store)[0]
    (evidence / "note.md").write_text(f"see {cited.rsplit('/', 1)[-1]}")
    assert cited in candidates(store)
    with pytest.raises(LifecycleError, match="retired"):
        store.import_manifest({**registry, "reference_roots": [{"path": str(evidence)}]})


def test_a_hold_lapses_after_its_last_day(lake):
    from datetime import timedelta

    _, store, _, _ = lake
    oid = candidates(store)[0]
    store.hold(oid, "Until tomorrow", until=date.today() + timedelta(days=1))
    assert oid not in candidates(store)
    with pytest.raises(LifecycleError, match="past"):
        store.hold(oid, "Already over", until=date.today() - timedelta(days=1))
    value = json.loads(store.registry_path.read_text())
    value["holds"][oid][0]["expires_at"] = (date.today() - timedelta(days=1)).isoformat()
    store.registry_path.write_text(json.dumps(value))
    assert oid in candidates(store)
    store.hold(candidates(store)[-1], "Another")  # a save records the lapsed hold's release
    releases = store.registry()["releases"]
    assert [r["object_id"] for r in releases if r["note"] == "hold expired"] == [oid]


def test_corrupt_registry_or_pointer_fails_closed(lake):
    cfg, store, _, _ = lake
    store.registry_path.write_text("[]")
    with pytest.raises(LifecycleError):
        preview_revision_retention(cfg.meta_root, keep=5)
    store.registry_path.unlink()
    (cfg.meta_root / "revisions/daily_bars/current.json").write_text("{}")
    with pytest.raises(RevisionConsistencyError):
        store.inspect()


def test_orphan_generation_and_legacy_are_blocked(lake):
    cfg, store, _, _ = lake
    orphan = cfg.meta_root / "revisions/data/daily_bars/orphan"
    orphan.mkdir()
    (orphan / "unique.txt").write_text("only copy")
    report = store.inspect()
    unknown = [o for o in report["objects"] if o["revision_id"] in ("legacy", "orphan")]
    assert all("unreceipted_generation" in o["blocked_reasons"] for o in unknown)
    assert not any(oid.endswith("/orphan") for oid in candidates(store, keep=5))
    with pytest.raises(LifecycleError):
        store.plan(selected_ids=["revision/daily_bars/orphan"])


def test_plan_tampering_and_cross_lake_registry_rejected(lake):
    cfg, store, _, _ = lake
    plan = store.plan(selected_ids=candidates(store)[:1])
    path = store.root / "plans" / f"{plan['plan_id']}.json"
    value = json.loads(path.read_text())
    value["keep"] = 1
    path.write_text(json.dumps(value))
    with pytest.raises(LifecycleError, match="modified"):
        store.purge(plan["plan_id"], maintenance=True, manifest=cfg.manifest_path)
    registry = store.registry()
    registry["meta_root"] = "/different/lake"
    store.registry_path.write_text(json.dumps(registry))
    with pytest.raises(LifecycleError, match="identity"):
        store.inspect()


def test_preview_and_plan_work_without_import_and_never_delete(lake):
    cfg, store, _, _ = lake
    store.registry_path.unlink()
    assert preview_revision_retention(cfg.meta_root, keep=2)
    assert store.plan(selected_ids=candidates(store)[:1])["purge_ids"]
    assert store.registry()["holds"] == {}
    assert len(list((cfg.meta_root / "revisions/data/daily_bars").iterdir())) == 8


def test_preview_does_not_create_an_empty_lake(tmp_path):
    meta = tmp_path / "absent/meta"
    assert preview_revision_retention(meta, keep=5) == []
    assert not meta.exists()


def test_cli_plan_previews_with_the_configured_keep(lake, monkeypatch):
    cfg, store, _, _ = lake
    monkeypatch.setattr("cnequity.cli.storage_cmds._cfg", lambda _: cfg)
    runner = CliRunner()
    result = runner.invoke(cli, ["storage", "plan", "--keep", "2"])
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report["keep"] == 2 and len(report["candidate_ids"]) == 5
    result = runner.invoke(cli, ["storage", "plan"])
    report = json.loads(result.stdout)
    assert report["keep"] == cfg.revision_keep_generations == 5
    assert len(report["candidate_ids"]) == 2
    result = runner.invoke(cli, ["storage", "plan", "--phase", "mark"])
    assert result.exit_code == 0
    assert "no longer used" in result.stderr
    assert len(list((cfg.meta_root / "revisions/data/daily_bars").iterdir())) == 8
    assert not list((store.root / "plans").glob("*.json")), "a preview saves no plan"


def test_clean_without_import_is_preview_only(lake, monkeypatch):
    cfg, store, _, _ = lake
    store.registry_path.unlink()
    monkeypatch.setattr("cnequity.cli.maintain_cmds._cfg", lambda _: cfg)
    result = CliRunner().invoke(cli, ["run", "clean"])
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report["dry_run"] is True
    assert report["bytes_freed"] == 0
    assert not store.registry_path.exists()
