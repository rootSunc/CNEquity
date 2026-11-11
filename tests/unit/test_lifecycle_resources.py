import os
from datetime import datetime, timedelta, timezone

import pytest

from cnequity.config import Config
from cnequity.storage.lifecycle import LifecycleError, LifecycleStore, reference_fingerprint
from cnequity.storage.source_snapshots import clean_source_snapshots
from cnequity.storage.staging_cleanup import clean_run_logs, preview_staging_cleanup


@pytest.fixture
def protected(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    store = LifecycleStore(cfg.meta_root)
    roots = [{"path": str(evidence)}]
    store.import_manifest(
        {
            "schema_version": 1,
            "meta_root": str(store.meta),
            "holds": {},
            "reference_roots": roots,
            "reference_fingerprint": reference_fingerprint(roots),
        }
    )
    return cfg, store, evidence


def age(path):
    old = (datetime.now(timezone.utc) - timedelta(days=90)).timestamp()
    os.utime(path, (old, old))


def test_hold_protects_orphan_staging_even_with_force(protected):
    cfg, store, _ = protected
    path = cfg.staging_root / "daily_bars/run_id=example"
    path.mkdir(parents=True)
    (path / "candidate.parquet").write_bytes(b"unique candidate")
    age(path)
    store.hold("staging/example", "repair input")
    result = preview_staging_cleanup(cfg, force=True)
    assert result.skipped_run_ids == ["example"]
    assert path.is_dir()


def test_log_hold_and_unlink_failure_are_not_reported_as_deletion(protected, monkeypatch):
    cfg, store, _ = protected
    path = cfg.data_root / "logs/cne-example.log"
    path.parent.mkdir(parents=True)
    path.write_text("diagnostic")
    age(path)
    store.hold("log/cne-example.log", "repair diagnostics")
    assert clean_run_logs(cfg.data_root).kept == 1
    free = path.with_name("cne-free.log")
    free.write_text("other")
    age(free)
    original = type(free).unlink

    def fail(target, *args, **kwargs):
        if target == free:
            raise PermissionError("test refusal")
        return original(target, *args, **kwargs)

    monkeypatch.setattr(type(free), "unlink", fail)
    with pytest.raises(PermissionError, match="test refusal"):
        clean_run_logs(cfg.data_root)
    assert free.exists() and path.exists()


def test_source_snapshot_hold_preserves_old_version(protected):
    cfg, store, _ = protected
    relative = "daily_bars/source=mock/data_version=v1/run_id=old"
    old = cfg.meta_root / "source_snapshots" / relative
    old.mkdir(parents=True)
    (old / "input.parquet").write_bytes(b"input")
    age(old)
    newer = old.with_name("run_id=new")
    newer.mkdir()
    (newer / "input.parquet").write_bytes(b"new")
    store.hold(f"source_snapshot/{relative}", "comparison baseline")
    result = clean_source_snapshots(cfg.meta_root)
    assert relative in result.kept_run_dirs
    assert old.exists()


@pytest.mark.parametrize("kind", ["staging", "source_snapshot", "log"])
def test_new_evidence_blocks_each_cleaner(protected, kind):
    cfg, _, evidence = protected
    (evidence / "new.json").write_text("{}")
    with pytest.raises(LifecycleError, match="References changed"):
        if kind == "staging":
            preview_staging_cleanup(cfg)
        elif kind == "log":
            clean_run_logs(cfg.data_root)
        else:
            clean_source_snapshots(cfg.meta_root)


def test_a_run_rewriting_lake_state_leaves_the_meta_import_valid(tmp_path):
    """The import a real lake used scanned all of `meta`; one run broke it."""
    cfg = Config(data_root=tmp_path / "lake")
    store = LifecycleStore(cfg.meta_root)
    (store.meta / "decisions").mkdir(parents=True)
    (store.meta / "decisions/kept.json").write_text("{}")
    (store.meta / "state").mkdir()
    (store.meta / "state/daily_bars.json").write_text('{"revision": 1}')
    roots = [{"path": str(store.meta)}]
    store.import_manifest(
        {
            "schema_version": 1,
            "meta_root": str(store.meta),
            "holds": {},
            "reference_roots": roots,
            "reference_fingerprint": reference_fingerprint(roots, meta_root=store.meta),
        }
    )
    (store.meta / "state/daily_bars.json").write_text('{"revision": 2}')
    (store.meta / "quality/findings").mkdir(parents=True)
    (store.meta / "quality/findings/run.json").write_text("{}")
    for directory in (
        "adj_factor_arbitration_cache",
        "derivations/minute_bars_30m",
        "payment_date_repairs",
        "repairs/etf_prelisting_debts",
        "stats",
    ):
        root = store.meta / directory
        root.mkdir(parents=True)
        (root / "run.json").write_text("{}")
    preview_staging_cleanup(cfg)

    (store.meta / "decisions/new.json").write_text("{}")
    with pytest.raises(LifecycleError, match="References changed"):
        preview_staging_cleanup(cfg)


def test_hold_cannot_target_outside_owned_namespace(protected):
    _, store, _ = protected
    with pytest.raises(LifecycleError, match="Unknown"):
        store.hold("log/../secret", "invalid")
