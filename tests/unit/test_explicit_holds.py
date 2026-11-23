"""The explicit-holds migration releases holds derived from file citations, and nothing else."""

from __future__ import annotations

from cnequity.config import Config
from cnequity.storage.explicit_holds import release_reference_holds
from cnequity.storage.lifecycle import LifecycleStore


def _registry(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    store = LifecycleStore(cfg.meta_root)
    notes = tmp_path / "private"
    cited, both = "a" * 32, "b" * 32
    store._save(
        {
            "schema_version": 1,
            "lake_id": "lake",
            "meta_root": str(store.meta),
            "holds": {
                f"revision/daily_bars/{cited}": [
                    {
                        "kind": "reference",
                        "source": str(notes / "audits/x.md"),
                        "revision_id": cited,
                    }
                ],
                f"revision/daily_bars/{both}": [
                    {"reason": "rollback"},
                    {
                        "kind": "unresolved_legacy_reference",
                        "source": str(store.meta / "snapshots/old/manifest.json"),
                        "revision_id": both,
                    },
                ],
                "staging/run": [{"reason": "repair input"}],
            },
            "reference_roots": [{"path": str(store.meta)}, {"path": str(notes)}],
            "reference_fingerprint": "old",
        }
    )
    generation = cfg.meta_root / "revisions/data/daily_bars" / cited
    generation.mkdir(parents=True)
    (generation / "part.parquet").write_bytes(b"12345")
    return cfg, store, cited, both


def test_preview_reports_what_loses_protection_and_changes_nothing(tmp_path):
    cfg, store, cited, _ = _registry(tmp_path)
    before = store.registry_path.read_bytes()

    report = release_reference_holds(cfg, apply=False)

    assert not report["applied"]
    assert report["released_objects"] == 2
    assert report["still_held"] == 1
    assert report["unprotected"] == [f"revision/daily_bars/{cited}"]
    assert report["citing_folders"] == {"meta/snapshots": 1, "private/audits": 1}
    assert report["generation_bytes_freed"] == 5
    assert report["catalog_bytes_freed"] is None
    assert store.registry_path.read_bytes() == before


def test_apply_releases_only_derived_holds_logs_them_and_retires_the_roots(tmp_path):
    cfg, store, cited, both = _registry(tmp_path)

    report = release_reference_holds(cfg, apply=True)

    assert report["applied"]
    registry = store.registry()
    assert registry["holds"] == {
        f"revision/daily_bars/{both}": [{"reason": "rollback"}],
        "staging/run": [{"reason": "repair input"}],
    }
    assert "reference_roots" not in registry and "reference_fingerprint" not in registry
    logged = {r["object_id"]: r["dropped"] for r in registry["releases"]}
    assert set(logged) == {f"revision/daily_bars/{cited}", f"revision/daily_bars/{both}"}
    assert all(r["note"].startswith("explicit-holds") for r in registry["releases"])
    again = release_reference_holds(cfg, apply=True)
    assert again["released_objects"] == 0 and not again["applied"]


def test_a_lake_without_a_registry_has_nothing_to_release(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    assert release_reference_holds(cfg, apply=True)["skipped"]


def test_release_reference_holds_reports_clear_path_mismatch_on_copied_lake(tmp_path):
    """A copied lake's lifecycle registry fails fast with both the recorded and current paths."""
    import json

    import pytest

    from cnequity.storage.explicit_holds import release_reference_holds
    from cnequity.storage.lifecycle import LifecycleError

    cfg = Config(data_root=tmp_path / "copied-lake")
    reg_path = cfg.meta_root / "lifecycle" / "registry.json"
    reg_path.parent.mkdir(parents=True, exist_ok=True)
    reg_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "lake_id": "lake-1",
                "meta_root": "/old/original/path/meta",
                "holds": {},
                "cases": [],
                "reference_roots": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(LifecycleError, match="/old/original/path/meta"):
        release_reference_holds(cfg, apply=True)
