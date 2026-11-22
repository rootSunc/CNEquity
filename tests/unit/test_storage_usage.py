"""`cne storage usage` counts what deletion frees, not what file sizes add up to."""

from __future__ import annotations

import json
import os
import sys
from datetime import date

import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config import Config
from cnequity.storage.usage import storage_usage


def _write(path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(os.urandom(size))


def test_a_hard_link_frees_nothing_until_every_link_is_in_one_part(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write(cfg.data_root / "staging/run/a.parquet", 64_000)
    (cfg.data_root / "logs").mkdir()
    os.link(cfg.data_root / "staging/run/a.parquet", cfg.data_root / "logs/a.parquet")
    _write(cfg.data_root / "staging/run/b.parquet", 64_000)
    os.link(cfg.data_root / "staging/run/b.parquet", cfg.data_root / "staging/run/b2.parquet")
    _write(cfg.data_root / "meta/raw/daily/page.json", 10_000)

    parts = storage_usage(cfg)["parts"]

    staging = parts["staging"]
    assert staging["files"] == 3 and staging["logical_bytes"] == 192_000
    assert staging["allocated_bytes"] >= 128_000  # b and b2 are one inode
    assert 64_000 <= staging["freeable_bytes"] < staging["allocated_bytes"]  # a stays via logs
    assert parts["logs"]["freeable_bytes"] == 0
    assert parts["meta/raw"]["files"] == 1


@pytest.mark.skipif(sys.platform != "darwin", reason="APFS clones")
def test_a_clone_in_another_part_frees_nothing_on_its_own(tmp_path):
    from cnequity.storage.file_copy import copy2_isolated

    cfg = Config(data_root=tmp_path / "lake")
    original = cfg.data_root / "staging/run/a.parquet"
    _write(original, 2_000_000)
    clone = cfg.data_root / "meta/snapshots/s/a.parquet"
    clone.parent.mkdir(parents=True)
    copy2_isolated(original, clone)

    report = storage_usage(cfg)

    assert report["clone_aware"]
    assert report["parts"]["meta/snapshots"]["allocated_bytes"] >= 2_000_000
    assert report["parts"]["meta/snapshots"]["freeable_bytes"] < 100_000
    assert report["parts"]["staging"]["freeable_bytes"] < 100_000


def test_lake_files_are_split_by_catalog_state_and_the_cli_reports_them(tmp_path, monkeypatch):
    from tests.catalog_seed import publish_rows
    from tests.unit.test_catalog_bridge import _rows

    from cnequity.storage.catalog_bridge import catalog_retention

    cfg = Config(data_root=tmp_path / "lake", revision_keep_generations=1)
    day = date(2024, 1, 2)
    for close in (10.0, 11.0, 12.0):
        publish_rows(cfg, "daily_bars", _rows(day, {"600000.SH": close}), run_id=f"{close}")
    assert catalog_retention(cfg, apply=True)["snapshots_expired"] > 0

    lake = storage_usage(cfg)["lake_files"]

    assert lake["current"]["files"] >= 1
    assert lake["expired"]["files"] >= 2
    assert lake["unregistered"]["files"] == 0
    assert lake["expired_between"][0] is not None and lake["grace_hours"] == 24

    monkeypatch.setattr("cnequity.cli.storage_cmds._cfg", lambda _: cfg)
    runner = CliRunner()
    shown = runner.invoke(cli, ["storage", "usage"])
    assert shown.exit_code == 0, shown.output
    assert "lake" in shown.output and "catalog-retention --apply" in shown.output
    as_json = runner.invoke(cli, ["storage", "usage", "--json"])
    assert json.loads(as_json.stdout)["lake_files"]["expired"]["files"] >= 2


def test_a_lake_without_a_catalog_reports_its_parts_only(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write(cfg.data_root / "backups/meta.tar.gz", 1_000)
    report = storage_usage(cfg)
    assert "lake_files" not in report
    assert report["total"]["files"] == 1
