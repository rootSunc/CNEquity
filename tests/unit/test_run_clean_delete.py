"""An interactive `cne run clean` may delete what the preview just listed."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone

import polars as pl
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config import Config
from cnequity.config.bootstrap import path_for_toml
from cnequity.orchestrator.manifest import Manifest
from cnequity.storage import StagingWriter
from cnequity.storage.revisions import RevisionStore
from cnequity.storage.staging_cleanup import list_staging_run_ids


def _config(tmp_path) -> str:
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
workers = 1

[tdx_protocol]
allow_mock = true
"""
    )
    return str(path)


def _bar() -> pl.DataFrame:
    trade_date = date(2024, 6, 28)
    return pl.DataFrame(
        [
            {
                "symbol": "000001.SZ",
                "trade_date": trade_date,
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 1000,
                "amount": 10_500.0,
                "source": "mock",
                "data_version": "v1",
                "fetched_at": "2024-06-28T00:00:00+00:00",
            }
        ]
    )


def _stage(cfg: Config, manifest: Manifest, *, compacted: bool) -> str:
    run_id = manifest.start_run("daily", {})
    manifest.start_batch(run_id, "batch-0", "daily_bars", "daily_bars")
    manifest.finish_batch(
        run_id,
        "batch-0",
        "success" if compacted else "failed",
        rows_written=1 if compacted else 0,
        error_message=None if compacted else "err",
    )
    if compacted:
        manifest.start_batch(run_id, "compact-0", "compact", "compact")
        manifest.finish_batch(run_id, "compact-0", "success", rows_written=1)
    manifest.finish_run(run_id, "success" if compacted else "failed")
    StagingWriter(cfg.staging_root).write_batch("daily_bars", run_id, "batch-0", _bar())
    return run_id


def _publish_two_generations(cfg: Config) -> str:
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    first_path = cfg.curated_root / "daily_bars" / "trade_date=2024-06-18" / "part-merged.parquet"
    first_path.parent.mkdir(parents=True)
    first_path.write_bytes(b"day-1")
    store.commit(
        "daily_bars",
        run_id="run-1",
        changed_files=[first_path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    second = cfg.curated_root / "daily_bars" / "trade_date=2024-06-19" / "part-merged.parquet"
    second.parent.mkdir(parents=True)
    second.write_bytes(b"day-2")
    receipt = store.commit(
        "daily_bars",
        run_id="run-2",
        changed_files=[second],
        schema_version=1,
        contract_fingerprint="contract",
    )
    assert receipt is not None
    return receipt.revision_id


def _objects(stdout: str) -> list[dict]:
    decoder = json.JSONDecoder()
    found = []
    index = 0
    text = stdout.strip()
    while index < len(text):
        value, offset = decoder.raw_decode(text[index:])
        found.append(value)
        index += offset
        while index < len(text) and text[index].isspace():
            index += 1
    return found


def test_noninteractive_clean_does_not_delete(tmp_path, monkeypatch):
    cfg_path = _config(tmp_path)
    cfg = Config(data_root=tmp_path / "data")
    _publish_two_generations(cfg)
    before = {path.name for path in (cfg.meta_root / "revisions" / "data" / "daily_bars").iterdir()}
    monkeypatch.setattr("cnequity.cli.maintain_cmds._stdin_is_tty", lambda: False)

    result = CliRunner().invoke(
        cli,
        ["run", "clean", "--config", cfg_path, "--keep-revision-generations", "1"],
        input="y\n",
    )

    assert result.exit_code == 0, result.output
    payloads = _objects(result.stdout)
    assert len(payloads) == 1
    assert payloads[0]["confirmation_required"] == "none"
    assert payloads[0]["bytes_freed"] == 0
    assert {
        path.name for path in (cfg.meta_root / "revisions" / "data" / "daily_bars").iterdir()
    } == before


def test_declining_the_prompt_deletes_nothing(tmp_path, monkeypatch):
    cfg_path = _config(tmp_path)
    cfg = Config(data_root=tmp_path / "data")
    current = _publish_two_generations(cfg)
    monkeypatch.setattr("cnequity.cli.maintain_cmds._stdin_is_tty", lambda: True)

    result = CliRunner().invoke(
        cli,
        ["run", "clean", "--config", cfg_path, "--keep-revision-generations", "1"],
        input="n\n",
    )

    assert result.exit_code == 0, result.output
    payloads = _objects(result.stdout)
    assert len(payloads) == 1
    assert payloads[0]["confirmation_required"] == "interactive"
    root = cfg.meta_root / "revisions" / "data" / "daily_bars" / current
    assert (root / "trade_date=2024-06-19" / "part-merged.parquet").read_bytes() == b"day-2"


def test_confirming_deletes_old_generations_and_compacted_staging_only(tmp_path, monkeypatch):
    cfg_path = _config(tmp_path)
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    compacted = _stage(cfg, manifest, compacted=True)
    unfinished = _stage(cfg, manifest, compacted=False)
    current = _publish_two_generations(cfg)
    log = cfg.data_root / "logs" / "cne-old.log"
    log.parent.mkdir()
    log.write_bytes(b"keep this log")
    old = (datetime.now(timezone.utc) - timedelta(days=90)).timestamp()
    os.utime(log, (old, old))
    monkeypatch.setattr("cnequity.cli.maintain_cmds._stdin_is_tty", lambda: True)

    result = CliRunner().invoke(
        cli,
        ["run", "clean", "--config", cfg_path, "--keep-revision-generations", "1"],
        input="y\ny\n",
    )

    assert result.exit_code == 0, result.output
    preview, deleted = _objects(result.stdout)
    assert preview["dry_run"] is True
    assert preview["bytes_freed"] == 0
    assert compacted in preview["candidate_run_ids"]
    assert unfinished not in preview["candidate_run_ids"]
    assert deleted["deleted"] is True
    removed = {
        revision_id
        for item in deleted["revision_generations"]
        for revision_id in item["removed_revision_ids"]
    }
    assert current not in removed
    assert removed
    assert deleted["staging"]["removed_run_ids"] == [compacted]
    names = {path.name for path in (cfg.meta_root / "revisions" / "data" / "daily_bars").iterdir()}
    assert current in names
    assert names.isdisjoint(removed)
    assert compacted not in list_staging_run_ids(cfg.staging_root)
    assert unfinished in list_staging_run_ids(cfg.staging_root)
    assert log.read_bytes() == b"keep this log"
    current_root = cfg.meta_root / "revisions" / "data" / "daily_bars" / current
    assert (current_root / "trade_date=2024-06-19" / "part-merged.parquet").read_bytes() == b"day-2"


def test_declining_staging_keeps_it_while_old_generations_can_go(tmp_path, monkeypatch):
    cfg_path = _config(tmp_path)
    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    compacted = _stage(cfg, manifest, compacted=True)
    current = _publish_two_generations(cfg)
    monkeypatch.setattr("cnequity.cli.maintain_cmds._stdin_is_tty", lambda: True)

    result = CliRunner().invoke(
        cli,
        ["run", "clean", "--config", cfg_path, "--keep-revision-generations", "1"],
        input="y\nn\n",
    )

    assert result.exit_code == 0, result.output
    _preview, deleted = _objects(result.stdout)
    assert "staging" not in deleted
    assert deleted["revision_generations"]
    assert current not in {
        revision_id
        for item in deleted["revision_generations"]
        for revision_id in item["removed_revision_ids"]
    }
    assert compacted in list_staging_run_ids(cfg.staging_root)
    current_root = cfg.meta_root / "revisions" / "data" / "daily_bars" / current
    assert (current_root / "trade_date=2024-06-19" / "part-merged.parquet").read_bytes() == b"day-2"


def test_in_use_staging_is_kept_even_when_deletion_is_accepted(tmp_path, monkeypatch):
    from cnequity.cli.maintain_cmds import _delete_preview_selection

    cfg = Config(data_root=tmp_path / "data")
    manifest = Manifest(cfg.manifest_path)
    unfinished = _stage(cfg, manifest, compacted=False)
    monkeypatch.setattr("cnequity.cli.maintain_cmds._confirm", lambda _text: True)

    _delete_preview_selection(
        cfg,
        keep_revision_generations=1,
        generation_candidates=0,
        staging_run_ids=[unfinished],
    )

    assert unfinished in list_staging_run_ids(cfg.staging_root)


def test_automatic_retention_does_not_delete_staging_or_the_current_generation(tmp_path):
    from cnequity.storage.revisions import retain_revision_generations

    cfg = Config(data_root=tmp_path / "data")
    current = _publish_two_generations(cfg)
    staging = cfg.staging_root / "daily_bars" / "run_id=live" / "part.parquet"
    staging.parent.mkdir(parents=True)
    staging.write_bytes(b"still in use")
    curated = cfg.curated_root / "daily_bars" / "trade_date=2024-06-19" / "part-merged.parquet"

    retain_revision_generations(cfg.meta_root, keep=1)

    assert staging.read_bytes() == b"still in use"
    assert curated.read_bytes() == b"day-2"
    current_root = cfg.meta_root / "revisions" / "data" / "daily_bars" / current
    assert (current_root / "trade_date=2024-06-19" / "part-merged.parquet").read_bytes() == b"day-2"


def test_dry_run_does_not_ask(tmp_path, monkeypatch):
    cfg_path = _config(tmp_path)
    cfg = Config(data_root=tmp_path / "data")
    _publish_two_generations(cfg)
    monkeypatch.setattr("cnequity.cli.maintain_cmds._stdin_is_tty", lambda: True)

    result = CliRunner().invoke(
        cli,
        ["run", "clean", "--dry-run", "--config", cfg_path, "--keep-revision-generations", "1"],
        input="y\n",
    )

    assert result.exit_code == 0, result.output
    payloads = _objects(result.stdout)
    assert len(payloads) == 1
    assert payloads[0]["confirmation_required"] == "none"
