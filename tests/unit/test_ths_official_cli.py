"""The `cne ths-official` surface.

The integration is optional, so the property that matters most is what happens
without a key: every command reports that it was skipped and touches nothing.
"""

import json

import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config.bootstrap import path_for_toml


@pytest.fixture
def lake_config(tmp_path):
    path = tmp_path / "cnequity.toml"
    path.write_text(f'[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n')
    return str(path)


def _run(args):
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    # `result.output` is stdout and stderr interleaved, and a command that gets
    # past the key/switch gates announces its log file on stderr. The JSON
    # contract is stdout alone — that is what `cne ... | jq` reads.
    return json.loads(result.stdout)


@pytest.mark.parametrize("command", ["capture", "backfill", "repair-bars"])
def test_every_command_is_inert_without_a_key(command, lake_config, monkeypatch):
    """A lake with no key keeps the sources it already has, and says so."""
    monkeypatch.delenv("HITHINK_FINANCE_API_KEY", raising=False)
    out = _run(["ths-official", command, "--config", lake_config])
    assert out["status"] == "skipped"
    assert "no API key" in out["reason"]


def test_a_key_alone_does_not_enable_the_source(lake_config, monkeypatch):
    """Holding a credential is not the same as opting the lake into the source."""
    monkeypatch.setenv("HITHINK_FINANCE_API_KEY", "sk-test")
    out = _run(["ths-official", "capture", "--config", lake_config])
    assert out["status"] == "skipped"
    assert "enabled = true" in out["reason"]


def test_backfill_needs_its_own_switch(tmp_path, monkeypatch):
    """Verification and content are separate switches, on purpose.

    Sharing one would make "check my data against a licensed peer" also mean
    "and rewrite nine years of it".
    """
    monkeypatch.setenv("HITHINK_FINANCE_API_KEY", "sk-test")
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n\n'
        "[sources.ths_official]\nenabled = true\nverify = true\nbackfill = false\n"
    )
    out = _run(["ths-official", "backfill", "--config", str(path)])
    assert out["status"] == "skipped"
    assert "backfill = true" in out["reason"]


def test_repair_bars_refuses_to_apply_without_the_content_switch(tmp_path, monkeypatch):
    """Switching an existing canonical owner is never a side effect of a flag."""
    monkeypatch.setenv("HITHINK_FINANCE_API_KEY", "sk-test")
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n\n'
        "[sources.ths_official]\nenabled = true\nbackfill = false\n"
    )
    out = _run(["ths-official", "repair-bars", "--config", str(path), "--apply"])
    assert out["status"] == "skipped"
    assert "backfill = true" in out["reason"]


def test_the_group_is_discoverable_from_the_root_help():
    result = CliRunner().invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "ths-official" in result.output


def test_backfill_can_be_scoped_to_named_symbols(tmp_path, monkeypatch):
    """A full sweep is 78 minutes; a handful lost to a DNS blip is not worth one.

    The step has always taken a symbol list — only the CLI withheld it, so the
    16 securities a transient transport error dropped could be recovered no
    other way than re-running the market.
    """
    import cnequity.steps.fundamentals as fundamentals

    seen: dict = {}

    def _capture(config, run_id, **kwargs):
        seen.update(kwargs)
        return {"rows_read": 0, "rows_written": 0}

    monkeypatch.setattr(fundamentals, "backfill_statement_gap_ths_official", _capture)
    monkeypatch.setenv("HITHINK_FINANCE_API_KEY", "sk-test")
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n\n'
        "[sources.ths_official]\nenabled = true\nverify = true\nbackfill = true\n"
    )

    _run(["ths-official", "backfill", "--config", str(path), "--symbols", "300543.SZ, 300545.SZ"])

    assert seen["symbols"] == ["300543.SZ", "300545.SZ"]


def test_backfill_without_symbols_still_means_the_whole_market(tmp_path, monkeypatch):
    """The scoped path must not become the default by accident."""
    import cnequity.steps.fundamentals as fundamentals

    seen: dict = {}

    def _capture(config, run_id, **kwargs):
        seen.update(kwargs)
        return {"rows_read": 0, "rows_written": 0}

    monkeypatch.setattr(fundamentals, "backfill_statement_gap_ths_official", _capture)
    monkeypatch.setenv("HITHINK_FINANCE_API_KEY", "sk-test")
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n\n'
        "[sources.ths_official]\nenabled = true\nverify = true\nbackfill = true\n"
    )

    _run(["ths-official", "backfill", "--config", str(path)])

    assert seen["symbols"] is None


def _writable_config(tmp_path):
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n\n'
        "[sources.ths_official]\nenabled = true\nverify = true\nbackfill = true\n"
    )
    return path


def test_a_staging_command_records_its_run(tmp_path, monkeypatch):
    """The run id named nothing, so `cne status` could not see the run at all.

    Worse than invisible: `cne run clean` files staging it cannot prove is
    finished under *skipped*, which is never reclaimed — unlike a manifest-less
    orphan, which ages out. Three `ths-*` runs had stranded 23MB that way.
    """
    import cnequity.steps.fundamentals as fundamentals
    from cnequity.config import load_config
    from cnequity.orchestrator.manifest import Manifest

    monkeypatch.setattr(
        fundamentals,
        "backfill_statement_gap_ths_official",
        lambda config, run_id, **kw: {"rows_read": 10, "rows_written": 10, "failed_symbols": 0},
    )
    monkeypatch.setenv("HITHINK_FINANCE_API_KEY", "sk-test")
    path = _writable_config(tmp_path)

    out = _run(["ths-official", "backfill", "--config", str(path)])

    assert out["run_id"].startswith("ths-backfill-"), "the readable prefix is what gets pasted"
    manifest = Manifest(load_config(path).manifest_path)
    record = manifest.get_run(out["run_id"])
    assert record is not None, "the run id named nothing"
    assert record["job_name"] == "ths_official_backfill"
    assert record["status"] == "success"
    assert record["rows_written"] == 10


def test_a_failed_sweep_closes_its_run_rather_than_leaving_it_running(tmp_path, monkeypatch):
    """A run left `running` is reconciled as a crash and blocks the next command."""
    import cnequity.steps.fundamentals as fundamentals
    from cnequity.config import load_config
    from cnequity.orchestrator.manifest import Manifest

    def _boom(config, run_id, **kw):
        raise RuntimeError("upstream refused the window")

    monkeypatch.setattr(fundamentals, "backfill_statement_gap_ths_official", _boom)
    monkeypatch.setenv("HITHINK_FINANCE_API_KEY", "sk-test")
    path = _writable_config(tmp_path)

    result = CliRunner().invoke(cli, ["ths-official", "backfill", "--config", str(path)])
    assert result.exit_code != 0

    manifest = Manifest(load_config(path).manifest_path)
    runs = [r for r in manifest.list_runs() if r["job_name"] == "ths_official_backfill"]
    assert len(runs) == 1
    assert runs[0]["status"] == "failed"
    assert "upstream refused the window" in (runs[0]["error_message"] or "")


def test_a_dry_run_records_nothing(tmp_path, monkeypatch):
    """It stages no rows, so there is nothing for a run to account for."""
    import cnequity.steps.rotation as rotation
    from cnequity.config import load_config
    from cnequity.orchestrator.manifest import Manifest

    monkeypatch.setattr(
        rotation,
        "resource_sector_bars_ths_official",
        lambda config, run_id, **kw: {"status": "dry_run", "rows_written": 0},
    )
    monkeypatch.setenv("HITHINK_FINANCE_API_KEY", "sk-test")
    path = _writable_config(tmp_path)

    out = _run(["ths-official", "resource-sectors", "--config", str(path)])

    assert out["status"] == "dry_run"
    manifest = Manifest(load_config(path).manifest_path)
    assert not [r for r in manifest.list_runs() if r["run_id"] == out["run_id"]]


def test_staging_becomes_reclaimable_once_its_run_is_published(tmp_path, monkeypatch):
    """The whole point of the run record, end to end.

    `clean_staging` reclaims a run's staging only once it can see the run is
    terminal *and* that a compact succeeded. With no run row it could establish
    neither, so both states looked alike and the staging sat in `skipped`
    forever — not aged out the way a manifest-less orphan is.
    """
    import json
    from datetime import date, datetime, timezone

    import polars as pl

    import cnequity.steps.fundamentals as fundamentals
    from cnequity.storage import StagingWriter

    def _stage(config, run_id, **kw):
        StagingWriter(config.staging_root).write_batch(
            "financial_statement_items",
            run_id,
            "batch-00000",
            pl.DataFrame(
                {
                    "symbol": ["600519.SH"],
                    "report_period": ["2016Q1"],
                    "statement_type": ["balance"],
                    "item_code": ["total_assets"],
                    "item_value": [1.0],
                    "announce_date": [date(2016, 4, 28)],
                    "source": ["ths_official"],
                    "data_version": ["v1"],
                    "fetched_at": [datetime.now(timezone.utc)],
                }
            ),
        )
        return {"rows_read": 1, "rows_written": 1, "failed_symbols": 0}

    monkeypatch.setattr(fundamentals, "backfill_statement_gap_ths_official", _stage)
    monkeypatch.setenv("HITHINK_FINANCE_API_KEY", "sk-test")
    path = _writable_config(tmp_path)
    run_id = _run(["ths-official", "backfill", "--config", str(path)])["run_id"]

    def bucket() -> str:
        result = CliRunner().invoke(cli, ["run", "clean", "--dry-run", "--config", str(path)])
        report = json.loads(result.stdout)
        return next(
            (
                k
                for k in ("removed_run_ids", "orphan_run_ids", "skipped_run_ids")
                if run_id in report[k]
            ),
            "nowhere",
        )

    # Held back while the rows exist only in staging — publishing is still owed.
    assert bucket() == "skipped_run_ids"
    assert (
        CliRunner()
        .invoke(cli, ["run", "compact", "--run-id", run_id, "--config", str(path)])
        .exit_code
        == 0
    )
    assert bucket() == "removed_run_ids"
