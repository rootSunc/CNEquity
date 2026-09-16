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
