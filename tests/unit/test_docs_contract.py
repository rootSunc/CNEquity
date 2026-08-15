"""Small checks that keep the public docs aligned with shipped CLI behavior."""

from pathlib import Path

from click.testing import CliRunner

from cn_market_lake.cli.main import cli

ROOT = Path(__file__).resolve().parents[2]


def test_cli_reference_covers_the_research_demo_flag():
    help_result = CliRunner().invoke(cli, ["demo", "--help"])
    assert help_result.exit_code == 0
    reference = (ROOT / "docs" / "reference" / "cli.md").read_text(encoding="utf-8")
    assert "`--research`" in reference
    assert "raw / hfq" in reference
    assert "--research" in help_result.output


def test_source_health_note_does_not_describe_removed_eastmoney_sticky_state():
    from cn_market_lake.diagnostics.source_health import PROBES_BY_KEY

    note = PROBES_BY_KEY["eastmoney_push2his"].note
    assert "sticky" not in note.lower()
    assert "proxy" in note


def test_citation_metadata_tracks_the_current_package_version():
    from cn_market_lake import __version__

    citation = (ROOT / "CITATION.cff").read_text(encoding="utf-8")
    assert f"version: {__version__}" in citation
    assert "license: Apache-2.0" in citation
    assert 'repository-code: "https://github.com/rootSunc/cn-market-lake"' in citation
