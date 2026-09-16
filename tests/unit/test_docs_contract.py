"""Small checks that keep the public docs aligned with shipped CLI behavior."""

from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore
from click.testing import CliRunner

from cnequity.cli.main import cli

ROOT = Path(__file__).resolve().parents[2]


def test_cli_reference_covers_the_research_demo_flag():
    help_result = CliRunner().invoke(cli, ["init", "--help"])
    assert help_result.exit_code == 0
    reference = (ROOT / "docs" / "reference" / "cli.md").read_text(encoding="utf-8")
    assert "`--research`" in reference
    assert "raw / hfq" in reference
    assert "--research" in help_result.output


def test_source_health_note_does_not_describe_removed_eastmoney_sticky_state():
    from cnequity.diagnostics.source_health import PROBES_BY_KEY

    note = PROBES_BY_KEY["eastmoney_push2his"].note
    assert "sticky" not in note.lower()
    assert "proxy" in note


def test_the_declared_version_is_the_packaged_version():
    """__version__ is hand-written, so nothing but a test keeps it honest.

    A release bumps pyproject and can silently leave the module behind; the
    wheel then reports one version and `cne --version` another.
    """
    from cnequity import __version__

    with (ROOT / "pyproject.toml").open("rb") as fh:
        declared = tomllib.load(fh)["project"]["version"]
    assert __version__ == declared, (
        f"src/cnequity/__init__.py says {__version__}, pyproject.toml says {declared}"
    )


def test_the_release_contract_for_this_version_exists():
    """The release workflow fails on a missing contract; fail here instead."""
    with (ROOT / "pyproject.toml").open("rb") as fh:
        version = tomllib.load(fh)["project"]["version"]
    assert (ROOT / "contracts" / f"v{version}.json").is_file(), (
        f"contracts/v{version}.json is missing; run `cne contract show > contracts/v{version}.json`"
    )


def test_citation_metadata_tracks_the_current_package_version():
    from cnequity import __version__

    citation = (ROOT / "CITATION.cff").read_text(encoding="utf-8")
    assert f"version: {__version__}" in citation
    assert "license: Apache-2.0" in citation
    assert 'repository-code: "https://github.com/rootSunc/CNEquity"' in citation


def test_documented_counts_match_the_registries():
    """Counts in prose rot silently; nothing else reads them.

    Found stale in one sweep: the steps module page said 40 registered steps
    when there were 47, and the contract page still described `pit_quality`
    falling back to the literal `strict` — a convergence that had already
    shipped as `not_applicable`.
    """
    from cnequity.diagnostics.source_health import PROBES_BY_KEY
    from cnequity.domain.datasets import DATASETS
    from conftest import PRISTINE_STEP_NAMES

    docs = ROOT / "docs"
    claims = [
        (docs / "modules" / "steps.md", f"**{len(PRISTINE_STEP_NAMES)} 个**注册 step"),
        (docs / "datasets" / "catalog.md", str(len(DATASETS))),
    ]
    for path, needle in claims:
        assert needle in path.read_text(encoding="utf-8"), (
            f"{path.name} no longer states {needle!r}"
        )

    # The README pair counts the probe registry rather than the dataset one.
    for name in ("README.md", "README.en.md"):
        text = (ROOT / name).read_text(encoding="utf-8")
        assert str(len(PROBES_BY_KEY)) in text, f"{name} lost the probe count"

    # `pit_quality` no longer falls back to a literal "strict" for non-PIT
    # tables; the contract page must not describe it as if it did.
    placeholders = [
        n
        for n, spec in DATASETS.items()
        if not getattr(spec, "pit", False) and getattr(spec, "pit_quality", None) == "strict"
    ]
    assert not placeholders, (
        f"{placeholders} are non-PIT but still carry pit_quality='strict'; "
        "docs/datasets/contract.md says that fallback is gone"
    )


def test_the_lockfile_records_the_current_version():
    """`uv run` silently rewrites a stale one, so it shows up as a phantom diff.

    The lock said 0.8.0 against a pyproject on 0.10.0.dev0, so every `uv run`
    — including the docs build — left `uv.lock` modified in the working tree.
    """
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore

    with (ROOT / "pyproject.toml").open("rb") as fh:
        declared = tomllib.load(fh)["project"]["version"]

    lock = (ROOT / "uv.lock").read_text(encoding="utf-8")
    marker = '[[package]]\nname = "cnequity"\nversion = "'
    assert marker in lock, "uv.lock no longer pins this package the expected way"
    locked = lock.split(marker, 1)[1].split('"', 1)[0]
    assert locked == declared, (
        f"uv.lock says {locked}, pyproject.toml says {declared}; run `uv lock`"
    )
