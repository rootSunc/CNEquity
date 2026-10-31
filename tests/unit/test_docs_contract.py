"""Small checks that keep the public docs aligned with shipped CLI behavior."""

from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore


ROOT = Path(__file__).resolve().parents[2]


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


def test_citation_metadata_tracks_the_current_package_version():
    from cnequity import __version__

    citation = (ROOT / "CITATION.cff").read_text(encoding="utf-8")
    assert f"version: {__version__}" in citation
    assert "license: Apache-2.0" in citation
    assert 'repository-code: "https://github.com/rootSunc/CNEquity"' in citation


def test_mcp_registry_metadata_tracks_the_package():
    """server.json is what the MCP Registry lists; it names a PyPI release, and
    the registry only accepts it when that release's README carries the
    mcp-name marker."""
    import json

    from cnequity import __version__

    server = json.loads((ROOT / "server.json").read_text(encoding="utf-8"))
    package = server["packages"][0]
    assert server["version"] == package["version"] == __version__
    assert package["identifier"] == "cnequity"
    assert len(server["description"]) <= 100
    marker = f"mcp-name: {server['name']}"
    assert marker in (ROOT / "README.pypi.md").read_text(encoding="utf-8")
    with (ROOT / "pyproject.toml").open("rb") as fh:
        scripts = tomllib.load(fh)["project"]["scripts"]
    # runtimeHint uvx runs the script named after the package.
    assert scripts["cnequity"] == scripts["cne"]


def test_documented_counts_match_the_registries():
    """Counts in prose rot silently; nothing else reads them.

    Found stale in one sweep: the steps module page said 40 registered steps
    when there were 47, and the contract page still described `pit_quality`
    falling back to the literal `strict` — a convergence that had already
    shipped as `not_applicable`.
    """
    from cnequity.domain.datasets import DATASETS

    claims = [
        (ROOT / "README.md", f"**{len(DATASETS)} 个数据集"),
        (ROOT / "README.en.md", f"**{len(DATASETS)} datasets"),
        (ROOT / "README.pypi.md", f"**{len(DATASETS)} 个数据集"),
    ]
    for path, needle in claims:
        assert needle in path.read_text(encoding="utf-8"), (
            f"{path.name} no longer states {needle!r}"
        )

    # `pit_quality` no longer falls back to a literal "strict" for non-PIT
    # tables; the contract page must not describe it as if it did.
    placeholders = [
        n
        for n, spec in DATASETS.items()
        if not getattr(spec, "pit", False) and getattr(spec, "pit_quality", None) == "strict"
    ]
    assert not placeholders, (
        f"{placeholders} are non-PIT but still carry pit_quality='strict'; "
        "docs/zh/datasets/contract.md says that fallback is gone"
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
