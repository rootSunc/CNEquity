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


def test_non_pit_datasets_do_not_advertise_strict_pit():
    from cnequity.domain.datasets import DATASETS

    placeholders = [
        n
        for n, spec in DATASETS.items()
        if not getattr(spec, "pit", False) and getattr(spec, "pit_quality", None) == "strict"
    ]
    assert not placeholders, f"{placeholders} are non-PIT but still carry pit_quality='strict'."


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


def _toml_paths(node: object, prefix: str = "") -> set[str]:
    paths: set[str] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            path = f"{prefix}.{key}" if prefix else key
            paths.add(path)
            paths.update(_toml_paths(value, path))
    elif isinstance(node, list):
        for item in node:
            paths.update(_toml_paths(item, prefix))
    return paths


def test_documented_config_sections_exist_in_example_toml():
    """Every active `[section]` documented in configuration.md must exist in the template."""
    import re

    example_path = ROOT / "configs" / "cnequity.example.toml"
    example_text = example_path.read_text(encoding="utf-8")
    valid_paths = _toml_paths(tomllib.loads(example_text)) | set(
        re.findall(r"^\s*#?\s*\[\[?([a-z0-9_.-]+)\]?\]\s*$", example_text, re.M)
    )

    pattern = re.compile(r"`\[\[?([a-z0-9_.<>一-鿿-]+)\]?\]`")
    for lang in ("zh", "en"):
        page = (ROOT / "docs" / lang / "getting-started" / "configuration.md").read_text(
            encoding="utf-8"
        )
        active = page.split("{#retired}", 1)[0]
        for raw in pattern.findall(active):
            cleaned = re.sub(r"\.<[^>]+>", "", raw)
            assert cleaned in valid_paths, (
                f"docs/{lang}/getting-started/configuration.md documents [{raw}], "
                f"which is not in configs/cnequity.example.toml"
            )


def test_documented_cli_commands_exist_in_click_registry():
    """Every `cne <command>` mentioned in docs must be a registered Click command or moved alias."""
    import re

    from cnequity.cli._root import MOVED
    from cnequity.cli.main import cli

    valid_top = set(cli.commands) | {cmd.split()[0] for cmd in MOVED}
    pattern = re.compile(r"`cne\s+([a-z][a-z0-9-]*)\b")
    unknown: list[str] = []
    for lang in ("zh", "en"):
        for path in sorted((ROOT / "docs" / lang).rglob("*.md")):
            for cmd in pattern.findall(path.read_text(encoding="utf-8")):
                if cmd not in valid_top:
                    unknown.append(f"{path.relative_to(ROOT)}: cne {cmd}")
    assert unknown == []
