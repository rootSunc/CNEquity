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
    """Every active `[section]` and `[section].key` in docs must exist in the template."""
    import re

    example_path = ROOT / "configs" / "cnequity.example.toml"
    example_text = example_path.read_text(encoding="utf-8")
    valid_paths = _toml_paths(tomllib.loads(example_text)) | set(
        re.findall(r"^\s*#?\s*\[\[?([a-z0-9_.-]+)\]?\]\s*$", example_text, re.M)
    )
    # Include commented-out `# key = ...` under their active or commented section in the template.
    current_section = ""
    for line in example_text.splitlines():
        if sec := re.match(r"^\s*#?\s*\[\[?([a-z0-9_.-]+)\]?\]\s*$", line):
            current_section = sec.group(1)
        elif current_section and (kv := re.match(r"^\s*#\s*([a-z][a-z0-9_]*)\s*=", line)):
            valid_paths.add(f"{current_section}.{kv.group(1)}")

    section_pattern = re.compile(r"`\[\[?([a-z0-9_.<>一-鿿-]+)\]?\]`")
    key_pattern = re.compile(r"`\[([a-z0-9_.-]+)\]\.([a-z][a-z0-9_]*)`")
    for lang in ("zh", "en"):
        for doc_path in sorted((ROOT / "docs" / lang).rglob("*.md")):
            page = doc_path.read_text(encoding="utf-8")
            active = page.split("{#retired}", 1)[0]
            if doc_path.name == "configuration.md":
                for raw in section_pattern.findall(active):
                    cleaned = re.sub(r"\.<[^>]+>", "", raw)
                    assert cleaned in valid_paths, (
                        f"{doc_path.relative_to(ROOT)} documents [{raw}], "
                        f"which is not in configs/cnequity.example.toml"
                    )
            for sec, key in key_pattern.findall(active):
                full_key = f"{sec}.{key}"
                assert full_key in valid_paths, (
                    f"{doc_path.relative_to(ROOT)} documents [{sec}].{key}, "
                    f"which is not in configs/cnequity.example.toml"
                )


def test_quickstart_offline_sample_commands_execute(tmp_path, monkeypatch):
    """The 3-line offline sample block in quickstart.md and index.md runs end-to-end."""
    import re
    import shlex

    from click.testing import CliRunner

    from cnequity.cli.main import cli

    block_re = re.compile(r"```bash\n(.*?cne init --profile sample.*?)```", re.S)
    blocks = []
    for rel in (
        "docs/zh/getting-started/quickstart.md",
        "docs/en/getting-started/quickstart.md",
        "docs/zh/index.md",
        "docs/en/index.md",
    ):
        match = block_re.search((ROOT / rel).read_text(encoding="utf-8"))
        assert match is not None, f"missing sample quickstart block in {rel}"
        cne_lines = [
            line.strip() for line in match.group(1).splitlines() if line.strip().startswith("cne ")
        ]
        blocks.append(cne_lines)

    assert all(b == blocks[0] for b in blocks), "sample quickstart commands drifted across pages"
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    for cmd_line in blocks[0]:
        args = shlex.split(cmd_line)[1:]
        result = runner.invoke(cli, args)
        assert result.exit_code == 0, f"`{cmd_line}` failed: {result.output}"
    assert "mock" in result.output


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
