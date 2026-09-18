"""Tests for cne config create / packaged example template."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config.bootstrap import (
    example_toml_text,
    render_example_toml,
    write_user_config,
)


def test_packaged_example_matches_repo_checkout():
    """A wheel install and a repo checkout must offer the same example config.

    Resolved from this file rather than the CWD: the old relative lookup made
    the test skip itself whenever pytest ran from anywhere but the repo root,
    which is exactly when drift would go unnoticed.
    """
    repo_example = Path(__file__).resolve().parents[2] / "configs" / "cnequity.example.toml"
    assert example_toml_text() == repo_example.read_text(encoding="utf-8")


def test_render_patches_data_root_and_darwin_workers():
    text = render_example_toml(data_root="/tmp/my-lake", platform="darwin")
    assert 'root = "/tmp/my-lake"' in text
    assert "workers = 1" in text
    assert "workers = 8" not in text


def test_render_patches_windows_workers():
    text = render_example_toml(data_root="C:/lake", platform="win32")
    assert 'root = "C:/lake"' in text
    assert "workers = 1" in text
    assert "workers = 8" not in text


def test_render_escapes_windows_backslashes_in_data_root():
    # PowerShell users often paste `C:\Users\…\lake`; TOML needs `\\`.
    text = render_example_toml(data_root=r"C:\Users\测试\lake", platform="win32")
    assert r'root = "C:\\Users\\测试\\lake"' in text
    assert "workers = 1" in text


def test_path_for_toml_makes_windows_tmp_paths_parseable():
    """Regression for windows-latest CI: bare ``C:\\Users\\…`` is invalid TOML."""
    import re
    import sys

    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib

    from cnequity.config.bootstrap import _toml_escape, path_for_toml

    # Simulate the GitHub Actions runner layout without requiring Windows.
    raw = Path(r"C:\Users\runneradmin\AppData\Local\Temp\pytest-0\test_cli0\data")
    # as_posix + escape is what path_for_toml does before resolve(); assert the
    # TOML grammar alone (resolve() on Unix would prefix the cwd).
    text = f'[data]\nroot = "{_toml_escape(raw.as_posix())}"\n'
    payload = tomllib.loads(text)
    assert "Users" in payload["data"]["root"]
    rendered = path_for_toml(Path("/tmp/lake"))
    if sys.platform == "win32":
        # resolve() on Windows yields a drive-letter POSIX form (e.g. D:/tmp/lake).
        assert re.match(r"^[A-Za-z]:/", rendered), rendered
    else:
        assert rendered.startswith("/")
    with pytest.raises(tomllib.TOMLDecodeError):
        tomllib.loads(f'[data]\nroot = "{raw}"\n')


def test_render_keeps_linux_workers():
    text = render_example_toml(platform="linux")
    assert "workers = 8" in text


def test_write_user_config_defaults_to_absolute_data_root(tmp_path, monkeypatch):
    """Bare ``cne config create`` must not leave ``./data/...`` for doctor to reject."""
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "configs" / "cnequity.toml"
    write_user_config(out, platform="linux")
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib

    payload = tomllib.loads(out.read_text(encoding="utf-8"))
    root = Path(payload["data"]["root"])
    assert root.is_absolute()
    assert root == (tmp_path / "data" / "cnequity").resolve()


def test_write_user_config_refuses_overwrite(tmp_path):
    out = tmp_path / "configs" / "cnequity.toml"
    write_user_config(out, platform="linux")
    assert out.is_file()
    with pytest.raises(FileExistsError):
        write_user_config(out, platform="linux")
    write_user_config(out, force=True, data_root=str(tmp_path / "data"), platform="linux")
    # Parse rather than substring-match: Windows paths are TOML-escaped (`\\`).
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib

    payload = tomllib.loads(out.read_text(encoding="utf-8"))
    assert Path(payload["data"]["root"]).resolve() == (tmp_path / "data").resolve()


def test_cli_config_create_and_validate(tmp_path):
    out = tmp_path / "cnequity.toml"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "config",
            "create",
            "--config",
            str(out),
            "--data-root",
            str(tmp_path / "lake"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert out.is_file()
    assert "已写入" in result.output

    again = runner.invoke(cli, ["config", "create", "--config", str(out)])
    assert again.exit_code != 0
    assert "already exists" in again.output

    ok = runner.invoke(cli, ["config", "validate", "--config", str(out)])
    assert ok.exit_code == 0, ok.output
    assert "配置检查通过" in ok.output


def test_resolve_config_missing_suggests_config_init(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["config", "validate"])
    assert result.exit_code != 0
    assert "cne config create" in result.output
