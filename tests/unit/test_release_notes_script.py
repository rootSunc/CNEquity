import runpy
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
NOTES = runpy.run_path(str(ROOT / "scripts/dev/release_notes.py"))

CHANGELOG = """# 更新日志

## [0.2.0] — 2026-01-02

- 新功能，见[说明](docs/zh/a.md#x)和[官网](https://example.com)与[锚点](#top)。

## [0.1.0] — 2026-01-01

- 初版。
"""


def test_section_is_isolated_and_links_are_pinned_to_the_tag():
    text = NOTES["render"](CHANGELOG, "0.2.0")
    assert "初版" not in text
    assert "pip install --upgrade cnequity==0.2.0" in text
    assert "(https://github.com/rootSunc/CNEquity/blob/v0.2.0/docs/zh/a.md#x)" in text
    assert "(https://example.com)" in text and "(#top)" in text


def test_missing_or_empty_section_is_an_error():
    with pytest.raises(ValueError):
        NOTES["changelog_section"](CHANGELOG, "0.3.0")
    with pytest.raises(ValueError):
        NOTES["changelog_section"]("## [1.0.0] — x\n\n## [0.9.0] — y\n- a\n", "1.0.0")


def test_current_release_has_a_changelog_section():
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore

    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    version = tomllib.loads(pyproject)["project"]["version"]
    assert NOTES["changelog_section"]((ROOT / "CHANGELOG.md").read_text(encoding="utf-8"), version)


def test_heading_headline_becomes_title_and_lead():
    changelog = CHANGELOG.replace("## [0.2.0] — 2026-01-02", "## [0.2.0] — 2026-01-02 · 新功能")
    assert NOTES["title"](changelog, "0.2.0") == "0.2.0 · 新功能"
    assert NOTES["render"](changelog, "0.2.0").startswith("**新功能**\n\n")
    assert NOTES["title"](CHANGELOG, "0.1.0") == "0.1.0"
    assert not NOTES["render"](CHANGELOG, "0.1.0").startswith("**")
