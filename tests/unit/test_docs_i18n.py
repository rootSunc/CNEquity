"""The Chinese and English docs are two independent sites with the same shape.

``docs/zh`` builds the root site and ``docs/en`` the ``/en/`` site. Each page
exists in both trees, both navs list the same pages, and neither tree links
into the other — a reader who picked a language stays in it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
ZH = ROOT / "docs" / "zh"
EN = ROOT / "docs" / "en"
_LINK = re.compile(r"\]\(([^)\s]+)\)")
_FENCE = re.compile(r"```.*?```", re.S)


def _pages(tree: Path) -> set[str]:
    return {p.relative_to(tree).as_posix() for p in tree.rglob("*.md")}


def _nav_pages(config: str) -> set[str]:
    class _Loader(yaml.SafeLoader):
        pass

    # mkdocs.yml carries a !!python tag for the slugifier; nav does not need it.
    _Loader.add_multi_constructor("tag:yaml.org,2002:python/", lambda *_: None)
    nav = yaml.load((ROOT / config).read_text(encoding="utf-8"), Loader=_Loader)["nav"]

    found: set[str] = set()

    def walk(items):
        for item in items:
            for value in item.values() if isinstance(item, dict) else [item]:
                if isinstance(value, list):
                    walk(value)
                else:
                    found.add(value)

    walk(nav)
    return found


def test_every_page_exists_in_both_languages():
    zh, en = _pages(ZH), _pages(EN)
    assert zh - en == set(), f"no English page for {sorted(zh - en)}"
    assert en - zh == set(), f"no Chinese page for {sorted(en - zh)}"


def test_both_navs_list_the_same_pages():
    assert _nav_pages("mkdocs.yml") == _nav_pages("mkdocs.en.yml")


@pytest.mark.parametrize(("tree", "other"), [(ZH, "en"), (EN, "zh")])
def test_no_page_links_into_the_other_tree(tree, other):
    leaks = []
    for page in tree.rglob("*.md"):
        for target in _LINK.findall(_FENCE.sub("", page.read_text(encoding="utf-8"))):
            if f"docs/{other}/" in target or target.startswith(f"../{other}/"):
                leaks.append(f"{page.relative_to(ROOT)} -> {target}")
    assert leaks == []


def test_english_pages_are_english():
    """Chinese may appear in code, in quoted program output and in the link to
    a page's Chinese original — not as the page's own prose."""
    mostly_chinese = []
    for page in EN.rglob("*.md"):
        prose = _FENCE.sub("", page.read_text(encoding="utf-8"))
        prose = re.sub(r"`[^`]*`", "", prose)
        cjk = sum(1 for ch in prose if "一" <= ch <= "鿿")
        words = len(re.findall(r"[A-Za-z]{2,}", prose))
        if cjk > max(40, words // 4):
            mostly_chinese.append(f"{page.relative_to(ROOT)} ({cjk} CJK / {words} words)")
    assert mostly_chinese == []


def test_english_generated_pages_match_the_code():
    """The English CLI tables are rendered from the same registry as the
    Chinese ones; a help text without an English entry fails here."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("sync_docs", ROOT / "scripts/dev/sync_docs.py")
    sync_docs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sync_docs)

    for target in ("cli-surface-en", "schema-en", "catalog-en"):
        path, expected, _ = sync_docs.TARGETS[target]()
        assert path.read_text(encoding="utf-8") == expected, f"{target} is stale"


def _doc_translations():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "doc_translations", ROOT / "scripts/dev/doc_translations.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_no_english_page_is_behind_its_chinese_original():
    """A Chinese edit without the matching English edit fails here; after
    translating, `python scripts/dev/doc_translations.py --mark <page>`."""
    assert _doc_translations().stale() == []


def test_translation_digest_tracks_prose_but_not_generated_columns(tmp_path, monkeypatch):
    module = _doc_translations()
    zh = tmp_path / "zh"
    (zh / "datasets").mkdir(parents=True)
    page = zh / "datasets" / "schema.md"
    page.write_text(
        "# 字段\n\n#### daily_bars\n\n| 列 | 类型 | 说明 |\n|--------|------|-------|\n"
        "| close | float64 | 收盘价 |\n| volume | int64 |  |\n\n说明文字。\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ZH", zh)
    before = module.digest("datasets/schema.md")

    # Code re-renders the table: a type changes, an undocumented column appears.
    text = page.read_text(encoding="utf-8")
    page.write_text(
        text.replace("| volume | int64 |  |", "| volume | float64 |  |\n| amount | float64 |  |"),
        encoding="utf-8",
    )
    assert module.digest("datasets/schema.md") == before

    # A person edits a description or the prose: the English page is now behind.
    page.write_text(
        page.read_text(encoding="utf-8").replace("收盘价", "未复权收盘价"), encoding="utf-8"
    )
    assert module.digest("datasets/schema.md") != before
