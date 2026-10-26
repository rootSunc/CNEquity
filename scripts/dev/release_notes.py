"""Render GitHub Release notes for a tag from its CHANGELOG.md section.

Relative links are pinned to the tag so they keep working from the release page.

A section heading may carry a headline after the date,
``## [0.20.0] — 2026-10-25 · 批量时点资料``; it becomes the release title
(``--title``) and the first line of the notes, so the release page says what
changed before the install command.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_URL = "https://github.com/rootSunc/CNEquity"
_LINK = re.compile(r"\]\((?!https?://|#|mailto:)([^)\s]+)\)")


def _heading(changelog: str, version: str) -> re.Match[str]:
    heading = re.compile(rf"^## \[{re.escape(version)}\]([^\n]*)\n", re.MULTILINE)
    match = heading.search(changelog)
    if match is None:
        raise ValueError(f"CHANGELOG.md has no section for {version}")
    return match


def headline(changelog: str, version: str) -> str:
    _, _, text = _heading(changelog, version).group(1).partition(" · ")
    return text.strip()


def title(changelog: str, version: str) -> str:
    text = headline(changelog, version)
    return f"{version} · {text}" if text else version


def changelog_section(changelog: str, version: str) -> str:
    match = _heading(changelog, version)
    following = re.compile(r"^## ", re.MULTILINE).search(changelog, match.end())
    end = following.start() if following else len(changelog)
    body = changelog[match.end() : end].strip()
    if not body:
        raise ValueError(f"CHANGELOG.md section for {version} is empty")
    return body


def render(changelog: str, version: str) -> str:
    tag = f"v{version}"
    body = _LINK.sub(
        lambda m: f"]({REPO_URL}/blob/{tag}/{m.group(1)})", changelog_section(changelog, version)
    )
    text = headline(changelog, version)
    lead = f"**{text}**\n\n" if text else ""
    return (
        f"{lead}CNEquity {version} 已发布至 [PyPI](https://pypi.org/project/cnequity/{version}/)。"
        "开源 A 股本地数据湖，用于量化研究、回测与 AI Agent。\n\n"
        f"```bash\npython -m pip install --upgrade cnequity=={version}\n```\n\n{body}\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag", help="release tag, e.g. v0.17.0")
    parser.add_argument("--changelog", type=Path, default=Path("CHANGELOG.md"))
    parser.add_argument("--title", action="store_true", help="print the release title only")
    args = parser.parse_args()
    if not re.fullmatch(r"v\d+\.\d+\.\d+", args.tag):
        print(f"not a stable release tag: {args.tag}", file=sys.stderr)
        return 2
    try:
        changelog = args.changelog.read_text(encoding="utf-8")
        version = args.tag[1:]
        sys.stdout.write(title(changelog, version) if args.title else render(changelog, version))
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
