"""Which English pages are behind their Chinese originals.

Each page in ``docs/en`` records a digest of the ``docs/zh`` page it was
translated from, in ``scripts/dev/i18n/doc_sources.json``. When the Chinese
page changes, the digests differ and the English page is reported stale until
someone updates the translation and marks it:

    python scripts/dev/doc_translations.py                      # list stale pages
    python scripts/dev/doc_translations.py --mark reference/mcp.md

The digest skips what code generates on both sides, so a code change that
re-renders a table does not ask for a translation: the CLI option and
side-effect pages entirely, the derivative capability table in sources.md, the
column names and types in schema.md (its hand-written descriptions count), and
the dataset counts ``sync_docs.py`` keeps in step with the registry.
``tests/unit/test_docs_i18n.py`` runs the check.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import doc_counts  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
ZH = REPO_ROOT / "docs" / "zh"
EN = REPO_ROOT / "docs" / "en"
MANIFEST = Path(__file__).resolve().parent / "i18n" / "doc_sources.json"

# Generated from code in both languages; nothing to translate by hand.
GENERATED = {"reference/cli-options.md", "reference/cli-surface.md"}
# Not a translation: the Chinese page includes CHANGELOG.md, the English one points to it.
UNPAIRED = {"changelog.md", "datasets/README.md"}
_DERIVATIVES = re.compile(
    r"<!-- derivative-capabilities:start -->.*?<!-- derivative-capabilities:end -->", re.S
)


def _translatable(page: str, text: str) -> str:
    text = doc_counts.mask(text)
    if page == "datasets/sources.md":
        text = _DERIVATIVES.sub("", text)
    if page == "datasets/schema.md":
        # Column tables: keep only the hand-written description cells.
        kept = []
        in_table = False
        for line in text.split("\n"):
            if line.startswith("| 列 "):
                in_table = True
                continue
            if in_table and line.startswith("|"):
                cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
                if len(cells) > 2 and cells[2] and not set(cells[2]) <= {"-"}:
                    kept.append(cells[2])
                continue
            in_table = False
            kept.append(line)
        text = "\n".join(kept)
    return text


def digest(page: str) -> str:
    text = (ZH / page).read_text(encoding="utf-8")
    return hashlib.sha256(_translatable(page, text).encode("utf-8")).hexdigest()


def pages() -> list[str]:
    found = {p.relative_to(EN).as_posix() for p in EN.rglob("*.md")}
    return sorted(found - GENERATED - UNPAIRED)


def load() -> dict[str, str]:
    return json.loads(MANIFEST.read_text(encoding="utf-8")) if MANIFEST.exists() else {}


def stale() -> list[str]:
    recorded = load()
    return [page for page in pages() if recorded.get(page) != digest(page)]


def mark(targets: list[str]) -> None:
    recorded = load()
    for page in targets:
        if page not in pages():
            raise SystemExit(f"{page}: not a translated page under docs/en")
        recorded[page] = digest(page)
    MANIFEST.write_text(
        json.dumps(dict(sorted(recorded.items())), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mark", nargs="+", metavar="PAGE", help="record PAGE as up to date")
    args = parser.parse_args()
    if args.mark:
        mark(args.mark)
        return 0
    behind = stale()
    for page in behind:
        print(f"docs/en/{page} is behind docs/zh/{page}")
    if behind:
        print(
            "\nUpdate the translation, then: python scripts/dev/doc_translations.py --mark <page>"
        )
    return int(bool(behind))


if __name__ == "__main__":
    raise SystemExit(main())
