"""The dataset counts quoted in prose, and where they are quoted.

Adding a dataset used to mean finding every "86 个数据集" by hand: the README
pair, the home pages, the guides, the comparison pages, and the core count in
the runbooks. Two of them still said 55 when this was written.
``sync_docs.py`` now rewrites the numbers from the registry; this module holds
the patterns so ``doc_translations.py`` can leave them out of the digest — a
count that follows the registry is not a change to translate.

Each pattern captures the number only, so the wording around it stays
hand-written. A listed file whose patterns all stop matching fails the sync:
reworded prose must be listed again rather than silently go stale.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

#: ``{count name: pattern}``; group 1 is the number.
PATTERNS: dict[str, re.Pattern[str]] = {
    "total": re.compile(
        r"(?<![\d.,])(\d+)(?=\*{0,2} ?(?:个数据集|datasets\b)"
        r"|</strong><span>(?:登记数据集|registered datasets))"
    ),
    # The home page's metric block says 采集 / collected for curated.
    "curated": re.compile(r"(?<![\d.,])(\d+)(?= 个 curated| curated\b| 个采集| collected\b)"),
    "derived": re.compile(r"(?<![\d.,])(\d+)(?= 个 derived| derived\b| 个派生)"),
    "core": re.compile(r"(?<![\d.,])(\d+)(?= 个核心档数据集| core datasets\b)"),
}

#: Files whose prose quotes the counts, relative to the repository root.
FILES = (
    "README.md",
    "README.en.md",
    "docs/zh/index.md",
    "docs/en/index.md",
    "docs/zh/guides/free-a-share-data.md",
    "docs/en/guides/free-a-share-data.md",
    "docs/zh/operations/runbook.md",
    "docs/en/operations/runbook.md",
    "docs/zh/architecture/comparison.md",
    "docs/en/architecture/comparison.md",
)


def render(text: str, counts: Mapping[str, int]) -> tuple[str, int]:
    """*text* with every quoted count replaced, and how many were found."""
    found = 0
    for name, pattern in PATTERNS.items():
        text, n = pattern.subn(str(counts[name]), text)
        found += n
    return text, found


def mask(text: str) -> str:
    """*text* with the quoted counts blanked, for the translation digest."""
    for pattern in PATTERNS.values():
        text = pattern.sub("N", text)
    return text
