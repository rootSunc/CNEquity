"""Keep new file replacements on the Windows-safe retry helpers.

Windows denies ``os.replace``/``os.rename`` over a file another handle has
open (an unlocked reader, AV, the search indexer, a sync client). New code
should go through ``storage.atomic.replace_with_retry`` or
``write_json_atomic``/``write_parquet_atomic``; a two-step directory swap
uses ``swap_with_backup``.
"""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

import cnequity

PACKAGE_ROOT = Path(cnequity.__file__).resolve().parent

ALLOWED_DIRECT_REPLACES = {
    # The retry helpers themselves.
    "file_replace.py": 1,
}


def _direct_replace_counts() -> Counter[str]:
    counts: Counter[str] = Counter()
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            func = getattr(node, "func", None)
            if (
                isinstance(node, ast.Call)
                and isinstance(func, ast.Attribute)
                and func.attr in {"replace", "rename"}
                and isinstance(func.value, ast.Name)
                and func.value.id == "os"
            ):
                counts[path.relative_to(PACKAGE_ROOT).as_posix()] += 1
    return counts


def test_no_new_direct_os_replace_outside_retry_helpers():
    counts = _direct_replace_counts()
    grown = {
        name: count
        for name, count in counts.items()
        if count > ALLOWED_DIRECT_REPLACES.get(name, 0)
    }
    assert not grown, (
        "Direct os.replace/os.rename fails on Windows while another handle has "
        f"the destination open; use storage.atomic.replace_with_retry: {grown}"
    )


def test_direct_replace_allowlist_has_no_stale_entries():
    counts = _direct_replace_counts()
    stale = {
        name: allowed
        for name, allowed in ALLOWED_DIRECT_REPLACES.items()
        if counts.get(name, 0) < allowed
    }
    assert not stale, f"Lower these counts after migrating their sites: {stale}"
