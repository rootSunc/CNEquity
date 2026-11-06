"""The dashboard's dataset label table must keep up with the registry."""

import re
from pathlib import Path

from cnequity.domain.datasets import DATASETS

_I18N = Path(__file__).resolve().parents[2] / "frontend" / "src" / "i18n.js"


def _labelled_datasets() -> set[str]:
    source = _I18N.read_text(encoding="utf-8")
    start = source.index("const DATASETS = {")
    block = source[start : source.index("};", start)]
    return set(re.findall(r"^\s*([a-z0-9_]+):\s*\[", block, re.MULTILINE))


def test_every_registered_dataset_has_a_dashboard_label():
    missing = sorted(set(DATASETS) - _labelled_datasets())
    assert not missing, f"add these to DATASETS in frontend/src/i18n.js: {missing}"
