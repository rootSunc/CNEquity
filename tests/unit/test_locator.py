"""One rule for where a dataset's logical directory lives."""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

import cnequity
from cnequity.config import Config
from cnequity.domain.datasets import DATASETS
from cnequity.storage.locator import layer_name, layer_root, logical_root


@pytest.mark.parametrize("name", sorted(DATASETS))
def test_every_dataset_resolves_to_its_registered_layer(tmp_path, name):
    cfg = Config(data_root=tmp_path)
    spec = DATASETS[name]
    expected = cfg.derived_root if spec.layer == "derived" else cfg.curated_root
    assert layer_name(name) == ("derived" if spec.layer == "derived" else "curated")
    assert layer_root(cfg, name) == expected
    assert logical_root(cfg, name) == expected / name
    # A spec and its name are interchangeable.
    assert logical_root(cfg, spec) == logical_root(cfg, name)


def test_any_object_with_both_roots_is_accepted(tmp_path):
    view = SimpleNamespace(curated_root=tmp_path / "c", derived_root=tmp_path / "d")
    assert logical_root(view, "daily_bars") == tmp_path / "c" / "daily_bars"
    assert logical_root(view, "adj_factors") == tmp_path / "d" / "adj_factors"


def test_unknown_dataset_is_an_error(tmp_path):
    with pytest.raises(KeyError):
        logical_root(Config(data_root=tmp_path), "no_such_dataset")


_INLINE_RULE = re.compile(
    r"derived_root\s+if\s+[^\n]*layer\s*==\s*[\"']derived[\"']"
    r"|[\"']derived[\"']\s+if\s+[^\n]*layer\s*==\s*[\"']derived[\"']\s+else\s+[\"']curated[\"']"
)


def test_no_module_reimplements_the_layer_rule():
    """The rule was copied into ~20 modules; keep it in storage/locator.py."""
    package = Path(cnequity.__file__).parent
    offenders = [
        f"{path.relative_to(package)}:{number}"
        for path in sorted(package.rglob("*.py"))
        if path.name != "locator.py"
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if _INLINE_RULE.search(line)
    ]
    assert offenders == [], "use cnequity.storage.locator instead: " + ", ".join(offenders)
