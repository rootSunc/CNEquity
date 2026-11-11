"""``cne`` starts without importing pandas or pypdf; the adapters load them on use."""

from __future__ import annotations

import subprocess
import sys

from cnequity._lazy import lazy_attribute, lazy_module

HEAVY = ("pandas", "pypdf", "pyarrow")


def test_cli_import_leaves_pandas_and_pypdf_unloaded():
    probe = (
        f"import sys, cnequity.cli.main\nprint(','.join(m for m in {HEAVY!r} if m in sys.modules))"
    )
    loaded = subprocess.run(
        [sys.executable, "-c", probe], check=True, capture_output=True, text=True
    ).stdout.strip()
    assert loaded == "", f"cne start-up imports {loaded}; use cnequity._lazy instead"


def test_lazy_module_resolves_attributes_on_use():
    json = lazy_module("json")
    assert json.loads("[1]") == [1]


def test_lazy_attribute_calls_the_real_callable():
    dumps = lazy_attribute("json", "dumps")
    assert dumps.__name__ == "dumps"
    assert dumps([1], separators=(",", ":")) == "[1]"
