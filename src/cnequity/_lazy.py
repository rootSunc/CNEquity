"""Import a heavy library when it is first used, not when its module loads.

``cne`` registers every pipeline step at start-up, which imports every
adapter. A handful of them read Excel or PDF files with pandas and pypdf, and
importing those two (with pyarrow, which pandas brings) cost every command a
noticeable pause, ``cne --help`` included, though most commands never parse a
spreadsheet.

The stand-ins keep the module-level names those adapters already used (``pd``,
``PdfReader``), so code and tests that replace them on the module still work.
"""

from __future__ import annotations

import importlib
from typing import Any


class _LazyModule:
    """``pd = lazy_module("pandas")``: ``pd.read_excel`` imports pandas on first use."""

    def __init__(self, name: str):
        object.__setattr__(self, "_lazy_name", name)

    def __getattr__(self, attribute: str) -> Any:
        return getattr(importlib.import_module(self._lazy_name), attribute)

    def __repr__(self) -> str:
        return f"<lazy module {self._lazy_name!r}>"


def lazy_module(name: str) -> Any:
    return _LazyModule(name)


def lazy_attribute(module: str, attribute: str) -> Any:
    """A callable that imports ``module.attribute`` the first time it is called."""

    def call(*args: Any, **kwargs: Any) -> Any:
        return getattr(importlib.import_module(module), attribute)(*args, **kwargs)

    call.__name__ = attribute
    call.__qualname__ = attribute
    call.__doc__ = f"``{module}.{attribute}``, imported on first call."
    return call
