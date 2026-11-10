"""Moved to ``cnequity.ops.scheduled``; this name stays importable and runnable.

An installed OS timer runs ``python -m cnequity.serve.ops.scheduled``, and
``cne schedule status`` compares that command exactly, so the timer keeps it.
"""

import sys

from cnequity.ops import scheduled as _moved

if __name__ == "__main__":
    raise SystemExit(_moved.main())

sys.modules[__name__] = _moved
