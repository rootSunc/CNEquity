"""Moved to ``cnequity.ops.job``; this name stays importable and runnable.

A job started before the move runs ``python -m cnequity.serve.ops.job``;
new jobs start ``cnequity.ops.job``.
"""

import sys

from cnequity.ops import job as _moved

if __name__ == "__main__":
    raise SystemExit(_moved.main())

sys.modules[__name__] = _moved
