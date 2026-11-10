"""Moved to ``cnequity.ops.preflight``; this name stays importable."""

import sys

from cnequity.ops import preflight as _moved

sys.modules[__name__] = _moved
