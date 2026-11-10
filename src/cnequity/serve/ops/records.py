"""Moved to ``cnequity.ops.records``; this name stays importable."""

import sys

from cnequity.ops import records as _moved

sys.modules[__name__] = _moved
