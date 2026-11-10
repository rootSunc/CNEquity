"""Moved to ``cnequity.ops.keys``; this name stays importable."""

import sys

from cnequity.ops import keys as _moved

sys.modules[__name__] = _moved
