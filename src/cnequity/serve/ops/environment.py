"""Moved to ``cnequity.ops.environment``; this name stays importable."""

import sys

from cnequity.ops import environment as _moved

sys.modules[__name__] = _moved
