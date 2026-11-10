"""Moved to ``cnequity.ops.plan``; this name stays importable."""

import sys

from cnequity.ops import plan as _moved

sys.modules[__name__] = _moved
