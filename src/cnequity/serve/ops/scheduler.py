"""Moved to ``cnequity.ops.scheduler``; this name stays importable."""

import sys

from cnequity.ops import scheduler as _moved

sys.modules[__name__] = _moved
