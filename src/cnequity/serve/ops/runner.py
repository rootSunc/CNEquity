"""Moved to ``cnequity.ops.runner``; this name stays importable."""

import sys

from cnequity.ops import runner as _moved

sys.modules[__name__] = _moved
