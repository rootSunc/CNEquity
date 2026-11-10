"""Moved to ``cnequity.ops.settings``; this name stays importable."""

import sys

from cnequity.ops import settings as _moved

sys.modules[__name__] = _moved
