"""Moved to ``cnequity.ops.catalog``; this name stays importable."""

import sys

from cnequity.ops import catalog as _moved

sys.modules[__name__] = _moved
