"""Moved to ``cnequity.ops.scheduler_backend``; this name stays importable."""

import sys

from cnequity.ops import scheduler_backend as _moved

sys.modules[__name__] = _moved
