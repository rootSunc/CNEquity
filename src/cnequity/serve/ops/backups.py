"""Moved to ``cnequity.ops.backups``; this name stays importable."""

import sys

from cnequity.ops import backups as _moved

sys.modules[__name__] = _moved
