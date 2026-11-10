"""Moved to ``cnequity.ops.event_schedules``; this name stays importable."""

import sys

from cnequity.ops import event_schedules as _moved

sys.modules[__name__] = _moved
