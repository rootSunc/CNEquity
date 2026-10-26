"""Raise the open-file soft limit before a pipeline process does any work.

Polars' streaming engine holds a descriptor per partition file while it scans,
and ``daily_bars`` alone is over 6,000 day files. A process started by launchd
gets a soft limit of 256 and one started by cron or systemd usually 1024, so a
streaming plan over that dataset failed with ``os error 24`` from the schedule
while the same command passed in a terminal. Measured 2026-10-26: the
``adj_factors`` self-heal scan failed at 256 and 1024 and passed at 4096.

The hard limit is left alone; only the soft limit is lifted toward it, which
any unprivileged process may do. Child processes inherit the raised value.
"""

from __future__ import annotations

import logging
import sys

logger = logging.getLogger(__name__)

#: Comfortably above what one whole-lake streaming scan needs.
TARGET_OPEN_FILES = 65536

# macOS refuses a soft limit above kern.maxfilesperproc even when the hard
# limit reports unlimited; OPEN_MAX is the value it always accepts.
_DARWIN_OPEN_MAX = 10240


def raise_open_file_limit(target: int = TARGET_OPEN_FILES) -> int | None:
    """Lift the soft ``RLIMIT_NOFILE`` to *target* (or the hard limit if lower).

    Returns the soft limit in effect afterwards, or ``None`` where the platform
    has no such limit (Windows).
    """
    try:
        import resource
    except ImportError:
        return None

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    want = target if hard == resource.RLIM_INFINITY else min(target, hard)
    if soft != resource.RLIM_INFINITY and soft >= want:
        return soft
    candidates = [want]
    if sys.platform == "darwin":
        candidates.append(min(want, _DARWIN_OPEN_MAX))
    for value in candidates:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (value, hard))
        except (ValueError, OSError):
            continue
        return value
    logger.debug("could not raise the open-file soft limit from %s", soft)
    return soft
