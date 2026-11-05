"""Atomic initialisation of run-scoped state that steps share through ``Config``.

Some caches are deliberately shared by every step of one run — the exchange
hosts a derivatives run found blocked, the suspensions ``daily_bars`` learned
before they reach curated — and they travel on the ``Config`` object because
that is the one thing every step and adapter already receives.

Steps in one dependency level run on threads that share that object. The
pattern each of them used, "if the marker is not this run, reset the caches
and set the marker", is a check-then-act: two parallel steps could both see a
stale marker, and the second reset would discard whatever the first had
already recorded (a blocked host is then requested again). This module makes
that one step atomic; the caches themselves stay where callers look for them.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from typing import Any

_LOCK = threading.Lock()


def ensure_run_scoped(
    config: Any,
    marker: str,
    scope: object,
    factories: Mapping[str, Callable[[], Any]],
) -> None:
    """Initialise *factories*' attributes on *config* once per *scope*.

    When ``config.<marker>`` already equals *scope*, nothing changes and every
    caller of this scope keeps sharing the same objects. Otherwise each
    attribute is replaced by a fresh ``factory()`` and the marker is set last,
    all under one process-wide lock, so no caller can observe the marker for a
    scope whose caches are not yet in place.
    """
    with _LOCK:
        if getattr(config, marker, None) == scope:
            return
        for name, factory in factories.items():
            setattr(config, name, factory())
        setattr(config, marker, scope)
