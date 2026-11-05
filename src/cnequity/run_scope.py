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
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date
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


# ---------------------------------------------------------------- backfill
#
# A run's backfill window used to be six loose private attributes on Config
# (``_backfill``, ``_backfill_start``, ``_backfill_end``, ``_backfill_symbols``,
# ``_backfill_workers``, ``_backfill_refetch``), read through ~170
# ``getattr(config, "_backfill_…", default)`` calls whose names nothing
# checked and set piecemeal by CLI commands and the engine. They are now one
# frozen value: read it with :func:`backfill_of`, change it with
# :func:`set_backfill`, which swaps the whole object. A copied Config shares
# it safely, and a step can never observe a half-updated window.


@dataclass(frozen=True)
class BackfillWindow:
    """The fetch window one run was asked for. Immutable; replace, don't edit."""

    #: Fetch history rather than just the trade date.
    active: bool = False
    start: date | None = None
    end: date | None = None
    #: Restrict the fetch to these symbols; ``None`` means every symbol.
    symbols: tuple[str, ...] | None = None
    workers: int = 1
    #: Re-request days the lake already holds.
    refetch: bool = False

    def symbol_list(self) -> list[str] | None:
        """``symbols`` as a fresh list, for callers that build on it."""
        return None if self.symbols is None else list(self.symbols)


#: Legacy attribute → field, for objects that predate :class:`BackfillWindow`
#: (test doubles, pickled configs) and for Config's compatibility properties.
LEGACY_BACKFILL_ATTRS: dict[str, str] = {
    "_backfill": "active",
    "_backfill_start": "start",
    "_backfill_end": "end",
    "_backfill_symbols": "symbols",
    "_backfill_workers": "workers",
    "_backfill_refetch": "refetch",
}

_DEFAULT_WINDOW = BackfillWindow()


def _normalise(field_name: str, value: Any) -> Any:
    if field_name == "symbols" and value is not None:
        return tuple(value)
    if field_name in {"active", "refetch"}:
        return bool(value)
    if field_name == "workers":
        return 1 if value is None else int(value)
    return value


def backfill_of(config: Any) -> BackfillWindow:
    """The backfill window *config* carries (an empty window when it has none).

    Accepts anything config-shaped: a ``Config``, or an older object that
    still carries the loose ``_backfill*`` attributes.
    """
    window = getattr(config, "backfill_window", None)
    if isinstance(window, BackfillWindow):
        return window
    if config is None:
        return _DEFAULT_WINDOW
    values = {}
    for attr, field_name in LEGACY_BACKFILL_ATTRS.items():
        if attr in getattr(config, "__dict__", {}):
            values[field_name] = _normalise(field_name, config.__dict__[attr])
    return replace(_DEFAULT_WINDOW, **values) if values else _DEFAULT_WINDOW


def set_backfill(config: Any, **changes: Any) -> BackfillWindow:
    """Replace *config*'s backfill window with one that has *changes* applied."""
    unknown = set(changes) - set(LEGACY_BACKFILL_ATTRS.values())
    if unknown:
        raise TypeError(f"unknown backfill field(s): {', '.join(sorted(unknown))}")
    window = replace(
        backfill_of(config),
        **{name: _normalise(name, value) for name, value in changes.items()},
    )
    object.__setattr__(config, "backfill_window", window)
    return window


@contextmanager
def backfill_override(config: Any, **changes: Any) -> Iterator[BackfillWindow]:
    """Apply *changes* for the block, then put the previous window back."""
    previous = backfill_of(config)
    window = set_backfill(config, **changes)
    try:
        yield window
    finally:
        object.__setattr__(config, "backfill_window", previous)
