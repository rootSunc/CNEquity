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
#: (test doubles, pickled configs) and for recorded retry scopes.
LEGACY_BACKFILL_ATTRS: dict[str, str] = {
    "_backfill": "active",
    "_backfill_start": "start",
    "_backfill_end": "end",
    "_backfill_symbols": "symbols",
    "_backfill_workers": "workers",
    "_backfill_refetch": "refetch",
}

_DEFAULT_WINDOW = BackfillWindow()
_MISSING = object()


def _declared(obj: Any, attr: str) -> Any:
    """*attr* as set on *obj* or declared on its class, without dynamic lookup.

    Plain ``getattr`` would ask a ``MagicMock`` config, which invents every
    attribute — and a made-up mock is truthy, switching every flag on.
    Properties are skipped: they compute a value, they do not hold one.
    """
    if attr in getattr(obj, "__dict__", {}):
        return obj.__dict__[attr]
    for cls in type(obj).__mro__:
        if attr in cls.__dict__:
            value = cls.__dict__[attr]
            return _MISSING if isinstance(value, property) else value
    return _MISSING


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
        value = _declared(config, attr)
        if value is not _MISSING:
            values[field_name] = _normalise(field_name, value)
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


# --------------------------------------------------------------- run flags
#
# One-off switches a command sets for a single run (repair modes, a forced
# reset, an alternate source) used to be more private attributes hung on
# Config — some declared, most not. They are one frozen value too, read with
# :func:`run_flags_of` and replaced with :func:`set_run_flags`.


@dataclass(frozen=True)
class RunFlags:
    """Per-run switches set by a command, read by the steps it runs."""

    #: ``cne backfill corporate_actions`` repair modes.
    ca_baostock_repair: bool = False
    ca_ths_repair: bool = False
    ca_payment_repair: bool = False
    ca_issuer_notice_only: bool = False
    #: Ex-dates to re-ask EastMoney about (``--ex-dates``); None when unused.
    ca_eastmoney_date_repair: tuple[date, ...] | None = None
    #: ``cne backfill daily_bars`` repair modes.
    bse_tip_repair: bool = False
    tdx_amount_repair: bool = False
    turnover_repair: bool = False
    turnover_peer_review: bool = False
    #: ``cne backfill sector_bars --force``: reset the checkpoint once.
    sector_bars_force: bool = False
    valuation_fill_em_outage: bool = False
    #: ``cne backfill futures_bars|option_bars``: refetch, source, batch id.
    derivatives_refresh: bool = False
    option_source: str | None = None
    derivative_batch: str | None = None
    commodity_via_eastmoney: bool = False
    #: ``cne mcp --live``: answer from live sources when there is no lake.
    mcp_live: bool = False


#: Legacy attribute → field, for older config-shaped objects and recorded
#: retry scopes that use the old names.
LEGACY_RUN_FLAG_ATTRS: dict[str, str] = {
    "_corporate_actions_baostock_repair": "ca_baostock_repair",
    "_corporate_actions_ths_repair": "ca_ths_repair",
    "_corporate_actions_payment_repair": "ca_payment_repair",
    "_corporate_actions_issuer_notice_only": "ca_issuer_notice_only",
    "_corporate_actions_eastmoney_date_repair": "ca_eastmoney_date_repair",
    "_bse_tip_repair": "bse_tip_repair",
    "_tdx_amount_repair": "tdx_amount_repair",
    "_turnover_repair": "turnover_repair",
    "_turnover_peer_review": "turnover_peer_review",
    "_sector_bars_force": "sector_bars_force",
    "_valuation_fill_em_outage": "valuation_fill_em_outage",
    "_derivatives_refresh": "derivatives_refresh",
    "_option_source": "option_source",
    "_derivative_batch": "derivative_batch",
    "_commodity_via_eastmoney": "commodity_via_eastmoney",
    "_mcp_live": "mcp_live",
}

_DEFAULT_FLAGS = RunFlags()
_FLAG_TEXT = {"option_source", "derivative_batch"}


def _normalise_flag(field_name: str, value: Any) -> Any:
    if field_name == "ca_eastmoney_date_repair":
        return None if value is None else tuple(value)
    if field_name in _FLAG_TEXT:
        return value
    return bool(value)


def run_flags_of(config: Any) -> RunFlags:
    """The run flags *config* carries (all off when it has none)."""
    flags = getattr(config, "run_flags", None)
    if isinstance(flags, RunFlags):
        return flags
    if config is None:
        return _DEFAULT_FLAGS
    values = {}
    for attr, field_name in LEGACY_RUN_FLAG_ATTRS.items():
        value = _declared(config, attr)
        if value is not _MISSING:
            values[field_name] = _normalise_flag(field_name, value)
    return replace(_DEFAULT_FLAGS, **values) if values else _DEFAULT_FLAGS


def set_run_flags(config: Any, **changes: Any) -> RunFlags:
    """Replace *config*'s run flags with one that has *changes* applied."""
    unknown = set(changes) - set(LEGACY_RUN_FLAG_ATTRS.values())
    if unknown:
        raise TypeError(f"unknown run flag(s): {', '.join(sorted(unknown))}")
    flags = replace(
        run_flags_of(config),
        **{name: _normalise_flag(name, value) for name, value in changes.items()},
    )
    object.__setattr__(config, "run_flags", flags)
    return flags
