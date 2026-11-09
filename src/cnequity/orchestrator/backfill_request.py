"""What one `cne backfill` run is asked to do, and whether it may.

``cne backfill`` and the ``cne repair`` commands built on it used to share one
Click function: the repairs passed nineteen hidden keyword arguments into it
through ``ctx.invoke``, and the options it accepted were checked dataset by
dataset in its body. Now each command builds a :class:`BackfillRequest`, and
:func:`validate` answers the one question in one place. What a dataset accepts
(``--source``, ``--refetch``, ``--workers``, how ``--symbols`` reaches the
step) is declared on its ``DatasetSpec``; a repair mode is a row of
:data:`REPAIRS`.

Nothing here touches a config or the lake. :func:`run_flags` and
:func:`window` say what the request puts on the run; the CLI applies them.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from cnequity.domain.datasets import DATASETS
from cnequity.i18n import _

#: Fetched from exchange files with the derivatives switches turned on.
DERIVATIVE_DATASETS = frozenset(
    {"futures_bars", "option_bars", "futures_contracts", "option_contracts", "futures_minute_bars"}
)


class BackfillRequestError(ValueError):
    """A request the command refuses; ``param_hint`` names the bad option."""

    def __init__(self, message: str, param_hint: str | None = None):
        super().__init__(message)
        self.param_hint = param_hint


@dataclass(frozen=True)
class RepairMode:
    """A `cne repair` command that runs on the backfill machinery."""

    dataset: str
    #: The label `--plan` prints under ``repair_modes``.
    plan_label: str
    #: Run flags it sets (see ``cnequity.run_scope.RunFlags``).
    flags: Mapping[str, Any] = field(default_factory=dict)


#: Every repair mode, by the name its command passes.
REPAIRS: dict[str, RepairMode] = {
    "payment-dates": RepairMode(
        "corporate_actions", "payment-date-repair", {"ca_payment_repair": True}
    ),
    "issuer-notices": RepairMode(
        "corporate_actions",
        "issuer-notice-repair",
        {"ca_payment_repair": True, "ca_issuer_notice_only": True},
    ),
    "baostock-actions": RepairMode(
        "corporate_actions", "baostock-repair", {"ca_baostock_repair": True}
    ),
    "ths-actions": RepairMode("corporate_actions", "ths-repair", {"ca_ths_repair": True}),
    # Its dates are the request's ``ex_dates``.
    "ex-dates": RepairMode("corporate_actions", "eastmoney-date-repair"),
    "bar-amounts": RepairMode("daily_bars", "tdx-amount-repair", {"tdx_amount_repair": True}),
    "bar-turnover": RepairMode("daily_bars", "turnover-repair", {"turnover_repair": True}),
    "bse-tip": RepairMode("daily_bars", "bse-tip-repair", {"bse_tip_repair": True}),
    "valuation-outage": RepairMode(
        "valuation_metrics", "fill-em-outage", {"valuation_fill_em_outage": True}
    ),
}


@dataclass(frozen=True)
class BackfillRequest:
    """One backfill, as asked for. Dates are parsed; nothing is checked yet."""

    dataset: str
    start: date | None = None
    end: date | None = None
    #: Upper-cased and de-duplicated by the caller; None means every symbol.
    symbols: tuple[str, ...] | None = None
    outstanding: bool = False
    workers: int = 1
    source: str | None = None
    refetch: bool = False
    exchanges: tuple[str, ...] = ()
    profile: str = "default"
    plan: bool = False
    #: A key of :data:`REPAIRS`, or None for a plain backfill.
    repair: str | None = None
    #: ``ex-dates`` repair: the ex-dates to ask EastMoney about.
    ex_dates: tuple[date, ...] = ()
    #: ``bar-turnover`` repair: review price conflicts against peers.
    peer_review: bool = False


def _spec(request: BackfillRequest):
    return DATASETS[request.dataset]


def _listed(names: list[str]) -> str:
    return ", ".join(names)


def validate(request: BackfillRequest) -> None:
    """Raise :class:`BackfillRequestError` when the request cannot run."""
    spec = _spec(request)
    if request.repair is not None:
        mode = REPAIRS[request.repair]
        if mode.dataset != request.dataset:
            raise BackfillRequestError(
                f"repair {request.repair} is for {mode.dataset}, not {request.dataset}"
            )
    if request.source is not None and request.source not in spec.backfill_source_overrides:
        if not spec.backfill_source_overrides:
            offered = [
                f"{name} ({', '.join(s.backfill_source_overrides)})"
                for name, s in DATASETS.items()
                if s.backfill_source_overrides
            ]
            raise BackfillRequestError(
                _("--source is supported for {datasets}").format(datasets=_listed(offered))
            )
        raise BackfillRequestError(
            _("{dataset} sources are {sources}").format(
                dataset=request.dataset, sources=", ".join(spec.backfill_source_overrides)
            ),
            param_hint="--source",
        )
    if request.refetch and spec.backfill_refetch is None:
        offered = [name for name, s in DATASETS.items() if s.backfill_refetch]
        raise BackfillRequestError(
            _("--refetch applies to {datasets}").format(datasets=_listed(offered))
        )
    if request.profile == "delisted":
        if request.dataset != "daily_bars":
            raise BackfillRequestError(_("--profile delisted only applies to daily_bars"))
        if (
            request.end
            or request.symbols is not None
            or request.outstanding
            or request.exchanges
            or request.workers != 1
        ):
            raise BackfillRequestError(
                _("--profile delisted only accepts --start, --plan and --config")
            )
        return
    derivatives = request.dataset in DERIVATIVE_DATASETS
    if request.exchanges and not derivatives:
        raise BackfillRequestError(_("--exchange / --refresh only apply to derivatives datasets"))
    if derivatives:
        if request.outstanding:
            raise BackfillRequestError(
                _(
                    "Derivatives debts are retried persistently by the daily update; give --start / --end to repair a historical range"
                )
            )
        if request.dataset == "futures_minute_bars" and (request.start or request.end):
            raise BackfillRequestError(
                _(
                    "futures_minute_bars can only collect a recent window and cannot request history by --start / --end"
                )
            )
        if request.symbols is not None and request.dataset != "futures_minute_bars":
            raise BackfillRequestError(
                _(
                    "Derivatives daily bars are fetched from exchange files; use --exchange, --symbols is not supported"
                )
            )
    if request.workers < 1:
        raise BackfillRequestError(_("--workers must be at least 1"))
    if request.workers > 1 and not spec.backfill_parallel_dates:
        raise BackfillRequestError(
            _(
                "--workers > 1 is only supported for margin_trading for now; other backfills use a single date-advancing lane"
            )
        )
    if request.start and request.end and request.start > request.end:
        # Transposing the two used to cost a full network sweep: the walk had
        # no days in it, the step raised, the engine logged the traceback, and
        # the command still printed status=success with rows_written=0.
        raise BackfillRequestError(_("--start must be on or before --end"))
    if request.repair == "ex-dates" and not request.ex_dates:
        raise BackfillRequestError(_("--eastmoney-date-repair needs --ex-dates"))
    if request.outstanding:
        if request.plan:
            raise BackfillRequestError(
                _("--plan needs a dataset scope; --outstanding repairs from the debt ledger")
            )
        if request.symbols is not None or request.start or request.end:
            raise BackfillRequestError(
                _(
                    "--outstanding takes its scope from the debt ledger; drop --symbols/--start/--end"
                )
            )


def run_flags(request: BackfillRequest) -> dict[str, Any]:
    """The ``RunFlags`` fields the request sets (derivative batch ids excluded)."""
    spec = _spec(request)
    flags: dict[str, Any] = {}
    if request.source and request.dataset == "option_bars":
        flags["option_source"] = request.source
    if request.dataset in DERIVATIVE_DATASETS:
        flags["derivatives_refresh"] = request.refetch and spec.backfill_refetch == "derivatives"
    if request.dataset == "sector_bars":
        flags["sector_bars_force"] = request.refetch and spec.backfill_refetch == "checkpoint"
    if request.repair is not None:
        flags.update(REPAIRS[request.repair].flags)
        if request.repair == "ex-dates":
            flags["ca_eastmoney_date_repair"] = sorted(set(request.ex_dates))
        if request.repair == "bar-turnover":
            flags["turnover_peer_review"] = request.peer_review
    return flags


def window(request: BackfillRequest) -> dict[str, Any]:
    """The ``BackfillWindow`` fields the request sets."""
    spec = _spec(request)
    values: dict[str, Any] = {
        "refetch": bool(request.refetch and spec.backfill_refetch == "scope"),
        "workers": request.workers,
    }
    if request.symbols is not None and spec.backfill_symbols_in_window:
        values["symbols"] = list(request.symbols)
    if request.start:
        values["start"] = request.start
    if request.end:
        values["end"] = request.end
    return values


def plan_modes(request: BackfillRequest) -> list[str]:
    """The ``repair_modes`` labels `--plan` prints."""
    modes = [REPAIRS[request.repair].plan_label] if request.repair else []
    if request.refetch and _spec(request).backfill_refetch == "checkpoint":
        modes.append("force")
    return modes
