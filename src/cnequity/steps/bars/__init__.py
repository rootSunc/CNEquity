"""L1 bar steps: ``daily_bars``, ``index_bars``, and the history and delisted sweeps.

One package instead of one 6,000-line module, split along the seams the code
already had. Each module calls a sibling's helpers through the module
(``gapfill._gapfill_tip_via_clist(...)``), so a test patches a helper where it
is defined. Only the public step and repair entry points are re-exported here;
reach private helpers through their module.

* ``bar_scope``, ``etf_debts`` — which symbols a run owes, and ownership;
* ``daily``, ``finish``, ``admission``, ``staging`` — the ``daily_bars`` step;
* ``tip_fetch``, ``gapfill``, ``gapfill_chain``, ``sina_bars`` — fetching the
  newest session and filling gaps;
* ``amounts``, ``turnover`` — repairs of stored amounts and turnover;
* ``index_history`` — ``index_bars``, ``daily_bars_history``, ``daily_bars_delisted``;
* ``ths_official`` — the paid THS API's deep-history repair and snapshots.
"""

from __future__ import annotations

# steps.common imports the orchestrator package, whose engine imports
# steps.common back. Load the orchestrator first, as the single module did by
# import order, so the submodules below find steps.common whole. Any
# orchestrator submodule runs the package's __init__; outcomes is the low one.
import cnequity.orchestrator.outcomes  # noqa: F401

# Importing a module registers its steps; keep every module listed.
from cnequity.steps.bars import (  # noqa: F401
    _base,
    admission,
    amounts,
    bar_scope,
    daily,
    etf_debts,
    finish,
    gapfill,
    gapfill_chain,
    index_history,
    sina_bars,
    staging,
    ths_official,
    tip_fetch,
    turnover,
)
from cnequity.steps.bars.admission import in_ingest_universe_symbol
from cnequity.steps.bars.amounts import (
    repair_bse_tip_amounts_from_curated,
    repair_sina_history_amounts_from_tdx,
)
from cnequity.steps.bars.daily import step_daily_bars
from cnequity.steps.bars.gapfill_chain import MULTIDAY_GAPFILL_CHAIN, GapFillStage
from cnequity.steps.bars.index_history import (
    step_daily_bars_delisted,
    step_daily_bars_history,
    step_index_bars,
    sweep_stock_bars_planned,
)
from cnequity.steps.bars.sina_bars import fetch_bars_via_sina
from cnequity.steps.bars.ths_official import (
    repair_deep_history_ths_official,
    snapshot_daily_bars_ths_official,
)
from cnequity.steps.bars.turnover import repair_daily_bar_turnover

__all__ = [
    "in_ingest_universe_symbol",
    "repair_bse_tip_amounts_from_curated",
    "repair_sina_history_amounts_from_tdx",
    "repair_daily_bar_turnover",
    "fetch_bars_via_sina",
    "GapFillStage",
    "MULTIDAY_GAPFILL_CHAIN",
    "step_daily_bars",
    "step_daily_bars_delisted",
    "step_daily_bars_history",
    "step_index_bars",
    "sweep_stock_bars_planned",
    "repair_deep_history_ths_official",
    "snapshot_daily_bars_ths_official",
]
