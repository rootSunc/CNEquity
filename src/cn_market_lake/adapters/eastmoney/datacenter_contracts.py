"""Inventory of EastMoney datacenter report → columns contracts.

Adapters keep their ``_REPORT`` / ``_COLUMNS`` constants as the runtime source
of truth; this module imports them so CI and the live page-1 probe have a
single place to iterate. When EM renames a column (``code=9501``), update the
adapter constant — the registry picks it up automatically — then re-run::

    uv run pytest -m network tests/unit/test_datacenter_live_contracts.py
"""

from __future__ import annotations

from dataclasses import dataclass

from cn_market_lake.adapters.eastmoney import capital as capital_mod
from cn_market_lake.adapters.eastmoney import consensus as consensus_mod
from cn_market_lake.adapters.eastmoney import corporate_actions as ca_mod
from cn_market_lake.adapters.eastmoney import earnings_disclosure as ed_mod
from cn_market_lake.adapters.eastmoney import economic_calendar as econ_mod
from cn_market_lake.adapters.eastmoney import fundamentals as fund_mod
from cn_market_lake.adapters.eastmoney import index_constituents as idx_mod
from cn_market_lake.adapters.eastmoney import institutional as inst_mod
from cn_market_lake.adapters.eastmoney import sectors as sectors_mod
from cn_market_lake.adapters.eastmoney import share_unlock as unlock_mod
from cn_market_lake.adapters.macro import indicators as macro_mod


@dataclass(frozen=True)
class DatacenterContract:
    """One ``fetch_datacenter(report, columns)`` call site."""

    name: str
    report: str
    columns: str
    # False for retired reports that still have an adapter stub.
    required: bool = True

    def column_set(self) -> frozenset[str]:
        return frozenset(c.strip() for c in self.columns.split(",") if c.strip())


def datacenter_contracts() -> tuple[DatacenterContract, ...]:
    """All known datacenter contracts (test inventory + live probe input)."""
    contracts: list[DatacenterContract] = [
        DatacenterContract(
            "corporate_actions",
            ca_mod._REPORT,
            ca_mod._COLUMNS,
        ),
        DatacenterContract(
            "share_unlock",
            unlock_mod._UNLOCK_REPORT,
            unlock_mod._UNLOCK_COLUMNS,
        ),
        DatacenterContract(
            "margin_trading",
            capital_mod._MARGIN_REPORT,
            capital_mod._MARGIN_COLUMNS,
        ),
        DatacenterContract(
            "northbound_holdings",
            capital_mod._NORTH_HOLD_REPORT,
            capital_mod._NORTH_HOLD_COLUMNS,
        ),
        DatacenterContract(
            "dragon_tiger",
            capital_mod._DRAGON_REPORT,
            capital_mod._DRAGON_COLUMNS,
        ),
        DatacenterContract(
            "block_trades",
            capital_mod._BLOCK_REPORT,
            capital_mod._BLOCK_COLUMNS,
        ),
        DatacenterContract(
            "institutional_holdings",
            inst_mod._HOLD_REPORT,
            inst_mod._HOLD_COLUMNS,
        ),
        DatacenterContract(
            "analyst_consensus",
            consensus_mod._CONSENSUS_REPORT,
            consensus_mod._CONSENSUS_COLUMNS,
        ),
        DatacenterContract(
            "earnings_disclosure",
            ed_mod._REPORT,
            ed_mod._COLUMNS,
        ),
        DatacenterContract(
            "index_constituents",
            idx_mod._REPORT,
            idx_mod._COLUMNS,
        ),
        # sectors.py and industry.py share this report/columns pair.
        DatacenterContract(
            "board_constituent",
            sectors_mod._BOARD_REPORT,
            sectors_mod._BOARD_COLUMNS,
        ),
        DatacenterContract(
            "macro_treasury",
            macro_mod._TREASURY_REPORT,
            macro_mod._TREASURY_COLUMNS,
        ),
        DatacenterContract(
            "macro_shibor",
            macro_mod._SHIBOR_REPORT,
            macro_mod._SHIBOR_COLUMNS,
        ),
        DatacenterContract(
            "macro_lpr",
            macro_mod._LPR_REPORT,
            macro_mod._LPR_COLUMNS,
        ),
        DatacenterContract(
            "economic_calendar",
            econ_mod._REPORT,
            econ_mod._COLUMNS,
            required=False,
        ),
    ]
    for report in fund_mod._REPORTS:
        contracts.append(
            DatacenterContract(
                f"financial_statement_items:{report.name}",
                report.name,
                report.columns,
            )
        )
    return tuple(contracts)


def required_datacenter_contracts() -> tuple[DatacenterContract, ...]:
    return tuple(c for c in datacenter_contracts() if c.required)
