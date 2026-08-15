"""Live page-1 probe: EastMoney has not renamed our contracted columns.

Run separately (needs network / may hit rate limits)::

    uv run pytest -m network tests/unit/test_datacenter_live_contracts.py -q
"""

from __future__ import annotations

import pytest

from cn_market_lake.adapters.eastmoney.datacenter import (
    EastMoneyDatacenterError,
    fetch_datacenter,
)
from cn_market_lake.adapters.eastmoney.datacenter_contracts import (
    required_datacenter_contracts,
)
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient


@pytest.mark.network
@pytest.mark.parametrize(
    "contract",
    required_datacenter_contracts(),
    ids=lambda c: c.name,
)
def test_live_datacenter_contract_page1(contract):
    """One row is enough: success without 9501, and keys cover the contract."""
    with EastMoneyClient() as client:
        try:
            rows = fetch_datacenter(
                client,
                contract.report,
                contract.columns,
                page_size=1,
                max_retries=2,
                retry_backoff_seconds=1.0,
            )
        except EastMoneyDatacenterError as exc:
            pytest.fail(f"{contract.name} ({contract.report}): {exc}")

    if not rows:
        # Empty is allowed (filter-less page may be vacant); schema already
        # accepted. Keys check only applies when EM returns a row.
        return
    keys = {str(k) for k in rows[0]}
    missing = contract.column_set() - keys
    assert not missing, (
        f"{contract.name} ({contract.report}): response missing columns {sorted(missing)}; "
        f"got {sorted(keys)}"
    )
