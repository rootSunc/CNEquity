"""index_bars must fail-loud on a partial symbol set (no watermark poison)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from cn_market_lake.adapters.tdx_protocol.client import TdxSourceError, fetch_index_bars


def test_fetch_index_bars_rejects_partial_symbol_set(monkeypatch):
    """One surviving index must not advance daily coverage for the other seven."""

    def fake_paginated(client, sym, start, end, **kwargs):
        if sym == "000852.SH":
            return [
                {
                    "symbol": sym,
                    "trade_date": start,
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 1,
                    "amount": 1.0,
                }
            ]
        raise RuntimeError(f"tdx down for {sym}")

    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.client.fetch_bars_paginated",
        fake_paginated,
    )
    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.client._quotes_client",
        lambda config=None: MagicMock(),
    )
    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.client._close_quotes_client",
        lambda client: None,
    )
    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.client.reset_tdx_server_cache",
        lambda: None,
    )
    # Exhaust retries quickly without sleeping on mock path.
    monkeypatch.setattr(
        "cn_market_lake.adapters.tdx_protocol.client._TDX_FETCH_ATTEMPTS",
        1,
    )

    with pytest.raises(TdxSourceError, match="000001.SH"):
        fetch_index_bars(date(2026, 7, 10), date(2026, 7, 10), allow_mock=False)
