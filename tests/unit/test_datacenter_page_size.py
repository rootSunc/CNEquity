"""pageSize is capped per report, not globally.

The 500 clamp is right for most reports and wrong for RPT_BOARD_CONSTITUENT: at
500 its ~92k rows are 185 pages, past the pageNumber cap, so sector_members
failed every capital run. What must hold is that opting out of the clamp is
safe — a report that answers a big pageSize with a short page still raises.
"""

from __future__ import annotations

import re
from datetime import date

import pytest

from cn_market_lake.adapters.eastmoney.datacenter import (
    _MAX_PAGE_NUMBER,
    EastMoneyDatacenterError,
    fetch_datacenter,
)
from cn_market_lake.adapters.eastmoney.sectors import fetch_sector_members

_BOARDS_PER_SYMBOL = 18


class FakeDatacenter:
    """Serves a table, enforcing the pageNumber cap and a per-report pageSize."""

    def __init__(self, rows: list[dict], *, honors_page_size: bool = True):
        self.rows = rows
        self.honors_page_size = honors_page_size
        self.page_sizes: list[int] = []

    def get(self, url: str, **kwargs):
        page = int(re.search(r"pageNumber=(\d+)", url).group(1))
        requested = int(re.search(r"pageSize=(\d+)", url).group(1))
        self.page_sizes.append(requested)
        if page > _MAX_PAGE_NUMBER:
            return _Resp({"success": False, "message": "服务器繁忙", "code": 9701})
        # A report that ignores the request serves 500 and counts pages at 500,
        # exactly as RPT_SHAREBONUS_DET does.
        size = requested if self.honors_page_size else min(requested, 500)
        start = (page - 1) * size
        return _Resp(
            {
                "success": True,
                "result": {
                    "data": self.rows[start : start + size],
                    "count": len(self.rows),
                    "pages": (len(self.rows) + size - 1) // size,
                },
            }
        )


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload

    def close(self):
        return None


def _board_table(symbols: int = 5200) -> list[dict]:
    return [
        {
            "SECURITY_CODE": f"{600000 + s:06d}",
            "BOARD_CODE": f"BK{b:04d}",
            "BOARD_NAME": f"board {b}",
            "BOARD_TYPE_NEW": "3",
        }
        for s in range(symbols)
        for b in range(_BOARDS_PER_SYMBOL)
    ]


def test_page_size_is_clamped_by_default():
    client = FakeDatacenter(_board_table(symbols=1), honors_page_size=False)
    fetch_datacenter(client, "RPT_TEST", "SECURITY_CODE", page_size=5000)
    assert client.page_sizes == [500]


def test_trusted_page_size_goes_out_verbatim():
    client = FakeDatacenter(_board_table(symbols=1))
    fetch_datacenter(client, "RPT_TEST", "SECURITY_CODE", page_size=5000, trust_page_size=True)
    assert client.page_sizes == [5000]


def test_trusting_a_report_that_lies_raises_instead_of_truncating():
    # The whole safety argument for trust_page_size: if the server quietly
    # serves 500 anyway, the short page is a truncation, not the last page.
    client = FakeDatacenter(_board_table(symbols=200), honors_page_size=False)
    with pytest.raises(EastMoneyDatacenterError, match="refusing truncated result"):
        fetch_datacenter(client, "RPT_TEST", "SECURITY_CODE", page_size=5000, trust_page_size=True)


def test_sector_members_reads_a_90k_row_board_table_without_hitting_the_cap():
    table = _board_table()
    client = FakeDatacenter(table)
    df = fetch_sector_members(date(2026, 8, 7), client=client)
    assert len(df) == len(table)
    assert max(client.page_sizes) == 5000
    # 19 pages, not 185 — the cap is not even approached.
    assert len(client.page_sizes) < _MAX_PAGE_NUMBER
