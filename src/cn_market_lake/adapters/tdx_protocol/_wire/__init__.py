"""Vendored TDX wire protocol client.

Derived from tdxpy 0.2.7 (MIT, https://github.com/mootdx/tdxpy) — see
LICENSE.tdxpy. Vendored rather than depended on because both tdxpy and its
sibling mootdx were last released in 2024 and are no longer maintained, and
mootdx additionally drags in `py-mini-racer`, a compiled V8 binding this
project has no use for.

Only the five calls this project makes are kept, against tdxpy's 22. The
extended market (`exhq`), local-file readers and the financial crawler are all
dropped, as is the pandas dependency — the lake converts to polars anyway.

The TDX wire format is a frozen legacy binary protocol; the parsers below are
fixed-width `struct.unpack` and do not track a moving upstream.
"""

from __future__ import annotations

from cn_market_lake.adapters.tdx_protocol._wire.constants import TDXParams
from cn_market_lake.adapters.tdx_protocol._wire.exceptions import (
    TdxConnectionError,
    TdxFunctionCallError,
    ValidationException,
)
from cn_market_lake.adapters.tdx_protocol._wire.parser.setup_commands import (
    SetupCmd1,
    SetupCmd2,
    SetupCmd3,
)
from cn_market_lake.adapters.tdx_protocol._wire.parser.std.get_history_transaction_data import (
    GetHistoryTransactionDataCmd,
)
from cn_market_lake.adapters.tdx_protocol._wire.parser.std.get_index_bars import GetIndexBarsCmd
from cn_market_lake.adapters.tdx_protocol._wire.parser.std.get_security_bars import (
    GetSecurityBarsCmd,
)
from cn_market_lake.adapters.tdx_protocol._wire.parser.std.get_security_count import (
    GetSecurityCountCmd,
)
from cn_market_lake.adapters.tdx_protocol._wire.parser.std.get_security_list import GetSecurityList
from cn_market_lake.adapters.tdx_protocol._wire.parser.std.get_transaction_data import (
    GetTransactionDataCmd,
)
from cn_market_lake.adapters.tdx_protocol._wire.parser.std.get_xdxr_info import GetXdXrInfo
from cn_market_lake.adapters.tdx_protocol._wire.socket_client import BaseSocketClient, last_ack_time

__all__ = [
    "TDXParams",
    "TdxConnectionError",
    "TdxFunctionCallError",
    "TdxWireClient",
    "ValidationException",
]

# TDX market ids: 0 = Shenzhen, 1 = Shanghai.
MARKET_SZ = 0
MARKET_SH = 1

# Daily K-line. TDX category ids are positional, not an enum in the protocol.
CATEGORY_DAILY = 9

# TDX refuses more than 800 rows per response; asking for more silently truncates.
MAX_PAGE = 800

# Transaction records page separately from bars, and deeper. The value comes
# from `TDXParams.MAX_TRANSACTION_COUNT`, which had sat unreferenced since the
# vendoring — public write-ups disagree between 1000 and 2000, so treat it as
# an upper bound to request and read the returned row count, not as a promise.
MAX_TICK_PAGE = TDXParams.MAX_TRANSACTION_COUNT


class TdxWireClient(BaseSocketClient):
    """The standard-market calls the lake needs, and nothing else."""

    def setup(self):
        SetupCmd1(self.client).call_api()
        SetupCmd2(self.client).call_api()
        SetupCmd3(self.client).call_api()

    @last_ack_time
    def get_security_bars(self, category: int, market: int, code: str, start: int, count: int):
        cmd = GetSecurityBarsCmd(self.client, lock=self.lock)
        cmd.setParams(category, market, code, start, min(int(count), MAX_PAGE))
        return cmd.call_api()

    @last_ack_time
    def get_index_bars(self, category: int, market: int, code: str, start: int, count: int):
        cmd = GetIndexBarsCmd(self.client, lock=self.lock)
        cmd.setParams(category, market, code, start, min(int(count), MAX_PAGE))
        return cmd.call_api()

    @last_ack_time
    def get_security_count(self, market: int):
        cmd = GetSecurityCountCmd(self.client, lock=self.lock)
        cmd.setParams(market)
        return cmd.call_api()

    @last_ack_time
    def get_security_list(self, market: int, start: int):
        cmd = GetSecurityList(self.client, lock=self.lock)
        cmd.setParams(market, start)
        return cmd.call_api()

    @last_ack_time
    def get_transaction_data(self, market: int, code: str, start: int, count: int):
        """Same-session transaction records, newest block first (start=0)."""
        cmd = GetTransactionDataCmd(self.client, lock=self.lock)
        cmd.setParams(market, code, int(start), min(int(count), MAX_TICK_PAGE))
        return cmd.call_api()

    @last_ack_time
    def get_history_transaction_data(
        self, market: int, code: str, start: int, count: int, date: int
    ):
        """Transaction records for a past session; ``date`` is an int yyyymmdd."""
        cmd = GetHistoryTransactionDataCmd(self.client, lock=self.lock)
        cmd.setParams(market, code, int(start), min(int(count), MAX_TICK_PAGE), int(date))
        return cmd.call_api()

    @last_ack_time
    def get_xdxr_info(self, market: int, code: str):
        cmd = GetXdXrInfo(self.client, lock=self.lock)
        cmd.setParams(market, code)
        return cmd.call_api()

    def do_heartbeat(self):
        """Keepalive packet. Required by HeartBeatThread, which calls it by name.

        A security-count request is the cheapest round trip the protocol has.
        Upstream passed ``secrets.randbelow(1)``, which is always 0 — Shenzhen.
        """
        return self.get_security_count(MARKET_SZ)

    def to_df(self, v):  # pragma: no cover - kept off the hot path deliberately
        raise NotImplementedError("the lake builds polars frames directly from these dicts")
