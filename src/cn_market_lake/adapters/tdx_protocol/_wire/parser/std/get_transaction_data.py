# cython: language_level=3
"""Same-session transaction records (当日分笔).

Ported from tdxpy 0.2.7's ``GetTransactionData`` (MIT, see ``LICENSE.tdxpy``)
with three upstream defects fixed:

* upstream binds the per-record trade count to ``num``, the same name it used
  for the record count driving the loop. ``range()`` is evaluated once so it
  does not misbehave today, but the shadowing is a trap;
* upstream divides the price by 100 unconditionally, which is only right for
  A-share stocks (coefficient 0.01). Funds and bonds use 0.001/0.0001, so the
  raw integer is returned here and scaling is left to the caller, which knows
  what the instrument is;
* the reserved trailing field is read and discarded, as upstream does, but is
  named so the layout stays readable.

``vol``, ``trade_count`` and ``direction`` all come off ``get_price`` — the
variable-length *integer* decoder — not off ``get_volume``, the custom float
used for K-line volumes. The denormal-zero correction in
:mod:`cn_market_lake.adapters.tdx_protocol._decode` therefore does not apply here.
"""

import struct
from collections import OrderedDict

from cn_market_lake.adapters.tdx_protocol._wire.helper import get_price, get_time
from cn_market_lake.adapters.tdx_protocol._wire.parser.base import BaseParser


class GetTransactionDataCmd(BaseParser):
    def setParams(self, market, code, start, count):  # noqa: N802 - upstream shape
        """
        :param market: 0 = 深圳, 1 = 上海
        :param code: 6 位证券代码
        :param start: 起始偏移，0 = 当日最后一条
        :param count: 本页条数上限
        """
        if type(code) is str:
            code = code.encode("utf-8")

        pkg = bytearray.fromhex("0c 17 08 01 01 01 0e 00 0e 00 c5 0f")
        pkg.extend(struct.pack("<H6sHH", market, code, start, count))

        self.send_pkg = pkg

    def parseResponse(self, body_buf):  # noqa: N802 - upstream shape
        """Rows oldest-first within the page, prices raw (undivided) integers."""
        pos = 0

        (record_count,) = struct.unpack("<H", body_buf[0:2])
        pos += 2

        ticks = []

        # Prices are delta-encoded from zero *within a page*, so a page is
        # self-contained — but a mis-parse corrupts every later row in it.
        last_price = 0

        for _ in range(record_count):
            hour, minute, pos = get_time(body_buf, pos)

            price_diff, pos = get_price(body_buf, pos)
            vol, pos = get_price(body_buf, pos)
            trade_count, pos = get_price(body_buf, pos)
            direction, pos = get_price(body_buf, pos)
            _reserved, pos = get_price(body_buf, pos)

            last_price = last_price + price_diff

            ticks.append(
                OrderedDict(
                    [
                        ("hour", hour),
                        ("minute", minute),
                        ("time", "%02d:%02d" % (hour, minute)),
                        ("price_raw", last_price),
                        ("vol", vol),
                        ("trade_count", trade_count),
                        ("direction", direction),
                    ]
                )
            )

        return ticks
