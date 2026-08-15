# cython: language_level=3
"""Transaction records for a past session (历史分笔).

Ported from tdxpy 0.2.7's ``GetHistoryTransactionData`` (MIT, see
``LICENSE.tdxpy``). Two differences from the same-session command next door,
both of them the source's, not ours:

* the response carries four filler bytes between the record count and the
  first record;
* there is **no per-record trade count**. The same-session command reports how
  many real trades were folded into a record; the historical one does not, so
  that column simply does not exist for any past session.

Upstream's ``setParams`` guards the date with
``if type(date) is (type(date) is str) or ...``, which evaluates to a bool
comparison and is therefore always False. This port takes an ``int`` in
``yyyymmdd`` form and validates it, rather than pretending to coerce.

Prices are returned raw for the same reason as in the same-session command:
the 1/100 scale is an A-share-stock assumption, not a protocol fact.
"""

import struct
from collections import OrderedDict

from cn_market_lake.adapters.tdx_protocol._wire.helper import get_price, get_time
from cn_market_lake.adapters.tdx_protocol._wire.parser.base import BaseParser


class GetHistoryTransactionDataCmd(BaseParser):
    def setParams(self, market, code, start, count, date):  # noqa: N802 - upstream shape
        """
        :param market: 0 = 深圳, 1 = 上海
        :param code: 6 位证券代码
        :param start: 起始偏移，0 = 当日最后一条
        :param count: 本页条数上限
        :param date: 交易日，int，形如 20260731
        """
        if type(code) is str:
            code = code.encode("utf-8")

        date = int(date)
        if not 19900000 < date < 21000000:
            raise ValueError(f"date must be an int like 20260731, got {date!r}")

        pkg = bytearray.fromhex("0c 01 30 01 00 01 12 00 12 00 b5 0f")
        pkg.extend(struct.pack("<IH6sHH", date, market, code, start, count))

        self.send_pkg = pkg

    def parseResponse(self, body_buf):  # noqa: N802 - upstream shape
        """Rows oldest-first within the page, prices raw (undivided) integers."""
        pos = 0

        (record_count,) = struct.unpack("<H", body_buf[0:2])
        pos += 2

        # Four bytes the same-session response does not have.
        pos += 4

        ticks = []
        last_price = 0

        for _ in range(record_count):
            hour, minute, pos = get_time(body_buf, pos)

            price_diff, pos = get_price(body_buf, pos)
            vol, pos = get_price(body_buf, pos)
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
                        ("direction", direction),
                    ]
                )
            )

        return ticks
