# cython: language_level=3
import struct
from collections import OrderedDict

from cn_market_lake.adapters.tdx_protocol._wire.helper import get_volume
from cn_market_lake.adapters.tdx_protocol._wire.parser.base import BaseParser


class GetSecurityList(BaseParser):
    def setParams(self, market, start):
        """
        设置参数
        :param market: 市场
        :param start: 开始位置
        """
        pkg = bytearray.fromhex("0c 01 18 64 01 01 06 00 06 00 50 04")
        pkg.extend(struct.pack("<HH", market, start))

        self.send_pkg = pkg

    def parseResponse(self, body_buf):
        pos = 0
        (num,) = struct.unpack("<H", body_buf[:2])

        pos += 2
        symbols = []

        for _ in range(num):
            one_bytes = body_buf[pos : pos + 29]

            (
                code,
                volunit,
                name_bytes,
                reversed_bytes1,
                decimal_point,
                pre_close_raw,
                reversed_bytes2,
            ) = struct.unpack("<6sH8s4sBI4s", one_bytes)

            code = code.decode("utf-8", errors="ignore")
            name = name_bytes.decode("gbk", errors="ignore")

            pre_close = get_volume(pre_close_raw)
            pos += 29

            rows = OrderedDict(
                [
                    ("code", code),
                    ("volunit", volunit),
                    ("decimal_point", decimal_point),
                    ("name", name),
                    ("pre_close", pre_close),
                ]
            )

            symbols.append(rows)

        return symbols
