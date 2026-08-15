# cython: language_level=3
import struct
from collections import OrderedDict

from cn_market_lake.adapters.tdx_protocol._wire.helper import get_datetime, get_price, get_volume
from cn_market_lake.adapters.tdx_protocol._wire.parser.base import BaseParser


class GetIndexBarsCmd(BaseParser):
    def setParams(self, category, market, code, start, count):
        """

        :param category:
        :param market:
        :param code:
        :param start:
        :param count:
        """
        if type(code) is str:
            code = code.encode("utf-8")

        self.category = category

        val = (
            0x10C,
            0x01016408,
            0x1C,
            0x1C,
            0x052D,
            market,
            code,
            category,
            1,
            start,
            count,
            0,
            0,
            0,
        )  # I + I +  H total 10 zero
        pkg = struct.pack("<HIHHHH6sHHHHIIH", *val)  # noqa

        self.send_pkg = pkg

    def parseResponse(self, body_buf):
        """

        :param body_buf:
        :return:
        """
        (ret_count,) = struct.unpack("<H", body_buf[0:2])

        pos = 2

        klines = []  # noqa
        pre_diff_base = 0

        for _ in range(ret_count):
            year, month, day, hour, minute, pos = get_datetime(self.category, body_buf, pos)  # noqa

            price_open_diff, pos = get_price(body_buf, pos)
            price_close_diff, pos = get_price(body_buf, pos)

            price_high_diff, pos = get_price(body_buf, pos)
            price_low_diff, pos = get_price(body_buf, pos)

            (vol_raw,) = struct.unpack("<I", body_buf[pos : pos + 4])
            vol = get_volume(vol_raw)

            pos += 4

            (db_vol_raw,) = struct.unpack("<I", body_buf[pos : pos + 4])
            db_vol = get_volume(db_vol_raw)

            pos += 4

            (up_count, down_count) = struct.unpack("<HH", body_buf[pos : pos + 4])

            pos += 4  # noqa

            open_ = self._cal_price1000(price_open_diff, pre_diff_base)
            price_open_diff = price_open_diff + pre_diff_base

            close = self._cal_price1000(price_open_diff, price_close_diff)
            high = self._cal_price1000(price_open_diff, price_high_diff)
            low = self._cal_price1000(price_open_diff, price_low_diff)

            pre_diff_base = price_open_diff + price_close_diff

            # 为了避免python处理浮点数的时候，浮点数运算不精确问题，这里引入了多余的代码
            kline = OrderedDict(
                [
                    ("open", open_),
                    ("close", close),
                    ("high", high),
                    ("low", low),
                    ("vol", vol),
                    ("amount", db_vol),
                    ("year", year),
                    ("month", month),
                    ("day", day),
                    ("hour", hour),
                    ("minute", minute),
                    ("datetime", f"{year:d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}"),
                    ("up_count", up_count),
                    ("down_count", down_count),
                ]
            )

            klines.append(kline)

        return klines
