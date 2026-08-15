# cython: language_level=3
import struct
from collections import OrderedDict

from cn_market_lake.adapters.tdx_protocol._wire.helper import get_datetime, get_volume
from cn_market_lake.adapters.tdx_protocol._wire.parser.base import BaseParser

XDXR_CATEGORY_MAPPING = {
    1: "除权除息",
    2: "送配股上市",
    3: "非流通股上市",
    4: "未知股本变动",
    5: "股本变化",
    6: "增发新股",
    7: "股份回购",
    8: "增发新股上市",
    9: "转配股上市",
    10: "可转债上市",
    11: "扩缩股",
    12: "非流通股缩股",
    13: "送认购权证",
    14: "送认沽权证",
}


class GetXdXrInfo(BaseParser):
    def setParams(self, market, code):
        """
        设置参数
        :param market: 市场
        :param code: 股票代码
        """
        if type(code) is str:
            code = code.encode("utf-8")

        pkg = bytearray.fromhex("0c 1f 18 76 00 01 0b 00 0b 00 0f 00 01 00")
        pkg.extend(struct.pack("<B6s", market, code))

        self.send_pkg = pkg

    def parseResponse(self, body_buf):
        """
        解析返回结果
        :param body_buf:
        :return:
        """
        pos = 0

        if len(body_buf) < 11:
            return []

        pos += 9  # skip 9
        (num,) = struct.unpack("<H", body_buf[pos : pos + 2])

        pos += 2
        rows = []

        def _get_v(v):
            if v == 0:
                return 0

            return get_volume(v)

        for _ in range(num):
            pos += 7
            pos += 1

            year, month, day, hour, minute, pos = get_datetime(9, body_buf, pos)
            (category,) = struct.unpack("<B", body_buf[pos : pos + 1])
            pos += 1

            suogu = None
            panqianliutong, panhouliutong, qianzongguben, houzongguben = None, None, None, None
            songzhuangu, fenhong, peigu, peigujia = None, None, None, None
            fenshu, xingquanjia = None, None

            if category == 1:
                fenhong, peigujia, songzhuangu, peigu = struct.unpack(
                    "<ffff", body_buf[pos : pos + 16]
                )
            elif category in [11, 12]:
                _, _, suogu, _ = struct.unpack("<IIfI", body_buf[pos : pos + 16])
            elif category in [13, 14]:
                xingquanjia, _, fenshu, _ = struct.unpack("<fIfI", body_buf[pos : pos + 16])
            else:
                panqianliutong_raw, qianzongguben_raw, panhouliutong_raw, houzongguben_raw = (
                    struct.unpack("<IIII", body_buf[pos : pos + 16])
                )
                panqianliutong = _get_v(panqianliutong_raw)
                panhouliutong = _get_v(panhouliutong_raw)
                qianzongguben = _get_v(qianzongguben_raw)
                houzongguben = _get_v(houzongguben_raw)

            pos += 16

            row = OrderedDict(
                [
                    ("year", year),
                    ("month", month),
                    ("day", day),
                    ("category", category),
                    ("name", self.get_category_name(category)),
                    ("fenhong", fenhong),
                    ("peigujia", peigujia),
                    ("songzhuangu", songzhuangu),
                    ("peigu", peigu),
                    ("suogu", suogu),
                    ("panqianliutong", panqianliutong),
                    ("panhouliutong", panhouliutong),
                    ("qianzongguben", qianzongguben),
                    ("houzongguben", houzongguben),
                    ("fenshu", fenshu),
                    ("xingquanjia", xingquanjia),
                ]
            )
            rows.append(row)

        return rows

    @staticmethod
    def get_category_name(category_id):
        """
        获取分类名称
        如果配置内没有，则返回 category_id
        :param category_id:
        :return: mixed
        """
        return XDXR_CATEGORY_MAPPING.get(category_id, str(category_id))
