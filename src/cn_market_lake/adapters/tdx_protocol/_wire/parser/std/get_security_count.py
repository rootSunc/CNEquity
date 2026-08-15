# cython: language_level=3
import struct

from cn_market_lake.adapters.tdx_protocol._wire.parser.base import BaseParser


class GetSecurityCountCmd(BaseParser):
    def setParams(self, market):
        """

        :param market:
        """
        pkg = bytearray.fromhex("0c 0c 18 6c 00 01 08 00 08 00 4e 04")

        pkg.extend(struct.pack("<H", market))
        pkg.extend(b"\x75\xc7\x33\x01")

        self.send_pkg = pkg

    def parseResponse(self, body_buf):
        """

        :param body_buf:
        :return:
        """
        (num,) = struct.unpack("<H", body_buf[:2])

        return num
