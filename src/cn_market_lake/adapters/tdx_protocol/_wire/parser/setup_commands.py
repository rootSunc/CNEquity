# cython: language_level=3
from cn_market_lake.adapters.tdx_protocol._wire.parser.base import BaseParser


class BaseSetup(BaseParser):
    def parseResponse(self, body_buf):
        """
        解析返回结果
        :param body_buf:
        :return:
        """
        return body_buf


class SetupCmd1(BaseSetup):
    def setup(self):
        self.send_pkg = bytearray.fromhex("0c 02 18 93 00 01 03 00 03 00 0d 00 01")


class SetupCmd2(BaseSetup):
    def setup(self):
        self.send_pkg = bytearray.fromhex("0c 02 18 94 00 01 03 00 03 00 0d 00 02")


class SetupCmd3(BaseSetup):
    def setup(self):
        self.send_pkg = bytearray.fromhex(
            "0c 03 18 99 00 01 20 00 20 00 db 0f d5 d0"
            "c9 cc d6 a4 a8 af 00 00 00 8f c2 25 40 13"
            "00 00 d5 00 c9 cc bd f0 d7 ea 00 00 00 02"
        )


class ExSetupCmd1(BaseParser):  # noqa
    def setup(self):
        self.send_pkg = bytearray.fromhex(
            "01 01 48 65 00 01 52 00 52 00 54 24 1f 32 c6 e5"
            "d5 3d fb 41 1f 32 c6 e5 d5 3d fb 41 1f 32 c6 e5"
            "d5 3d fb 41 1f 32 c6 e5 d5 3d fb 41 1f 32 c6 e5"
            "d5 3d fb 41 1f 32 c6 e5 d5 3d fb 41 1f 32 c6 e5"
            "d5 3d fb 41 1f 32 c6 e5 d5 3d fb 41 cc e1 6d ff"
            "d5 ba 3f b8 cb c5 7a 05 4f 77 48 ea"
        )
