# cython: language_level=3
from cn_market_lake.adapters.tdx_protocol._wire.parser.base import BaseParser


class RawParser(BaseParser):
    def setParams(self, pkg):
        """
        设置发送数据包
        :param pkg: send pkg
        """
        self.send_pkg = pkg

    def parseResponse(self, body_buf):
        """
        解析结果
        :param body_buf: buff
        :return:
        """
        return body_buf
