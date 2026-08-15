# cython: language_level=3
import abc
import datetime
import struct
import zlib

from cn_market_lake.adapters.tdx_protocol._wire.logger import logger

try:
    import cython  # noqa

    if cython.compiled:

        def buffer(x):
            return x

except ImportError:
    pass


class SocketClientNotReady(Exception):
    pass


class SendPkgNotReady(Exception):
    pass


class SendRequestPkgFails(Exception):
    pass


class ResponseHeaderRecvFails(Exception):
    pass


class ResponseRecvFails(Exception):
    pass


RSP_HEADER_LEN = 0x10


class BaseParser:
    def __init__(self, client, lock=None):
        """

        :rtype: object
        """

        self.send_pkg = None
        self.data = None

        self.rsp_header_len = RSP_HEADER_LEN
        self.rsp_header = None
        self.rsp_body = None

        self.client = client
        self.lock = lock or None

        self.category = None

    def setParams(self, *args, **xargs):  # noqa
        """
        构建请求
        :return:
        """
        pass

    @abc.abstractmethod
    def parseResponse(self, body_buf):  # noqa
        """
        解析结果
        :param body_buf:
        """
        pass

    @staticmethod
    def _parse_date(num):
        """

        :param num:
        :return:
        """
        month = (num % 2048) // 100
        year = num // 2048 + 2004
        day = (num % 2048) % 100

        return year, month, day

    @staticmethod
    def _parse_time(num):
        """

        :param num:
        :return:
        """
        return (num // 60), (num % 60)

    @staticmethod
    def _cal_price1000(base_p, diff):
        return float(base_p + diff) / 1000

    def setup(self):
        pass

    def call_api(self):
        """

        :return:
        """
        if self.lock:
            with self.lock:
                logger.debug("sending thread lock api call")
                result = self._call_api()
        else:
            result = self._call_api()

        return result

    def _call_api(self):
        """

        :return:
        """
        self.setup()

        if not self.client:
            raise SocketClientNotReady("socket client not ready")

        if not self.send_pkg:
            raise SendPkgNotReady("send pkg not ready")

        # 发送数据包
        descended = self.client.send(self.send_pkg)

        logger.debug(f"descended, {descended}")

        self.client.send_pkg_num += 1
        self.client.send_pkg_bytes += descended
        self.client.last_api_send_bytes = descended

        if not self.client.first_pkg_send_time:
            self.client.first_pkg_send_time = datetime.datetime.now()

        if descended != len(self.send_pkg):
            logger.debug("send bytes error")
            raise SendRequestPkgFails("send fails")
        else:
            # 接收数据
            logger.debug("send rsp_header_len:" + str(self.rsp_header_len))
            head_buf = self.client.recv(self.rsp_header_len)

            # 长度符合
            if len(head_buf) == self.rsp_header_len:
                self.client.recv_pkg_num += 1
                self.client.recv_pkg_bytes += self.rsp_header_len

                # 解包
                _, _, _, zip_size, unzip_size = struct.unpack("<IIIHH", head_buf)

                logger.debug(f"zip size is: {zip_size}")
                logger.debug(f"unzip size is: {unzip_size}")

                body_buf = bytearray()
                last_api_recv_bytes = self.rsp_header_len

                while True:
                    # 接收数据包
                    buf = self.client.recv(zip_size)
                    len_buf = len(buf)

                    self.client.recv_pkg_num += 1
                    self.client.recv_pkg_bytes += len_buf

                    last_api_recv_bytes += len_buf
                    body_buf.extend(buf)

                    # 接收数据到结束
                    if not buf or len_buf == 0 or len(body_buf) == zip_size:
                        logger.debug("接收数据到结束")
                        break

                self.client.last_api_recv_bytes = last_api_recv_bytes

                if len(buf) == 0:
                    logger.debug("接收数据体失败服务器断开连接")
                    raise ResponseRecvFails("接收数据体失败服务器断开连接")

                if zip_size != unzip_size:
                    logger.debug("需要解压, 解压数据")
                    body_buf = zlib.decompress(body_buf)

                return self.parseResponse(body_buf)

            else:
                # 长度不符合抛出异常
                logger.debug("head_buf is not 0x10")
                raise ResponseHeaderRecvFails("head_buf is not 0x10 : " + str(head_buf))
