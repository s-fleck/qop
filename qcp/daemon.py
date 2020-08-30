import struct
import json
from typing import Dict, Union
import tempfile
from qcp import tasks
from pathlib import Path
import logging
import socket

PREHEADER_LEN: int = 2


class QcpDaemon:
    port = 9393
    stats = None  # container that implements transfer statistics
    queue = None
    __is_listening = False

    def __init__(self, port: int = 9393, queue_path=tempfile.mkstemp(".sqlite3")):
        self.port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # ADDRESS_FAMILY: INTERNET (ip4), tcp
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.queue_path = queue_path
        self.new_queue()

    def __enter__(self):
        self._socket.bind(("127.0.0.1", self.port))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.is_listening:
            self.close()

    def close(self):
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()
        self.is_listening = False

    def listen(self, port=9393):
        lg = logging.getLogger(__name__)
        self._socket.listen(10)
        self.is_listening = True

        while True:
            client, address = self._socket.accept()
            lg.info(f'client connected: {address}')
            req = client.recv(1024)
            rsp = self.handle_request(req)

            if rsp.body["type"] == -1:
                lg.info(f"received kill task; shutting down server: {rsp.encode()}")
                client.close()
                self.close()
                break
            elif rsp.body["type"] > 0:
                lg.info(f"received task: {rsp.encode()}")
                self.queue.put(tasks.Task.from_dict(rsp.body))
            else:
                lg.debug(f"received unknown message: {rsp.encode()}")

            client.sendall(rsp.encode())

    @property
    def is_listening(self) -> bool:
        return self.__is_listening

    @is_listening.setter
    def is_listening(self, x: bool) -> None:
        lg = logging.getLogger(__name__)

        if x:
            lg.info(f"qcp-daemon listening on port {self.port}")
        else:
            lg.debug("socket closed")

        self.__is_listening = x

    @staticmethod
    def handle_request(req):
        t = RawMessage(req).decode()

        return RawMessage(req).decode()

    def serve_queue(self, n=100):
        pass

    def stop(self):
        pass

    def new_queue(self, queue_path: Path = tempfile.mkstemp(".sqlite3")[1]):
        self.queue = tasks.TaskQueue(path=queue_path)


class Message:
    """Container for requests sent to the qcp daemon"""
    def __init__(self, body: Union[Dict, tasks.Task]) -> None:
        """

        :rtype: object
        """
        if isinstance(body, tasks.Task):
            body = body.__dict__
        else:
            assert isinstance(body, dict)

        self.body = body

    def encode(self) -> bytes:
        if isinstance(self.body, dict):
            body: bytes = bytes(json.dumps(self.body), "utf-8")
            header: Dict = {
                "content-length": len(body),
                "content-type": "text/json"
            }
            header: bytes = bytes(json.dumps(header), "utf-8")
            header_len: bytes = struct.pack("!H", len(header))  # network-endianess, unsigned long integer (4 bytes)
            logging.getLogger("qcp.daemon").debug(f'encoding message with header_length={int(struct.unpack("!H", header_len)[0])} and content_length={len(body)}')
            return header_len + header + body

    def __repr__(self) -> str:
        return f"Message: {self.body.__repr__()}"


class RawMessage:
    """Container for responses from the qcp daemon"""

    def __init__(self, raw) -> None:
        assert isinstance(raw, bytes)
        self.raw: bytes = raw

    def encode(self) -> bytes:
        return self.raw

    def decode(self) -> Message:
        logging.getLogger("qcp.daemon").debug(f'decoding message with header_length={self.header_len}')
        return Message(self.body)

    @property
    def header_len(self) -> int:
        return int(struct.unpack("!H", self.raw[:PREHEADER_LEN])[0])

    @property
    def _header(self) -> bytes:
        return self.raw[PREHEADER_LEN:(self.header_len + PREHEADER_LEN)]

    @property
    def header(self) -> Dict:
        return json.loads(self._header.decode("utf-8"))

    @property
    def _body(self) -> bytes:
        start = PREHEADER_LEN + self.header_len
        return self.raw[start:start + self.header["content-length"]]

    @property
    def body(self) -> Dict:
        return json.loads(self._body.decode("utf-8"))

    def __repr__(self) -> str:
        return f"RawMessage: {self.decode().__repr__()}"
