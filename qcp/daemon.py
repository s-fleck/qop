import struct
import json
from typing import Dict
import tempfile
from qcp import operations
from pathlib import Path
import logging
import socket

PREHEADER_LEN: int = 2


class QcpDaemon:
    port = 54993
    stats = None  # container that implements transfer statistics
    queue = None

    def __init__(self, port: int = 54993, queue_path=tempfile.mkstemp(".sqlite3")):
        self.port = port
        self.queue_path = queue_path
        self.new_queue()

    def start(self, port=9393):
        lg = logging.getLogger(__name__)
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # ADDRESS_FAMILY: INTERNET (ip4), tcp
        server.bind(("127.0.0.1", port))
        server.listen(10)
        lg.info(f"qcp-daemon listening on port {9393}")

        while True:
            client, address = server.accept()
            lg.info(f'client connected: {address}')
            req = client.recv(1024)
            if not req:
                break
            rsp = self.handle_request(req)
            lg.debug(f"message received: {rsp.encode()}")
            client.sendall(rsp.encode())

    def handle_request(self, req):
        return RawMessage(req).decode()

    def serve_queue(self, n=100):
        pass

    def stop(self):
        pass

    def new_queue(self, queue_path: Path = tempfile.mkstemp(".sqlite3")[1]):
        self.queue = operations.OperationQueue(path=queue_path)


class Message:
    """Container for requests sent to the qcp daemon"""
    def __init__(self, body: Dict) -> None:
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

            return header_len + header + body


class RawMessage:
    """Container for responses from the qcp daemon"""

    def __init__(self, raw) -> None:
        assert isinstance(raw, bytes)
        self.raw: bytes = raw

    def encode(self) -> bytes:
        return self.raw

    def decode(self) -> Message:
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
