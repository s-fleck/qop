import struct
import json
from typing import Dict
import tempfile
from qcp import OperationQueue
from pathlib import Path

PREHEADER_LEN: int = 2



class QcpDaemon:
    port = 54993
    stats = None  # container that implements transfer statistics
    queue = None

    def __init__(self, port: int = 54993, queue_path=tempfile.mkstemp(".sqlite3")):
        self.port =  port
        self.queue = OperationQueue.OperationQueue(path=queue_path)

    def start(self):
        pass

    def handle_request(self):
        pass

    def serve_queue(self, n = 100):
        pass

    def stop(self):
        pass

    def new_queue(self, queue_path: Path = tempfile.mkstemp(".sqlite3")):
        pass


class Request:
    """Container for requests sent to the qcp daemon"""
    def __init__(self, body) -> None:
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


class Response:
    """Container for responses from the qcp daemon"""

    def __init__(self, raw) -> None:
        assert isinstance(raw, bytes)
        self.raw: bytes = raw

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
