import struct
import json
from typing import Dict


PREHEADER_LEN: int = 2


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
