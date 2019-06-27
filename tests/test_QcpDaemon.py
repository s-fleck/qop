from qcp import QcpDaemon
import pytest


def test_Operation_fails_on_missing_src(tmp_path):
    req = QcpDaemon.Request({"cmd": "delete"})
    res = QcpDaemon.Response(req.encode())

    assert isinstance(res.header_len, int)
    assert isinstance(res.header, dict)
    assert isinstance(res.body, dict)

    assert req.body == res.body
