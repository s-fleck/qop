from qcp import daemon
import pytest
import threading
import socket
from qcp import operations
from pathlib import Path

tmp_path = Path("/tmp")

def test_daemon_can_be_started(tmp_path):

    def background_daemon(queue_path=tmp_path.joinpath("qcp.db")):
        qcpd = daemon.QcpDaemon(queue_path=queue_path)
        qcpd.start()

    qcpd_thread = threading.Thread(target=background_daemon, args=[tmp_path.joinpath("qcp.db")])
    qcpd_thread.daemon = True
    qcpd_thread.start()

    req = daemon.Message(operations.EchoOperation("blah").to_dict())

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(("127.0.0.1", 9393))
    client.sendall(req.encode())
    res = client.recv(1024)
    res = daemon.RawMessage(res).decode()

    assert(req.__dict__ == res.__dict__)

    req = daemon.Message(operations.KillOperation("").to_dict())
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(("127.0.0.1", 9393))
    client.sendall(req.encode())
    client.close()
    qcpd_thread.join()
    qcpd_thread.is_alive()


def test_Operation_fails_on_missing_src(tmp_path):
    req = QcpDaemon.Request({"cmd": "delete"})
    res = QcpDaemon.Response(req.encode())

    assert isinstance(res.header_len, int)
    assert isinstance(res.header, dict)
    assert isinstance(res.body, dict)

    assert req.body == res.body
