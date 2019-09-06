from qcp import daemon
import pytest
import threading
import socket
from qcp import tasks
from pathlib import Path

tmp_path = Path("/tmp")


def test_daemon_can_be_started_and_stopped(tmp_path):

    def background_daemon(queue_path=tmp_path.joinpath("qcp.db")):
        qcpd = daemon.QcpDaemon(queue_path=queue_path)
        qcpd.start()

    qcpd_thread = threading.Thread(target=background_daemon, args=[tmp_path.joinpath("qcp.db")])
    qcpd_thread.daemon = True
    qcpd_thread.start()

    req = daemon.Message(tasks.EchoTask("blah"))

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(("127.0.0.1", 9393))
    client.sendall(req.encode())
    res = client.recv(1024)
    res = daemon.RawMessage(res).decode()

    assert(req.__dict__ == res.__dict__)

    req = daemon.Message(tasks.KillTask())
    assert qcpd_thread.is_alive() is True

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(("127.0.0.1", 9393))
    client.sendall(req.encode())
    client.close()

    qcpd_thread.join(timeout=1)
    assert qcpd_thread.is_alive() is False
