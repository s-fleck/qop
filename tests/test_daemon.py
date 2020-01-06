import pytest
import threading
import socket
from qcp import tasks, daemon
from pathlib import Path

tmp_path = Path("/tmp")


@pytest.fixture()
def dummy_daemon():
    dummy = daemon.QcpDaemon(queue_path=tmp_path.joinpath("qcp.db"))
    with dummy as dummy_daemon:
        thread = threading.Thread(target=dummy_daemon.listen)
        thread.daemon = True
        thread.start()
        yield dummy


def test_daemon_can_be_started_and_stopped(tmp_path, dummy_daemon):
    # send and received a message
    req = daemon.Message(tasks.EchoTask("blah"))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect(("127.0.0.1", 54993))
        client.sendall(req.encode())
        res = client.recv(1024)
        res = daemon.RawMessage(res).decode()

    assert(req.__dict__ == res.__dict__)


def test_daemon_can_be_killed(dummy_daemon):
    req_kill = daemon.Message(tasks.KillTask())
    req_echo = daemon.Message(tasks.EchoTask("this should fail"))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect(("127.0.0.1", 54993))
        client.sendall(req_kill.encode())

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect(("127.0.0.1", 54993))

        with pytest.raises(ConnectionResetError):
            client.sendall(req_echo.encode())
