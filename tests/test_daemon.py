import pytest
import threading
import socket
from qop import tasks, daemon
from qop.globals import Command
from pathlib import Path
from time import sleep
import subprocess
tmp_path = Path("/tmp")


QOPD = Path("../qopd.py").absolute()


@pytest.fixture(scope="session", autouse=True)
def start_qop_daemon(request):
    proc = subprocess.Popen(["python3", QOPD, "--queue", '<temp>', "--log-file", "/dev/null"])
    sleep(0.5)
    request.addfinalizer(proc.kill)


def test_daemon_can_be_started_and_stopped(tmp_path):
    # send and received a message
    req = daemon.Message(tasks.EchoTask("blah"))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect(("127.0.0.1", 9393))
        client.sendall(req.encode())
        res = client.recv(1024)
        res = daemon.RawMessage(res).decode()

    assert(req.__dict__['body'] == res.__dict__['body']['task'])


def test_daemon_can_be_killed():
    req_kill = daemon.Message(tasks.CommandTask(Command.KILL))
    req_echo = daemon.Message(tasks.EchoTask("this should fail"))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect(("127.0.0.1", 9393))
        client.sendall(req_kill.encode())
        sleep(0.5)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        with pytest.raises(ConnectionRefusedError):
            client.connect(("127.0.0.1", 9393))
            client.sendall(req_echo.encode())
