import pytest
from qop import tasks, daemon, utils
from qop.enums import Command
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

    tsk = tasks.EchoTask("blah")
    client = daemon.QopClient()
    res = client.send_task(tsk)
    sleep(0.1)

    assert tsk.to_dict() == res['task']


def test_daemon_sends_status_updates():
    """daemon can executes tasks and sends the appropriate stats if asked"""
    client = daemon.QopClient()
    client.send_command(Command.FLUSH)

    client.send_task(tasks.EchoTask("test"))
    p = client.get_queue_progress()
    assert p.pending == 1
    assert p.ok == 0
    assert p.fail == 0
    assert p.skip == 0
    assert p.fail == 0
    assert p.running == 0
    assert p.total == 1

    client.send_task(tasks.EchoTask("test2"))
    sleep(0.1)
    p = client.get_queue_progress()
    assert p.pending == 2
    assert p.total == 2

    client.send_task(tasks.FailTask())
    sleep(0.1)
    p = client.get_queue_progress()
    assert p.total == 3
    assert p.pending == 3

    client.send_command(Command.START)
    sleep(0.2)
    p = client.get_queue_progress()
    assert p.pending == 0
    assert p.ok == 2
    assert p.fail == 1
    assert p.total == 3
    client.send_command(Command.FLUSH)

    p = client.get_queue_progress()
    assert p.total == 0


def test_daemon_can_be_killed():
    """verify daemon can be shut down"""
    client = daemon.QopClient()
    assert client.is_server_alive() is True
    client.send_command(Command.KILL)
    assert client.is_server_alive() is False