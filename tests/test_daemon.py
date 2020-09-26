import pytest
from qop import tasks, daemon, utils
from qop.config import Command
from time import sleep
import subprocess


QOPD = utils.get_project_root("qopd.py")


def wait_for_queue(timeout=30):
    i = 0
    client = daemon.QopClient()
    while True:
        sleep(0.1)
        i = i+1
        if not client.is_queue_active():
            break
        elif i > timeout * 10:
            raise TimeoutError


@pytest.fixture(scope="session", autouse=True)
def start_qop_daemon(request):
    proc = subprocess.Popen(["python3", QOPD, "--queue", '<temp>', "--log-file", "/dev/null"])
    sleep(0.5)
    request.addfinalizer(proc.kill)


def test_daemon_can_be_started_and_stopped(tmp_path):
    # send and received a message

    tsk = tasks.EchoTask("blah")
    client = daemon.QopClient()
    res = client.send_command(Command.QUEUE_PUT, payload=tsk)
    sleep(0.1)

    assert tsk.to_dict() == res['payload']


def test_daemon_sends_status_updates():
    """daemon can executes tasks and sends the appropriate stats if asked"""
    client = daemon.QopClient()
    client.send_command(Command.QUEUE_FLUSH_ALL)

    client.send_command(Command.QUEUE_PUT, payload=tasks.EchoTask("test"))
    p = client.get_queue_progress()
    assert p.pending == 1
    assert p.ok == 0
    assert p.fail == 0
    assert p.skip == 0
    assert p.fail == 0
    assert p.running == 0
    assert p.total == 1

    client.send_command(Command.QUEUE_PUT, payload=tasks.EchoTask("test2"))
    sleep(0.1)
    p = client.get_queue_progress()
    assert p.pending == 2
    assert p.total == 2

    client.send_command(Command.QUEUE_PUT, payload=tasks.FailTask())
    sleep(0.1)
    p = client.get_queue_progress()
    assert p.total == 3
    assert p.pending == 3

    client.send_command(Command.QUEUE_START)
    wait_for_queue()
    p = client.get_queue_progress()
    assert p.running == 0
    assert p.ok == 2
    assert p.fail == 1
    assert p.total == 3

    client.send_command(Command.QUEUE_FLUSH_ALL)
    p = client.get_queue_progress()
    assert p.total == 0


def test_daemon_can_be_killed():
    """verify daemon can be shut down"""
    client = daemon.QopClient()
    assert client.is_daemon_active() is True
    client.send_command(Command.DAEMON_STOP)
    sleep(0.1)
    assert client.is_daemon_active() is False
