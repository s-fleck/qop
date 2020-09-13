import pytest
import subprocess


@pytest.fixture(scope="session", autouse=True)
def start_qcp_daemon(request):
    proc = subprocess.Popen(["nohup", "python3", "../qcpd.py"])
    request.addfinalizer(proc.kill)


def test_simple_copy_operation(tmp_path):
    """qcp can copy a file"""

    tf = tmp_path.joinpath("test.txt")
    td = tmp_path.joinpath("copy")
    td.mkdir()
    f = open(tf, "w+")
    f.write("foobar")
    subprocess.run(["python3", "../qcp.py", "copy", tf, td])

    assert tf.exists()
    assert td.joinpath("test.txt").exists()

    subprocess.run(["python3", "../qcp.py", "kill"])
