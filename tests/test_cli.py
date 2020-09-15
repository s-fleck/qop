import pytest
import subprocess


@pytest.fixture(scope="session", autouse=True)
def start_qop_daemon(request):
    proc = subprocess.Popen(["nohup", "python3", "../qopd.py"])
    request.addfinalizer(proc.kill)


def test_simple_copy_operation(tmp_path):
    """qop can copy a file"""

    tf = tmp_path.joinpath("test.txt")
    td = tmp_path.joinpath("copy")
    td.mkdir()
    f = open(tf, "w+")
    f.write("foobar")
    subprocess.run(["python3", "../qop.py", "copy", tf, td])

    assert tf.exists()
    assert td.joinpath("test.txt").exists()

    subprocess.run(["python3", "../qop.py", "kill"])


def test_simple_copy_operation(tmp_path):
    """qop can copy a file"""

    td = tmp_path.joinpath("copy")
    td.mkdir()

    root_path = "tests/test_Scanner"

    subprocess.run(["python3", "../qop.py", "copy", "test_Scanner", td], cwd=root_path)
    subprocess.run(["python3", "../qop.py", "copy", "test_Scanner/*.flac", td], cwd=root_path)
    subprocess.run(["python3", "../qop.py", "copy", "*", td], cwd=root_path)
    subprocess.run(["python3", "../qop.py", "copy", ".", td], cwd=root_path)
    subprocess.run(["python3", "../qop.py", "-r", "copy", "test_Scanner", td], cwd=root_path)
    subprocess.run(["python3", "../qop.py", "-r", "copy", "test_Scanner/*.flac", td], cwd=root_path)
    subprocess.run(["python3", "../qop.py", "-r", "copy", "*", td], cwd=root_path)
    subprocess.run(["python3", "../qop.py", "-r", "copy", ".", td], cwd=root_path)

    assert tf.exists()
    assert td.joinpath("test.txt").exists()

    subprocess.run(["python3", "../qop.py", "kill"])