import pytest
import subprocess
from pathlib import Path
import shutil
from time import sleep


QOP = Path("../qop.py").absolute()
QOPD = Path("../qopd.py").absolute()


@pytest.fixture(scope="session", autouse=True)
def start_qop_daemon(request):
    proc = subprocess.Popen(["python3", QOPD, "--queue", '<temp>', "--log-file", "/dev/null"])
    sleep(0.5)
    request.addfinalizer(proc.kill)



@pytest.fixture()
def testfile_tree(tmp_path):
    root = tmp_path.joinpath("copy")
    root.mkdir()

    src = Path(root).joinpath("src")
    src.mkdir()
    dst = Path(root).joinpath("dst")
    dst.mkdir()

    def make_dummy_file(path):
        if not path.parent.exists():
            path.parent.mkdir(parents=True)
        f = open(path, "w+")
        f.write("foobar")

    make_dummy_file(src.joinpath("foo/bar.txt"))
    make_dummy_file(src.joinpath("foo/bar.flac"))
    make_dummy_file(src.joinpath("baz.txt"))
    make_dummy_file(src.joinpath("baz.flac"))

    yield root, src, dst


def test_copy_a_file(testfile_tree):
    """qop can copy a file"""
    root, src, dst = testfile_tree

    subprocess.run(["python3", QOP, "--log-level", "FATAL", "copy", src.joinpath("baz.txt"), dst], cwd=root)
    sleep(1)
    assert src.joinpath("baz.txt").exists()
    assert dst.joinpath("baz.txt").exists()


def test_copy_a_directory(testfile_tree):
    """qop can copy a file"""
    root, src, dst = testfile_tree

    subprocess.run(["python3", QOP, "-v", "--log-level", "FATAL", "copy", src, dst], cwd=root)
    sleep(1)
    assert src.joinpath("baz.txt").exists()
    assert dst.joinpath("src/baz.txt").exists()
    assert dst.joinpath("src/foo/bar.txt").exists()
