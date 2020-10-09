import pytest
import subprocess
import re

from pathlib import Path
from time import sleep
from qop import _utils, _utils_tests
from mediafile import MediaFile

QOP = _utils.get_project_root("qop.py")
QOPD = _utils.get_project_root("qopd.py")


def match_true(x) -> bool:
    pat = re.compile(".*True.*")
    return bool(pat.match(str(x)))


def match_false(x) -> bool:
    pat = re.compile(".*False.*")
    return bool(pat.match(str(x)))


def wait_for_queue(timeout=30):
    i = 0
    while True:
        sleep(0.1)
        i = i+1
        o = subprocess.run(["python3", QOP, "-v", "--log-file", "/dev/null", "queue", "is-active"], capture_output=True)
        if match_false(o.stdout):
            break
        elif i > timeout * 10:
            raise TimeoutError


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

    _utils_tests.make_dummy_file(src.joinpath("foo/bar.txt"))
    _utils_tests.make_dummy_flac(src.joinpath("foo/bar.flac"))
    _utils_tests.make_dummy_file(src.joinpath("baz.txt"))
    _utils_tests.make_dummy_flac(src.joinpath("baz.flac"))
    _utils_tests.make_dummy_flac(root.joinpath("nocopy.flac"))

    yield root, src, dst


def test_copy_a_file(testfile_tree):
    """qop can copy a file"""
    root, src, dst = testfile_tree

    subprocess.run(["python3", QOP, "--log-level", "FATAL", "copy", src.joinpath("baz.txt"), dst], cwd=root)
    wait_for_queue()
    assert src.joinpath("baz.txt").exists()
    assert dst.joinpath("baz.txt").exists()


def test_copy_a_directory(testfile_tree):
    """qop can copy a file"""
    root, src, dst = testfile_tree

    subprocess.run(["python3", QOP, "-v", "--log-file", "/dev/null", "copy", src, dst], cwd=root)
    wait_for_queue()
    assert src.joinpath("baz.txt").exists()
    assert dst.joinpath("src/baz.txt").exists()
    assert dst.joinpath("src/foo/bar.txt").exists()


def test_convert_an_audio_file(testfile_tree):
    """qop can copy a file"""
    root, src_dir, dst_dir = testfile_tree

    src = src_dir.joinpath("sine.flac")
    dst = dst_dir.joinpath("sine.mp3")  # expected destination name

    _utils_tests.make_dummy_flac(src)
    f = MediaFile(src)
    f.artist = "foobar"
    f.save()

    subprocess.run(["python3", QOP, "-v", "--log-file", "/dev/null", "convert", src, dst_dir], cwd=root)
    wait_for_queue()

    assert src.exists()
    assert dst.exists()
    assert MediaFile(dst).artist == f.artist


def test_convert_an_audio_file_with_quality_settings(testfile_tree):
    """qop can copy a file"""
    root, src_dir, dst_dir = testfile_tree

    src = src_dir.joinpath("sine.flac")
    dst = dst_dir.joinpath("sine.mp3")

    _utils_tests.make_dummy_flac(src)

    subprocess.run(["python3", QOP, "-v", "--log-file", "/dev/null", "convert", src, dst_dir, '--parameters', '-q:a', '4'], cwd=root)
    wait_for_queue()

    assert src.exists()
    assert dst.exists()


def test_convert_a_directory_recursively(testfile_tree):
    """qop can copy a file"""
    root, src, dst = testfile_tree

    subprocess.run(["python3", QOP, "-v", "--log-file", "/dev/null", "convert", src, dst, "--include", "flac", "--convert-only", "flac"], cwd=root)
    wait_for_queue()
    subprocess.run(["python3", QOP, "-v", "queue", "start"], cwd=root)
    wait_for_queue()
    assert src.joinpath("baz.flac").exists()
    assert src.joinpath("foo/bar.flac").exists()
    assert dst.joinpath("src/baz.mp3").exists()
    assert dst.joinpath("src/foo/bar.mp3").exists()
    assert not dst.joinpath("src/nocopy.mp3").exists()

    # filters have excluded unnecessary files
    assert not dst.joinpath("src/baz.txt").exists()
    assert not dst.joinpath("src/foo/bar.txt").exists()
    assert not dst.joinpath("baz.txt").exists()
    assert not dst.joinpath("src/baz.txt").exists()
    assert not dst.joinpath("src/foo/bar.txt").exists()


def test_manage_the_queue(testfile_tree):
    """qop can copy a file"""
    root, src, dst = testfile_tree

    # ensure -e only enqueues a task without starting the queue
    subprocess.run(["python3", QOP, "-v", "--log-level", "CRITICAL", "copy", "-e", "foo", "bar"])
    o = subprocess.run(["python3", QOP, "-v", "--log-level", "CRITICAL", "queue", "is-active"], capture_output=True)
    assert match_false(o.stdout)

    # the task enqueued above will fail because foo does not exist
    subprocess.run(["python3", QOP, "-v", "--log-level", "CRITICAL", "queue", "start"], capture_output=True)
    wait_for_queue()
    o = subprocess.run(["python3", QOP, "-v", "--log-level", "CRITICAL", "queue", "is-active"], capture_output=True)
    assert match_false(o.stdout)


def test_manage_the_daemon(testfile_tree):
    """daemon can be started and stopped"""
    root, src, dst = testfile_tree

    subprocess.run(["python3", QOP, "-v", "--log-level", "CRITICAL", "daemon", "stop"])
    o = subprocess.run(["python3", QOP, "-v", "--log-level", "CRITICAL", "daemon", "is-active"], capture_output=True)
    assert match_false(o.stdout)

    subprocess.run(["python3", QOP, "-v", "--log-level", "CRITICAL", "daemon", "restart"])
    o = subprocess.run(["python3", QOP, "-v", "--log-level", "CRITICAL", "daemon", "is-active"], capture_output=True)
    assert match_true(o.stdout)

    subprocess.run(["python3", QOP, "-v", "--log-level", "CRITICAL", "daemon", "destroy"])
    o = subprocess.run(["python3", QOP, "-v", "--log-level", "CRITICAL", "daemon", "is-active"], capture_output=True)
    assert match_false(o.stdout)
