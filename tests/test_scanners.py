import pytest
from pathlib import Path

from qop import _utils, _utils_tests
from qop import scanners

QOP = _utils.get_project_root("qop.py")
QOPD = _utils.get_project_root("qopd.py")


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


def test_scan_a_directory_recursively(testfile_tree):
    """qop can copy a file"""
    root, src, dst = testfile_tree
    s = scanners.Scanner()
    i = 0
    for f in s.scan(root):
        i += 1

    assert i == 8


def test_scan_a_directory_recursively_include_list(testfile_tree):
    """qop can copy a file"""
    root, src, dst = testfile_tree

    s = scanners.IncludeScanner(exts=["flac"])

    i = 0
    for f in s.scan(root):
        i += 1
        assert f.is_dir() or f.suffix == ".flac"

    assert i == 3


def test_scan_a_directory_recursively_exclude_list(testfile_tree):
    """qop can copy a file"""
    root, src, dst = testfile_tree

    s = scanners.ExcludeScanner(exts=["flac"])

    i = 0
    for f in s.scan(root):
        i += 1
        assert f.is_dir() or f.suffix != ".flac"

    assert i == 5
