from typhon import OperationQueue
import pytest
import atexit


def test_Operation_fails_on_missing_file():
    """OperationDelete raises an error if target file does not exist"""
    with pytest.raises(FileNotFoundError):
        OperationQueue.OperationDelete('foo')


def test_OperationDelete(tmp_path):
    """OperationDelete can be instantiated"""
    src = tmp_path.joinpath("foo")
    src.touch()

    op = OperationQueue.OperationDelete(src)
    assert op.src.exists()
    op.execute()
    assert not op.src.exists()


def test_OperationCopy(tmp_path):
    """OperationCopy copies a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = OperationQueue.OperationCopy(src, dst)

    assert op.src.exists()
    assert not op.dst.exists()
    op.execute()
    assert op.src.exists()
    assert op.dst.exists()


def test_OperationMove(tmp_path):
    """OperationMove moves a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = OperationQueue.OperationMove(src, dst)

    assert op.src.exists()
    assert not op.dst.exists()
    op.execute()
    assert not op.src.exists()
    assert op.dst.exists()