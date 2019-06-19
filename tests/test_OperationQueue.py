from typhon import OperationQueue
import pytest
import atexit


def test_OperationDelete(tmp_path):
    """OperationDelete can be instantiated"""
    temp_file = tmp_path.joinpath("foo")
    temp_file.touch()
    atexit.register(temp_file.unlink)

    op = OperationQueue.OperationDelete(temp_file)
    assert op.path.exists()

    op.execute()
    assert not op.path.exists()
    atexit.unregister(temp_file.unlink)


def test_OperationDelete_init_fails_on_missing_file():
    """OperationDelete raises an error if target file does not exist"""
    with pytest.raises(FileNotFoundError):
        OperationQueue.OperationDelete('foo')

