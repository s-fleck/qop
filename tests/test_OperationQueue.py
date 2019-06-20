from typhon import OperationQueue
import pytest


def test_Operation_fails_on_missing_src(tmp_path):
    src = tmp_path.joinpath("foo")
    """instantiating Operation raises an error if target file does not exist"""

    # GIVEN src does not exists
    # WHEN instantiating Operation
    # THEN raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        OperationQueue.Operation(src)

    # GIVEN src exists
    # WHEN instantiating Operation
    # THEN succeed
    src.touch()
    op = OperationQueue.Operation(src)
    op.validate()

    # GIVEN src does not exist
    # WHEN validating Operation
    # THEN raise FileNotFoundError
    src.unlink()
    with pytest.raises(FileNotFoundError):
        op.validate()


def test_OperationDelete(tmp_path):
    """OperationDelete deletes files"""
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


def test_OperationCopy_fails_on_existing_dst(tmp_path):
    """OperationCopy fails if target exists"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = OperationQueue.OperationCopy(src, dst)
    op.execute()

    # GIVEN dst exists
    # WHEN executing OperationCopy
    # THEN raise FileExistsError
    with pytest.raises(FileExistsError):
        op.execute()

    # GIVEN dst exists
    # WHEN instantiating OperationCopy
    # THEN raise FileExistsError
    with pytest.raises(FileExistsError):
        OperationQueue.OperationCopy(src, dst)


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


