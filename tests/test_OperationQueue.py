from typhon import OperationQueue
import pytest
import queue


def test_Operation_fails_on_missing_src(tmp_path):
    src = tmp_path.joinpath("foo")
    """instantiating or validating Operation raises an error if file does not exist"""

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


def test_Operations_can_be_compared():
    """Operations can be compared"""
    op1 = OperationQueue.Operation('one', priority=1, validate=False)
    op2 = OperationQueue.Operation('two', priority=2, validate=False)
    op3 = OperationQueue.Operation('three', priority=3, validate=False)

    assert op1 == op1
    assert op1 != op2
    assert op1 < op2
    assert op1 < op3
    assert op3 > op1


def test_OperationDelete(tmp_path):
    """OperationDelete deletes a file"""
    src = tmp_path.joinpath("foo")
    src.touch()

    op = OperationQueue.DeleteOperation(src)
    assert op.src.exists()
    op.execute()
    assert not op.src.exists()


def test_OperationCopy(tmp_path):
    """OperationCopy copies a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = OperationQueue.CopyOperation(src, dst)

    assert op.src.exists()
    assert not op.dst.exists()
    op.run()
    assert op.src.exists()
    assert op.dst.exists()


def test_OperationCopy_fails_on_existing_dst(tmp_path):
    """OperationCopy fails if dst file exists"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = OperationQueue.CopyOperation(src, dst)
    op.run()

    # GIVEN dst exists
    # WHEN executing OperationCopy
    # THEN raise FileExistsError
    with pytest.raises(FileExistsError):
        op.run()

    # GIVEN dst exists
    # WHEN instantiating OperationCopy
    # THEN raise FileExistsError
    with pytest.raises(FileExistsError):
        OperationQueue.CopyOperation(src, dst)


def test_OperationMove(tmp_path):
    """OperationMove moves a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = OperationQueue.MoveOperation(src, dst)

    assert op.src.exists()
    assert not op.dst.exists()
    op.run()
    assert not op.src.exists()
    assert op.dst.exists()


def test_OperationQueue_sorts_by_priority():
    """Operations are inserted into the OperationsQueue by their priority"""
    op1 = OperationQueue.Operation('one', priority=1, validate=False)
    op2 = OperationQueue.Operation('two', priority=2, validate=False)
    op3 = OperationQueue.Operation('three', priority=3, validate=False)

    l = OperationQueue.OperationQueue()
    l.put(op2)
    l.put(op1)
    l.put(op3)

    assert l.get_op(timeout=1) == op1
    assert l.get_op(timeout=1) == op2
    assert l.get_op(timeout=1) == op3

    with pytest.raises(queue.Empty):
        l.get_op(timeout=0.01)
