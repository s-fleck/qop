from typhon import OperationQueue, Converter, utils
import pytest


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


def test_ConvertOperation(tmp_path):
    """Dummy CopyConverter just copies a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = OperationQueue.ConvertOperation(src, dst, Converter.CopyConverter())

    assert op.src.exists()
    assert not op.dst.exists()
    op.run()
    assert op.src.exists()
    assert op.dst.exists()


def test_OperationQueue_sorts_by_priority(tmp_path):
    """Operations are inserted into the OperationsQueue by their priority"""
    op1 = OperationQueue.Operation('one', priority=1, validate=False)
    op2 = OperationQueue.Operation('two', priority=2, validate=False)
    op3 = OperationQueue.Operation('three', priority=3, validate=False)

    db = tmp_path.joinpath("typhon.db")
    l = OperationQueue.OperationQueue(path=db)

    l.put(op2)
    l.put(op1)
    l.put(op3)
    res = l.con.cursor().execute("SELECT status from operations").fetchall()
    assert all([el[0] == 2 for el in res])

    or1 = l.get()
    or2 = l.get()
    or3 = l.get()
    res = l.con.cursor().execute("SELECT status from operations").fetchall()
    assert all([el[0] == 1 for el in res])
    assert or1.serialize() == op1.serialize()
    assert or2.serialize() == op2.serialize()
    assert or3.serialize() == op3.serialize()

    with pytest.raises(IndexError):
        l.get()

    l.mark_done(or1)
    l.mark_done(or2)
    l.mark_done(or3)
    res = l.con.cursor().execute("SELECT status from operations").fetchall()
    assert all([el[0] == 0 for el in res])


def test_ConvertOperation_serializes_properly(tmp_path):
    """ConvertOperations can be inserted into the OperationsQueue"""
    f1 = utils.get_project_root("tests", "test_Converter", "16b.flac")
    op1 = OperationQueue.ConvertOperation(f1, 'od', priority=1, validate=False, converter=Converter.CopyConverter())
    op2 = OperationQueue.ConvertOperation(f1, 'td', priority=2, validate=False, converter=Converter.OggConverter())

    db = tmp_path.joinpath("typhon.db")
    l = OperationQueue.OperationQueue(path=db)

    l.put(op2)
    l.put(op1)
    res = l.con.cursor().execute("SELECT status from operations").fetchall()
    assert all([el[0] == 2 for el in res])

    or1 = l.get()
    or2 = l.get()
    res = l.con.cursor().execute("SELECT status from operations").fetchall()
    assert all([el[0] == 1 for el in res])
    assert or1.serialize() == op1.serialize()
    assert or2.converter.serialize() == op2.converter.serialize()
    assert or2.converter.serialize() == op2.converter.serialize()

    with pytest.raises(IndexError):
        l.get()

    l.mark_done(or1)
    l.mark_done(or2)
    res = l.con.cursor().execute("SELECT status from operations").fetchall()
    assert all([el[0] == 0 for el in res])
