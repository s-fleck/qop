from qcp import utils, operations, converters
import pytest


def test_Operation_fails_on_missing_src(tmp_path):
    src = tmp_path.joinpath("foo")
    """instantiating or validating Operation raises an error if file does not exist"""

    # GIVEN src does not exists
    # WHEN instantiating Operation
    # THEN raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        operations.Operation(src)

    # GIVEN src exists
    # WHEN instantiating Operation
    # THEN succeed
    src.touch()
    op = operations.Operation(src)
    op.validate()

    # GIVEN src does not exist
    # WHEN validating Operation
    # THEN raise FileNotFoundError
    src.unlink()
    with pytest.raises(FileNotFoundError):
        op.validate()


def test_Operations_can_be_compared():
    """operations can be compared"""
    op1 = operations.Operation('one', priority=1, validate=False)
    op2 = operations.Operation('two', priority=2, validate=False)
    op3 = operations.Operation('three', priority=3, validate=False)

    assert op1 == op1
    assert op1 != op2
    assert op1 < op2
    assert op1 < op3
    assert op3 > op1


def test_OperationDelete(tmp_path):
    """OperationDelete deletes a file"""
    src = tmp_path.joinpath("foo")
    src.touch()

    op = operations.DeleteOperation(src)
    assert op.src.exists()
    op.execute()
    assert not op.src.exists()


def test_OperationCopy(tmp_path):
    """OperationCopy copies a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = operations.CopyOperation(src, dst)

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

    op = operations.CopyOperation(src, dst)
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
        operations.CopyOperation(src, dst)


def test_OperationMove(tmp_path):
    """OperationMove moves a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = operations.MoveOperation(src, dst)

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

    op = operations.ConvertOperation(src, dst, converters.CopyConverter())

    assert op.src.exists()
    assert not op.dst.exists()
    op.run()
    assert op.src.exists()
    assert op.dst.exists()


def test_OperationQueue_sorts_by_priority(tmp_path):
    """operations are inserted into the OperationsQueue by their priority"""
    op1 = operations.Operation('one', priority=1, validate=False)
    op2 = operations.Operation('two', priority=2, validate=False)
    op3 = operations.Operation('three', priority=3, validate=False)

    oq = operations.OperationQueue(path=tmp_path.joinpath("qcp.db"))
    oq.put(op2)
    oq.put(op1)
    oq.put(op3)

    res = oq.con.cursor().execute("SELECT status from operations").fetchall()
    assert all([el[0] == 0 for el in res])
    assert oq.n_ops == 3

    or1 = oq.pop()
    or2 = oq.pop()
    or3 = oq.pop()
    res = oq.con.cursor().execute("SELECT status from operations").fetchall()
    assert all([el[0] == 1 for el in res])
    assert or1.to_dict() == op1.to_dict()
    assert or2.to_dict() == op2.to_dict()
    assert or3.to_dict() == op3.to_dict()

    with pytest.raises(IndexError):
        oq.pop()

    oq.mark_done(or1.oid)
    oq.mark_done(or2.oid)
    oq.mark_done(or3.oid)
    res = oq.con.cursor().execute("SELECT status from operations").fetchall()
    assert all([el[0] == 2 for el in res])


def test_ConvertOperation_serializes_properly(tmp_path):
    """ConvertOperations can be inserted into the OperationsQueue"""
    f1 = utils.get_project_root("tests", "test_Converter", "16b.flac")

    op1 = operations.ConvertOperation(f1, 'od', priority=1, validate=False, converter=Converter.CopyConverter())
    op2 = operations.ConvertOperation(f1, 'td', priority=2, validate=False, converter=Converter.OggConverter())

    oq = operations.OperationQueue(path=tmp_path.joinpath("qcp.db"))
    oq.put(op2)
    oq.put(op1)

    res = oq.con.cursor().execute("SELECT status from operations").fetchall()
    assert all([el[0] == 0 for el in res])

    or1 = oq.pop()
    or2 = oq.pop()
    res = oq.con.cursor().execute("SELECT status from operations").fetchall()
    assert all([el[0] == 1 for el in res])
    assert or1.to_dict() == op1.to_dict()
    assert or2.converter.to_dict() == op2.converter.to_dict()
    assert or2.converter.to_dict() == op2.converter.to_dict()

    with pytest.raises(IndexError):
        oq.pop()

    oq.mark_done(or1.oid)
    oq.mark_done(or2.oid)
    res = oq.con.cursor().execute("SELECT status from operations").fetchall()
    assert all([el[0] == 2 for el in res])


def test_OperationQueue_peek(tmp_path):
    """OperationQueue peek() behaves like pop() but without removing the element from the list"""

    op1 = operations.Operation('one', priority=1, validate=False)
    op2 = operations.Operation('two', priority=2, validate=False)
    op3 = operations.Operation('three', priority=3, validate=False)

    oq = operations.OperationQueue(path=tmp_path.joinpath("qcp.db"))
    oq.put(op2)
    oq.put(op1)
    oq.put(op3)

    o1 = oq.peek()
    o2 = oq.peek()
    o3 = oq.pop()

    assert o1.to_dict() == o2.to_dict()
    assert o1.to_dict() == o3.to_dict()


def test_OperationQueue_get_queue(tmp_path):
    """OperationQueue peek() behaves like pop() but without removing the element from the list"""

    op1 = operations.Operation('one', priority=1, validate=False)
    op2 = operations.CopyOperation('two', "2", priority=2, validate=False)
    op3 = operations.DeleteOperation('three', priority=3, validate=False)

    print(op1)
    print(op2)
    print(op3)

    oq = operations.OperationQueue(path=tmp_path.joinpath("qcp.db"))
    oq.put(op2)
    oq.put(op1)
    oq.put(op3)

    oq.print_queue()

    ol = list(oq.get_queue())

    assert ol[0].to_dict() == op1.to_dict()
    assert ol[1].to_dict() == op2.to_dict()
    assert ol[2].to_dict() == op3.to_dict()
