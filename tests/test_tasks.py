from qcp import utils, tasks, converters
import pytest


def test_FileTask_fails_on_missing_src(tmp_path):
    src = tmp_path.joinpath("foo")
    """instantiating or validating Task raises an error if file does not exist"""

    # GIVEN src does not exists
    # WHEN instantiating Task
    # THEN raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        tasks.FileTask(src)

    # GIVEN src exists
    # WHEN instantiating Task
    # THEN succeed
    src.touch()
    op = tasks.FileTask(src)
    op.__validate__()

    # GIVEN src does not exist
    # WHEN validating Task
    # THEN raise FileNotFoundError
    src.unlink()
    with pytest.raises(FileNotFoundError):
        op.__validate__()


def test_Tasks_can_be_compared():
    """tasks can be compared"""
    op1 = tasks.EchoTask('one')
    op2 = tasks.EchoTask('two')
    assert op1 == op1
    assert op1 != op2


def test_TaskQueueElement_can_be_compared():
    """tasks can be compared"""
    op1 = tasks.TaskQueueElement(tasks.EchoTask('one'), 1)
    op2 = tasks.TaskQueueElement(tasks.EchoTask('two'), 2)
    op3 = tasks.TaskQueueElement(tasks.EchoTask('three'), 3)

    assert op1 == op1
    assert op1 != op2
    assert op1 < op2
    assert op1 < op3
    assert op3 > op1


def test_TaskDelete(tmp_path):
    """TaskDelete deletes a file"""
    src = tmp_path.joinpath("foo")
    src.touch()

    op = tasks.DeleteTask(src)
    assert op.src.exists()
    op.run()
    assert not op.src.exists()


def test_CopyTask(tmp_path):
    """TaskCopy copies a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = tasks.CopyTask(src, dst)

    assert op.src.exists()
    assert not op.dst.exists()
    op.run()
    assert op.src.exists()
    assert op.dst.exists()

def test_CopyTask_can_be_serialized(tmp_path):
    """TaskCopy copies a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    tsk = tasks.CopyTask(src, dst)
    assert tsk == tasks.from_dict(tsk.__dict__)


def test_CopyTask_fails_on_existing_dst(tmp_path):
    """TaskCopy fails if dst file exists"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = tasks.CopyTask(src, dst)
    op.run()

    # GIVEN dst exists
    # WHEN executing TaskCopy
    # THEN raise FileExistsError
    with pytest.raises(FileExistsError):
        op.run()

    # GIVEN dst exists
    # WHEN instantiating TaskCopy
    # THEN raise FileExistsError
    with pytest.raises(FileExistsError):
        tasks.CopyTask(src, dst)


def test_MoveTask(tmp_path):
    """TaskMove moves a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = tasks.MoveTask(src, dst)

    assert op.src.exists()
    assert not op.dst.exists()
    op.run()
    assert not op.src.exists()
    assert op.dst.exists()


def test_MoveTask_can_be_serialized(tmp_path):
    """TaskCopy copies a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    tsk = tasks.MoveTask(src, dst)
    assert tsk == tasks.from_dict(tsk.__dict__)


def test_ConvertTask(tmp_path):
    """Dummy CopyConverter just copies a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = tasks.ConvertTask(src, dst, converters.CopyConverter())

    assert op.src.exists()
    assert not op.dst.exists()
    op.run()
    assert op.src.exists()
    assert op.dst.exists()


def test_ConvertTask_can_be_serialized(tmp_path):
    """TaskCopy copies a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    tsk = tasks.ConvertTask(src, dst, converters.CopyConverter())
    assert tsk == tasks.from_dict(tsk.__dict__)


def test_TaskQueue_sorts_by_priority(tmp_path):
    """tasks are inserted into the TasksQueue by their priority"""
    op1 = tasks.EchoTask('one')
    op2 = tasks.EchoTask('two')
    op3 = tasks.EchoTask('three')

    oq = tasks.TaskQueue(path=tmp_path.joinpath("qcp.db"))
    oq.put(op2, 2)
    oq.put(op1, 1)
    oq.put(op3, 3)

    res = oq.con.cursor().execute("SELECT status from tasks").fetchall()
    assert all([el[0] == 0 for el in res])
    assert oq.n_ops == 3

    or1 = oq.pop()
    or2 = oq.pop()
    or3 = oq.pop()
    res = oq.con.cursor().execute("SELECT status from tasks").fetchall()
    assert all([el[0] == 1 for el in res])

    # marking an object as done changes is status
    oq.mark_done(or1.oid)
    oq.mark_done(or2.oid)
    oq.mark_done(or3.oid)
    res = oq.con.cursor().execute("SELECT status from tasks").fetchall()
    assert all([el[0] == 2 for el in res])

    # object has not changed by serialisation (except for oid attribute)
    or1.__delattr__("oid")
    or2.__delattr__("oid")
    or3.__delattr__("oid")
    assert or1 == op1
    assert or2 == op2
    assert or3 == op3

    with pytest.raises(IndexError):
        oq.pop()


def test_ConvertTask_serializes_properly(tmp_path):
    """ConvertTasks can be inserted into the TasksQueue"""
    f1 = utils.get_project_root("tests", "test_Converter", "16b.flac")

    op1 = tasks.ConvertTask(f1, 'od', validate=False, converter=converters.CopyConverter())
    op2 = tasks.ConvertTask(f1, 'td', validate=False, converter=converters.OggConverter())

    oq = tasks.TaskQueue(path=tmp_path.joinpath("qcp.db"))
    oq.put(op2)
    oq.put(op1)

    res = oq.con.cursor().execute("SELECT status from tasks").fetchall()
    assert all([el[0] == 0 for el in res])

    or1 = oq.pop()
    or2 = oq.pop()
    res = oq.con.cursor().execute("SELECT status from tasks").fetchall()
    assert all([el[0] == 1 for el in res])
    assert or1.to_dict() == op1.to_dict()
    assert or2.converter.to_dict() == op2.converter.to_dict()
    assert or2.converter.to_dict() == op2.converter.to_dict()

    with pytest.raises(IndexError):
        oq.pop()

    oq.mark_done(or1.oid)
    oq.mark_done(or2.oid)
    res = oq.con.cursor().execute("SELECT status from tasks").fetchall()
    assert all([el[0] == 2 for el in res])


def test_TaskQueue_peek(tmp_path):
    """TaskQueue peek() behaves like pop() but without removing the element from the list"""

    op1 = tasks.Task('one', priority=1, validate=False)
    op2 = tasks.Task('two', priority=2, validate=False)
    op3 = tasks.Task('three', priority=3, validate=False)

    oq = tasks.TaskQueue(path=tmp_path.joinpath("qcp.db"))
    oq.put(op2)
    oq.put(op1)
    oq.put(op3)

    o1 = oq.peek().to_dict()
    o2 = oq.peek().to_dict()
    o3 = oq.pop().to_dict()

    assert o1 == o2
    assert o1 == o3
