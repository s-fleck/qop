from qop import tasks, converters, _utils
from qop.exceptions import FileExistsAndIsIdenticalError
from qop.constants import Status
from pathlib import Path
import pytest
from time import sleep
from pydub import generators
import datetime
import mediafile
from mediafile import MediaFile


def wait_for_queue(queue: tasks.TaskQueue, timeout=30):
    i = 0
    while True:
        sleep(0.1)
        i = i + 1
        if not queue.is_active():
            break
        elif i > timeout * 10:
            raise TimeoutError


def test_Tasks_can_be_checked_for_equality():
    """Tasks can be checked for equality"""
    op1 = tasks.EchoTask('one')
    op2 = tasks.EchoTask('two')
    assert op1 == op1
    assert op1 != op2


def test_FileTask_fails_on_missing_src(tmp_path):
    src = tmp_path.joinpath("foo")
    """Instantiating or validating FileTask raises an error if file does not exist"""

    # GIVEN src does not exists
    # WHEN instantiating Task
    # THEN raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        tasks.FileTask(src).__validate__()

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


def test_DeleteTask(tmp_path):
    """DeleteTask deletes a file"""
    src = tmp_path.joinpath("foo")
    src.touch()

    op = tasks.DeleteTask(src)
    assert Path(op.src).exists()
    op.start()
    assert not Path(op.src).exists()


def test_CopyTask(tmp_path):
    """CopyTask copies a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = tasks.CopyTask(src, dst)

    assert Path(op.src).exists()
    assert not Path(op.dst).exists()
    op.start()
    assert Path(op.src).exists()
    assert Path(op.dst).exists()


def test_CopyTask_can_be_serialized(tmp_path):
    """CopyTask can be serialized to a dict"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    tsk = tasks.CopyTask(src, dst)
    # validate gets overwritten by from_dict
    assert tsk == tasks.Task.from_dict(tsk.__dict__)


def test_CopyTask_fails_on_existing_dst(tmp_path):
    """CopyTask fails if dst file exists"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = tasks.CopyTask(src, dst)
    op.start()
    with pytest.raises(FileExistsAndIsIdenticalError):
        op.start()

    with pytest.raises(FileExistsAndIsIdenticalError):
        tasks.CopyTask(src, dst).__validate__()


def test_MoveTask(tmp_path):
    """MoveTask moves a file"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    op = tasks.MoveTask(src, dst)

    assert Path(op.src).exists()
    assert not Path(op.dst).exists()
    op.start()
    assert not Path(op.src).exists()
    assert Path(op.dst).exists()


def test_MoveTask_can_be_serialized(tmp_path):
    """MoveTask can be serialized to dict"""
    src = tmp_path.joinpath("foo")
    dst = tmp_path.joinpath("bar")
    src.touch()

    tsk = tasks.MoveTask(src, dst)

    # validate field gets overwritten by from_dict
    assert tsk == tasks.Task.from_dict(tsk.__dict__)


def test_SimpleConvertTask(tmp_path):
    """SimpleConvertTask can convert an audio file"""
    src = tmp_path.joinpath("sine.flac")
    dst = tmp_path.joinpath("sine.mp3")

    sound = generators.Sine(440).to_audio_segment()
    sound.export(src, format="flac")

    tsk = tasks.SimpleConvertTask(src, dst, converter=converters.Mp3Converter())
    tsk.start()

    assert src.exists()
    assert dst.exists()


def test_SimpleConvertTask_can_keep_or_remove_album_art(tmp_path):
    """SimpleConvertTask can convert an audio file"""
    src = tmp_path.joinpath("sine.flac")
    mp3_art = tmp_path.joinpath("sine_art.mp3")
    mp3_noart = tmp_path.joinpath("sine_noart.mp3")
    ogg_art = tmp_path.joinpath("sine_art.ogg")
    ogg_noart = tmp_path.joinpath("sine_noart.ogg")

    sound = generators.Sine(440).to_audio_segment()
    sound.export(src, format="flac")

    # setup a source flac file with a cover image
    image_file = _utils.get_project_root().joinpath("tests/data/cover.jpg")
    with open(image_file, 'rb') as f:
        cover = f.read()
        cover = mediafile.Image(data=cover, desc=u'album cover', type=mediafile.ImageType.front)
    f = MediaFile(src)
    f.images = [cover]
    f.save()

    # by default, the converters preserve the album art
    # .. for Mp3Converter
    tasks.SimpleConvertTask(src, mp3_art, converter=converters.Mp3Converter()).start()
    g = MediaFile(mp3_art)
    assert f.images[0].data == cover.data
    assert f.images[0].mime_type == 'image/jpeg'
    assert f.images[0].data == g.images[0].data

    # ... for OggConverter
    tasks.SimpleConvertTask(src, ogg_art, converter=converters.OggConverter()).start()
    g = MediaFile(ogg_art)
    assert f.images[0].data == cover.data
    assert f.images[0].mime_type == 'image/jpeg'
    assert f.images[0].data == g.images[0].data

    # remove_art=True removes art
    # ... for Mp3Converter
    tasks.SimpleConvertTask(src, mp3_noart, converter=converters.Mp3Converter(remove_art=True)).start()
    g = MediaFile(mp3_noart)
    assert f.images[0].data == cover.data
    assert g.images == []

    # remove_art=True removes art
    # ... for OggConverter
    tasks.SimpleConvertTask(src, ogg_noart, converter=converters.OggConverter(remove_art=True)).start()
    g = MediaFile(ogg_noart)
    assert f.images[0].data == cover.data
    assert g.images == []


def test_ConvertTask(tmp_path):
    """ConvertTask can convert an audio file in a two-step process"""
    src = tmp_path.joinpath("sine.flac")
    dst = tmp_path.joinpath("sine.mp3")

    sound = generators.Sine(440).to_audio_segment()
    sound.export(src, format="flac")

    tsk = tasks.ConvertTask(src, dst, converter=converters.Mp3Converter(), tempdir=tmp_path)
    tsk.start()
    assert src.exists()
    assert not dst.exists()

    # mock `oid` that is used to match a task to its follow_up_task under production conditions. oid fulfils no function
    # if the follow_up_task is not executed by a TaskQueue.
    tsk.oid = 1
    tsk.spawn().start()
    assert src.exists()
    assert dst.exists()


def test_TaskQueueElements_can_be_ordered_by_priority():
    """TaskQueueElements can be ordered by their priority"""
    op1 = tasks.TaskQueueElement(tasks.EchoTask('one'), 1)
    op2 = tasks.TaskQueueElement(tasks.EchoTask('two'), 2)
    op3 = tasks.TaskQueueElement(tasks.EchoTask('three'), 3)

    assert op1 == op1
    assert op1 != op2
    assert op1 < op2
    assert op1 < op3
    assert op3 > op1


def test_TaskQueue_can_run_baisc_tasks(tmp_path):
    """TaskQueue can queue and run tasks"""
    src = tmp_path.joinpath("foo")
    src.touch()

    q = tasks.TaskQueue(tmp_path.joinpath("qop.db"))
    q.put(tasks.CopyTask(src, tmp_path.joinpath("copied_file")))
    q.run()
    wait_for_queue(q)
    assert tmp_path.joinpath("copied_file").is_file()
    q.put(tasks.MoveTask(tmp_path.joinpath("copied_file"), tmp_path.joinpath("moved_file")))
    q.run()
    wait_for_queue(q)
    assert not tmp_path.joinpath("copied_file").is_file()
    assert tmp_path.joinpath("moved_file").is_file()
    q.put(tasks.DeleteTask(tmp_path.joinpath("moved_file")))
    q.run()
    wait_for_queue(q)
    assert not tmp_path.joinpath("moved_file").is_file()
    assert src.is_file()
    assert q.active_processes() == 0


def test_TaskQueue_can_run_convert_tasks(tmp_path):
    """Ensure all TaskQueue transfer and convert processes are closed after the queue finishes processing all tasks"""

    # test this on a ConvertTask because that launches additional processes that we also want to ensure are shut down
    sound = generators.Sine(440).to_audio_segment()
    src = tmp_path.joinpath("sine.flac")
    dst = tmp_path.joinpath("sine.mp3")

    sound.export(src, format="flac")
    q = tasks.TaskQueue(tmp_path.joinpath("qop.db"))
    q.put(tasks.ConvertTask(src, dst, converter=converters.OggConverter()))
    q.run()
    wait_for_queue(q)

    assert src.exists()
    assert dst.exists()
    assert q.n_active == 0
    assert q.active_processes() == 0


def test_TaskQueue_sorts_by_priority(tmp_path):
    """Tasks are retrieved from the TasksQueue in order of their priority"""
    op1 = tasks.EchoTask('one')
    op2 = tasks.EchoTask('two')
    op3 = tasks.EchoTask('three')

    oq = tasks.TaskQueue(path=tmp_path.joinpath("qop.db"))
    oq.put(op2, 2)
    oq.put(op1, 1)
    oq.put(op3, 3)

    res = oq.con.cursor().execute("SELECT status from tasks").fetchall()

    assert all([el[0] == Status.PENDING for el in res])
    assert oq.n_total == 3

    or1 = oq.pop()
    or2 = oq.pop()
    or3 = oq.pop()
    res = oq.con.cursor().execute("SELECT status from tasks").fetchall()
    assert all([el[0] == Status.ACTIVE for el in res])

    # marking an object as done changes its status
    oq.set_status(or1.oid, Status.OK)
    oq.set_status(or2.oid, Status.OK)
    oq.set_status(or3.oid, Status.OK)
    res = oq.con.cursor().execute("SELECT status from tasks").fetchall()
    assert all([el[0] == Status.OK for el in res])

    # object has not changed by serialisation (except for oid attribute)
    or1.__delattr__("oid")
    or2.__delattr__("oid")
    or3.__delattr__("oid")
    assert or1 == op1
    assert or2 == op2
    assert or3 == op3

    with pytest.raises(IndexError):
        oq.pop()


def test_TaskQueue_peek_does_not_modify_queue(tmp_path):
    """TaskQueue peek() behaves like pop() but without modifying the queue"""
    op1 = tasks.EchoTask('one')
    op2 = tasks.EchoTask('two')
    op3 = tasks.EchoTask('three')

    oq = tasks.TaskQueue(path=tmp_path.joinpath("qop.db"))
    oq.put(op2, 1)
    oq.put(op1, 1)
    oq.put(op3, 3)

    o1 = oq.peek().__dict__
    o2 = oq.peek().__dict__
    o3 = oq.pop().__dict__
    o3.__delitem__('oid')
    o3['oid'] = None

    assert o1 == o2
    assert o1 == o3


def test_TaskQueue_fetch(tmp_path):
    """TaskQueue.fetch() fetches the contents of the queue without modifying it"""
    src = tmp_path.joinpath("foo")
    src.touch()

    q = tasks.TaskQueue(tmp_path.joinpath("qop.db"))
    q.put(tasks.CopyTask(src, tmp_path.joinpath("copied_file")))
    q.put(tasks.DeleteTask(tmp_path.joinpath("copied_file"), validate=False))
    q.put(tasks.DeleteTask(tmp_path.joinpath("foo")))

    # all three are pending
    assert len(q.fetch(status=None, n=5)) == 3
    assert len(q.fetch(status=(Status.PENDING, Status.OK), n=3)) == 3
    assert len(q.fetch(status=None, n=None)) == 3
    assert len(q.fetch(status=(Status.PENDING,), n=None)) == 3

    q.run()
    sleep(0.5)
    assert len(q.fetch(status=None, n=5)) == 3
    assert len(q.fetch(status=Status.FAIL, n=5)) == 0
    assert len(q.fetch(status=Status.OK, n=5)) == 3


def test_TaskQueue_runs_nonblocking(tmp_path):
    """Ensure processing of the queue happens in a background process and does not block the main process"""
    src = tmp_path.joinpath("foo")
    src.touch()

    q = tasks.TaskQueue(tmp_path.joinpath("qop.db"))

    tick = datetime.datetime.now().timestamp()
    q.put(tasks.SleepTask(2))
    q.run()
    tock = datetime.datetime.now().timestamp()
    sleep(1)

    assert tick - tock < 1
    assert q.n_active == 1
    sleep(2)
    assert q.n_active == 0

