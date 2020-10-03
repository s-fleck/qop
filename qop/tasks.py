"""
This module defines classes for working with :class:`Tasks <qop.tasks.Task>`. A task is an atomic operation - such as copying,
moving or transcoding a file - that can be stored and executed at a later date (and usually only once).

:class:`~qop.tasks.TaskQueue` is a persistent (via `sqlite3 <https://docs.python.org/3.8/library/sqlite3.html>`_),
prioritized queue with `multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`_ support that can
store and execute an arbitrary number of tasks.

"""


import shutil
import os
import json
import sqlite3
import uuid
import filecmp
import multiprocessing
import logging
from pathlib import Path
from typing import Union, Optional, Dict, Tuple, List
from time import sleep

import appdirs
from colorama import init, Fore

from qop.constants import Status, TaskType, Pathish
from qop.exceptions import AlreadyUnderEvaluationError, FileExistsAndIsIdenticalError, FileExistsAndCannotBeComparedError
from qop import converters, _utils


init()

lg = logging.getLogger(__name__)
CONVERT_CACHE_DIR = Path(appdirs.user_cache_dir("qop")).joinpath("convert_temp")


class TaskQueue:
    """
    A persistent, prioritized queue with multi process support. Use sqlite3 as a storage backend.

    :param transfer_processes: A list of file transfer processes
    :param convert_processes: A list of audio transcode processes
    """
    transfer_processes = []
    convert_processes = []

    def __init__(self, path: Pathish, max_transfer_processes=1, max_convert_processes=multiprocessing.cpu_count() - 1) -> None:
        """
        Instantiate a TaskQueue

        A TaskQueue is a sqlite3 database with the following columns
        - priority: integer value, the lower the value the earlier the task will be processed
        - task: json representation of the task to execute
        - status: status of the task (ok, active, fail,... see enums.Status)
        - lock: str lock id. NULL except for currently active tasks. usually an uuid

        :param path: Path to store the persistent queue
        :type path: Path or str
        :param max_transfer_processes: maximum number of processes spawned for file transfer operations. Should usually
            be 1.
        :type max_transfer_processes: int
        :param max_convert_processes: maximum number of processes spawned for converting audio files. Defaults to
            `number-of-cpu-cores - 1`.
        :type max_convert_processes: int
        """
        path = Path(path).resolve()
        if path.exists():
            lg.info(f"using existing queue {path}")
        else:
            lg.info(f"initializing new queue {path}")

        self.max_transfer_processes = max_transfer_processes
        self.max_convert_processes = max_convert_processes
        self.con = sqlite3.connect(path, isolation_level="EXCLUSIVE", timeout=10)
        self.path = Path(path)

        cur = self.con.cursor()
        cur.execute("""
           CREATE TABLE IF NOT EXISTS tasks (
              priority INTEGER NOT NULL,
              task TEXT NOT NULL,
              status INTEGER NOT NULL,
              lock TEXT,
              parent INTEGER,
              UNIQUE(task, status)              
            )              
        """)
        self.con.commit()

    def put(self, task: "Task", priority: int = 10, parent: Optional[int] = None) -> None:
        """
        Enqueue a Task

        :param task: Task to be added to the queue
        :param priority: (optional) priority for executing `task` (tasks with lower priority will be executed earlier)
        :param parent: (optional) only for child tasks, oid/_ROWID_ of the task that spawned this task
        """

        lg.debug(f"trying to inserted task {task.to_dict()}")
        cur = self.con.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO tasks (priority, task, status, parent) VALUES (?, ?, ?, ?)",
            (priority, task.to_json(), Status.PENDING, parent)
        )

        hammer_commit(self.con)
        lg.debug(f"inserted task {task.to_dict()}")

    def pop(self, task_type_include: Optional[TaskType] = None, task_type_exclude: Optional[TaskType] = None) -> "Task":
        """
        Retrieves a :class:`~qop.tasks.Task` and sets its status in the queue to :class:`Status.ACTIVE <qop.constants.Status>`

        :raises AlreadyUnderEvaluationError: If trying to pop a tasks that is already being processed  (i.e. if a race
            condition occurs if the queue is processed in parallel)
        """
        cur = self.con.cursor()

        assert task_type_include is None or task_type_exclude is None

        if task_type_include is not None:
            cur.execute(
                f"SELECT _ROWID_ FROM tasks WHERE status = ? AND task LIKE '__type___{int(task_type_include)}%' ORDER BY priority LIMIT 1",
                (Status.PENDING,))
        elif task_type_exclude is not None:
            cur.execute(
                f"SELECT _ROWID_ FROM tasks WHERE status = ? AND task NOT LIKE '__type___{int(task_type_exclude)}%' ORDER BY priority LIMIT 1",
                (Status.PENDING,))
        else:
            cur.execute("SELECT _ROWID_ FROM tasks WHERE status = ? ORDER BY priority LIMIT 1", (Status.PENDING,))

        oid = cur.fetchall()[0][0].__str__()

        # insert a lock UUID into the table so that we can ensure not second thread tries to execute the same
        # task
        lock = uuid.uuid4().hex
        self.set_status(oid, Status.ACTIVE, lock)
        cur.execute("SELECT lock, task FROM tasks WHERE _ROWID_ = ?", (oid,))
        record = cur.fetchall()[0]
        cur.close()

        if record[0] != lock:
            raise AlreadyUnderEvaluationError

        task = Task.from_dict(json.loads(record[1]))
        lg.debug(f"popped task {task}")
        task.oid = oid
        return task

    def peek(self) -> "Task":
        """
        Retrieves a :class:`~qop.tasks.Task` without changing its status in the queue
        """
        cur = self.con.cursor()
        cur.execute("SELECT lock, task from tasks ORDER BY priority LIMIT 1")
        record = cur.fetchall()[0]
        oid = record[0]

        if oid is not None:
            oid = str(oid)

        task = Task.from_dict(json.loads(record[1]))
        task.oid = oid
        return task

    def print(self, status: Union[Tuple, int, None] = None, n: int = 10) -> None:
        """
        Print an overview of the queue

        :param n: number of tasks to fetch
        :param status: If not None, only fetch Tasks of the given status(es)
        """
        assert isinstance(n, int) and (n > 0)
        records = self.fetch(n=n, status=status)
        for record in records:
            print(f"[{record[0]}] {Task.from_dict(json.loads(record[1]))}")

    def fetch(self, status: Union[Tuple, int, Status, None] = None, n: Optional[int] = None) -> List:
        """
        Retrieve the queue

        :param n: number of tasks to fetch
        :param status: If not None, only fetch Tasks of the given status(es)

        :return a dict containing n queued tasks
        """

        if status is not None:
            if isinstance(status, int):
                status = (status,)
            elif isinstance(status, Status):
                status = (int(status),)
            if len(status) > 0:
                status = tuple(int(s) for s in status)
            else:
                raise ValueError("illegal status")

            cur = self.con.cursor()
            if n:
                cur.execute(
                    f"SELECT status, task FROM tasks "
                    f"WHERE status IN ({','.join(['?' for x in status])}) "
                    f"ORDER BY priority LIMIT ?",
                    status + (n,)
                )
            else:
                cur.execute(
                    f"SELECT status, task FROM tasks "
                    f"WHERE status IN ({','.join(['?' for x in status])})"
                    "ORDER BY priority",
                    status
                )
        else:
            cur = self.con.cursor()
            if n:
                cur.execute("SELECT status, task from tasks ORDER BY priority LIMIT ?", (str(n),))
            else:
                cur.execute("SELECT status, task from tasks ORDER BY priority")

        res = cur.fetchall()
        cur.close()
        res = [{"priority":x[0], "task":json.loads(x[1])} for x in res]
        return res

    def reset_active_tasks(self) -> None:
        """
        Reset all active tasks to :class:`Status.PENDING <qop.constants.Status>`
        """
        lg.info(f"set all active tasks to pending")
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = ?, lock = NULL where status = ?", (int(Status.PENDING), int(Status.ACTIVE)))
        hammer_commit(self.con)
        cur.close()

    def set_status(self, oid: int, status: Status, lock: str = None) -> None:
        """
        Set the :class:`~qop.constants.Status` of the queued task with _ROWID_ `oid`

        :param oid: _ROWID_ of the task to mark
        :param status: :class:`~qop.constants.Status` to set
        :param lock: Unique ID for locking queued tasks. The lock value
            can be querried by the executing task to verify that it is the
            only tasks that attempts to run this specific task.
            Must be `None` except for switching tasks to *active*.
        """
        lg.info(f"mark {oid} {status.name}")
        cur = self.con.cursor()

        if status == Status.ACTIVE:
            assert lock is not None
            cur.execute("UPDATE tasks SET status = ?, lock = ? where _ROWID_ = ?", (int(status), lock, oid))
        else:
            assert lock is None
            cur.execute("UPDATE tasks SET status = ?, lock = NULL where _ROWID_ = ?", (int(status), oid))

        hammer_commit(self.con)
        cur.close()

    def run(self, ip=None, port=None) -> None:
        """Execute all pending tasks"""

        # remove finished que runs
        self.transfer_processes = [p for p in self.transfer_processes if p.is_alive()]
        self.convert_processes = [p for p in self.convert_processes if p.is_alive()]

        if self.n_pending < 1:
            lg.info("queue is empty")
            return None

        if len(self.transfer_processes) >= self.max_transfer_processes:
            lg.debug(f"already active {self.max_transfer_processes} queues")

        if len(self.convert_processes) >= self.max_convert_processes:
            lg.debug(f"already active {self.max_convert_processes} convert queues")

        while len(self.transfer_processes) < self.max_transfer_processes:
            lg.info("starting new queue runner")
            p = multiprocessing.Process(target=self.__start_run_process, args=(ip, port, None, TaskType.CONVERT))
            p.start()
            self.transfer_processes.append(p)

        while len(self.convert_processes) < self.max_convert_processes:
            lg.info("starting new convert queue runner")
            p = multiprocessing.Process(target=self.__start_run_process, args=(ip, port, TaskType.CONVERT, None))
            p.start()
            self.convert_processes.append(p)

    def __start_run_process(
            self,
            ip: Optional[str] = None,
            port: Optional[int] = None,
            task_type_include: Optional[TaskType] = None,
            task_type_exclude: Optional[TaskType] = None
    ) -> None:
        """
        Launch a single process that executes the tasks stored in the queue. This function is called internally
        by self.run() and should not be called directly.

        If a daemon is specified via *port* and *ip*, the queue will terminate once it can no longer affirm that the
        daemon is running. This is relevant in case the daemon is not closed via `qop daemon stop` but killed
        or simply crashes.

        *task_type_include* and *task_type_exclude* are passed to 
        :func:`qop.tasks.TaskQueue.pop`

        :param ip: (optional)ip of the daemon that runs this queue
        :param port: port of the daemon
        :param task_type_include: see .pop()
        :param task_type_exclude: see .pop()
        """
        progress = self.progress()
        while progress.pending > 0 or progress.active > 0:
            progress = self.progress()

            if ip is not None:
                if _utils.is_daemon_active(ip=ip, port=port) is False:
                    lg.fatal("cannot find daemon thread. stopping queue.")
                    break
            try:
                op = self.pop(task_type_include=task_type_include, task_type_exclude=task_type_exclude)
            except:
                lg.debug("waiting for more tasks of correct status")
                sleep(1)
                continue
            try:
                op.start()
                lg.info(f"task finished: {op}")
                self.set_status(op.oid, Status.OK)
                try:
                    follow_up = op.spawn()
                    self.put(follow_up, priority=-1, parent=op.oid)
                    lg.info(f"spawned childtask: {follow_up}")
                except:
                    pass

                if op.parent_oid is not None:
                    self.set_status(op.parent_oid, Status.OK)
                    lg.info(f"parent task finished: {op.parent_oid}")

            except:
                lg.error(f"task failed: {op}", exc_info=True)
                self.set_status(op.oid, Status.FAIL)
                if op.parent_oid is not None:
                    self.set_status(op.parent_oid, Status.FAIL)
                    lg.info(f"parent task completed: {op.parent_oid}")

        try:
            shutil.rmtree(CONVERT_CACHE_DIR)
        except:
            pass

        lg.info("queue is finished")

    def stop(self) -> None:
        for p in self.convert_processes + self.transfer_processes:
            p.terminate()
        self.reset_active_tasks()

    def flush(self, status: Union[Status, int, None] = None) -> None:
        """empty the queue"""
        cur = self.con.cursor()
        if status is None:
            cur.execute("DELETE FROM tasks")
            lg.info("flushing queue")
        else:
            cur.execute("DELETE FROM tasks where status == ?", (int(status),))
            lg.info(f"flushing tasks with status '{status.name}' from queue")
        hammer_commit(self.con)
        cur.close()

    def facts(self) -> Dict:
        ap_convert = self.active_processes("convert")
        ap_transfer = self.active_processes("transfer")
        pr = self.progress()

        return {
            "queue.path": str(self.path),
            "queue.active": (ap_convert + ap_transfer) > 0,
            "processes.max": self.max_transfer_processes + self.max_convert_processes,
            "processes.max.transfer": self.max_transfer_processes,
            "processes.max.convert": self.max_convert_processes,
            "processes.active": ap_convert + ap_transfer,
            "processes.active.transfer": ap_transfer,
            "processes.active.convert": ap_convert,
            "tasks.pending": pr.pending,
            "tasks.ok": pr.ok,
            "tasks.skip": pr.skip,
            "tasks.active": pr.active,
            "tasks.fail": pr.fail,
            "tasks.total": pr.total
        }

    def is_active(self) -> bool:
        return self.active_processes() > 0

    def active_processes(self, type=None):
        if type is None:
            l = self.transfer_processes + self.convert_processes
        elif type == "transfer":
            l = self.transfer_processes
        elif type == "convert":
            l = self.convert_processes
        else:
            raise ValueError

        res = 0
        for p in l:
            if p.is_alive():
                res += 1
        return res

    @property
    def n_total(self) -> int:
        """Count of all tasks in queue (including failed and completed)"""
        cur = self.con.cursor()
        res = cur.execute("SELECT COUNT(1) from tasks").fetchall()[0][0]
        cur.close()
        return res

    @property
    def n_pending(self) -> int:
        """Number of pending tasks"""
        cur = self.con.cursor()
        res = cur.execute("SELECT COUNT(1) FROM tasks WHERE status = ?", (int(Status.PENDING),)).fetchall()[0][0]
        cur.close()
        return res

    @property
    def n_active(self) -> int:
        """Count of currently active tasks"""
        cur = self.con.cursor()
        res = cur.execute("SELECT COUNT(1) FROM tasks WHERE status = ?", (int(Status.ACTIVE),)).fetchall()[0][0]
        cur.close()
        return res

    @property
    def n_ok(self) -> int:
        """count of completed tasks"""
        cur = self.con.cursor()
        res = cur.execute("SELECT COUNT(1) from tasks WHERE status = ?", (int(Status.OK),)).fetchall()[0][0]
        cur.close()
        return res

    @property
    def n_fail(self) -> int:
        """count of completed tasks"""
        cur = self.con.cursor()
        res = cur.execute("SELECT COUNT(1) from tasks WHERE status = ?", (int(Status.FAIL),)).fetchall()[0][0]
        cur.close()
        return res

    @property
    def n_skip(self) -> int:
        """count of completed tasks"""
        cur = self.con.cursor()
        res = cur.execute("SELECT COUNT(1) from tasks WHERE status = ?", (int(Status.SKIP),)).fetchall()[0][0]
        cur.close()
        return res

    def progress(self, include_children: bool = False) -> "QueueProgress":
        cur = self.con.cursor()
        if include_children:
            cur.execute("SELECT status, COUNT(1) from tasks GROUP BY status")
        else:
            cur.execute("SELECT status, COUNT(1) FROM tasks WHERE parent is NULL GROUP BY status")
        res = cur.fetchall()
        cur.close()

        return QueueProgress.from_list(res)


class Task:
    """Abstract class for qop Tasks. Should not be instantiated directly."""

    """optional rowid of the task in the queue. only for tasks that were retrieved from the queue."""
    oid = None
    parent_oid = None

    def __init__(self) -> None:
        self.type = 0

    def start(self) -> None:
        """Run a task"""
        raise NotImplementedError

    @staticmethod
    def from_dict(x: Dict) -> "Task":
        """Create a Task of the appropriate subclass from a python dict"""
        lg.debug(f"parsing task {x}")
        task_type = x["type"]

        if task_type == 0:
            return Task()
        elif task_type == TaskType.ECHO:
            return EchoTask(x["msg"])
        elif task_type == TaskType.FILE:
            return FileTask(x["src"])
        elif task_type == TaskType.DELETE:
            return DeleteTask(x["src"])
        elif task_type == TaskType.COPY:
            return CopyTask(x["src"], x["dst"])
        elif task_type == TaskType.MOVE:
            return MoveTask(x["src"], x["dst"], x['parent_oid'])
        elif task_type == TaskType.CONVERT_SIMPLE:
            return SimpleConvertTask(x['src'], x['dst'], converter=converters.Converter.from_dict(x["converter"]))
        elif task_type == TaskType.CONVERT:
            return ConvertTask(x['src'], x['dst'], converter=converters.Converter.from_dict(x["converter"]))
        elif task_type == TaskType.FAIL:
            return FailTask()
        elif task_type == TaskType.SLEEP:
            return SleepTask(x['seconds'])
        else:
            raise UnknownTaskTypeError

    def __repr__(self) -> str:
        return 'NULL'

    def __eq__(self, other) -> bool:
        return self.__dict__ == other.__dict__

    def __ne__(self, other) -> bool:
        return self.__dict__ != other.__dict__

    def to_dict(self) -> Dict:
        r = self.__dict__.copy()

        for el in ("src", "dst"):
            if el in r.keys():
                r[el] = str(r[el])
        return r

    def color_repr(self, color=True):
        self.__repr__()

    def __validate__(self) -> None:
        pass

    def to_json(self):
        return json.dumps(self.to_dict())


class EchoTask(Task):
    """Log a message"""
    def __init__(self,  msg: str) -> None:
        super().__init__()
        self.msg = msg
        self.type = TaskType.ECHO

    def start(self) -> None:
        print(self.msg)

    def __repr__(self) -> str:
        return f'Echo: "{self.msg}"'

    def color_repr(self, color=True):
        if color:
            op = Fore.YELLOW + "Echo" + Fore.RESET
            msg = Fore.BLUE + self.msg + Fore.RESET
            return f'{op} {msg}'
        else:
            return self.__repr__()


class SleepTask(Task):
    """Log a message"""
    def __init__(self,  seconds: float) -> None:
        super().__init__()
        self.seconds = seconds
        self.type = TaskType.SLEEP

    def start(self) -> None:
        lg.debug(f"sleeping for {self.seconds} seconds")
        sleep(self.seconds)
        lg.debug("woke up")

    def __repr__(self) -> str:
        return f'Sleep: "{self.seconds}"'

    def color_repr(self, color=True):
        if color:
            op = Fore.YELLOW + "Sleep" + Fore.RESET
            msg = Fore.BLUE + self.seconds + Fore.RESET
            return f'{op} {msg}'
        else:
            return self.__repr__()


class FailTask(Task):
    """Log a message"""
    def __init__(self) -> None:
        super().__init__()
        self.msg = "This task always fails"
        self.type = TaskType.FAIL

    def start(self) -> None:
        raise AssertionError

    def __repr__(self) -> str:
        return f'Fail: Always raise an error"'


class FileTask(Task):
    """Abstract class for all file-based tasks"""
    def __init__(self, src: Pathish) -> None:
        super().__init__()
        self.src = Path(src).resolve()
        self.type = None

    def start(self) -> None:
        pass

    def __validate__(self) -> None:
        if not self.src.exists():
            raise FileNotFoundError(f'{self.src} does not exist')
        elif not (self.src.is_dir() or self.src.is_file()):
            raise TypeError(f'{self.src} is neither a file nor directory')


class DeleteTask(FileTask):
    """Delete a file"""
    def __init__(self, src: Pathish, validate: bool = True) -> None:
        super().__init__(src=src)
        self.type = TaskType.DELETE

    def start(self) -> None:
        os.unlink(self.src)

    def __repr__(self) -> str:
        return f'DEL {self.src}'


class CopyTask(FileTask):
    """Copy a file"""
    def __init__(self, src: Pathish, dst: Pathish) -> None:
        super().__init__(src=src)
        self.dst = Path(dst).resolve()
        self.type = TaskType.COPY

    def color_repr(self, color=True) -> str:
        if color:
            op = Fore.YELLOW + "COPY" + Fore.RESET
            arrow = Fore.YELLOW + "->" + Fore.RESET
            src = self.src
            dst = self.dst
            return f'{op} {src} {arrow} {dst}'
        else:
            return self.__repr__()

    def __repr__(self) -> str:
        return f'COPY {self.src} -> {self.dst}'

    def __validate__(self) -> None:
        super().__validate__()
        if self.dst.exists():
            if filecmp.cmp(self.dst, self.src):
                raise FileExistsAndIsIdenticalError
            else:
                raise FileExistsError

    def start(self) -> None:
        self.__validate__()
        if not self.dst.parent.exists():
            self.dst.parent.mkdir(parents=True)

        if self.src.is_dir():
            shutil.copytree(self.src, self.dst)
        else:
            shutil.copy(self.src, self.dst)


class MoveTask(CopyTask):
    """Move a file"""
    def __init__(self, src: Pathish, dst: Pathish, parent_oid=None) -> None:
        super().__init__(src=src, dst=dst)
        self.type = TaskType.MOVE
        self.parent_oid = parent_oid

    def start(self) -> None:
        super().__validate__()
        if not self.dst.parent.exists():
            self.dst.parent.mkdir(parents=True)

        shutil.move(self.src, self.dst)

    def color_repr(self, color=True) -> str:
        if color:
            op = Fore.YELLOW + "MOVE" + Fore.RESET
            arrow = Fore.YELLOW + "->" + Fore.RESET
            src = self.src
            dst = self.dst
            return f'{op} {src} {arrow} {dst}'
        else:
            return self.__repr__()

    def __repr__(self) -> str:
        return f'MOVE {self.src} -> {self.dst}'


class SimpleConvertTask(CopyTask):
    """convert an audio file"""
    def __init__(self, src: Pathish, dst: Pathish, converter: converters.Converter) -> None:
        super().__init__(src=src, dst=dst)
        self.type = TaskType.CONVERT_SIMPLE
        self.converter = converter
        self.src = Path(src).resolve()
        self.dst = Path(dst).resolve()

    def start(self) -> None:
        super().__validate__()
        self.converter.run(self.src, self.dst)

    def color_repr(self, color=True) -> str:
        if color:
            op = Fore.YELLOW + "SCON" + Fore.RESET
            arrow = Fore.YELLOW + "->" + Fore.RESET
            src = self.src
            dst = self.dst
            return f'{op} {src} {arrow} {dst}'
        else:
            return self.__repr__()

    def __repr__(self) -> str:
        return f'SCON {self.src} -> {self.dst}'

    def __validate__(self) -> None:
        if not self.src.exists():
            raise FileNotFoundError
        if self.dst.exists():
            raise FileExistsAndCannotBeComparedError

    def to_dict(self) -> Dict:
        r = self.__dict__.copy()
        for el in ("src", "dst"):
            if el in r.keys():
                r[el] = str(r[el])
        r["converter"] = self.converter.to_dict()
        return r


class ConvertTask(SimpleConvertTask):
    """
    ConvertTask transcodes an audio file to a temporary directory and then adds a move task to the queue.
    This makes it possible to cleanly separate transcode and transfer processes.
    """
    def __init__(self, src: Pathish, dst: Pathish, converter: converters.Converter, tempdir=CONVERT_CACHE_DIR) -> None:
        super().__init__(src=src, dst=dst, converter=converter)
        self.type = TaskType.CONVERT
        self.tmpdst = tempdir.joinpath(uuid.uuid4().hex)

    def start(self) -> None:
        super().__validate__()
        lg.debug(f"converting file to temporary destination: {self.tmpdst}")
        self.converter.run(self.src, self.tmpdst)

    def spawn(self) -> MoveTask:
        """
        Spawns a follow-up task. If a task has a :func:`spawn` method, :class:`~qop.tasks.TaskQueue` calls it internally
        after fetching the original Task. The follow-up task is then enqueued with maximum priority. If this newly
        spawned task fails, the parent task will also be considered failed.
        """
        if self.oid is None:
            raise ValueError(
                "Spawn requires that the parent Task has an `oid` attribute that links it to a row in a TaskQueue. "
            )

        return MoveTask(self.tmpdst, self.dst, parent_oid=self.oid)

    def color_repr(self, color=True) -> str:
        if color:
            op = Fore.YELLOW + "CONV" + Fore.RESET
            arrow = Fore.YELLOW + "->" + Fore.RESET
            src = self.src
            dst = self.dst
            return f'{op} {src} {arrow} {dst}'
        else:
            return self.__repr__()

    def __repr__(self) -> str:
        return f'CONV {self.src} -> {self.dst}'

    def to_dict(self) -> Dict:
        r = self.__dict__.copy()
        for el in ("src", "dst", "tmpdst"):
            if el in r.keys():
                r[el] = str(r[el])
        r["converter"] = self.converter.to_dict()
        return r


class TaskQueueElement:
    """An enqueued Task"""

    task = None  #: A Task
    status = None  #: Status of the queued Task
    priority = None  #: Priority of the queued Task

    def __init__(self, task: Task, priority: 1) -> None:
        self.task = task
        self.priority = priority

    def __lt__(self, other) -> bool:
        return self.priority < other.priority

    def __gt__(self, other) -> bool:
        return self.priority > other.priority

    def __eq__(self, other) -> bool:
        return self.__dict__ == other.__dict__

    def __ne__(self, other) -> bool:
        return self.__dict__ != other.__dict__


class QueueProgress:
    """Info on the current status of the Queue"""

    def __init__(self,  pending: int, ok: int, skip: int, fail: int, active: int):
        self.ok = ok
        self.pending = pending
        self.skip = skip
        self.active = active
        self.fail = fail

    @property
    def total(self):
        return self.ok + self.pending + self.skip + self.active + self.fail

    @staticmethod
    def from_dict(x: Dict) -> "QueueProgress":
        return QueueProgress(
            pending=x['pending'],
            ok=x['ok'],
            skip=x['skip'],
            fail=x['fail'],
            active=x['active']
        )

    @staticmethod
    def from_list(x: List) -> "QueueProgress":
        """Convert a list (for example as returned by an SQL SELECT statement) to a QueueProgress object"""
        res = {
            "pending": 0,
            "ok": 0,
            "skip": 0,
            "active": 0,
            "fail": 0
        }

        for el in x:
            if el[0] == Status.PENDING:
                res.update({"pending": el[1]})
            elif el[0] == Status.OK:
                res.update({"ok": el[1]})
            elif el[0] == Status.SKIP:
                res.update({"skip": el[1]})
            elif el[0] == Status.ACTIVE:
                res.update({"active": el[1]})
            elif el[0] == Status.FAIL:
                res.update({"fail": el[1]})

        return QueueProgress.from_dict(res)

    def to_dict(self) -> Dict:
        return {
            "total": self.total,
            "pending":self.pending,
            "ok": self.ok,
            "fail": self.fail,
            "skip": self.skip,
            "active": self.active
         }

    def fmt_summary(self):
        return f'  [progress] total {self.total} | pending:  {self.pending} | ok: {self.ok} | fail: {self.fail} | active: {self.active}]'


def hammer_commit(con, max_tries=10):
    if max_tries <= 1:
        con.commit()
    else:
        try:
            con.commit()
        except:
            sleep(0.1)
            hammer_commit(con, max_tries=max_tries - 1)


class UnknownTaskTypeError(ValueError):
    pass
