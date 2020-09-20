from pathlib import Path
from typing import Union, Optional, Dict, Tuple, List
from qop import converters
import shutil
import os
import json
import sqlite3
import appdirs
import uuid

import logging
from qop.enums import TaskType, Status, Command
from qop.exceptions import AlreadyUnderEvaluationError, FileExistsAndIsIdenticalError, FileExistsAndCannotBeComparedError
from qop import utils
from colorama import init, Fore
import filecmp
from multiprocessing import Process
from time import sleep

init()

Pathish = Union[Path, str]

lg = logging.getLogger("qop.tasks")

CONVERT_CACHE_DIR = Path(appdirs.user_cache_dir("qop")).joinpath("convert_temp")


class Task:
    """Abstract class for qop Tasks. Should not be instantiated directly."""


    """optional rowid of the task in the queue. only for tasks that were retrieved from the queue."""
    oid = None
    parent_oid = None

    def __init__(self) -> None:
        self.type = 0

    def run(self) -> None:
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
        elif task_type == TaskType.CONVERT:
            return ConvertTask(x['src'], x['dst'], converter=converters.Converter.from_dict(x["converter"]))
        elif task_type == TaskType.CONVERT2:
            return ConvertTask2(x['src'], x['dst'], converter=converters.Converter.from_dict(x["converter"]))
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

    def run(self) -> None:
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

    def run(self) -> None:
        sleep(self.seconds)

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

    def run(self) -> None:
        raise AssertionError

    def __repr__(self) -> str:
        return f'Fail: Always raise an error"'


class FileTask(Task):
    """Abstract class for all file-based tasks"""
    def __init__(self, src: Pathish) -> None:
        super().__init__()
        self.src = Path(src).resolve()
        self.type = None

    def run(self) -> None:
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

    def run(self) -> None:
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

    def run(self) -> None:
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

    def run(self) -> None:
        super().__validate__()
        if not self.dst.parent.exists():
            self.dst.parent.mkdir(parents=True)

        if self.src.is_dir():
            shutil.move(self.src, self.dst)
        else:
            shutil.move(self.src, self.dst)

    def __repr__(self) -> str:
        return f'MOVE {self.src} -> {self.dst}'


class ConvertTask(CopyTask):
    """convert an audio file"""
    def __init__(self, src: Pathish, dst: Pathish, converter: converters.Converter) -> None:
        super().__init__(src=src, dst=dst)
        self.type = TaskType.CONVERT
        self.converter = converter
        self.src = Path(src).resolve()
        self.dst = Path(dst).resolve()

    def run(self) -> None:
        super().__validate__()
        self.converter.run(self.src, self.dst)

    def color_repr(self, color=True) -> str:
        if color:
            op = Fore.YELLOW + "CON1" + Fore.RESET
            arrow = Fore.YELLOW + "->" + Fore.RESET
            src = self.src
            dst = self.dst
            return f'{op} {src} {arrow} {dst}'
        else:
            return self.__repr__()

    def __repr__(self) -> str:
        return f'CONVERT {self.src} -> {self.dst}'

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


class ConvertTask2(ConvertTask):
    """
        ConvertTask2 transcodes an audio file to a temporary directory and then adds a move task to the queue.
        This makes it possible to cleanly separate transcode and transfer processes.
    """
    def __init__(self, src: Pathish, dst: Pathish, converter: converters.Converter) -> None:
        super().__init__(src=src, dst=dst, converter=converter)
        self.type = TaskType.CONVERT2
        self.tmpdst = CONVERT_CACHE_DIR.joinpath(uuid.uuid4().hex)

    def run(self) -> None:
        super().__validate__()
        lg.debug(f"converting file to temporary destination: {self.tmpdst}")
        self.converter.run(self.src, self.tmpdst)

    def follow_up_task(self) -> MoveTask:
        # follow_up_task requires that the task was retrieved from the queue and therefore already as an oid that
        # links it to a row in the queue
        assert self.oid is not None
        return MoveTask(self.tmpdst, self.dst, parent_oid=self.oid)

    def color_repr(self, color=True) -> str:
        if color:
            op = Fore.YELLOW + "CON2" + Fore.RESET
            arrow = Fore.YELLOW + "->" + Fore.RESET
            src = self.src
            dst = self.dst
            return f'{op} {src} {arrow} {dst}'
        else:
            return self.__repr__()

    def __repr__(self) -> str:
        return f'CONVERT2 {self.src} -> {self.dst}'

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


class TaskQueue:
    """A prioritzed queue for tasks"""

    processes = []
    convert_processes = []

    def __init__(self, path: Pathish) -> None:
        """
        Instantiate a TaskQueue

        A TaskQueue is a sqlite3 database with the following columns
        - priority: integer value, the lower the value the earlier the task will be processed
        - task: json representation of the task to execute
        - status: status of the task (ok, running, fail,... see enums.Status)
        - owner: integer id of the python object executing the task. only for running tasks. (see `help(id)`)

        :param path: Path to store the persistent queue
        :type path: Path or str
        """

        if path.exists():
            lg.info(f"using existing queue {path}")
        else:
            lg.info(f"initializing new queue {path}")

        self.con = sqlite3.connect(path, isolation_level="EXCLUSIVE", timeout=10)
        self.path = Path(path)
        cur = self.con.cursor()
        cur.execute("""
           CREATE TABLE IF NOT EXISTS tasks (
              priority INTEGER,
              task TEXT,
              status INTEGER,
              owner INTEGER,
              UNIQUE(task, status)              
            )              
        """)
        self.con.commit()

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
        res = cur.execute("SELECT COUNT(1) FROM tasks WHERE status = ?", (int(Status.PENDING), )).fetchall()[0][0]
        cur.close()
        return res

    @property
    def n_running(self) -> int:
        """Count of currently running tasks"""
        cur = self.con.cursor()
        res = cur.execute("SELECT COUNT(1) FROM tasks WHERE status = ?", (int(Status.RUNNING),)).fetchall()[0][0]
        cur.close()
        return res

    @property
    def n_ok(self) -> int:
        """count of completed tasks"""
        cur = self.con.cursor()
        res = cur.execute("SELECT COUNT(1) from tasks WHERE status = ?", (int(Status.OK), )).fetchall()[0][0]
        cur.close()
        return res

    @property
    def n_fail(self) -> int:
        """count of completed tasks"""
        cur = self.con.cursor()
        res = cur.execute("SELECT COUNT(1) from tasks WHERE status = ?", (int(Status.FAIL), )).fetchall()[0][0]
        cur.close()
        return res

    @property
    def n_skip(self) -> int:
        """count of completed tasks"""
        cur = self.con.cursor()
        res = cur.execute("SELECT COUNT(1) from tasks WHERE status = ?", (int(Status.SKIP),)).fetchall()[0][0]
        cur.close()
        return res

    def progress(self) -> "QueueProgress":
        cur = self.con.cursor()
        res = cur.execute("SELECT status, COUNT(1) from tasks GROUP BY status").fetchall()
        cur.close()
        x = {
            "pending": 0,
            "ok": 0,
            "skip": 0,
            "running": 0,
            "fail": 0
        }
        for el in res:
            if el[0] == Status.PENDING:
                x.update({"pending": el[1]})
            elif el[0] == Status.OK:
                x.update({"ok": el[1]})
            elif el[0] == Status.SKIP:
                x.update({"skip": el[1]})
            elif el[0] == Status.RUNNING:
                x.update({"running": el[1]})
            elif el[0] == Status.FAIL:
                x.update({"fail": el[1]})

        return QueueProgress.from_dict(x)

    def put(self, task: "Task", priority: int = 10) -> None:
        """
        Enqueue a task

        :param task: Task to be added to the queue
        :type task: Task
        :param priority: (optional) priority for executing `task` (tasks with lower priority will be executed earlier)
        :type priority: int
        """

        lg.debug(f"trying to inserted task {task.to_dict()}")
        cur = self.con.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO tasks (priority, task, status) VALUES (?, ?, ?)", (priority, task.to_json(), Status.PENDING)
        )

        hammer_commit(self.con)

        lg.debug(f"inserted task {task.to_dict()}")

    def pop(self, task_type_include: Optional[TaskType] = None, task_type_exclude: Optional[TaskType] = None) -> "Task":
        """
        Retrieves Task object and sets status of Task in database to "in progress" (1)

        :raises AlreadyUnderEvaluationError: If trying to pop a tasks that is already being processed  (i.e. if a race
        condition occurs if the queue is processed in parallel)
        """
        cur = self.con.cursor()

        assert task_type_include is None or task_type_exclude is None

        if task_type_include is not None:
            cur.execute(f"SELECT _ROWID_ FROM tasks WHERE status = ? AND task LIKE '__type___{int(task_type_include)}%' ORDER BY priority LIMIT 1", (Status.PENDING,))
        elif task_type_exclude is not None:
            cur.execute(f"SELECT _ROWID_ FROM tasks WHERE status = ? AND task NOT LIKE '__type___{int(task_type_exclude)}%' ORDER BY priority LIMIT 1", (Status.PENDING,))
        else:
            cur.execute("SELECT _ROWID_ FROM tasks WHERE status = ? ORDER BY priority LIMIT 1", (Status.PENDING, ))

        oid = cur.fetchall()[0][0].__str__()
        self.mark_running(oid, id(self))

        # ensure that the task was not assigned to a second thread
        cur.execute("SELECT owner, task FROM tasks WHERE _ROWID_ = ?", (oid, ))
        record = cur.fetchall()[0]
        if record[0] != id(self):
            raise AlreadyUnderEvaluationError
        
        task = Task.from_dict(json.loads(record[1]))
        lg.debug(f"popped task {task}")
        task.oid = oid
        return task

    def peek(self) -> "Task":
        """
        Retrieves Task object without changing its status in the queue
        """
        cur = self.con.cursor()
        cur.execute("SELECT owner, task from tasks ORDER BY priority LIMIT 1")
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
        :type n: int
        :param status: If not None, only fetch Tasks of the given status(es)
        :type status: `int`, `None` or a `tuple` of `int`
        """
        assert isinstance(n, int) and (n > 0)
        records = self.fetch(n=n, status=status)
        for record in records:
            print(f"[{record[0]}] {Task.from_dict(json.loads(record[1]))}")

    def fetch(self, status: Union[Tuple, int, Status, None] = None, n: Optional[int] = None) -> List:
        """
        Retrieve the queue

        :param n: number of tasks to fetch
        :type n: int
        :param status: If not None, only fetch Tasks of the given status(es)
        :type status: `int`, `None` or a `tuple` of `int`

        :return a dict containing n queued tasks
        """

        if status is not None:
            if isinstance(status, int):
                status = (status, )
            elif isinstance(status, Status):
                status = (int(status), )
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
                    status + (n, )
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
                cur.execute("SELECT status, task from tasks ORDER BY priority LIMIT ?", (str(n), ))
            else:
                cur.execute("SELECT status, task from tasks ORDER BY priority")

        res = cur.fetchall()
        cur.close()
        res = [{"priority": x[0], "task": json.loads(x[1])} for x in res]

        return res

    def replace_status(self, status_from: Status, status_to: Status) -> None:
        """
        Mark the operation with one status as another status

        :param oid: ID of the task to mark
        :type oid: int
        """
        lg.info(f"mark all tasks with status {status_from.name} as {status_to.name}")
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = ? where status = ?", (int(status_to), int(status_from)))
        hammer_commit(self.con)

    def mark_pending(self, oid: int) -> None:
        """
        Mark the operation with the _ROWID_ `oid` as "pending" (0)

        :param oid: ID of the task to mark
        :type oid: int
        """
        lg.info(f"mark {oid} pending")
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = ?, owner = NULL where _ROWID_ = ?", (int(Status.PENDING), oid))
        hammer_commit(self.con)

    def mark_running(self, oid: int, owner: int) -> None:
        """Mark the operation with the _ROWID_ `oid` as "running" (1). The "owner" Id is to ensure no two processes
        are trying to execute the same operation

        :param oid: ID of the task to mark
        :type oid: int
        :param owner: Id of the process that is handling the operation
        :type owner: int
        """
        lg.debug(f"mark {oid} running")
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = ?, owner = ? where _ROWID_ = ?", (int(Status.RUNNING), owner, oid))
        hammer_commit(self.con)

    def mark_ok(self, oid: int) -> None:
        """
        Mark the operation with the _ROWID_ `oid` as "done" (2)
        :param oid: ID of the task to mark
        :type oid: int
        """
        lg.debug(f"mark {oid} completed")
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = ?, owner = NULL where _ROWID_ = ?", (int(Status.OK), oid))
        hammer_commit(self.con)

    def mark_fail(self, oid: int) -> None:
        """
        Mark the operation with the _ROWID_ `oid` as "failed" (-1)

        :param oid: ID of the task to mark
        :type oid: int
        """
        lg.debug(f"mark {oid} failed")
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = ?, owner = NULL where _ROWID_ = ?", (int(Status.FAIL), oid))
        hammer_commit(self.con)

    def run(self, max_processes: int = 1, max_convert_processes: int = 1, ip=None, port=None) -> None:
        """Execute all pending tasks"""

        # remove finished que runs
        self.processes = [p for p in self.processes if p.is_alive()]
        self.convert_processes = [p for p in self.convert_processes if p.is_alive()]

        if self.n_pending < 1:
            lg.warning("queue is empty")
            return None

        if len(self.processes) < max_processes:
            lg.info("starting new queue runner")
            p = Process(target=run_queue, args=(self, ip, port, None, TaskType.CONVERT2))
            p.start()
            self.processes.append(p)
        else:
            lg.debug(f"already running {max_processes} queues")

        if len(self.convert_processes) < max_convert_processes:
            lg.info("starting new convert queue runner")
            p = Process(target=run_queue, args=(self, ip, port, TaskType.CONVERT2, None))
            p.start()
            self.convert_processes.append(p)
        else:
            lg.debug(f"already running {max_convert_processes} convert queues")

    def stop(self) -> None:
        for p in self.processes:
            p.terminate()

        self.replace_status(Status.RUNNING, Status.PENDING)

    @property
    def active_processes(self):
        res = 0
        for p in self.processes:
            if p.is_alive():
                res += 1
        return res

    def flush(self, status: Union[Status, int, None] = None) -> None:
        """empty the queue"""
        cur = self.con.cursor()
        if status is None:
            cur.execute("DELETE FROM tasks")
        else:
            cur.execute("DELETE FROM tasks where status == ?", (int(status), ))
        hammer_commit(self.con)


def run_queue(queue: TaskQueue, ip=None, port=None, task_type_include=None, task_type_exclude=None) -> None:

    progress = queue.progress()

    while progress.pending > 0 or progress.running > 0:

        if ip is not None:
            if utils.is_server_alive(ip=ip, port=port) is False:
                lg.fatal("cannot find server thread. stopping queue.")
                break

        try:
            op = queue.pop(task_type_include=task_type_include, task_type_exclude=task_type_exclude)
        except:
            lg.debug("waiting for more tasks of correct status")
            sleep(1)
            continue

        try:
            op.run()
            if op.type == TaskType.CONVERT2:
                follow_up = op.follow_up_task()
                queue.put(follow_up, priority=-1)
                lg.info(f"convert task completed, queuing move to final destination: {follow_up}")
            else:
                lg.info(f"task completed: {op}")
                queue.mark_ok(op.oid)

            if op.parent_oid is not None:
                queue.mark_ok(op.parent_oid)
                lg.info(f"parent task completed: {op.parent_oid}")

        except:
            lg.error(f"task failed: {op}")
            queue.mark_fail(op.oid)
            if op.parent_oid is not None:
                queue.mark_fail(op.parent_oid)
                lg.info(f"parent task completed: {op.parent_oid}")

        progress = queue.progress()

    try:
        shutil.rmtree(CONVERT_CACHE_DIR)
    except:
        pass

    lg.info("queue is finished")


class QueueProgress:
    """Info on the current status of the Queue"""

    def __init__(self,  pending: int, ok: int, skip: int, fail: int, running: int):
        self.ok = ok
        self.pending = pending
        self.skip = skip
        self.running = running
        self.fail = fail

    @property
    def total(self):
        return self.ok + self.pending + self.skip + self.running + self.fail

    @staticmethod
    def from_dict(x: Dict) -> "QueueProgress":
        return QueueProgress(
            pending=x['pending'],
            ok=x['ok'],
            skip=x['skip'],
            fail=x['fail'],
            running=x['running']
        )

    def to_dict(self) -> Dict:
        return {
            "total": self.total,
            "pending":self.pending,
            "ok": self.ok,
            "fail": self.fail,
            "skip": self.skip,
            "running": self.running
         }

    def fmt_summary(self):
        return f'  [progress] total {self.total} | pending:  {self.pending} | ok: {self.ok} | fail: {self.fail} | running: {self.running}]'


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
