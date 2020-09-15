from pathlib import Path
from typing import Union, Optional, Dict, Tuple, List
from qop import converters
import shutil
import os
import json
import sqlite3
import sys
import logging
from qop.globals import TaskType, Status, Command


Pathish = Union[Path, str]

lg = logging.getLogger("qop.tasks")


class Task:
    """Abstract class for qop Tasks. Should not be instantiated directly."""

    def __init__(self) -> None:
        self.type = 0

    def run(self) -> None:
        """Run a task"""
        raise NotImplementedError

    @staticmethod
    def from_dict(x: Dict, validate: Optional[bool] = None) -> "Task":
        """Create a Task of the appropriate subclass from a python dict"""
        logging.getLogger("qop.tasks").debug(f"parsing task {x}")
        task_type = x["type"]

        if validate is None and "validate" in x.keys():
            validate = x["validate"]

        if task_type == TaskType.KILL:
            return KillTask()
        elif task_type == 0:
            return Task()
        elif task_type == 1:
            return EchoTask(x["msg"])
        elif task_type == 2:
            return FileTask(x["src"], validate=validate)
        elif task_type == 3:
            return DeleteTask(x["src"], validate=validate)
        elif task_type == 4:
            return CopyTask(x["src"], x["dst"], validate=validate)
        elif task_type == 5:
            return MoveTask(x["src"], x["dst"], validate=validate)
        elif task_type == 6:
            return ConvertTask(x['src'], x['dst'], converter=converters.Converter.from_dict(x["converter"]))
        else:
            raise ValueError

    def __repr__(self) -> str:
        return 'NULL'

    def __eq__(self, other) -> bool:
        return self.__dict__ == other.__dict__

    def __ne__(self, other) -> bool:
        return self.__dict__ != other.__dict__

    def to_dict(self) -> Dict:
        return self.__dict__.copy()

    def to_json(self):
        r = self.to_dict()

        for el in ("src", "dst"):
            if el in r.keys():
                r[el] = str(r[el])

        return r


class CommandTask(Task):
    def __init__(self, command) -> None:
        super().__init__()
        self.type = 0
        self.command = command

    def __repr__(self) -> str:
        return f'COMMAND: command'


class KillTask(Task):
    def __init__(self) -> None:
        super().__init__()
        self.type = TaskType.KILL

    def __repr__(self) -> str:
        return f'KILL'


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


class FileTask(Task):
    """Abstract class for all file-based tasks"""
    def __init__(self, src: Pathish, validate: bool = True) -> None:
        super().__init__()
        self.validate = validate
        self.src = Path(src).as_posix()
        self.type = None
        if validate:
            self.__validate__()

    def __validate__(self) -> None:
        if not Path(self.src).exists():
            raise FileNotFoundError(f'{self.src} does not exist')
        elif not (Path(self.src).is_dir() or Path(self.src).is_file()):
            raise TypeError(f'{self.src} is neither a file nor directory')


class DeleteTask(FileTask):
    """Delete a file"""
    def __init__(self, src: Pathish, validate: bool = True) -> None:
        super().__init__(src=src, validate=validate)
        self.type = TaskType.DELETE

    def run(self) -> None:
        os.unlink(self.src)

    def __repr__(self) -> str:
        return f'DEL {self.src}'


class CopyTask(FileTask):
    """Copy a file"""
    def __init__(self, src: Pathish, dst: Pathish, validate: bool = True) -> None:
        super().__init__(src=src, validate=False)
        self.dst = Path(dst).as_posix()
        self.type = TaskType.COPY
        self.validate = validate
        if validate:
            self.__validate__()

    def __repr__(self) -> str:
        return f'COPY {self.src} -> {self.dst}'

    def __validate__(self) -> None:
        super().__validate__()
        if Path(self.dst).exists():
            raise FileExistsError

    def run(self) -> None:
        self.__validate__()
        shutil.copy(self.src, self.dst)


class MoveTask(CopyTask):
    """Move a file"""
    def __init__(self, src: Pathish, dst: Pathish, validate: bool = True) -> None:
        super().__init__(src=src, dst=dst, validate=validate)
        self.type = TaskType.MOVE

    def run(self) -> None:
        super().__validate__()
        shutil.move(self.src, self.dst)

    def __repr__(self) -> str:
        return f'MOVE {self.src} -> {self.dst}'


class ConvertTask(CopyTask):
    """convert an audio file"""
    def __init__(self, src: Pathish, dst: Pathish, converter: converters.Converter, validate: bool = True) -> None:
        super().__init__(src=src, dst=dst, validate=validate)
        self.type = TaskType.CONVERT
        self.converter = converter
        self.src = src
        self.dst = dst

    def run(self) -> None:
        super().__validate__()
        self.converter.run(self.src, self.dst)

    def __repr__(self) -> str:
        return f'CONVERT {self.src} -> {self.dst}'

    def to_dict(self) -> Dict:
        r = self.__dict__.copy()
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
    def __init__(self, path: Pathish) -> None:
        """
        Instantiate a TaskQueue

        :param path: Path to store the persistent queue
        :type path: Path or str
        """

        if path.exists():
            lg.info(f"initializing new queue {path}")
        else:
            lg.info(f"using existing queue {path}")

        self.con = sqlite3.connect(path, isolation_level="EXCLUSIVE")
        self.path = Path(path)
        cur = self.con.cursor()
        cur.execute("""
           CREATE TABLE IF NOT EXISTS tasks (
              priority INTEGER,
              task TEXT,
              status INTEGER,
              owner INTEGER              
            )              
        """)
        self.con.commit()

    @property
    def n_total(self) -> int:
        """Count of all tasks in queue (including failed and completed)"""
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) from tasks").fetchall()[0][0]

    @property
    def n_pending(self) -> int:
        """Number of pending tasks"""
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) FROM tasks WHERE status = 0").fetchall()[0][0]

    @property
    def n_running(self) -> int:
        """Count of currently running tasks"""
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) FROM tasks WHERE status = 1").fetchall()[0][0]

    @property
    def n_done(self) -> int:
        """count of completed tasks"""
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) from tasks WHERE status = 2").fetchall()[0][0]

    @property
    def n_failed(self) -> int:
        """count of completed tasks"""
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) from tasks WHERE status = -1").fetchall()[0][0]

    @property
    def summary(self) -> Dict:
        return {
            "total": self.n_total,
            "pending": self.n_pending,
            "done": self.n_done,
            "running": self.n_running,
            "failed": self.n_failed
        }

    def put(self, task: "Task", priority: Optional[int] = None) -> None:
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
            "INSERT INTO tasks (priority, task, status) VALUES (?, ?, ?)", (priority, json.dumps(task.to_dict()), 0)
        )
        self.con.commit()
        lg.debug(f"inserted task {task.to_dict()}")

    def pop(self) -> "Task":
        """
        Retrieves Task object and sets status of Task in database to "in progress" (1)

        :raises AlreadyUnderEvaluationError: If trying to pop a tasks that is already being processed  (i.e. if a race
        condition occurs if the queue is processed in parallel)
        """
        cur = self.con.cursor()
        cur.execute("SELECT _ROWID_ from tasks WHERE status = 0 ORDER BY priority LIMIT 1")
        oid = cur.fetchall()[0][0].__str__()
        self.mark_running(oid, id(self))

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
        cur.execute("SELECT * from tasks ORDER BY priority LIMIT 1")
        record = cur.fetchall()[0]
        oid = record[0].__str__()
        task = Task.from_dict(json.loads(record[1]), validate=False)
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

    def fetch(self, status: Union[Tuple, int, None] = None, n: Optional[int] = None) -> List:
        """
        Print an overview of the queue

        :param n: number of tasks to fetch
        :type n: int
        :param status: If not None, only fetch Tasks of the given status(es)
        :type status: `int`, `None` or a `tuple` of `int`

        :return a dict containing n queued tasks
        """
        cur = self.con.cursor()

        if isinstance(status, int):
            status = (status, )

        if status:
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
            if n:
                cur.execute("SELECT status, task from tasks ORDER BY priority LIMIT ?", (str(n), ))
            else:
                cur.execute("SELECT status, task from tasks ORDER BY priority")

        return cur.fetchall()

    def mark_pending(self, oid: int) -> None:
        """
        Mark the operation with the _ROWID_ `oid` as "pending" (0)

        :param oid: ID of the task to mark
        :type oid: int
        """
        logging.getLogger("qop/tasks").info(f"task {oid} pending")
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = 0, owner = NULL where _ROWID_ = ?", (oid, ))
        self.con.commit()

    def mark_running(self, oid: int, owner: int) -> None:
        """Mark the operation with the _ROWID_ `oid` as "running" (1). The "owner" Id is to ensure no two processes
        are trying to execute the same operation

        :param oid: ID of the task to mark
        :type oid: int
        :param owner: Id of the process that is handling the operation
        :type owner: int
        """
        lg.debug(f"task {oid} started")
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = 1, owner = ? where _ROWID_ = ?", (owner, oid))
        self.con.commit()

    def mark_done(self, oid: int) -> None:
        """
        Mark the operation with the _ROWID_ `oid` as "done" (2)
        :param oid: ID of the task to mark
        :type oid: int
        """
        lg.info(f"task {oid} completed")
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = 2, owner = NULL where _ROWID_ = ?", (oid, ))
        self.con.commit()

    def mark_failed(self, oid: int) -> None:
        """
        Mark the operation with the _ROWID_ `oid` as "failed" (-1)

        :param oid: ID of the task to mark
        :type oid: int
        """
        logging.getLogger("qop/tasks").error(f"task {oid} failed")
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = -1, owner = NULL where _ROWID_ = ?", (oid, ))
        self.con.commit()

    def run(self) -> None:
        """Execute all pending tasks"""
        if self.n_pending < 1:
            logging.getLogger().warning("queue is empty")

        while self.n_pending > 0:
            print(self.n_pending)
            op = self.pop()
            try:
                logging.getLogger("qop.tasks").debug(f"inserting {op.oid}")
                op.run()
                self.mark_done(op.oid)
            except:
                self.mark_failed(op.oid)
                logging.getLogger("qop.tasks").error(sys.exc_info()[0])

    def pause(self) -> None:
        raise NotImplementedError


class AlreadyUnderEvaluationError(Exception):
    """This Task is already being processed by a different worker"""
    pass
