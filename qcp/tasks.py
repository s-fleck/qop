from pathlib import Path
from typing import Union, Dict, Optional
import shutil
import os
import json
import sqlite3
from qcp import converters


Pathish = Union[Path, str]
Converter_ = converters.Converter


class Task:
    oid = None

    def __init__(self) -> None:
        self.type = 0

    def __repr__(self) -> str:
        return 'NULL'


class EchoTask(Task):
    def __init__(self,  msg: str) -> None:
        self.msg = msg
        self.type = 1
        super().__init__()

    def __repr__(self) -> str:
        return f'Echo: "{self.msg}"'


class KillTask(Task):
    def __init__(self) -> None:
        self.type = -1
        super().__init__()

    def __repr__(self) -> str:
        return 'KILL'


class FileTask(Task):
    def __init__(self, src: Pathish, validate: bool = True) -> None:
        self.src = Path(src)
        self.validate = validate
        if validate:
            self.__validate__()
        super().__init__()

    def __validate__(self) -> None:
        if not self.src.exists():
            raise FileNotFoundError(f'{self.src} does not exist')
        elif not (self.src.is_dir() or self.src.is_file()):
            raise TypeError(f'{self.src.as_posix()} is neither a file nor directory')


class DeleteTask(FileTask):
    def __init__(self, src: Pathish, validate: bool = True) -> None:
        self.type = 2
        super().__init__(src=src, validate=validate)

    def run(self):
        os.unlink(self.src)

    def __repr__(self) -> str:
        return f'DEL {self.src}'


class CopyTask(FileTask):
    def __init__(self, src: Pathish, dst: Pathish, validate: bool = True) -> None:
        self.type = 3
        self.dst = Path(dst)
        super().__init__(src=src, validate=validate)

    def __repr__(self) -> str:
        return f'COPY {self.src} -> {self.dst}'

    def __validate__(self) -> None:
        super().__validate__()
        if self.dst.exists():
            raise FileExistsError

    def run(self) -> None:
        self.__validate__()
        shutil.copy(self.src, self.dst)


class MoveTask(CopyTask):
    def __init__(self, src: Pathish, dst: Pathish, validate: bool = True) -> None:
        super().__init__(src=src, dst=dst, validate=validate)
        self.type = 4

    def run(self) -> None:
        super().__validate__()
        shutil.move(self.src, self.dst)

    def __repr__(self) -> str:
        return f'MOVE {self.src} -> {self.dst}'


class ConvertTask(CopyTask):
    def __init__(self, src: Pathish, dst: Pathish, converter: Converter_, validate: bool = True) -> None:
        super().__init__(src, dst, validate=validate)
        self.converter = converter
        self.type = 5

    def run(self) -> None:
        self.converter.run(self.src, self.dst)

    def __repr__(self) -> str:
        return f'CONV {self.src} -> {self.dst}'


class TaskQueueElement:
    def __init__(self, task: Task, priority: 1):
        self.task = task
        self.status = None
        self.priority = priority

    def __lt__(self, other) -> bool:
        return self.priority < other.priority

    def __gt__(self, other) -> bool:
        return self.priority > other.priority

    def __eq__(self, other) -> bool:
        return self.__dict__ == other.__dict__

    def __ne__(self, other) -> bool:
        return self.__dict__ != other.__dict__




Task_ = Union[Task, ConvertTask]


def from_dict(op) -> Task_:
    if op[1] == 3:
        cv = converters.from_json(op[4])
        res = ConvertTask(priority=op[0], src=op[2], dst=op[3], converter=cv)
    else:
        res = Task(priority=op[0], src=op[2], validate=False)

    return res


class TaskQueue:
    def __init__(self, path: Pathish = 'qcp.db') -> None:
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
    def n_ops(self) -> int:
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) from tasks").fetchall()[0][0]

    def n_pending(self) -> int:
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) FROM tasks WHERE status = 0").fetchall()[0][0]

    def n_running(self) -> int:
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) FROM tasks WHERE status = 1").fetchall()[0][0]

    def n_done(self) -> int:
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) from tasks WHERE status = 2").fetchall()[0][0]

    def n_failed(self) -> int:
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) from tasks WHERE status = -1").fetchall()[0][0]

    def put(self, task: Task_, priority: Optional[int] = None) -> None:
        cur = self.con.cursor()
        cur.execute(
            "INSERT INTO tasks (priority, task, status) VALUES (?, ?, ?)", (priority, json.dumps(task.__dict__), 0)
        )
        self.con.commit()

    def pop(self) -> Task_:
        """Retrieves Task object and sets status of Task in database to "in progress" (1)"""
        cur = self.con.cursor()
        cur.execute("SELECT _ROWID_ from tasks WHERE status = 0 ORDER BY priority LIMIT 1")
        oid = cur.fetchall()[0][0].__str__()
        self.mark_running(oid, id(self))

        cur.execute("SELECT owner, task FROM tasks WHERE _ROWID_ = ?", oid)
        record = cur.fetchall()[0]
        print(record)
        if record[0] != id(self):
            raise AlreadyUnderEvaluationError

        task = from_dict(record)[1]
        task.oid = oid
        return task

    def peek(self, n: int = 1) -> Task_:
        """Retrieves Task object and sets status of Task in database to "in progress" (1)"""
        assert isinstance(n, int) and n > 0
        assert n == 1  # currently only 1 is supported
        cur = self.con.cursor()
        cur.execute("SELECT * from tasks ORDER BY priority LIMIT ?", (str(n), ))

        record = cur.fetchall()[0]
        oid = record[0].__str__()
        task = from_dict(record)
        task.oid = oid
        return task

    def get_queue(self, n: int = 10):
        assert isinstance(n, int) and n > 0
        cur = self.con.cursor()
        cur.execute("SELECT * from tasks ORDER BY priority LIMIT ?", (str(n), ))
        records = cur.fetchall()
        return map(from_dict, records)

    def print_queue(self, n: int = 10):
        q = self.get_queue(n=n)
        print(f'\nQueue with {self.n_ops} queued Tasks')
        [print(el) for el in q]

    def mark_pending(self, oid: int):
        """Mark the operation with the _ROWID_ `oid` as "pending" (0)"""
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = 0, owner = NULL where _ROWID_ = ?", (oid, ))
        self.con.commit()

    def mark_running(self, oid: int, owner: int):
        """Mark the operation with the _ROWID_ `oid` as "running" (1). The "owner" Id is to ensure no two processes
        are trying to execute the same operation"""
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = 1, owner = ? where _ROWID_ = ?", (owner, oid))
        self.con.commit()

    def mark_done(self, oid: int) -> None:
        """Mark the operation with the _ROWID_ `oid` as "done" (2)"""
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = 2, owner = NULL where _ROWID_ = ?", (oid, ))
        self.con.commit()

    def mark_failed(self, oid: int) -> None:
        """Mark the operation with the _ROWID_ `oid` as "failed" (-1)"""
        cur = self.con.cursor()
        cur.execute("UPDATE tasks SET status = -1, owner = NULL where _ROWID_ = ?", (oid, ))
        self.con.commit()

    def run(self) -> None:
        raise NotImplementedError


class AlreadyUnderEvaluationError(Exception):
    """This Task is already being processed by a different worker"""
    pass
