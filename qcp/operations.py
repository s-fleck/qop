from pathlib import Path
from typing import Union, Dict, Optional
import shutil
import json
import sqlite3
from qcp import converters


Pathish = Union[Path, str]
Converter_ = converters.Converter


class Operation:
    oid = None

    def __init__(self, src: Pathish, priority: int = 1, validate: bool = True) -> None:
        self.priority = priority
        self.src = Path(src).resolve()
        if validate:
            self.validate()

    def validate(self) -> None:
        if not self.src.exists():
            raise FileNotFoundError
        elif not (self.src.is_dir() or self.src.is_file()):
            raise TypeError(f'{self.src.as_posix()} is neither a file nor directory')

    def __lt__(self, other) -> bool:
        return self.priority < other.priority

    def __gt__(self, other) -> bool:
        return self.priority > other.priority

    def __eq__(self, other) -> bool:
        return self.__dict__ == other.__dict__

    def __ne__(self, other) -> bool:
        return self.__dict__ != other.__dict__

    def to_dict(self) -> Dict:
        return {"type": 0, "src": self.src.as_posix(), "dst": None, "priority": self.priority}

    def __repr__(self) -> str:
        return f'NULL {self.src}'


class EchoOperation(Operation):
    def __init__(self,  src: Pathish, priority: int = 1) -> None:
        self.priority = priority
        self.src = Path(src).resolve()


class DeleteOperation(Operation):
    def execute(self) -> None:
        self.validate()
        if self.src.is_file():
            self.src.unlink()
        elif self.src.is_dir():
            self.src.rmdir()
        else:
            raise

    def __repr__(self) -> str:
        return f'DEL {self.src}'

    def to_dict(self) -> Dict:
        return {"type": 1, "src": self.src.as_posix(), "dst": None, "priority": self.priority}


class CopyOperation(Operation):
    def __init__(self, src: Pathish, dst: Pathish, priority: int = 1, validate: bool = True) -> None:
        self.dst = Path(dst).resolve()
        super().__init__(src, priority=priority, validate=validate)

    def __repr__(self) -> str:
        return f'COPY {self.src} -> {self.dst}'

    def validate(self) -> None:
        super().validate()
        if self.dst.exists():
            raise FileExistsError

    def run(self) -> None:
        self.validate()
        shutil.copy(self.src, self.dst)

    def to_dict(self) -> Dict:
        return {"type": 2, "src": self.src.as_posix(), "dst": self.dst.as_posix(), "priority": self.priority}


class MoveOperation(CopyOperation):
    def run(self) -> None:
        self.validate()
        shutil.move(self.src, self.dst)

    def __repr__(self) -> str:
        return f'MOVE {self.src} -> {self.dst}'


class ConvertOperation(CopyOperation):
    def __init__(self, src: Pathish, dst: Pathish, converter: Converter_, priority: int = 1, validate: bool = True) -> None:
        super().__init__(src, dst, priority=priority, validate=validate)
        self.converter = converter

    def run(self) -> None:
        self.converter.run(self.src, self.dst)

    def to_dict(self) -> Dict:
        return {"priority": self.priority, "type": 3, "src": self.src.as_posix(), "dst": self.dst.as_posix()}

    def __repr__(self) -> str:
        return f'CONV {self.src} -> {self.dst}'


Operation_ = Union[Operation, ConvertOperation]


def from_dict(op) -> Operation_:
    if op[1] == 3:
        cv = converters.from_json(op[4])
        res = ConvertOperation(priority=op[0], src=op[2], dst=op[3], converter=cv)
    else:
        res = Operation(priority=op[0], src=op[2], validate=False)

    return res


class OperationQueue:
    def __init__(self, path: Pathish = 'qcp.db') -> None:
        self.con = sqlite3.connect(path, isolation_level="EXCLUSIVE")
        self.path = Path(path)

        cur = self.con.cursor()
        cur.execute("""
           CREATE TABLE IF NOT EXISTS operations (
              priority INTEGER,
              type INTEGER,
              src TEXT,
              dst TEXT,
              opts TEXT,
              status INTEGER,
              owner INTEGER
            )              
        """)
        self.con.commit()

    @property
    def n_ops(self) -> int:
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) from operations").fetchall()[0][0]

    def n_pending(self) -> int:
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) FROM operations WHERE status = 0").fetchall()[0][0]

    def n_running(self) -> int:
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) FROM operations WHERE status = 1").fetchall()[0][0]

    def n_done(self) -> int:
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) from operations WHERE status = 2").fetchall()[0][0]

    def n_failed(self) -> int:
        cur = self.con.cursor()
        return cur.execute("SELECT COUNT(1) from operations WHERE status = -1").fetchall()[0][0]

    def put(self, op: Operation_, priority: Optional[int] = None) -> None:
        dd = op.to_dict()

        if priority is not None:
            dd.priority = priority

        cur = self.con.cursor()

        if type(op).__name__ == "ConvertOperation":
            dd["opts"] = json.dumps(op.converter.to_dict())
        else:
            dd["opts"] = None

        cur.execute("INSERT INTO OPERATIONS VALUES (:priority, :type, :src, :dst, :opts, 0, NULL)", dd)
        self.con.commit()

    def pop(self) -> Operation_:
        """Retrieves Operation object and sets status of Operation in database to "in progress" (1)"""
        cur = self.con.cursor()
        cur.execute("SELECT _ROWID_ from operations WHERE status = 0 ORDER BY priority LIMIT 1")
        oid = cur.fetchall()[0][0].__str__()
        self.mark_running(oid, id(self))

        cur.execute("SELECT priority, type, src, dst, opts, owner FROM operations WHERE _ROWID_ = ?", oid)
        record = cur.fetchall()[0]
        if record[5] != id(self):
            raise AlreadyUnderEvaluationError

        op = from_dict(record)
        op.oid = oid
        return op

    def peek(self, n: int = 1) -> Operation_:
        """Retrieves Operation object and sets status of Operation in database to "in progress" (1)"""
        assert isinstance(n, int) and n > 0
        cur = self.con.cursor()
        cur.execute("SELECT * from operations ORDER BY priority LIMIT ?", (str(n), ))

        record = cur.fetchall()[0]
        oid = record[0].__str__()
        op = from_dict(record)
        op.oid = oid
        return op

    def get_queue(self, n: int = 10):
        assert isinstance(n, int) and n > 0
        cur = self.con.cursor()
        cur.execute("SELECT * from operations ORDER BY priority LIMIT ?", (str(n), ))
        records = cur.fetchall()
        return map(from_dict, records)

    def print_queue(self, n: int = 10):
        q = self.get_queue(n=n)
        print(f'\nQueue with {self.n_ops} queued Operations')
        [print(el) for el in q]

    def mark_pending(self, oid: int):
        """Mark the operation with the _ROWID_ `oid` as "pending" (0)"""
        cur = self.con.cursor()
        cur.execute("UPDATE operations SET status = 0, owner = NULL where _ROWID_ = ?", (oid, ))
        self.con.commit()

    def mark_running(self, oid: int, owner: int):
        """Mark the operation with the _ROWID_ `oid` as "running" (1). The "owner" Id is to ensure no two processes
        are trying to execute the same operation"""
        cur = self.con.cursor()
        cur.execute("UPDATE operations SET status = 1, owner = ? where _ROWID_ = ?", (owner, oid))
        self.con.commit()

    def mark_done(self, oid: int) -> None:
        """Mark the operation with the _ROWID_ `oid` as "done" (2)"""
        cur = self.con.cursor()
        cur.execute("UPDATE operations SET status = 2, owner = NULL where _ROWID_ = ?", (oid, ))
        self.con.commit()

    def mark_failed(self, oid: int) -> None:
        """Mark the operation with the _ROWID_ `oid` as "failed" (-1)"""
        cur = self.con.cursor()
        cur.execute("UPDATE operations SET status = -1, owner = NULL where _ROWID_ = ?", (oid, ))
        self.con.commit()

    def run(self) -> None:
        raise NotImplementedError


class AlreadyUnderEvaluationError(Exception):
    """This Operation is already being processed by a different worker"""
    pass
