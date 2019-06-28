from pathlib import Path
from typing import Union, Optional, Dict
from qcp import Converter
import json
import shutil


class Operation:
    def __init__(self, src: Union[Path, str], priority: int = 1, validate: bool = True) -> None:
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

    def serialize(self) -> Dict:
        return {"type": 0, "src": self.src.as_posix(), "dst": None, "priority": self.priority}


class DeleteOperation(Operation):
    def execute(self) -> None:
        self.validate()
        if self.src.is_file():
            self.src.unlink()
        elif self.src.is_dir():
            self.src.rmdir()
        else:
            raise

    def serialize(self) -> Dict:
        return {"type": 1, "src": self.src.as_posix(), "dst": None, "priority": self.priority}


class CopyOperation(Operation):
    def __init__(self, src: Union[Path, str], dst: Union[Path, str], priority: int = 1, validate: bool = True) -> None:
        self.dst = Path(dst).resolve()
        super().__init__(src, priority=priority, validate=validate)

    def validate(self) -> None:
        super().validate()
        if self.dst.exists():
            raise FileExistsError

    def run(self) -> None:
        self.validate()
        shutil.copy(self.src, self.dst)

    def serialize(self) -> Dict:
        return {"type": 2, "src": self.src.as_posix(), "dst": self.dst.as_posix(), "priority": self.priority}


class MoveOperation(CopyOperation):
    def run(self) -> None:
        self.validate()
        shutil.move(self.src, self.dst)


class ConvertOperation(CopyOperation):
    def __init__(self, src: Union[Path, str], dst: Union[Path, str], converter: Converter, priority: int = 1, validate: bool = True) -> None:
        super().__init__(src, dst, priority=priority, validate=validate)
        self.converter = converter

    def run(self) -> None:
        self.converter.run(self.src, self.dst)

    def serialize(self) -> Dict:
        return {"priority": self.priority, "type": 3, "src": self.src.as_posix(), "dst": self.dst.as_posix()}


class OperationQueue:
    def __init__(self, path='qcp.db') -> None:
        import sqlite3
        self.con = sqlite3.connect(path, isolation_level="EXCLUSIVE")
        self.path = path

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

    def put(self, op: Union[Operation, ConvertOperation], priority: Optional[int] = None) -> None:
        dd = op.serialize()

        if priority is not None:
            dd.priority = priority

        cur = self.con.cursor()

        if type(op).__name__ == "ConvertOperation":
            dd["opts"] = json.dumps(op.converter.serialize())
        else:
            dd["opts"] = None

        cur.execute("INSERT INTO OPERATIONS VALUES (:priority, :type, :src, :dst, :opts, 2, NULL)", dd)
        self.con.commit()

    def pop(self) -> Union[Operation, ConvertOperation]:
        """Retrieves Operation object and sets status of Operation in database to "in progress" (1)"""
        cur = self.con.cursor()
        cur.execute("""
            SELECT _ROWID_ from operations WHERE status = 2 ORDER BY priority LIMIT 1        
        """)
        record = cur.fetchall()[0]
        oid = record[0].__str__()
        cur.execute(
            "UPDATE operations SET status = 1, owner = :owner where _ROWID_ = :oid AND status = 2",
            {"oid": oid, "owner": id(self)},
        )
        self.con.commit()

        cur.execute("SELECT priority, type, src, dst, opts, owner FROM operations WHERE _ROWID_ = ?", oid)
        record = cur.fetchall()[0]
        if record[5] != id(self):
            raise AlreadyUnderEvaluationError

        if record[1] == 3:
            cv = Converter.from_json(record[4])
            op = ConvertOperation(priority=record[0], src=record[2], dst=record[3], converter=cv)
        else:
            op = Operation(priority=record[0], src=record[2], validate=False)

        op.oid = oid
        return op

    def mark_done(self, op):
        """Marks operation as "done" (0)"""
        cur = self.con.cursor()
        cur.execute("UPDATE operations SET status = 0, owner = NULL where _ROWID_ = ? AND status = 1", op.oid)
        self.con.commit()
        cur.execute("SELECT status FROM operations WHERE _ROWID_ = ?", op.oid)
        res = cur.fetchall()
        assert len(res) == 1
        assert res[0][0] == 0

    def run(self) -> None:
        raise NotImplementedError


class AlreadyUnderEvaluationError(Exception):
    """This Operation is already being processed by a different worker"""
    pass
