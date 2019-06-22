from pathlib import Path
from typing import Union, Optional, Dict
from typhon import Converter
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
        return {"type": 1, "src": self.src.as_posix(), "dst": None, "priority": self.priority}


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
    def __init__(self, src: Union[Path, str], dst: Union[Path, str]) -> None:
        self.dst = Path(dst).resolve()
        super().__init__(src)

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
    def __init__(self, src: Union[Path, str], dst: Union[Path, str], converter: Converter) -> None:
        super().__init__(src, dst)
        self.converter = converter

    def run(self) -> None:
        self.converter.run(self.src, self.dst)

    def serialize(self) -> Dict:
        return {"priority": self.priority, "type": 3, "src": self.src.as_posix(), "dst": self.dst.as_posix()}


class OperationQueue:
    def __init__(self, path='typhon.db') -> None:
        import sqlite3
        self.conn = sqlite3.connect(path)

        cur = self.conn.cursor()
        cur.execute("""
           CREATE TABLE IF NOT EXISTS operations (
              priority INTEGER,
              type INTEGER,
              src TEXT,
              dst TEXT,
              status INTEGER
            )              
        """)
        self.conn.commit()

    def put(self, op: Operation, priority: Optional[int] = None) -> None:
        dd = op.serialize()

        if priority is not None:
            dd.priority = priority

        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO OPERATIONS
                VALUES (:priority, :type, :src, :dst, 2)
        """, dd)

    def get(self, timeout=0) -> Operation:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT _ROWID_, priority, type, src, dst FROM operations WHERE status = 2 ORDER BY priority LIMIT 1        
        """)
        record = cur.fetchall()[0]
        cur.execute("UPDATE operations SET status = 1 where _ROWID_ = :id AND status = 2",  {"id": record[0].__str__()})
        self.conn.commit()
        op = Operation(priority=record[1], src=record[3], validate=False)
        op.oid = record[0]
        return op

    def mark_done(self, op):
        cur = self.conn.cursor()
        cur.execute("UPDATE operations SET status = 0 where _ROWID_ = :id AND status = 1", {"id": op.oid})
        self.conn.commit()
        cur.execute("SELECT * FROM operations WHERE _ROWID_ = :id", {"id": op.oid})
        res = cur.fetchall()
        assert len(res) == 1

    def run(self) -> None:
        raise NotImplementedError
