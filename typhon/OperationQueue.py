from pathlib import Path
from typing import Union, Optional
from queue import PriorityQueue
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



class OperationDelete(Operation):
    def execute(self) -> None:
        self.validate()
        if self.src.is_file():
            self.src.unlink()
        elif self.src.is_dir():
            self.src.rmdir()
        else:
            raise


class OperationCopy(Operation):
    def __init__(self, src: Union[Path, str], dst: Union[Path, str]) -> None:
        self.dst = Path(dst).resolve()
        super().__init__(src)

    def validate(self) -> None:
        super().validate()
        if self.dst.exists():
            raise FileExistsError

    def execute(self) -> None:
        self.validate()
        shutil.copy(self.src, self.dst)


class OperationMove(OperationCopy):
    def execute(self) -> None:
        self.validate()
        shutil.move(self.src, self.dst)


class OperationQueue:
    def __init__(self) -> None:
        self.ops = PriorityQueue()
        self.ok = PriorityQueue()
        self.failed = PriorityQueue()

    def put(self, op: Operation, priority: Optional[int] = None) -> None:
        if priority is not None:
            op.priority = priority
        self.ops.put(op)

    def get_op(self, timeout=0) -> Operation:
        op = self.ops.get(timeout=timeout)
        self.ops.task_done()
        return op

    def run(self) -> None:
        op = self.ops.get()
        op.execute()
        self.ok.put(op)
        self.ops.task_done()