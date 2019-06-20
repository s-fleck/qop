from pathlib import Path
from typing import Union
import shutil

class OperationQueue:
    def add_delete(self):
        raise NotImplementedError

    def push(self):
        raise NotImplementedError

    def remove(id):
        raise NotImplementedError


class Operation:
    def __init__(self, src: Union[Path, str]) -> None:
        self.src = Path(src).resolve()
        self.validate()

    def validate(self) -> None:
        if not self.src.exists():
            raise FileNotFoundError
        elif not (self.src.is_dir() or self.src.is_file()):
            raise TypeError(f'{self.src.as_posix()} is neither a file nor directory')


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
