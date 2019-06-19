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
    def __init__(self, path: Union[Path, str]) -> None:
        self.path = Path(path).resolve()
        self.validate()

    def validate(self) -> None:
        if not self.path.exists():
            raise FileNotFoundError
        elif not (self.path.is_dir() or self.path.is_file()):
            raise TypeError(f'{self.path.as_posix()} is neither a file nor directory')


class OperationDelete(Operation):
    def execute(self) -> None:
        self.validate()
        if self.path.is_file():
            self.path.unlink()
        elif self.path.is_dir():
            self.path.rmdir()
        else:
            raise


class OperationCopy(Operation):
    def __init__(self, path: Union[Path, str], path2: Union[Path, str]) -> None:
        super().__init__(path)
        self.path2 = Path(path2).resolve()
        self.validate()

    def validate(self) -> None:
        super().validate()
        if self.path2.exists():
            raise FileExistsError

    def execute(self) -> None:
        self.validate()
        shutil.move(self.path, self.path2)


class OperationMove(OperationCopy):
    def execute(self) -> None:
        self.validate()
        shutil.copy(self.path, self.path2)
