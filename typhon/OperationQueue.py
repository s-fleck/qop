from pathlib import Path


class OperationQueue:
    def add_delete(self):
        raise NotImplementedError

    def push(self):
        raise NotImplementedError

    def remove(id):
        raise NotImplementedError


class Operation:
    def __init__(self):
        raise NotImplementedError


class OperationDelete:
    def __init__(self, path: Path) -> None:
        self.path = Path(path).resolve()
        self.validate()

    def validate(self):
        if not self.path.exists():
            raise FileNotFoundError
        elif not (self.path.is_dir() or self.path.is_file()):
            raise TypeError(f'{self.path.as_posix()} is neither a file nor directory')

    def execute(self):
        self.validate()
        if self.path.is_file():
            self.path.unlink()
        elif self.path.is_dir():
            self.path.rmdir()
        else:
            raise


class OperationMove:
    def __init__(self):
        raise NotImplementedError

