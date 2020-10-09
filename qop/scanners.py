"""
Used by qop.py to traverse the directory tree when looking for files to transfer
"""


from pathlib import Path
from qop.constants import Pathish
import logging
from typing import Generator


class Scanner:
    def __init__(self) -> None:
        pass

    def scan(self, root: Pathish) -> Generator[Path, None, None]:
        root = Path(root).resolve()
        if not root.is_dir():
            yield root
        else:
            for p in root.rglob("*"):
                yield p.resolve()


class PassScanner(Scanner):
    def scan(self, root: Pathish) -> Generator[Path, None, None]:
        yield Path(root).resolve()


class ExcludeScanner(Scanner):
    def __init__(self, exts: list) -> None:
        self.exts = exts

    def scan(self, root: Pathish) -> Generator[Path, None, None]:
        root = Path(root).resolve()
        logging.getLogger("qop.scanners").debug(f"collecting files without extensions {','.join(self.exts)}")
        exts = ["." + e for e in self.exts]

        if not root.is_dir():
            if root.suffix not in exts:
                yield root
        else:
            for p in root.rglob("*"):
                if p.suffix not in exts:
                    yield p.resolve()


class IncludeScanner(Scanner):
    def __init__(self, exts: list) -> None:
        self.exts = exts

    def scan(self, root: Pathish) -> Generator[Path, None, None]:
        root = Path(root).resolve()
        logging.getLogger("qop.scanners").debug(f"collecting files with extensions {','.join(self.exts)}")
        exts = ["." + e for e in self.exts]

        if not root.is_dir():
            if root.suffix in exts:
                yield root
        else:
            for p in root.rglob("*"):
                if p.suffix in exts:
                    yield p.resolve()
