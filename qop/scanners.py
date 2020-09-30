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

    def run(self, root: Pathish) -> Path:
        root = Path(root).resolve()
        if not root.is_dir():
            yield root
        else:
            for p in root.rglob("*"):
                yield p.resolve()


class PassScanner:
    def run(self, root: Pathish) -> Generator[Path, None, None]:
        yield Path(root).resolve()


class BlacklistScanner:
    def __init__(self, extensions: list) -> None:
        self.extensions = extensions

    def run(self, root: Pathish) -> Generator[Path, None, None]:
        root = Path(root).resolve()
        logging.getLogger("qop.scanners").debug(f"collecting files without extensions {','.join(self.extensions)}")
        exts = ["." + e for e in self.extensions]

        if not root.is_dir():
            if root.suffix not in exts:
                yield root
        else:
            for p in root.rglob("*"):
                if p.suffix not in exts:
                    yield p.resolve()


class WhitelistScanner:
    def __init__(self, extensions: list) -> None:
        self.extensions = extensions

    def run(self, root: Pathish) -> Generator[Path, None, None]:
        root = Path(root).resolve()
        logging.getLogger("qop.scanners").debug(f"collecting files with extensions {','.join(self.extensions)}")
        exts = ["." + e for e in self.extensions]

        if not root.is_dir():
            if root.suffix in exts:
                yield root
        else:
            for p in root.rglob("*"):
                if p.suffix in exts:
                    yield p.resolve()
