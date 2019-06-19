import os
from pathlib import Path


def list_trash (path, recursive = True):
    p = Path(path)
    assert p.exists()
    return p.rglob("**/*.jpg")


def list_duplicates (path, recursive = True):
    p = Path(path)

    if not p.exists():
        raise FileNotFoundError(path)

    return p.rglob("**/*.jpg")
