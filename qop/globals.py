from enum import IntEnum

PREHEADER_LEN: int = 2


class Command(IntEnum):
    START = 1
    PAUSE = 2
    FLUSH = 3
    KILL = 4
    INFO = 5
    PROGRESS = 6


class TaskType(IntEnum):
    COMMAND = 0
    ECHO = 1
    FILE = 2
    DELETE = 3
    COPY = 4
    MOVE = 5
    CONVERT = 6
    KILL = 99


class Status(IntEnum):
    FAIL = 0
    OK = 1
    SKIP = 2
    RUNNING = 3
    PENDING = 4


class FileExistsAndIsIdenticalError(Exception):
    pass


def is_enum_member(x: int, enum):
    try:
        enum(x)
        return True
    except ValueError:
        return False
