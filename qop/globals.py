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
    SKIP = 2
    OK = 1
    FAIL = 0


class FileExistsAndIsIdenticalError(Exception):
    pass
