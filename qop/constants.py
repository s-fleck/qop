"""
This module defines global constants and enums. The numeric values of the integer enums 
are used to represent the respective value when serializing qop objects into a TaskQueue
sqlite3 database.
"""

from enum import IntEnum
from typing import Union
from pathlib import Path

Pathish = Union[Path, str]


class Status(IntEnum):
    """
    Status codes are used by
    
      - TaskQueue to represent the status of a task in the queue, and
      - StatusMessage to indicate whether a command sent to the server was successful or not
    """
    FAIL = -1
    PENDING = 0
    OK = 1
    SKIP = 2
    ACTIVE = 3


class ConverterType(IntEnum):
    """Types of converters; used when serializing converters to json"""
    COPY = 0
    MP3 = 1
    OGG = 2


class Command(IntEnum):
    """Commands codes that can be sent to the daemon via a CommandMessage. See :class:`~qop.daemon.QopClient` for usage examples."""
    DAEMON_START = 101
    DAEMON_STOP = 102
    DAEMON_IS_ACTIVE = 103
    DAEMON_FACTS = 104
    QUEUE_START = 201
    QUEUE_STOP = 202
    QUEUE_IS_ACTIVE = 203
    QUEUE_PUT = 204
    QUEUE_FLUSH_PENDING = 205
    QUEUE_FLUSH_ALL = 206
    QUEUE_PROGRESS = 207
    QUEUE_ACTIVE_PROCESSES = 208
    QUEUE_SHOW = 209
    QUEUE_MAX_PROCESSES = 210


class PayloadClass(IntEnum):
    """Types of Payloads that can be part of CommandMessage or StatusMessage"""
    VALUE = 1  # a single value {"value": <value>}
    TASK = 2
    QUEUE_PROGRESS = 3
    TASK_LIST = 4
    DAEMON_FACTS = 5


class TaskType(IntEnum):
    """Types of Tasks. Corresponds to the subclasses of tasks.Task"""
    ECHO = 1
    FILE = 2
    DELETE = 3
    COPY = 4
    MOVE = 5
    CONVERT_SIMPLE = 6
    FAIL = 7
    SLEEP = 8
    CONVERT = 9
