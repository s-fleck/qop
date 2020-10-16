"""
This module defines classes for the daemon and the client that the command 
line programs provided by qop are based on.

  - The client sends :class:`CommandMessages <qop.daemon.CommandMessage>` via 
    TCP to the daemon to enqueue new tasks,
    tell it to start/stop processing the queue, query information, etc... .
  - The daemon manages a single :class:`~qop.tasks.TaskQueue` that stores and 
    processes :class:`Tasks <qop.tasks.Task>`. For every command it receives, 
    it responds with a :class:`~qop.daemon.StatusMessage` that contains 
    the status of the operation (ok, fail, skip) as well as an optional JSON 
    payload.

Communication diagram::

    +-------------+                        +-------------+   +-------------+
    |             | ---[CommandMessage]--> |             |   |             |
    |  QopClient  |                        |  QopDaemon  |---|  TaskQueue  |
    |             | <--[StatusMessage]---- |             |   |             |
    +-------------+                        +-------------+   +-------------+
"""

# https://realpython.com/python-sockets/#application-client-and-server

import tempfile
import socket
import logging
import struct
import json
import sys
from typing import Dict, Union, Optional
from pathlib import Path
from time import sleep

from qop import tasks, _utils
from qop.exceptions import FileExistsAndShouldBeSkippedError
from qop.constants import Status, Command, PayloadClass, Pathish

PREHEADER_LEN: int = 2
lg = logging.getLogger(__name__)


class QopDaemon:
    port = None
    stats = None  # container that implements transfer statistics
    queue = None

    def __init__(
        self,
        port: int = 9393,
        queue_path: Pathish = Path(tempfile.gettempdir()).joinpath("qop-temp.sqlite3"),
        persist_queue: bool = False
    ):
        """
        QopDaemon manages a :class:`~qop.tasks.TaskQueue`. It can insert
        tasks, tell the queue to start or stop processing tasks, and query
        information about the queue.  It accepts 
        :class:`CommandMessages <qop.daemon.CommandMessage>`, for example sent
        by `~qop.daemon.QopClient`.

        :param port: Port to bind the daemon to
        :param queue_path: Path for storing the transfer queue
        :param persist_queue: Whether or not to delete the queue when the daemon is stopped
        """
        self.port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # ADDRESS_FAMILY: INTERNET (ip4), tcp
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.new_queue(path=Path(queue_path))
        self.queue.reset_active_tasks()
        self.persist_queue = persist_queue

    def __enter__(self):
        self._socket.bind(("127.0.0.1", self.port))
        lg.info(f"QopDaemon listening on port {self.port}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.queue.stop()

        try:
            self.close()
        except:
            pass

        if not self.persist_queue:
            self.queue.path.unlink()

    def close(self):
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()

    def listen(self, port=9393):
        lg = logging.getLogger(__name__)
        self._socket.listen(10)

        while True:
            client, address = self._socket.accept()
            lg.debug(f'client connected: {address}')
            req = client.recv(1024)
            lg.debug(f"processing request {req}")

            if self.queue.is_active():
                if len(self.queue.convert_processes) < self.queue.max_convert_processes:
                    self.queue.start()
                elif len(self.queue.transfer_processes) < self.queue.max_transfer_processes:
                    self.queue.start()
                elif len(self.queue.convert_processes) > self.queue.max_convert_processes:
                    self.queue.stop()
                    self.queue.start()
                elif len(self.queue.transfer_processes) > self.queue.max_transfer_processes:
                    self.queue.stop()
                    self.queue.start()

            if not req:
                continue

            try:
                dd = self.handle_request(req)
                command = dd.body['command']

                if command == Command.DAEMON_START:
                    raise NotImplementedError

                elif command == Command.DAEMON_STOP:
                    client.sendall(StatusMessage(Status.OK, "shutting down server").encode())
                    self.queue.stop()
                    client.close()
                    self.close()
                    break

                elif command == Command.DAEMON_IS_ACTIVE:
                    client.sendall(StatusMessage(Status.OK, payload={"value": True}, payload_class=PayloadClass.VALUE).encode())

                elif command == Command.DAEMON_FACTS:
                    client.sendall(StatusMessage(Status.OK, payload=self.facts(), payload_class=PayloadClass.DAEMON_FACTS).encode())

                elif command == Command.QUEUE_START:
                    self.queue.start(ip="127.0.0.1", port=self.port)
                    lg.info("starting queue")
                    client.sendall(StatusMessage(Status.OK, "start processing queue").encode())

                elif command == Command.QUEUE_STOP:
                    if self.queue.active_processes() > 0:
                        self.queue.stop()
                        lg.info("stopped queue")
                        client.sendall(StatusMessage(Status.OK, "pause processing queue").encode())
                    else:
                        lg.info("cannot stop queue: no queues are active")
                        client.sendall(StatusMessage(Status.SKIP, "no active queues found").encode())

                elif command == Command.QUEUE_IS_ACTIVE:
                    if self.queue.active_processes() > 0:
                        client.sendall(StatusMessage(Status.OK, "queue is active", payload={"value": True}, payload_class=PayloadClass.VALUE).encode())
                    else:
                        client.sendall(StatusMessage(Status.OK, "queue not active", payload={"value": False}, payload_class=PayloadClass.VALUE).encode())

                elif command == Command.QUEUE_PROGRESS:
                    client.sendall(StatusMessage(Status.OK, payload=self.queue.progress().to_dict(), payload_class=PayloadClass.QUEUE_PROGRESS).encode())

                elif command == Command.QUEUE_ACTIVE_PROCESSES:
                    client.sendall(StatusMessage(Status.OK, payload={
                        "transfer":  self.queue.active_processes(type="transfer"),
                        "convert": self.queue.active_processes(type="convert")
                    }).encode())

                elif command == Command.QUEUE_MAX_PROCESSES:
                    client.sendall(StatusMessage(
                        Status.OK,
                        payload={"value": self.queue.max_transfer_processes + self.queue.max_convert_processes},
                        payload_class=PayloadClass.VALUE
                    ).encode())

                elif command == Command.QUEUE_FLUSH_ALL:
                    self.queue.flush()
                    client.sendall(StatusMessage(Status.OK, "flushed queue").encode())

                elif command == Command.QUEUE_FLUSH_PENDING:
                    self.queue.flush(status=Status.PENDING)
                    client.sendall(StatusMessage(Status.OK, "flushed pending tasks from queue").encode())

                elif command == Command.QUEUE_SHOW:
                    res = self.queue.fetch(status=Status.ACTIVE)
                    client.sendall(StatusMessage(Status.OK, "retrieved active tasks", payload=res, payload_class=PayloadClass.TASK_LIST).encode())

                elif dd.body['command'] == Command.QUEUE_PUT:
                    tsk = tasks.Task.from_dict(dd.body['payload'])
                    try:
                        tsk.__validate__()
                        self.queue.put(tsk)
                        lg.debug(f"enqueued task {tsk}")
                        client.sendall(StatusMessage(Status.OK, payload=tsk, payload_class=PayloadClass.TASK).encode())
                    except FileExistsAndShouldBeSkippedError:
                        msg = f"destination exists"
                        lg.debug(msg)
                        client.sendall(StatusMessage(Status.SKIP, msg=msg, payload=tsk, payload_class=PayloadClass.TASK).encode())
                    except FileExistsError:
                        msg = f"destination exists and differs from source"
                        lg.error(msg)
                        client.sendall(StatusMessage(Status.FAIL, msg=msg, payload=tsk, payload_class=PayloadClass.TASK).encode())
                    except:
                        msg = str(sys.exc_info())
                        lg.error(msg)
                        client.sendall(StatusMessage(Status.FAIL, msg=msg, payload=tsk, payload_class=PayloadClass.TASK).encode())
                else:
                    msg = f"unknown command {dd.body['command']}"
                    lg.error(msg)
                    client.sendall(StatusMessage(Status.FAIL, msg).encode())

            except:
                lg.error(f"unknown error processing request {req}: {sys.exc_info()}")
                info = sys.exc_info()
                lg.error(info, exc_info=info)
                client.sendall(StatusMessage(Status.FAIL, msg=str(info[0]) + str(info[1])).encode())

    @staticmethod
    def handle_request(req):
        return Message.from_bytes(req)

    def new_queue(self, path: Path):
        self.queue = tasks.TaskQueue(path=path)

    def facts(self) -> Dict:
        dinfo = {
            "port": self.port,
            "queue.persist": self.persist_queue,
        }
        dinfo.update(self.queue.facts())
        return dinfo


class QopClient:
    """    
    A simple client for interacting with QopDaemon. Usage example:

    .. code-block:: python
    
        tsk = tasks.EchoTask("foo")
        client = daemon.QopClient()
        client.send_command(Command.QUEUE_PUT, payload=tsk)
        client.send_command(Command.QUEUE_START) 

    """
    def __init__(self, ip: str = "127.0.0.1", port: int = 9393):
        self.ip = ip
        self.port = port
        self.stats = {"ok": 0, "fail": 0, "skip": 0}

    def gather_facts(self, max_tries=10) -> Dict:
        if max_tries == 1:
            res = self.send_command(Command.DAEMON_FACTS)
        else:
            try:
                res = self.send_command(Command.DAEMON_FACTS)
            except:
                sleep(0.1)
                return self.gather_facts(max_tries=max_tries - 1)

        return res['payload']

    def get_queue_progress(self, max_tries=10) -> tasks.QueueProgress:  # TODO: deprecated
        if max_tries == 1:
            res = self.send_command(Command.QUEUE_PROGRESS)
        else:
            try:
                res = self.send_command(Command.QUEUE_PROGRESS)
            except:
                sleep(0.1)
                return self.get_queue_progress(max_tries=max_tries - 1)

        return tasks.QueueProgress.from_dict(res['payload'])

    def is_daemon_active(self) -> bool:
        return _utils.is_daemon_active(self.ip, self.port)

    @property
    def max_processes(self) -> list:
        return self.send_command(Command.QUEUE_MAX_PROCESSES)['payload']['value']

    @property
    def active_tasks(self) -> list:
        return self.send_command(Command.QUEUE_SHOW)['payload']

    @property
    def active_processes(self) -> int:
        x = self.send_command(Command.QUEUE_ACTIVE_PROCESSES)['payload']
        return x['transfer'] + x['convert']

    def is_queue_active(self) -> int:
        x = self.send_command(Command.QUEUE_IS_ACTIVE)['payload']['value']
        return x

    """
    Send a CommandMessage to the server

    :param command: Command to send
    :param payload: Optional payload to send along with the command (usually a Task)
    """
    def send_command(
      self, 
      command: Command, 
      payload: Union[None, Dict, tasks.Task, list] = None
    ) -> Dict:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((self.ip, self.port))
            req = CommandMessage(command, payload=payload)
            client.sendall(req.encode())
            res = Message.from_bytes(client.recv(2048)).body

            # track enqueued tasks of this client
            if command == Command.QUEUE_PUT:
                if res['status'] == Status.OK:
                    self.stats['ok'] = self.stats['ok'] + 1
                if res['status'] == Status.SKIP:
                    self.stats['skip'] = self.stats['skip'] + 1
                if res['status'] == Status.FAIL:
                    self.stats['fail'] = self.stats['fail'] + 1

            return res


class Message:
    """Container for messages sent between :class:`~qop.daemon.QopDaemon` and :class:`~qop.daemon.QopClient`."""
    def __init__(
            self,
            body: Dict,
            extra_headers: Optional[Dict] = None
    ) -> None:
        """

        :rtype: object
        """
        if extra_headers is None:
            extra_headers = {}

        self.body = body
        self.extra_headers = extra_headers

    def encode(self) -> bytes:
        body = bytes(json.dumps(self.body), "utf-8")

        header = {"content-length": len(body), "content-type": "text/json"}
        header.update(self.extra_headers)
        header = bytes(json.dumps(header), "utf-8")
        header_len: bytes = struct.pack("!H", len(header))  # network-endianess, unsigned long integer (4 bytes)

        lg.debug(f'encoding message {body} with header_length={int(struct.unpack("!H", header_len)[0])} and content_length={len(body)}')
        return header_len + header + body


    @staticmethod
    def from_bytes(x: bytes) -> "Message":
        logging.getLogger("qop.daemon").debug(f"decoding message '{x}'")

        header_len = int(struct.unpack("!H", x[:PREHEADER_LEN])[0])
        raw_header = x[PREHEADER_LEN:(header_len + PREHEADER_LEN)]
        header = json.loads(raw_header.decode("utf-8"))

        body_start = PREHEADER_LEN + header_len
        raw_body = x[body_start:body_start + header["content-length"]]
        body = json.loads(raw_body.decode("utf-8"))

        del header['content-length']
        return Message(body=body, extra_headers=header)

    def __repr__(self) -> str:
        return f"Message: {self.body.__repr__()}"


class StatusMessage(Message):
    """
    Class responses sent by :class:`~qop.daemon.QopDaemon` to 
    :class:`~qop.daemon.QopClient`.
    """

    def __init__(self, status: int, msg: Optional[str] = None, payload=None, payload_class=None) -> None:
        """
        :param status: Status-code returned from the server (see enums.Status)
        :type status: Status
        :param payload: Optional payload returned by the server. Can be a Dict or an object with a to_dict Method
          (usually a tasks.Task)
        :type payload: Dict, tasks.Task

        :rtype: object
        """

        body = {"status": int(status)}

        if msg is not None:
            body.update({"msg": msg})

        if payload is not None:
            try:
                payload = payload.to_dict()
            except:
                pass

            body.update({"payload": payload})

            if payload_class is not None:
                body.update({"payload_class": payload_class})

        super().__init__(
            body=body,
            extra_headers={"message-class": "StatusMessage"}
        )
        

class CommandMessage(Message):
    """
    Class for commands sent by :class:`~qop.daemon.QopClient` to 
    :class:`~qop.daemon.QopDaemon`.
    """

    def __init__(self, command: Command, payload=None, payload_class=None) -> None:
        """
        :param command: :class:`Command <qop.constants.Command>` to send to the 
            daemon
        :param payload: Optional payload of the command. This can be a Dict or 
            any Object that has a `to_dict()` method. Usually this is a
            :class:`~qop.tasks.Task`.
        """

        body = {"command": int(command)}

        if payload is not None:
            try:
                payload = payload.to_dict()
            except:
                pass

            body.update({"payload": payload})

            if payload_class is not None:
                body.update({"payload_class": payload_class})

        super().__init__(
            body=body,
            extra_headers={"message-class": "CommandMessage"}
        )
