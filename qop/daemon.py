import tempfile
import socket
import logging
import struct
import json
import sys
from typing import Dict, Union, Optional
from pathlib import Path
from time import sleep

from qop import tasks, utils
from qop.exceptions import FileExistsAndShouldBeSkippedError
from qop.enums import TaskType, Status, Command, PREHEADER_LEN, PayloadClass


Pathish = Union[Path, str]
lg = logging.getLogger("qop.daemon")


class QopDaemon:
    port = None
    stats = None  # container that implements transfer statistics
    queue = None
    __is_listening = False

    def __init__(
        self,
        port: int = 9393,
        queue_path: Pathish = Path(tempfile.gettempdir()).joinpath("qop-temp.sqlite3"),
        persist_queue: bool = False
    ):
        self.port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # ADDRESS_FAMILY: INTERNET (ip4), tcp
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.new_queue(path=Path(queue_path))
        self.queue.reset_running_tasks()
        self.persist_queue = persist_queue

    def __enter__(self):
        self._socket.bind(("127.0.0.1", self.port))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.is_listening:
            self.close()

        if not self.persist_queue:
            self.queue.path.unlink()

    def close(self):
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()
        self.is_listening = False

    def listen(self, port=9393):
        lg = logging.getLogger(__name__)
        self._socket.listen(10)
        self.is_listening = True

        while True:
            client, address = self._socket.accept()
            lg.debug(f'client connected: {address}')
            req = client.recv(1024)
            lg.debug(f"processing request {req}")

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

                elif command == Command.DAEMON_IS_RUNNING:
                    client.sendall(StatusMessage(Status.OK, payload={"value": True}, payload_class=PayloadClass.VALUE).encode())

                elif command == Command.QUEUE_START:
                    self.queue.run(ip="127.0.0.1", port=self.port)
                    lg.info("starting queue")
                    client.sendall(StatusMessage(Status.OK, "start processing queue").encode())

                elif command == Command.QUEUE_STOP:
                    if self.queue.active_processes > 0:
                        self.queue.stop()
                        lg.info("stopped queue")
                        client.sendall(StatusMessage(Status.OK, "pause processing queue").encode())
                    else:
                        lg.info("cannot stop queue: no queues are running")
                        client.sendall(StatusMessage(Status.SKIP, "no running queues found").encode())

                elif command == Command.QUEUE_IS_ACTIVE:
                    self.queue.flush(status=Status.PENDING)
                    if self.queue.active_processes > 0:
                        client.sendall(StatusMessage(Status.OK, "queue is running", payload={"value": True}, payload_class=PayloadClass.VALUE).encode())
                    else:
                        client.sendall(StatusMessage(Status.OK, "queue not running", payload={"value": False}, payload_class=PayloadClass.VALUE).encode())

                elif command == Command.QUEUE_PROGRESS:
                    client.sendall(StatusMessage(Status.OK, payload=self.queue.progress().to_dict(), payload_class=PayloadClass.QUEUE_PROGRESS).encode())

                elif command == Command.QUEUE_ACTIVE_PROCESSES:
                    client.sendall(StatusMessage(Status.OK, payload={"value": self.queue.active_processes}, payload_class=PayloadClass.VALUE).encode())

                elif command == Command.QUEUE_FLUSH_ALL:
                    self.queue.flush()
                    client.sendall(StatusMessage(Status.OK, "flushed queue").encode())

                elif command == Command.QUEUE_FLUSH_PENDING:
                    self.queue.flush(status=Status.PENDING)
                    client.sendall(StatusMessage(Status.OK, "flushed pending tasks from queue").encode())

                elif command == Command.QUEUE_SHOW:
                    res = self.queue.fetch(status=Status.RUNNING)
                    client.sendall(StatusMessage(Status.OK, "retrieved running tasks", payload=res, payload_class=PayloadClass.TASK_LIST).encode())

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
                client.sendall(StatusMessage(Status.FAIL, msg=str(sys.exc_info()[0])).encode())


    @property
    def is_listening(self) -> bool:
        return self.__is_listening

    @is_listening.setter
    def is_listening(self, x: bool) -> None:
        lg = logging.getLogger(__name__)

        if x:
            lg.info(f"qop-daemon listening on port {self.port}")
        else:
            lg.debug("socket closed")

        self.__is_listening = x

    @staticmethod
    def handle_request(req):
        return RawMessage(req).decode()

    def new_queue(self, path: Path):
        self.queue = tasks.TaskQueue(path=path)


class QopClient:
    def __init__(self, ip: str = "127.0.0.1", port: int = 9393):
        self.ip = ip
        self.port = port
        self.stats = {"ok": 0, "fail": 0, "skip": 0}

    def get_queue_progress(self, max_tries=10) -> tasks.QueueProgress:
        if max_tries == 1:
            res = self.send_command(Command.QUEUE_PROGRESS)
        else:
            try:
                res = self.send_command(Command.QUEUE_PROGRESS)
            except:
                sleep(0.1)
                return self.get_queue_progress(max_tries=max_tries - 1)

        return tasks.QueueProgress.from_dict(res['payload'])

    def is_server_alive(self) -> bool:
        return utils.is_server_alive(self.ip, self.port)

    def get_active_processes(self) -> int:
        return self.send_command(Command.QUEUE_ACTIVE_PROCESSES)['payload']['value']

    """
    Send a CommandMessage to the server

    :param command: Command to send
    :type Command
    :param payload: Optional payload to send along with the command (usually a Task)
    :type None, Dict, Task    
    """
    def send_command(self, command: Command, payload: Union[None, Dict, tasks.Task] = None) -> Dict:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((self.ip, self.port))
            req = CommandMessage(command, payload=payload)
            client.sendall(req.encode())
            res = RawMessage(client.recv(2048)).decode().body

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
    """Container for requests sent to the qop daemon"""
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

    def __repr__(self) -> str:
        return f"Message: {self.body.__repr__()}"


class RawMessage:
    """Container for responses from the qop daemon"""

    def __init__(self, raw) -> None:
        assert isinstance(raw, bytes)
        self.raw: bytes = raw

    def encode(self) -> bytes:
        return self.raw

    def decode(self) -> Message:
        logging.getLogger("qop.daemon").debug(f'decoding message with header_length={self.header_len}')
        return Message(self.body)

    @property
    def header_len(self) -> int:
        return int(struct.unpack("!H", self.raw[:PREHEADER_LEN])[0])

    @property
    def _header(self) -> bytes:
        return self.raw[PREHEADER_LEN:(self.header_len + PREHEADER_LEN)]

    @property
    def header(self) -> Dict:
        header = json.loads(self._header.decode("utf-8"))
        assert header is not None
        return header

    @property
    def _body(self) -> bytes:
        start = PREHEADER_LEN + self.header_len
        return self.raw[start:start + self.header["content-length"]]

    @property
    def body(self) -> Dict:
        return json.loads(self._body.decode("utf-8"))

    def __repr__(self) -> str:
        return f"RawMessage: {self.decode().__repr__()}"


class StatusMessage(Message):
    """Messages sent from the daemon to the client to inform it on the status of an operation"""

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
    """Messages to send commands from the client to the server"""

    def __init__(self, command: Command, payload=None, payload_class=None) -> None:
        """
        :param command: Command-code to send to the server (see enums.Command)
        :type command: Command
        :param payload: Optional payload of the command. This can be a Dict or any Object that has a to_dict() Method.
          Currently the only practical payload is a Task.
        :type payload: tasks.Task, None

        :rtype: object
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
