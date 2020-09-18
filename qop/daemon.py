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
from qop.exceptions import FileExistsAndIsIdenticalError
from qop.enums import TaskType, Status, Command, PREHEADER_LEN, is_enum_member


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

            if not req:
                continue

            try:
                rsp = self.handle_request(req)
                lg.debug(f"processing request {req}")

                # commands are not added to the queue, but executed as they are received
                if rsp.body["type"] == TaskType.COMMAND:
                    lg.debug(f"received command: {Command(rsp.body['command']).name}")

                    if rsp.body["command"] == Command.KILL:
                        client.sendall(StatusMessage(Status.OK, "shutting down server").encode())
                        self.queue.stop()
                        client.close()
                        self.close()
                        break

                    elif rsp.body["command"] == Command.ALIVE:
                        client.sendall(StatusMessage(Status.OK).encode())

                    elif rsp.body["command"] == Command.INFO:
                        client.sendall(Message(self.queue.progress().to_dict(), extra_headers={"class": "QueueProgress"}).encode())

                    elif rsp.body["command"] == Command.ISACTIVE:
                        client.sendall(Message({"active_processes": self.queue.active_processes()}).encode())

                    elif rsp.body["command"] == Command.START:
                        self.queue.run(ip="127.0.0.1", port=self.port)
                        lg.info("starting queue")
                        client.sendall(StatusMessage(Status.OK, "start processing queue").encode())

                    elif rsp.body["command"] == Command.PAUSE:
                        if self.queue.active_processes() > 0:
                            self.queue.stop()
                            lg.info("stopped queue")
                            client.sendall(StatusMessage(Status.OK, "pause processing queue").encode())
                        else:
                            lg.info("cannot stop queue: no queues are running")
                            client.sendall(StatusMessage(Status.SKIP, "no running queues found").encode())

                    elif rsp.body["command"] == Command.FLUSH:
                        self.queue.flush()
                        client.sendall(StatusMessage(Status.OK, "flushed queue").encode())

                    else:
                        msg = f"unknown command {rsp.body['command']}"
                        lg.error(msg)
                        client.sendall(StatusMessage(Status.FAIL, msg).encode())

                # tasks are added to the queue
                elif is_enum_member(rsp.body["type"], TaskType):
                    tsk = tasks.Task.from_dict(rsp.body)
                    try:
                        tsk.__validate__()
                        self.queue.put(tsk)
                        lg.debug(f"enqueued task {tsk}")
                        client.sendall(StatusMessage(Status.OK, task=tsk).encode())
                    except FileExistsAndIsIdenticalError:
                        msg = f"destination exists"
                        lg.debug(msg)
                        client.sendall(StatusMessage(Status.SKIP, msg=msg, task=tsk).encode())
                    except FileExistsError:
                        msg = f"destination exists and differs from source"
                        lg.error(msg)
                        client.sendall(StatusMessage(Status.FAIL, msg=msg, task=tsk).encode())
                    except:
                        msg = str(sys.exc_info())
                        lg.error(msg)
                        client.sendall(StatusMessage(Status.FAIL, msg=msg, task=tsk).encode())
                else:
                    msg = f"unknown task {rsp.body['type']}"
                    lg.error(msg)
                    client.sendall(StatusMessage(Status.FAIL, msg=msg).encode())

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

    def serve_queue(self, n=100):
        pass

    def stop(self):
        pass

    def new_queue(self, path: Path):
        self.queue = tasks.TaskQueue(path=path)


class QopClient:
    def __init__(self, ip: str = "127.0.0.1", port: int = 9393):
        self.ip = ip
        self.port = port
        self.stats = {"ok": 0, "fail": 0, "skip": 0}

    def get_queue_progress(self, max_tries=10) -> tasks.QueueProgress:
        if max_tries == 1:
            res = self.send_command(Command.INFO)
        else:
            try:
                res = self.send_command(Command.INFO)
            except:
                sleep(0.1)
                return self.get_queue_progress(max_tries=max_tries - 1)

        return tasks.QueueProgress.from_dict(res)

    def is_server_alive(self) -> bool:
        return utils.is_server_alive(self.ip, self.port)

    def get_active_processes(self) -> int:
        return self.send_command(Command.ISACTIVE)['active_processes']

    def send_command(self, command: Command) -> Dict:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((self.ip, self.port))
            req = Message(tasks.CommandTask(command))
            client.sendall(req.encode())
            res = RawMessage(client.recv(1024)).decode().body
            lg.info(res)
            return res

    def send_task(self, task: tasks.Task) -> Dict:
        """
        Instantiate a TaskQueue

        :param task: the Task to send to the server to enqueue
        :param summary: a Dict with the keys 'ok', 'skip' and 'fail' to store the status of the insert operation in
        :param verbose: WIP
        """

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((self.ip, self.port))
            client.sendall(Message(task).encode())
            res = RawMessage(client.recv(1024)).decode().body

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
            body: Union[Dict, tasks.Task, list],
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
        if isinstance(self.body, tasks.Task):
            body = self.body.to_dict()
            for el in ("src", "dst"):
                if el in body.keys():
                    body[el] = str(body[el])
        else:
            body = self.body
        body = bytes(json.dumps(body), "utf-8")

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

    def __init__(self, status: int, msg: Optional[str] = None, task=None) -> None:
        """

        :rtype: object
        """

        if task is not None:
            task = task.to_dict()

        super().__init__(
            body={"status": int(status), "msg": msg, "task": task},
            extra_headers={"class": "StatusMessage"}
        )
