import struct
import json
from typing import Dict, Union, Optional
import tempfile
from qop import tasks
from pathlib import Path
import logging
import socket
import sys
from qop.globals import TaskType, Status, Command, PREHEADER_LEN
from qop.exceptions import FileExistsAndIsIdenticalError

Pathish = Union[Path, str]


class QopDaemon:
    port = 9393
    stats = None  # container that implements transfer statistics
    queue = None
    __is_listening = False

    def __init__(self, port: int, queue_path: Pathish, persist_queue: bool = True):
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
                if rsp.body["type"] == tasks.TaskType.COMMAND:
                    lg.info(f"received command: {rsp.encode()}")

                    if rsp.body["command"] == Command.KILL:
                        client.sendall(StatusMessage(Status.OK, "shutting down server").encode())
                        client.close()
                        self.close()
                        break

                    if rsp.body["command"] == Command.INFO:
                        client.sendall(Message(self.queue.summary).encode())

                    if rsp.body["command"] == Command.START:
                        self.queue.run()
                        client.sendall(StatusMessage(Status.OK, "start processing queue").encode())

                    if rsp.body["command"] == Command.PAUSE:
                        self.queue.pause()
                        client.sendall(StatusMessage(Status.OK, "pause processing queue").encode())

                    else:
                        msg = f"unknown command {rsp.body['command']}"
                        lg.error(msg)
                        client.sendall(StatusMessage(Status.FAIL, msg).encode())

                # tasks are added to the queue
                elif rsp.body["type"] > 0:
                    tsk = tasks.Task.from_dict(rsp.body)
                    try:
                        tsk.__validate__()
                        self.queue.put(tsk)
                        lg.info(msg)
                        client.sendall(StatusMessage(Status.OK, task=tsk).encode())
                    except FileExistsAndIsIdenticalError:
                        msg = f"destination exists"
                        lg.info(msg)
                        client.sendall(StatusMessage(Status.SKIP, msg=msg, task=tsk).encode())
                    except FileExistsError:
                        msg = f"destination exists and differs from source"
                        lg.error(msg)
                        client.sendall(StatusMessage(Status.FAIL, msg=msg, task=tsk).encode())
                    except:
                        msg = f"{sys.exc_info()}"
                        lg.error(msg)
                        client.sendall(StatusMessage(Status.FAIL, msg=msg, task=tsk).encode())
                else:
                    msg = f"unknown task {rsp.body['type']}"
                    lg.error(msg)
                    client.sendall(StatusMessage(Status.FAIL, msg=msg).encode())

            except:
                lg.error(f"error processing request {req}: {sys.exc_info()}")
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


class Message:
    """Container for requests sent to the qop daemon"""
    def __init__(self, body: Union[Dict, tasks.Task, list]) -> None:
        """

        :rtype: object
        """
        if isinstance(body, tasks.Task):
            body = body.to_dict()
            for el in ("src", "dst"):
                if el in body.keys():
                    body[el] = str(body[el])

        self.body = body

    def encode(self) -> bytes:
        body: bytes = bytes(json.dumps(self.body), "utf-8")
        header: Dict = {
            "content-length": len(body),
            "content-type": "text/json"
        }
        header: bytes = bytes(json.dumps(header), "utf-8")
        header_len: bytes = struct.pack("!H", len(header))  # network-endianess, unsigned long integer (4 bytes)
        logging.getLogger("qop.daemon").debug(f'encoding message {body} with header_length={int(struct.unpack("!H", header_len)[0])} and content_length={len(body)}')
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
        return json.loads(self._header.decode("utf-8"))

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

        self.body = {"status": int(status), "msg": msg, "task": task}
