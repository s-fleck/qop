import argparse
from qcp import daemon
from qcp import tasks
from qcp import converters
import socket

from pathlib import Path
import logging

logging.basicConfig(level="DEBUG")

parser = argparse.ArgumentParser()
parser.add_argument("operation", type=str, help="operation to execute")
parser.add_argument("source", type=str, help="path", nargs="+")
parser.add_argument("directory", help="destination directory", default="<none>", nargs="?")
args = parser.parse_args()

dst_dir = args.source[-1]

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
    client.connect(("127.0.0.1", 9393))

    for src in args.source[:-1]:

        if args.operation == "echo":
            req = daemon.Message(tasks.EchoTask(msg=src))
        else:
            src = Path(src)
            dst = Path(dst_dir).joinpath(src.name)

            if args.operation == "copy":
                req = daemon.Message(tasks.CopyTask(src=src, dst=dst))
            elif args.operation == "convert":
                req = daemon.Message(tasks.ConvertTask(src=src, dst=dst, converter=converters.OggConverter()))
            else:
                logging.getLogger("qcp/cli").fatal(f'"{args.operation}" is not a supported operation')
                raise ValueError("operation not supported")

        client.sendall(req.encode())
        res = client.recv(1024)
        logging.getLogger("qcp.daemon").info(res)
