import argparse
from qcp import daemon
from qcp import tasks
import socket
from pathlib import Path
import logging

logging.basicConfig(level="DEBUG")

parser = argparse.ArgumentParser()
parser.add_argument("--echo", help="makes the server log a string")
parser.add_argument("--copy", help="copy a file")
parser.add_argument("--destination", help="destination of the copy")
args = parser.parse_args()

if args.echo:
    req = daemon.Message(tasks.EchoTask(msg=args.echo))

if args.copy:
    req = daemon.Message(tasks.CopyTask(src=args.copy, dst=args.destination))


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
    client.connect(("127.0.0.1", 9393))
    client.sendall(req.encode())
    res = client.recv(1024)
    logging.getLogger("qcp.daemon").info(res)
