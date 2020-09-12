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
parser.add_argument("source", type=str, help="source (if applicable)")
parser.add_argument("destination", help="destination (if applicable)", default="<none>")
args = parser.parse_intermixed_args()

print(args.operation)

if args.operation == "echo":
    req = daemon.Message(tasks.EchoTask(msg=args.source))
elif args.operation == "copy":
    req = daemon.Message(tasks.CopyTask(src=args.source, dst=args.destination))
elif args.operation == "convert":
    req = daemon.Message(tasks.ConvertTask(src=args.source, dst=args.destination, converter=converters.OggConverter()))

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
    client.connect(("127.0.0.1", 9393))
    client.sendall(req.encode())
    res = client.recv(1024)
    logging.getLogger("qcp.daemon").info(res)
