#! /usr/bin/env python3

import argparse
import socket
import logging
from qop import daemon, tasks, converters, scanners
from qop.globals import *
from pathlib import Path


# args
parser = argparse.ArgumentParser()
parser.add_argument("operation", type=str, help="operation to execute")
parser.add_argument("source", type=str, help="path", nargs="*")
parser.add_argument("--log-level", type=str, help="python-logging log level: DEBUG (10), INFO (20), WARNING (30), ERROR (40), CRITICAL (50)", default="WARNING")
parser.add_argument("--log-file", type=str, help="optional path to redirect logging to")
parser.add_argument("-r", "--recursive", action="store_true")
args = parser.parse_args()


# init logging
lg = logging.getLogger("qop.cli-daemon")

if args.log_level.isdigit():
    log_level = int(args.log_level)
else:
    log_level = args.log_level.upper()

if args.log_file is not None:
    logging.basicConfig(level=log_level, filename=args.log_file)
else:
    logging.basicConfig(level=log_level)

logging.getLogger("qop").setLevel("INFO")

def send_command(command):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect(("127.0.0.1", 9393))
        req = daemon.Message(tasks.CommandTask(command))
        client.sendall(req.encode())
        res = client.recv(1024)
        lg.info(res)


def enqueue_task(task):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect(("127.0.0.1", 9393))
        req = daemon.Message(tasks.CommandTask(command))
        client.sendall(req.encode())
        res = client.recv(1024)
        lg.info(res)


# commands
if args.operation == "kill":
    send_command(Command.KILL)

elif args.operation == "start":
    send_command(Command.START)

elif args.operation == "pause":
    send_command(Command.PAUSE)

elif args.operation == "info":
    send_command(Command.INFO)

elif args.source:
    dst_dir = args.source[-1]
    sources = args.source[:-1]

    if args.recursive:
        scanner = scanners.Scanner()
        sources = scanner.run(sources)
    else:
        sources = [{"root": Path(".").resolve(), "paths": sources}]

    for source in sources:
        for src in source['paths']:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect(("127.0.0.1", 9393))
                lg.debug(f"inserting {src}")

                if args.operation == "echo":
                    tsk = tasks.EchoTask(msg=" ".join(args.source))
                else:
                    src = Path(src).resolve()
                    dst = Path(dst_dir).resolve().joinpath(src.relative_to(source['root']))

                    if args.operation == "copy":
                        tsk = tasks.CopyTask(src=src, dst=dst)
                    elif args.operation == "convert":
                        tsk = tasks.ConvertTask(src=src, dst=dst, converter=converters.OggConverter())
                    elif args.operation == "sc":
                        to_convert = (".flac", ".wav", ".ape")
                        if src.suffix in to_convert:
                            tsk = tasks.ConvertTask(src=src, dst=dst.with_suffix(".ogg"), converter=converters.OggConverter(bitrate="256k"))
                        else:
                            tsk = tasks.CopyTask(src=src, dst=dst)
                    else:
                        lg.fatal(f'"{args.operation}" is not a supported operation')
                        raise ValueError("operation not supported")

                client.sendall(daemon.Message(tsk).encode())
                res = daemon.RawMessage(client.recv(1024)).decode()

                print(f"{Status(res.body['status']).name} {res.body['msg']} {tasks.Task.from_dict(res.body['task']).__repr__()}")
                client.close()
