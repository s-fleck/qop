#! /usr/bin/env python3

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
parser.add_argument("source", type=str, help="path", nargs="*")
parser.add_argument("--log-level", type=str, help="python-logging log level: DEBUG (10), INFO (20), WARNING (30), ERROR (40), CRITICAL (50)", default="INFO")
parser.add_argument("--log-file", type=str, help="optional path to redirect logging to")
args = parser.parse_args()

lg = logging.getLogger("qcp.cli-client")

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
    client.connect(("127.0.0.1", 9393))

    if args.operation == "kill":
        req = daemon.Message(tasks.KillTask())
        client.sendall(req.encode())
        res = client.recv(1024)
        lg.info(res)

    elif args.source:
        dst_dir = args.source[-1]

        for src in args.source[:-1]:
            if args.operation == "echo":
                tsk = tasks.EchoTask(msg=" ".join(args.source))
            else:
                src = Path(src).absolute()
                dst = Path(dst_dir).joinpath(src.name)

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
                    logging.getLogger("qcp/cli").fatal(f'"{args.operation}" is not a supported operation')
                    raise ValueError("operation not supported")

            client.sendall(daemon.Message(tsk).encode())
            res = client.recv(1024)
            logging.getLogger("qcp.daemon").info(res)
