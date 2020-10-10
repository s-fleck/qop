#! /usr/bin/env python3

import logging
from pathlib import Path
from qop import daemon
import argparse
import tempfile
import appdirs


parser = argparse.ArgumentParser()
parser.add_argument("--log-level", type=str, help="python-logging log level: DEBUG (10), INFO (20), WARNING (30), ERROR (40), CRITICAL (50)", default="INFO")
parser.add_argument("--log-file", type=str, help="optional path to redirect logging to")
parser.add_argument("--queue", type=str, help="name of the queue (cannot be specified at the same time as queue-path)")
parser.add_argument("--queue-path", type=str, help="path to the queue (cannot be specified at the same time as queue)")

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


# find path for tasks queue
if args.queue_path is not None:
    if args.qeueu is not None:
        raise ValueError("cannot specify --queue and --queue-path at the same time")

    queue_path = Path(args.qeueu_path)

elif args.queue == "<temp>":
    queue_path = Path(tempfile.gettempdir()).joinpath("qop-temp.sqlite3")

elif args.queue is not None:
    queue_path = Path(appdirs.user_cache_dir("qop")).joinpath(f"{args.queue}.sqlite3")

else:
    queue_path = Path(appdirs.user_cache_dir("qop")).joinpath("default.sqlite3")

if not queue_path.parent.exists():
    queue_path.parent.mkdir(parents=True)
    lg.info(f"created default directory for qop queues: '{queue_path.parent}'")


# launch daemon
with daemon.QopDaemon(port=9393, queue_path=queue_path, persist_queue=(args.queue != "<temp>")) as qopd:
    qopd.listen()

