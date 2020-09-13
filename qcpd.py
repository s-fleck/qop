import logging
from pathlib import Path
from qcp import daemon
import argparse
import tempfile


parser = argparse.ArgumentParser()
parser.add_argument("--log-level", type=str, help="python-logging log level: DEBUG (10), INFO (20), WARNING (30), ERROR (40), CRITICAL (50)", default="INFO")
parser.add_argument("--log-file", type=str, help="optional path to redirect logging to")
parser.add_argument("--queue", type=str, help="name of the queue (cannot be specified at the same time as queue-path)", default="<temp>")
parser.add_argument("--queue-path", type=str, help="path to the queue (cannot be specified at the same time as queue)")
args = parser.parse_args()

if args.queue == "<temp>":
    assert args.queue_path is None
    queue_path = Path(tempfile.gettempdir()).joinpath("qcp_tempqueue.sqlite3")
else:
    queue_path = parser.queue_path

if args.log_file is not None:
    logging.basicConfig(
        level=args.log_level,
        filename=args.log_file
    )
else:
    logging.basicConfig(level=args.log_level)

with daemon.QcpDaemon(port=9393, queue_path=args.queue, persist_queue=(args.queue != "<temp>")) as qcpd:
    qcpd.listen()
