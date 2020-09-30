#! /usr/bin/env python3

import argparse
import logging
import shutil

from qop import daemon, tasks, _cli
from qop.constants import Command

# globals
LOSSY_AUDIO = ("mp3", "ogg")
LOSSLESS_AUDIO = ("flac", "wav", "ape", "wv")
AUDIO_FILES = (LOSSY_AUDIO, LOSSLESS_AUDIO)

# args
parser = argparse.ArgumentParser()
parser.set_defaults(fun=_cli.handle_missing_args, start_daemon=False, parser=parser)
subparsers = parser.add_subparsers()

# copy
parser_copy = subparsers.add_parser("copy", help="copy a file")
parser_copy.set_defaults(fun=_cli.handle_copy_convert_move, mode="copy", start_daemon=True)

# move
parser_move = subparsers.add_parser("move", help="move a file")
parser_move.set_defaults(fun=_cli.handle_copy_convert_move, mode="move", start_daemon=True)

# convert
parser_convert = subparsers.add_parser("convert", help="convert an audio file")
parser_convert.set_defaults(fun=_cli.handle_copy_convert_move, start_daemon=True, mode="convert")
parser_convert.add_argument("-a", "--remove-art", action="store_true", help="remove album art from file tags", default=False)
g = parser_convert.add_mutually_exclusive_group()
g.add_argument("-c", "--convert-only", nargs="+", type=str, help="extensions of files to convert")
g.add_argument("-C", "--convert-not", nargs="+", type=str, help="extensions of files not to convert")
g.add_argument("-K", "--convert-none", action="store_true", help="copy all files without transcoding (useful in combination with --remove-art)", default=False)

# shared copy/convert/move arguments
for p in [parser_copy, parser_convert, parser_move]:
    p.add_argument("paths", type=str, nargs="+", help="SOURCE...DIR. An abritrary number of paths to copy and a single destination directory")
    p.add_argument("-r", "--recursive", action="store_true", help="recurse")
    p.add_argument("-e", "--enqueue-only", action="store_true", help="enqueue only. do not start processing queue.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("-i", "--include", nargs="+", type=str, help="keep only files with these extensions")
    g.add_argument("-I", "--exclude", nargs="+", type=str, help="keep only files that do not have these extensions")

# re
parser_re = subparsers.add_parser("re", help="repeat the last copy/convert/move operation on different source paths")
parser_re.set_defaults(fun=_cli.handle_re, start_daemon=True)
parser_re.add_argument("paths", type=str, nargs="+", help="source files to be copied/moved/converted")
parser_re.add_argument("-d", "--destination", type=str, nargs=1, help="an optional destination (otherwise the last destination will be used)")

# echo
parser_echo = subparsers.add_parser("echo", help="echo text (for testing the server)")
parser_echo.add_argument("msg", type=str, help="path", nargs="+", default="echo")
parser_echo.set_defaults(fun=_cli.handle_echo, start_daemon=True)

# progress
parser_progress = subparsers.add_parser("progress", help="show progress bar")
parser_progress.set_defaults(fun=_cli.handle_queue_progress, start_daemon=True)

# queue management
parser_queue = subparsers.add_parser("queue", help="manage the file processing queue (start, stop, ...)")
parser_queue.set_defaults(start_daemon=True, fun=_cli.handle_missing_args, parser=parser_queue)
parser_queue_sub = parser_queue.add_subparsers()
parser_queue_sub.add_parser("start", help="start processing the queue").set_defaults(fun=_cli.handle_simple_command, command=Command.QUEUE_START)
parser_queue_sub.add_parser("stop",  help="stop processing the queue").set_defaults(fun=_cli.handle_simple_command, command=Command.QUEUE_STOP)
parser_queue_sub.add_parser("flush", help="completely reset the queue (including finished, failed and skipped tasks)").set_defaults(fun=_cli.handle_simple_command, command=Command.QUEUE_FLUSH_ALL)
parser_queue_sub.add_parser("flush-pending", help="remove all pending tasks from the queue").set_defaults(fun=_cli.handle_simple_command, command=Command.QUEUE_FLUSH_PENDING)
parser_queue_sub.add_parser("progress", help="show interactive progress bar").set_defaults(fun=_cli.handle_queue_progress)
parser_queue_sub.add_parser("active", help="show number of active queues (usually just one)").set_defaults(fun=_cli.handle_simple_command, command=Command.QUEUE_ACTIVE_PROCESSES)
parser_queue_sub.add_parser("is-active", help="show number of active queues (usually just one)").set_defaults(fun=_cli.handle_simple_command, command=Command.QUEUE_IS_ACTIVE)
parser_queue_sub.add_parser("show", help="show the queue").set_defaults(fun=_cli.handle_simple_command, command=Command.QUEUE_SHOW)

# daemon management
parser_daemon = subparsers.add_parser("daemon", help="manage the daemon process")
parser_daemon.set_defaults(start_daemon=False, fun=_cli.handle_missing_args, parser=parser_daemon)
parser_daemon_sub = parser_daemon.add_subparsers()
parser_daemon_sub.add_parser("restart", help="restart the daemon").set_defaults(fun=_cli.handle_daemon_restart)
parser_daemon_sub.add_parser("stop", help="stop the daemon").set_defaults(fun=_cli.handle_daemon_stop)
parser_daemon_sub.add_parser("is-active", help="check if daemon is alive").set_defaults(fun=_cli.handle_daemon_is_active)
parser_daemon_sub.add_parser("destroy", help="immediately terminate the daemon and empty the queue").set_defaults(fun=_cli.handle_daemon_destroy, start_daemon=True)
parser_daemon_sub.add_parser("facts", help="return information about the daemon").set_defaults(fun=_cli.handle_simple_command, command = Command.DAEMON_FACTS, start_daemon=False)

# global options
parser.add_argument("--log-level", type=str, help="python-logging log level: DEBUG (10), INFO (20), WARNING (30), ERROR (40), CRITICAL (50)", default="WARNING")
parser.add_argument("--log-file", type=str, help="optional path to redirect logging to")
parser.add_argument("-v", "--verbose", action="store_true", help="Enqueue only without starting the queue. Note that this does not stop the queue if it is already active.")

args = parser.parse_args()


# init logging
if args.log_level.isdigit():
    log_level = int(args.log_level)
else:
    log_level = args.log_level.upper()

if args.log_file is not None:
    logging.basicConfig(level=log_level, filename=args.log_file, force=True)
else:
    logging.basicConfig(level=log_level, force=True)


# init client and start daemon if necessary
client = daemon.QopClient(ip="127.0.0.1", port=9393)
if args.start_daemon:
    _cli.handle_daemon_start(args, client)
    _cli.wait_for_daemon(client, timeout=10)

if not client.is_daemon_active():
    try:
        shutil.rmtree(tasks.CONVERT_CACHE_DIR)
    except:
        pass


# execute the command
res = args.fun(args, client)
print(_cli.format_response(res))
exit(0)
