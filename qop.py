#! /usr/bin/env python3

import argparse
import logging
from pathlib import Path
from colorama import init, Fore
from typing import Dict, Union, Optional
from time import sleep
from tqdm import tqdm

from qop import daemon, tasks, scanners
from qop.enums import Status, Command

init()



lg = logging.getLogger("qop.cli-daemon")


def format_response(rsp) -> str:
    res = color_status(rsp['status']) + " "

    if "task" in rsp.keys() and rsp['task'] is not None:
        res =res + tasks.Task.from_dict(rsp['task']).color_repr()

    if 'msg' in rsp.keys():
        res = res + f"{Fore.YELLOW}[{rsp['msg']}]{Fore.RESET}"

    return res


def format_response_summary(x) -> str:
    total = str(x['ok'] + x['skip'] + x['fail']).rjust(6, " ")
    ok = Fore.GREEN + str(x['ok']).rjust(6, " ") + Fore.RESET
    skip = Fore.BLUE + str(x['skip']).rjust(6, " ") + Fore.RESET
    fail = Fore.RED + str(x['fail']).rjust(6, " ") + Fore.RESET

    return f"  [enqueue: {total} | ok: {ok} | skip: {skip} | fail: {fail}]"


def color_status(x: int):
    x = Status(x)
    pad = 4
    if x == Status.OK:
        return f"{Fore.GREEN}{x.name.rjust(pad, ' ')}{Fore.RESET}"
    elif x == Status.SKIP:
        return f"{Fore.BLUE}{x.name.rjust(pad, ' ')}{Fore.RESET}"
    else:
        return f"{Fore.RED}{x.name.rjust(pad, ' ')}{Fore.RESET}"


# subcommand handlers
def handle_echo(args, client) -> Dict:
    return client.send_task(tasks.EchoTask(msg=" ".join(args.msg)))


def handle_copy(args, client) -> Dict:
    sources = args.paths[:-1]
    dst_dir = args.paths[-1]
    is_queue_running = client.get_active_processes() > 0

    assert isinstance(dst_dir, str)
    assert len(sources) > 0
    assert sources != dst_dir

    if args.recursive:
        scanner = scanners.Scanner()
        sources = scanner.run(sources)

    for source in sources:
        if not isinstance(source, Dict):
            source = {
                "root": Path(source).absolute().parent,
                "paths": [Path(source).absolute()]
            }

        for src in source['paths']:
            lg.debug(f"inserting {src}")
            src = Path(src).resolve()
            dst = Path(dst_dir).resolve().joinpath(src.relative_to(source['root']))
            tsk = tasks.CopyTask(src=src, dst=dst)
            res = client.send_task(tsk)
            
            if not args.enqueue_only and not is_queue_running:
                client.send_command(Command.START)
                sleep(0.1)
                info = tasks.QueueProgress.from_dict(client.send_command(Command.INFO))
                is_queue_running = True

            if args.verbose:
                print(format_response(res))

            print(format_response_summary(client.stats), end="\r")

    return {"status": Status.OK, "msg": "enqueue finished"}


def daemon_stop(args, client):
    return client.send_command(Command.KILL)

def daemon_destroy(args, client):
    client.send_command(Command.FLUSH)
    return client.send_command(Command.KILL)


def daemon_start(args, client):
    raise NotImplementedError


def daemon_is_alive(args, client):
    return client.is_server_alive()


def queue_start(args, client):
    return client.send_command(Command.START)


def queue_stop(args, client):
    return client.send_command(Command.PAUSE)


def queue_is_active(args, client):
    return client.send_command(Command.ISACTIVE)


def queue_info(args, client):
    return client.send_command(Command.INFO)


def queue_flush(args, client):
    raise NotImplementedError


def queue_flush_all(args, client):
    return client.send_command(Command.FLUSH)


def queue_progress(args, client):
    info = client.get_queue_progress()

    with tqdm(total=info.total, initial=info.total - info.pending) as pbar:
        for i in range(info.total):
            sleep(0.5)
            info = client.get_queue_progress()
            pbar.update(info.total - info.pending - pbar.n)

            if not client.is_server_alive() or client.get_active_processes() < 1:
                break

    if info.total == info.ok + info.skip:
        return {"status": Status.OK, "msg": "all files transferred successfully"}
    else:
        return {"status": Status.FAIL, "msg": "could not transfer all files"}



# args
parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()

# main operations
parser_copy = subparsers.add_parser("copy", help="copy a file")
parser_copy.set_defaults(fun=handle_copy)

parser_convert = subparsers.add_parser("convert", help="convert an audio file")
parser_sac = subparsers.add_parser("sac", help="smart-audio-convert")

# shared arguments
for p in [parser_copy, parser_convert, parser_sac]:
    p.add_argument("paths", type=str, nargs="+", help="SOURCE...DIR. An abritrary number of paths to copy and a single destination directory")
    p.add_argument("-r", "--recursive", action="store_true", help="recurse")
    p.add_argument("-e", "--enqueue-only", action="store_true", help="enqueue only. do not start processing queue.")

parser_echo = subparsers.add_parser("echo", help="echo text (for testing the server)")
parser_echo.add_argument("msg", type=str, help="path", nargs="+", default="echo")
parser_echo.set_defaults(fun=handle_echo)

# queue management
parser_queue = subparsers.add_parser("queue", help="manage the file processing queue (start, stop, ...)")
parser_queue_sub = parser_queue.add_subparsers()
parser_queue_sub.add_parser("start", help="start processing the queue").set_defaults(fun=queue_start)
parser_queue_sub.add_parser("stop",  help="stop processing the queue").set_defaults(fun=queue_stop)
parser_queue_sub.add_parser("flush", help="remove all pending tasks from the queue").set_defaults(fun=queue_flush)
parser_queue_sub.add_parser("flush-all", help="completely reset the queue (including finished, failed and skipped tasks)").set_defaults(fun=queue_flush_all)
parser_queue_sub.add_parser("progress", help="show interactive progress bar").set_defaults(fun=queue_progress)
parser_queue_sub.add_parser("active", help="show number of active queues (usually just one)").set_defaults(fun=queue_is_active)

# daemon management
parser_daemon = subparsers.add_parser("daemon", help="manage the daemon process")
parser_daemon_sub = parser_daemon.add_subparsers()
parser_daemon_sub.add_parser("start", help="start the daemon").set_defaults(fun=daemon_start)
parser_daemon_sub.add_parser("stop", help="stop the daemon").set_defaults(fun=daemon_stop)
parser_daemon_sub.add_parser("alive", help="check if daemon is alive").set_defaults(fun=daemon_is_alive)
parser_daemon_sub.add_parser("destroy", help="immediately terminate the daemon and empty the queue").set_defaults(fun=daemon_destroy)


# global optional arguments
parser.add_argument("--log-level", type=str, help="python-logging log level: DEBUG (10), INFO (20), WARNING (30), ERROR (40), CRITICAL (50)", default="WARNING")
parser.add_argument("--log-file", type=str, help="optional path to redirect logging to")
parser.add_argument("-v", "--verbose", action="store_true", help="Enqueue only without starting the queue. Note that this does not stop the queue if it is already running.")

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


# client
client = daemon.QopClient(ip="127.0.0.1", port=9393)

res = args.fun(args, client)
print(format_response(res))

if not args.enqueue_only:
    client.send_command(Command.START)
