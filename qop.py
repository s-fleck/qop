#! /usr/bin/env python3

import argparse
import logging
import pickle
import appdirs
import subprocess
import shutil

from pathlib import Path
from colorama import init, Fore
from typing import Dict, Union, Optional
from time import sleep
from tqdm import tqdm

from qop import daemon, tasks, scanners, converters
from qop.enums import Status, Command, PayloadClass

init()
lg = logging.getLogger("qop.cli-daemon")


# globals
LOSSY_AUDIO = ("mp3", "ogg")
LOSSLESS_AUDIO = ("flac", "wav", "ape", "wv")
AUDIO_FILES = (LOSSY_AUDIO, LOSSLESS_AUDIO)


def handle_missing_args(args, client):
    args.parser.print_help()
    args.parser.exit()


def format_response(rsp) -> str:
    res = color_status(rsp['status']) + " "

    try:
        plc = PayloadClass(rsp['payload_class'])
    except:
        plc = None

    if "payload" in rsp.keys() and rsp['payload'] is not None:
        payload = rsp['payload']
        if plc == PayloadClass.VALUE:
            res = res + str(payload['value']) + " "
        elif plc == PayloadClass.TASK:
            res = res + tasks.Task.from_dict(payload).color_repr() + " "
        elif plc == PayloadClass.TASK_LIST:
            res = "\n".join([tasks.Task.from_dict(x['task']).color_repr() for x in payload]) + "\n\n" + res
        else:
            res = res + str(payload)

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
    return client.send_command(Command.QUEUE_PUT, payload=tasks.EchoTask(msg=" ".join(args.msg)))


def handle_re(args, client) -> Dict:

    with open(Path(appdirs.user_cache_dir('qop')).joinpath('last_args.pickle'), 'rb') as f:
        last_args = pickle.load(f)

    if args.destination is not None:
        last_args.paths = args.paths + args.destination
    else:
        last_args.paths = args.sources + [last_args.paths[-1]]

    # global args that should not be overriden
    last_args.verbose = args.verbose
    last_args.log_file = args.log_file
    last_args.log_level = args.log_level

    if last_args.mode == "convert":
        return handle_convert(last_args, client)
    else:
        return handle_copy_move(last_args, client)


def handle_copy_move(args, client) -> Dict:
    sources = args.paths[:-1]
    dst_dir = args.paths[-1]
    is_queue_running = client.is_queue_active()

    assert isinstance(dst_dir, str)
    assert len(sources) > 0
    assert sources != dst_dir

    # for use by `qop re`
    args_cache = Path(appdirs.user_cache_dir('qop')).joinpath('last_args.pickle')
    if args_cache.exists():
        args_cache.unlink()
    with open(args_cache, 'wb') as f:
        args.parser = None
        pickle.dump(args, f, pickle.HIGHEST_PROTOCOL)

    if args.include is not None:
        scanner = scanners.ScannerWhitelist(args.include)
        sources = scanner.run(sources)
    elif args.exclude is not None:
        scanner = scanners.ScannerBlacklist(args.exclude)
        sources = scanner.run(sources)

    for source in sources:
        if not isinstance(source, Dict):
            source = {
                "root": Path(source).resolve().parent,
                "paths": [Path(source).resolve()]
            }

        for src in source['paths']:
            lg.debug(f"inserting {src}")
            src = Path(src).resolve()
            dst = Path(dst_dir).resolve().joinpath(src.relative_to(source['root']))
            if args.mode == "move":
                tsk = tasks.MoveTask(src=src, dst=dst)
            elif args.mode == "copy":
                tsk = tasks.CopyTask(src=src, dst=dst)
            else:
                raise ValueError

            rsp = client.send_command(Command.QUEUE_PUT, payload=tsk)

            if not is_queue_running and not args.enqueue_only:
                client.send_command(Command.QUEUE_START)
                is_queue_running = True

            if args.verbose:
                print(format_response(rsp))

            print(format_response_summary(client.stats), end="\r")

    if not args.enqueue_only:
        client.send_command(Command.QUEUE_START)
    return {"status": Status.OK, "msg": "enqueue finished"}


def handle_convert(args, client) -> Dict:
    sources = args.paths[:-1]
    dst_dir = args.paths[-1]
    is_queue_running = client.is_queue_active()

    assert isinstance(dst_dir, str)
    assert len(sources) > 0
    assert sources != dst_dir

    # for use by `qop re`
    args_cache = Path(appdirs.user_cache_dir('qop')).joinpath('last_args.pickle')
    if args_cache.exists():
        args_cache.unlink()
    with open(args_cache, 'wb') as f:
        args.parser = None
        pickle.dump(args, f, pickle.HIGHEST_PROTOCOL)

    if args.include is not None:
        scanner = scanners.ScannerWhitelist(args.include)
    elif args.exclude is not None:
        scanner = scanners.ScannerBlacklist(args.exclude)
    else:
        scanner = scanners.Scanner()

    sources = scanner.run(sources)

    conv = converters.Mp3Converter(remove_art=args.remove_art)  # TODO
    conv_copy = converters.CopyConverter(remove_art=args.remove_art)

    if args.convert_only is not None:
        conv_include = ["." + e for e in args.convert_only]
        conv_mode = "include"
    elif args.convert_not is not None:
        conv_exclude = ["." + e for e in args.convert_not]
        conv_mode = "exclude"
    elif args.convert_none:
        conv_mode = "none"
    else:
        conv_mode = "all"

    for source in sources:
        if not isinstance(source, Dict):
            source = {
                "root": Path(source).resolve().parent,
                "paths": [Path(source).resolve()]
            }

        for src in source['paths']:
            lg.debug(f"inserting {src}")
            src = Path(src).resolve()
            dst = Path(dst_dir).resolve().joinpath(src.relative_to(source['root']))

            if conv_mode == "all":
                dst = Path(dst).resolve().with_suffix(".mp3")
                tsk = tasks.ConvertTask(src=src, dst=dst, converter=conv)
            elif conv_mode == "include" and src.suffix in conv_include:
                dst = Path(dst).resolve().with_suffix(".mp3")
                tsk = tasks.ConvertTask(src=src, dst=dst, converter=conv)
            elif conv_mode == "exclude" and src.suffix not in conv_exclude:
                dst = Path(dst).resolve().with_suffix(".mp3")
                tsk = tasks.ConvertTask(src=src, dst=dst, converter=conv)
            else:
                tsk = tasks.SimpleConvertTask(src=src, dst=dst, converter=conv_copy)

            rsp = client.send_command(Command.QUEUE_PUT, payload=tsk)

            if not is_queue_running and not args.enqueue_only:
                client.send_command(Command.QUEUE_START)
                is_queue_running = True

            if args.verbose:
                print(format_response(rsp))

            print(format_response_summary(client.stats), end="\r")

    if not args.enqueue_only:
        client.send_command(Command.QUEUE_START)
    return {"status": Status.OK, "msg": "enqueue finished"}


def daemon_stop(args, client) -> Dict:
    if not client.is_daemon_active():
        return {"status": Status.SKIP, "msg": "daemon is not running", "payload": {"value": True}, "payload_class": PayloadClass.VALUE}

    return client.send_command(Command.DAEMON_STOP)


def daemon_destroy(args, client) -> Dict:
    client.send_command(Command.QUEUE_FLUSH_ALL)
    return client.send_command(Command.DAEMON_STOP)


def daemon_start(args, client) -> Dict:
    # launch daemon
    if client.is_daemon_active():
        return {"status": Status.SKIP, "msg": "daemon is already running", "payload": {"value": True}, "payload_class": PayloadClass.VALUE}
    else:
        qop_dir = Path(__file__).resolve().parent
        subprocess.Popen(["nohup", "python3", qop_dir.joinpath("qopd.py"), "--queue", '<temp>'], close_fds=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        sleep(0.1)
        return daemon_is_active(args, client)


def daemon_restart(args, client) -> Dict:
    was_running = client.is_daemon_active()
    daemon_stop(args, client)
    sleep(0.1)
    was_stopped = not client.is_daemon_active()
    daemon_start(args, client)
    sleep(0.1)
    is_running = client.is_daemon_active()

    if is_running:
        if not was_running:
            return {"status": Status.OK, "msg": "daemon started", "payload": {"value": True}, "payload_class": PayloadClass.VALUE}
        elif not was_stopped:
            return {"status": Status.FAIL, "msg": "daemon is still running but was not restarted", "payload": {"value": True}, "payload_class":PayloadClass.VALUE}
        else:
            return {"status": Status.OK, "msg": "daemon restarted", "payload": {"value": True}, "payload_class": PayloadClass.VALUE}
    else:
        if was_running:
            return {"status": Status.FAIL, "msg": "could not restart daemon (daemon is offline)", "payload": {"value": False}, "payload_class": PayloadClass.VALUE}
        else:
            return {"status": Status.FAIL, "msg": "could not start daemon (daemon is offline)", "payload": {"value": False}, "payload_class": PayloadClass.VALUE}


def daemon_is_active(args, client):
    if client.is_daemon_active():
        return {"status": Status.OK, "msg": "daemon is running", "payload": {"value": True}, "payload_class": PayloadClass.VALUE}
    else:
        return {"status": Status.OK, "msg": "no daemon found", "payload":  {"value": False}, "payload_class": PayloadClass.VALUE}


def handle_simple_command(args, client):
    return client.send_command(args.command)


def queue_progress(args, client):
    info = client.get_queue_progress()

    if info.total == 0:
        return {"status": Status.OK, "msg": "queue is empty"}

    CPL = "\033[A"  # ANSI move cursor previous line
    EL  = "\033[K"  # ANSI erase line

    max_lines = client.active_processes + 1

    print("\n" * max_lines)

    with tqdm(total=info.total, initial=info.total - info.pending) as pbar:
        while True:
            sleep(0.1)

            try:
                info = client.get_queue_progress()
                is_daemon_active = client.is_daemon_active()
                active_processes = client.active_processes
                transfers = client.active_tasks['payload']
                transfers_str = "\n".join([tasks.Task.from_dict(x['task']).color_repr() for x in transfers])
            except:
                continue

            lbs = '\n' * (max_lines - len(transfers) + (len(transfers) > 0))
            clr = (CPL + EL) * max_lines

            pbar.write(clr, end="")
            pbar.write(transfers_str + lbs, end="")

            pbar.update(info.total - info.pending - pbar.n)
            pbar.set_description(f"{active_processes} processes")

            if not is_daemon_active or active_processes < 1:
                pbar.write(clr)
                break

    if info.total == info.ok + info.skip:
        return {"status": Status.OK, "msg": "all files transferred successfully"}
    else:
        return {"status": Status.FAIL, "msg": "could not transfer all files"}


# args
parser = argparse.ArgumentParser()
parser.set_defaults(fun=handle_missing_args, start_daemon=False, parser=parser)
subparsers = parser.add_subparsers()

# copy
parser_copy = subparsers.add_parser("copy", help="copy a file")
parser_copy.set_defaults(fun=handle_copy_move, mode="copy", start_daemon=True)

# move
parser_move = subparsers.add_parser("move", help="move a file")
parser_move.set_defaults(fun=handle_copy_move, mode="move", start_daemon=True)

# convert
parser_convert = subparsers.add_parser("convert", help="convert an audio file")
parser_convert.set_defaults(fun=handle_convert, start_daemon=True, mode="convert")
parser_convert.add_argument("-a", "--remove-art", action="store_true", help="remove album art from file tags", default=False)
g = parser_convert.add_mutually_exclusive_group()
g.add_argument("-c", "--convert-only", nargs="+", type=str, help="extensions of files to convert")
g.add_argument("-C", "--convert-not", nargs="+", type=str, help="extensions of files not to convert")
g.add_argument("-K", "--convert-none", action="store_true", help="copy all files without transcoding (useful in combination with --remove-art)", default=False)


# shared arguments
for p in [parser_copy, parser_convert, parser_move]:
    p.add_argument("paths", type=str, nargs="+", help="SOURCE...DIR. An abritrary number of paths to copy and a single destination directory")
    p.add_argument("-r", "--recursive", action="store_true", help="recurse")
    p.add_argument("-e", "--enqueue-only", action="store_true", help="enqueue only. do not start processing queue.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("-i", "--include", nargs="+", type=str, help="keep only files with these extensions")
    g.add_argument("-I", "--exclude", nargs="+", type=str, help="keep only files that do not have these extensions")

# re
parser_re = subparsers.add_parser("re", help="repeat the last copy/convert/move operation on different source paths")
parser_re.set_defaults(fun=handle_re, start_daemon=True)
parser_re.add_argument("sources", type=str, nargs="+", help="source files to be copied/moved/converted")
parser_re.add_argument("-d", "--destination", type=str, nargs=1, help="an optional destination (otherwise the last destination will be used)")

# echo
parser_echo = subparsers.add_parser("echo", help="echo text (for testing the server)")
parser_echo.add_argument("msg", type=str, help="path", nargs="+", default="echo")
parser_echo.set_defaults(fun=handle_echo, start_daemon=True)

parser_progress = subparsers.add_parser("progress", help="show progress bar")
parser_progress.set_defaults(fun=queue_progress, start_daemon=True)


# queue management
parser_queue = subparsers.add_parser("queue", help="manage the file processing queue (start, stop, ...)")
parser_queue.set_defaults(start_daemon=True, fun=handle_missing_args, parser=parser_queue)
parser_queue_sub = parser_queue.add_subparsers()
parser_queue_sub.add_parser("start", help="start processing the queue").set_defaults(fun=handle_simple_command, command=Command.QUEUE_START)
parser_queue_sub.add_parser("stop",  help="stop processing the queue").set_defaults(fun=handle_simple_command, command=Command.QUEUE_STOP)
parser_queue_sub.add_parser("flush", help="completely reset the queue (including finished, failed and skipped tasks)").set_defaults(fun=handle_simple_command, command=Command.QUEUE_FLUSH_ALL)
parser_queue_sub.add_parser("flush-pending", help="remove all pending tasks from the queue").set_defaults(fun=handle_simple_command, command=Command.QUEUE_FLUSH_PENDING)
parser_queue_sub.add_parser("progress", help="show interactive progress bar").set_defaults(fun=queue_progress)
parser_queue_sub.add_parser("active", help="show number of active queues (usually just one)").set_defaults(fun=handle_simple_command, command=Command.QUEUE_ACTIVE_PROCESSES)
parser_queue_sub.add_parser("is-active", help="show number of active queues (usually just one)").set_defaults(fun=handle_simple_command, command=Command.QUEUE_IS_ACTIVE)
parser_queue_sub.add_parser("show", help="show the queue").set_defaults(fun=handle_simple_command, command=Command.QUEUE_SHOW)


# daemon management
parser_daemon = subparsers.add_parser("daemon", help="manage the daemon process")
parser_daemon.set_defaults(start_daemon=False, fun = handle_missing_args, parser=parser_daemon)
parser_daemon_sub = parser_daemon.add_subparsers()
parser_daemon_sub.add_parser("restart", help="restart the daemon").set_defaults(fun=daemon_restart)
parser_daemon_sub.add_parser("stop", help="stop the daemon").set_defaults(fun=daemon_stop)
parser_daemon_sub.add_parser("is-active", help="check if daemon is alive").set_defaults(fun=daemon_is_active)
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
if args.start_daemon:
    daemon_start(args, client)
    for i in range(100):
        sleep(0.1)
        if client.is_daemon_active():
            break

res = args.fun(args, client)
print(format_response(res))

if not client.is_daemon_active():
    try:
        shutil.rmtree(tasks.CONVERT_CACHE_DIR)
    except:
        pass
