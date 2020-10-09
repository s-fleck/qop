"""
Helper functions for the qop.py CLI application
"""


import pickle
import subprocess
import logging
from pathlib import Path
from typing import Dict, Union, Optional
from time import sleep
import json

import appdirs
from tqdm import tqdm
from colorama import init, Fore

from qop import tasks, scanners, converters
from qop.constants import Status, Command, PayloadClass

init()  # init terminal colors
lg = logging.getLogger(__name__)

# ANSI Escapes
CPL = "\033[A"  # ANSI move cursor previous line
EL = "\033[K"   # ANSI erase line


def handle_missing_args(args, client):
    args.parser.print_help()
    args.parser.exit()


def handle_echo(args, client) -> Dict:
    return client.send_command(Command.QUEUE_PUT, payload=tasks.EchoTask(msg=" ".join(args.msg)))


def handle_re(args, client) -> Dict:

    with open(Path(appdirs.user_cache_dir('qop')).joinpath('last_args.pickle'), 'rb') as f:
        last_args = pickle.load(f)

    if args.destination is not None:
        last_args.paths = args.paths + args.destination
    else:
        last_args.paths = args.paths + [last_args.paths[-1]]

    # global args that should not be overriden
    last_args.verbose = args.verbose
    last_args.log_file = args.log_file
    last_args.log_level = args.log_level

    return handle_copy_convert_move(last_args, client)


def handle_copy_convert_move(args, client) -> Dict:
    sources = args.paths[:-1]
    dst_dir = Path(args.paths[-1]).resolve()
    is_queue_active = client.is_queue_active()

    assert isinstance(dst_dir, Path)
    assert len(sources) > 0
    assert sources != dst_dir

    # for use by `qop re`
    args_cache = Path(appdirs.user_cache_dir('qop')).joinpath('last_args.pickle')
    if args_cache.exists():
        args_cache.unlink()
    with open(args_cache, 'wb') as f:
        args.parser = None
        pickle.dump(args, f, pickle.HIGHEST_PROTOCOL)

    # setup scanner
    if args.include is not None:
        scanner = scanners.IncludeScanner(args.include)
    elif args.exclude is not None:
        scanner = scanners.ExcludeScanner(args.exclude)
    elif args.mode == "convert":
        scanner = scanners.Scanner()
    else:
        scanner = scanners.PassScanner()

    # setup converter (if necessary)
    if args.mode == "convert":
        conv = converters.PydubConverter(remove_art=args.remove_art, parameters=args.parameters)  # TODO
        conv_copy = converters.CopyConverter(remove_art=args.remove_art)

        if args.convert_only is not None:
            conv_mode = "include"
            conv_exts = ["." + e for e in args.convert_only]
        elif args.convert_not is not None:
            conv_mode = "exclude"
            conv_exts = ["." + e for e in args.convert_not]
        elif args.convert_none:
            conv_mode = "none"
            conv_exts = None
        else:
            conv_mode = "all"
            conv_exts = None
    else:
        conv_mode = None
        conv_exts = None
        conv = None
        conv_copy = None

    for source in sources:
        root = Path(source).resolve().parent
        children = scanner.scan(source)

        for src in children:
            lg.debug(f"inserting {src}")
            src = Path(src).resolve()
            dst = Path(dst_dir).resolve().joinpath(src.relative_to(root))

            # setup convert task
            if args.mode == "convert":
                if conv_mode == "all":
                    dst = dst.with_suffix("." + conv.ext)
                    tsk = tasks.ConvertTask(src=src, dst=dst, converter=conv)
                elif conv_mode == "include" and src.suffix in conv_exts:
                    dst = dst.with_suffix("." + conv.ext)
                    tsk = tasks.ConvertTask(src=src, dst=dst, converter=conv)
                elif conv_mode == "exclude" and src.suffix not in conv_exts:
                    dst = dst.with_suffix("." + conv.ext)
                    tsk = tasks.ConvertTask(src=src, dst=dst, converter=conv)
                elif args.remove_art:
                    tsk = tasks.SimpleConvertTask(src=src, dst=dst, converter=conv_copy)
                else:
                    tsk = tasks.CopyTask(src=src, dst=dst)
            elif args.mode == "move":
                tsk = tasks.MoveTask(src=src, dst=dst)
            elif args.mode == "copy":
                tsk = tasks.CopyTask(src=src, dst=dst)
            else:
                raise ValueError

            rsp = client.send_command(Command.QUEUE_PUT, payload=tsk)

            if not is_queue_active and not args.enqueue_only:
                client.send_command(Command.QUEUE_START)
                is_queue_active = True

            if args.verbose:
                print(format_response(rsp))

            print(format_response_summary(client.stats), end="\r")

    if not args.enqueue_only:
        client.send_command(Command.QUEUE_START)
    return {"status": Status.OK, "msg": "enqueue finished"}


def handle_daemon_stop(args, client) -> Dict:
    if not client.is_daemon_active():
        return {"status": Status.SKIP, "msg": "daemon is not active", "payload": {"value": True}, "payload_class": PayloadClass.VALUE}

    return client.send_command(Command.DAEMON_STOP)


def handle_daemon_destroy(args, client) -> Dict:
    client.send_command(Command.QUEUE_FLUSH_ALL)
    return client.send_command(Command.DAEMON_STOP)


def handle_daemon_start(args, client) -> Dict:
    # launch daemon
    if client.is_daemon_active():
        return {"status": Status.SKIP, "msg": "daemon is already active", "payload": {"value": True}, "payload_class": PayloadClass.VALUE}
    else:
        qop_exc = Path(__file__).resolve().parents[1].joinpath("qopd.py")
        assert qop_exc.exists()
        subprocess.Popen(["nohup", "python3", qop_exc, "--queue", '<temp>'], close_fds=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_for_daemon(client, timeout=10)
        return handle_daemon_is_active(args, client)


def handle_daemon_restart(args, client) -> Dict:
    was_active = client.is_daemon_active()
    handle_daemon_stop(args, client)
    wait_for_daemon(client, status=0)
    was_stopped = not client.is_daemon_active()
    handle_daemon_start(args, client)
    wait_for_daemon(client)
    is_active = client.is_daemon_active()

    if is_active:
        if not was_active:
            return {"status": Status.OK, "msg": "daemon started", "payload": {"value": True}, "payload_class": PayloadClass.VALUE}
        elif not was_stopped:
            return {"status": Status.FAIL, "msg": "daemon is still active but was not restarted", "payload": {"value": True}, "payload_class":PayloadClass.VALUE}
        else:
            return {"status": Status.OK, "msg": "daemon restarted", "payload": {"value": True}, "payload_class": PayloadClass.VALUE}
    else:
        if was_active:
            return {"status": Status.FAIL, "msg": "could not restart daemon (daemon is offline)", "payload": {"value": False}, "payload_class": PayloadClass.VALUE}
        else:
            return {"status": Status.FAIL, "msg": "could not start daemon (daemon is offline)", "payload": {"value": False}, "payload_class": PayloadClass.VALUE}


def handle_daemon_is_active(args, client):
    if client.is_daemon_active():
        return {"status": Status.OK, "msg": "daemon is active", "payload": {"value": True}, "payload_class": PayloadClass.VALUE}
    else:
        return {"status": Status.OK, "msg": "no daemon found", "payload":  {"value": False}, "payload_class": PayloadClass.VALUE}


def handle_simple_command(args, client):
    return client.send_command(args.command)


def handle_queue_progress(args, client):
    facts = client.gather_facts()

    if facts['tasks.total'] == 0:
        return {"status": Status.OK, "msg": "queue is empty"}

    max_processes = facts["processes.max"]

    with tqdm(total=facts['tasks.total'], initial=facts['tasks.total'] - facts['tasks.pending']) as pbar:
        bars = [tqdm(bar_format="{desc}") for x in range(max_processes + 1)]
        while True:
            sleep(0.1)
            active_tasks = client.active_tasks

            for i in range(len(bars) - 1):
                # keep bar 0 empty so that we get a blank line between the real progress bar and the tasks
                if 0 < i <= len(active_tasks):
                    t = tasks.Task.from_dict(active_tasks[i - 1]['task']).color_repr()
                    bars[i].desc = t
                    bars[i].update()
                elif i > len(active_tasks):
                    bars[i].desc = EL
                    bars[i].update()
            try:
                facts = client.gather_facts()
                pbar.set_description(f"{facts['processes.active']} processes")
                pbar.update(facts['tasks.total'] - facts['tasks.pending'] - pbar.n)
            except:
                pass

            if not client.is_daemon_active() or facts['processes.active'] < 1:
                break

    if facts['tasks.total'] == facts['tasks.ok'] + facts['tasks.skip']:
        return {"status": Status.OK, "msg": "all files transferred successfully"}
    else:
        return {"status": Status.FAIL, "msg": "could not transfer all files"}


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
            res = res + '\n' + json.dumps(payload, indent=4)

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


def wait_for_daemon(client, timeout: int = 10, status: int = 1) -> None:
    """
    :param client: QopClient
    :param timeout: Maximum time to wait for the daemon to respond. defaults to 10 seconds.
    :param status: `1`: wait for the daemon to start, `0` wait for the daemon to stop
    :return: None
    """
    for i in range(timeout * 10):
        sleep(0.1)

        if status == 1:
            if i > 10:
                print(f"\033[KWaiting for daemon to start (timeout: {timeout - i // 10}s)", end="\r")
            if client.is_daemon_active():
                return None
        elif status == 0:
            if i > 10:
                print(f"\033[KWaiting for daemon to stop (timeout: {timeout - i // 10}s)", end="\r")
            if not client.is_daemon_active():
                return None
        else:
            raise ValueError("status must be `0` or `1`")

    raise TimeoutError("could not connect to daemon")
