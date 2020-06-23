import argparse
from qcp import daemon
from qcp import client
from qcp import tasks
import threading
import socket
from pathlib import Path
import logging

logging.basicConfig(filename="/home/hoelk/test.log", level="DEBUG")

tmp_path = Path("/tmp")
parser = argparse.ArgumentParser()
parser.add_argument("echo", help="echo the string you use here")
args = parser.parse_args()

req = daemon.Message(tasks.EchoTask(args.echo))

server = daemon.QcpDaemon(queue_path=tmp_path.joinpath("qcp.db"))

with server as dummy_server:
    thread = threading.Thread(target=dummy_server.listen)
    thread.daemon = True
    thread.start()

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
    client.connect(("127.0.0.1", 54993))
    client.sendall(req.encode())
    res = client.recv(1024)
    res = daemon.RawMessage(res).decode()

print(res)