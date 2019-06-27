import socket
import subprocess
import sys
import logging

lg = logging.getLogger(__name__)
logging.basicConfig(level="DEBUG")

HEADERSIZE = 10

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(("127.0.0.1", 9393))
print("connected to existing server")

cmd = "command blah blah"
cmd = f'{len(cmd):<{HEADERSIZE}}{cmd}'

print(cmd)
client.send(bytes(cmd, "utf-8"))

while True:
    msg = ""
    is_new_msg = True

    while True:
        msg_part = client.recv(16)

        if is_new_msg:
            is_new_msg = False
            lg.debug(f"received {msg_part}")
            len_msg = int(msg_part[:HEADERSIZE])
            lg.debug(f'new message length: {len_msg}')

        msg += msg_part.decode("utf-8")
        lg.debug(f'msg part {msg_part}')

        if len(msg) - HEADERSIZE == len_msg:
            lg.debug("full msg received")
            lg.debug(msg[HEADERSIZE:])
            msg = ""
            is_new_msg = True
            client.close()


# try:
#     s.connect((socket.gethostname(), 9393))
#     print("connected to existing server")
# except:
#     print("Launching server")
#     subprocess.Popen((sys.executable, 'qcpd.py'))
#     print('connecting')
#     s.connect((socket.gethostname(), 9393))
#     print("connected")
