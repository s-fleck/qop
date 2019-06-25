import socket
import subprocess
import sys

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    s.connect((socket.gethostname(), 9393))
    print("connected to existing server")
except:
    print("Launching server")
    subprocess.Popen((sys.executable, 'qcpd.py'))
    print('connecting')
    s.connect((socket.gethostname(), 9393))
    print("connected")

s.send(bytes("hoelk2", "utf-8"))

msg = s.recv(1024)
print(msg.decode("utf-8"))