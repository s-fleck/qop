import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # ADDRESS_FAMILY: INTERNET (ip4), tcp
s.bind((socket.gethostname(), 9393))
s.listen(10)


while True:
    conn, address = s.accept()
    print(f'Connection from {address} has been established!')
    cmd = conn.recv(1024)

    conn.send(f'Welcome {cmd} {address}', 'utf-8')
    conn.close()

