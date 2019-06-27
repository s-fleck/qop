import socket
import logging

lg = logging.getLogger(__name__)
logging.basicConfig(level="DEBUG")

HEADERSIZE = 10
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # ADDRESS_FAMILY: INTERNET (ip4), tcp
server.bind(("127.0.0.1", 9393))
server.listen(10)
lg.info("Started qcp daemon")

while True:
    client, address = server.accept()
    lg.info(f'Established connection with client {address}')
    cmd = client.recv(1024)

    reply = f'{len(cmd):<{HEADERSIZE}}{cmd}' # add fixed length header that includes length of the message to the msg
    lg.debug("Sending reply: " + reply)
    client.send(bytes(reply, 'utf-8'))



