from pathlib import Path
import socket

def get_project_root(*args) -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent.joinpath(*args)


def is_server_alive(ip: str, port: int):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((ip, port))
            return True
    except:
        return False