"""
Internal utility functions
"""

from qop import constants
from pathlib import Path
import socket
import shutil

from mediafile import MediaFile
from qop.constants import Pathish


def get_project_root(*args) -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent.joinpath(*args).resolve()


def is_daemon_active(ip: str, port: int):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((ip, port))
            return True
    except:
        return False


def transfer_tags(src: Pathish, dst: Pathish, remove_art: bool = False) -> None:
    src = Path(src).resolve()
    dst = Path(dst).resolve()
    f = MediaFile(src)
    g = MediaFile(dst)

    for field in f.fields():
        if remove_art and field in ("art", "images"):
            continue
        try:
            setattr(g, field, getattr(f, field))
        except:
            pass

    g.save()


def purge_convert_cache():
    try:
        shutil.rmtree(constants.CONVERT_CACHE_DIR)
    except:
        pass
