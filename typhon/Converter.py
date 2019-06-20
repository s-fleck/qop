import shutil
import pydub
from pathlib import Path

class Converter:
    pass


class CopyConverter:
    """Dummy converter that only copies a file without any processing"""
    def __init__(self) -> None:
        pass

    def run(self, src, dst):
        shutil.copy(src, dst)



class OggConverter:
    """Dummy converter that only copies a file without any processing"""
    def __init__(self, bitrate: str = "192") -> None:
        self.bitrate = bitrate
        pass

    def run(self, src: Path, dst: Path):
        x = pydub.AudioSegment.from_file(src)
        x.export(dst, format="ogg")