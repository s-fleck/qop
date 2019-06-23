import shutil
import pydub
from pathlib import Path
from typing import Union, Optional


class Converter:
    def serialize(self):
        {}


class CopyConverter(Converter):
    """Dummy converter that only copies a file without any processing"""
    def __init__(self) -> None:
        pass

    def run(self, src: Union[Path, str], dst: Union[Path, str]):
        shutil.copy(src, dst)


class OggConverter(Converter):
    """Convert audio files to ogg vorbis"""
    def __init__(self, bitrate: str = "192k") -> None:
        self.bitrate = bitrate
        pass

    def run(self, src: Union[Path, str], dst: Union[Path, str]) -> None:
        src = Path(src).resolve()
        dst = Path(dst).resolve()
        x = pydub.AudioSegment.from_file(src)
        x.export(dst, format="ogg", bitrate=self.bitrate)

    def serialize(self):
        {"bitrate": self.bitrate}
