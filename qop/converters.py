import shutil
import pydub
from pathlib import Path
from typing import Union, Optional, Dict
import json


class Converter:
    def to_dict(self) -> Dict:
        return {}

    @staticmethod
    def from_dict(x: Dict) -> "Converter":
        if x['type'] == 1:
            return OggConverter(bitrate=x['bitrate'])
        else:
            raise ImportError("Unknown 'type': {}")

    @staticmethod
    def from_json(s: str) -> "Converter":
        """

        :rtype: object
        """
        dd = json.loads(s=s)
        return Converter.from_dict(dd)

    def run(self, src: Union[Path, str], dst: Union[Path, str]):
        raise NotImplementedError()

    def __eq__(self, other) -> bool:
        return self.__dict__ == other.__dict__

    def __ne__(self, other) -> bool:
        return self.__dict__ != other.__dict__


class CopyConverter(Converter):
    """Dummy converter that only copies a file without any processing"""

    def __init__(self) -> None:
        pass

    def run(self, src: Union[Path, str], dst: Union[Path, str]):
        shutil.copy(src, dst)

    def to_dict(self) -> Dict:
        return {"type": 0}


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

    def to_dict(self) -> Dict:
        return {"type": 1, "bitrate": self.bitrate}


Converter_ = Union[Converter, OggConverter]