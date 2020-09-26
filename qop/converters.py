import shutil
import json
from pathlib import Path
from typing import Union, Optional, Dict

import pydub
import mutagen
from mutagen import id3

from qop.enums import ConverterType

Pathish = Union[Path, str]

class Converter:
    def to_dict(self) -> Dict:
        return {}

    @staticmethod
    def from_dict(x: Dict) -> "Converter":
        t = ConverterType(x['type'])
        if t == ConverterType.COPY:
            return CopyConverter(remove_art=x['remove_art'])
        if t == ConverterType.MP3:
            return Mp3Converter(remove_art=x['remove_art'])
        elif t == ConverterType.OGG:
            return OggConverter(bitrate=x['bitrate'], remove_art=x['remove_art'])
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

    def do_remove_art(self, file: Path):
        f = mutagen.File(file)

        try:
            f.delall("APIC")
        except:
            try:
                f.clear_pictures()
            except:
                pass

        f.save()


class CopyConverter(Converter):
    """Dummy converter that only copies a file without any processing"""

    def __init__(self, remove_art: bool = False) -> None:
        self.remove_art = remove_art

    def run(self, src: Pathish, dst: Pathish):
        src = Path(src).resolve()
        dst = Path(dst).resolve()
        if not dst.parent.exists():
            dst.parent.mkdir(parents=True)

        shutil.copy(src, dst)
        if self.remove_art:
            self.do_remove_art(dst)

    def to_dict(self) -> Dict:
        return {"type": ConverterType.COPY, "remove_art": self.remove_art}


class OggConverter(CopyConverter):
    """Convert audio files to ogg vorbis"""

    def __init__(self, bitrate: str = "192k", remove_art: bool = False) -> None:
        super().__init__(remove_art=remove_art)
        self.bitrate = bitrate
        self.remove_art = remove_art
        self.ext = "ogg"

    def run(self, src: Union[Path, str], dst: Union[Path, str]) -> None:
        src = Path(src).resolve()
        dst = Path(dst).resolve()
        if not dst.parent.exists():
            dst.parent.mkdir(parents=True)

        x = pydub.AudioSegment.from_file(src)
        x.export(dst, format="ogg")
        if self.remove_art:
            self.do_remove_art(dst)

    def to_dict(self) -> Dict:
        return {"type": ConverterType.OGG, "bitrate": self.bitrate, "remove_art": self.remove_art}


class Mp3Converter(CopyConverter):
    """Convert audio files to mp3"""

    def __init__(self, remove_art: bool = False) -> None:
        super().__init__(remove_art=remove_art)
        self.ext = "mp3"

    def run(self, src: Union[Path, str], dst: Union[Path, str]) -> None:
        src = Path(src).resolve()
        dst = Path(dst).resolve()
        if not dst.parent.exists():
            dst.parent.mkdir(parents=True)

        x = pydub.AudioSegment.from_file(src)
        x.export(dst, format="mp3", parameters=["-q:a", "0"])
        if self.remove_art:
            self.do_remove_art(dst)

    def to_dict(self) -> Dict:
        return {"type": ConverterType.MP3, "remove_art": self.remove_art}

    def do_remove_art(self, file: Path):
        f = id3.ID3(file)
        f.delall("APIC")
        f.save()


Converter_ = Union[Converter, OggConverter, Mp3Converter]
