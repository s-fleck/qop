"""
Converters are used by :class:`~qop.tasks.ConvertTask` and :class:`~qop.tasks.SimpleConvertTask` to transcode audiofiles
"""


import shutil
import json
from pathlib import Path
from typing import Union, Dict, Tuple, List, Optional

import pydub
from mediafile import MediaFile

from qop.constants import ConverterType, Pathish
from qop import _utils


class Converter:
    """Abstract base class for Converters"""

    remove_art = False

    def to_dict(self) -> Dict:
        raise NotImplementedError


    @staticmethod
    def from_dict(x: Dict) -> "Converter":
        """
        Create a Converter object from python dict that contains the necessary keys
        """
        t = ConverterType(x['type'])
        if t == ConverterType.COPY:
            conv = CopyConverter()
        elif t == ConverterType.PYDUB:
            conv = PydubConverter()
        else:
            raise ImportError("Unknown 'ConverterType': {}")

        conv.__dict__.update(x)
        return conv


    @staticmethod
    def from_json(s: str) -> "Converter":
        """
        Deserialize a Converter from JSON
        """
        dd = json.loads(s=s)
        return Converter.from_dict(dd)

    def start(self, src: Union[Path, str], dst: Union[Path, str]):
        raise NotImplementedError()

    def __eq__(self, other) -> bool:
        return self.__dict__ == other.__dict__

    def __ne__(self, other) -> bool:
        return self.__dict__ != other.__dict__

    def _do_remove_art(self, file: Path):
        """Remove album art during conversion"""
        f = MediaFile(file)

        try:
            delattr(f, "art")
        except AttributeError:
            pass

        try:
            delattr(f, "images")
        except AttributeError:
            pass

        f.save()


class CopyConverter(Converter):
    """Converter that copies a file without transcoding (but may modify the files tags!)"""

    def __init__(self, remove_art: bool = False) -> None:
        self.remove_art = remove_art

    def start(self, src: Pathish, dst: Pathish):
        src = Path(src).resolve()
        dst = Path(dst).resolve()
        if not dst.parent.exists():
            dst.parent.mkdir(parents=True)

        shutil.copy(src, dst)
        if self.remove_art:
            self._do_remove_art(dst)

    def to_dict(self) -> Dict:
        return {"type": ConverterType.COPY, "remove_art": self.remove_art}


class PydubConverter(CopyConverter):
    """
    Convert audio files using pydub. See :meth:`pydub.AudioSegment.export`
    (`link <https://github.com/jiaaro/pydub/blob/master/API.markdown>`_) for more details on the meaning of
    the parameters. Defaults to mp3 via lame with V0 quality (best possible VBR quality).

    :param: remove_art Remove all album art (image) tags during conversion
    :param: parameters named arguments passed on to :func:`pydub.AudioSegment.export` (and from there to ffmpeg)
      when starting the conversion.
    """
    def __init__(
            self,
            remove_art: bool = False,
            format: str = "mp3",
            codec: Optional[str] = None,
            bitrate: Optional[str] = None,
            parameters: Union[List[str], Tuple[str], None] = ("-q:a", "0"),  # lame V0
            tags: Optional[str] = None,
            id3v2_version='4'
    ) -> None:
        super().__init__(remove_art=remove_art)
        self.format = format
        self.codec = codec
        self.bitrate = bitrate
        self.parameters = list(parameters)
        self.tags = tags
        self.id3v2_version = id3v2_version

    @property
    def ext(self) -> str:
        return self.format

    def start(self, src: Union[Path, str], dst: Union[Path, str]) -> None:
        src = Path(src).resolve()
        dst = Path(dst).resolve()
        if not dst.parent.exists():
            dst.parent.mkdir(parents=True)

        x = pydub.AudioSegment.from_file(src)
        x.export(
            dst,
            format=self.format,
            codec=self.codec,
            bitrate=self.bitrate,
            parameters=self.parameters,
            tags=self.tags,
            id3v2_version=self.id3v2_version
        )
        _utils.transfer_tags(src, dst, remove_art=self.remove_art)
        if self.remove_art:
            self._do_remove_art(dst)

    def to_dict(self) -> Dict:
        return {
            "type": ConverterType.PYDUB,
            "remove_art": self.remove_art,
            "format": self.format,
            "codec": self.codec,
            "bitrate": self.bitrate,
            "tags": self.tags,
            "id3v2_version": self.id3v2_version,
            'parameters': self.parameters
        }


Converter_ = Union[Converter, PydubConverter]
