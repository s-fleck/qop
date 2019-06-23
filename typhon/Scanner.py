import shutil
import pydub
from pathlib import Path
from typing import Union, Optional, Generator
from typhon import Converter
import re

class Scanner:

    def __init__(self) -> None:
        self.whitelist = ("*.flac", "*.mp3", "*.wav", "*.ogg", "*.ape", "*.wv", "*.aac")
        self.blacklist = ()
        self.to_convert = ("*.flac", "*.wav", "*.ape", "*.wv")
        self.converter = Converter.OggConverter()
        self.queue = ()

        pat = re.compile("|".join(["(" + el + "$)" for el in self.whitelist]))
        print(pat)
        self.whitelist = pat

    def run(self, path: Path) -> None:
        files = path.rglob("|".join(self.whitelist))
        self.queue = files
