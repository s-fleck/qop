from pathlib import Path
import re
import logging
import itertools

logging.basicConfig(level="DEBUG")

class Scanner:

    def __init__(self) -> None:
        self.whitelist = ("flac", "mp3", "wav", "ogg", "ape", "wv", "aac")
        self.blacklist = ()

    def run(self, paths):
        paths = [Path(el).resolve() for el in paths]

        res = []

        logging.getLogger("qcp.scanners").debug(f"collecting files with extensions {','.join(self.whitelist)}")
        wl = ["." + e for e in self.whitelist]

        for path in paths:
            r = {"root": path.resolve().parent, "paths": set()}
            if not path.is_dir():
                if path.suffix in wl:
                    r['paths'].add(path)
            else:
                for p in path.rglob("*"):
                    if p.suffix in wl:
                        r['paths'].add(p.resolve())

            res.append(r)

        return res
