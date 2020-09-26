from pathlib import Path
import logging


class Scanner:
    def __init__(self) -> None:
        pass

    def run(self, paths):
        paths = [Path(el).resolve() for el in paths]
        res = []
        logging.getLogger("qop.scanners").debug(f"collecting files")

        for path in paths:
            r = {"root": path.resolve().parent, "paths": set()}
            if not path.is_dir():
                r['paths'].add(path)
            else:
                for p in path.rglob("*"):
                    r['paths'].add(p.resolve())

            res.append(r)

        return res


class ScannerBlacklist:
    def __init__(self, extensions: list) -> None:
        self.extensions = extensions

    def run(self, paths):
        paths = [Path(el).resolve() for el in paths]
        res = []
        logging.getLogger("qop.scanners").debug(f"collecting files without extensions {','.join(self.extensions)}")
        exts = ["." + e for e in self.extensions]

        for path in paths:
            r = {"root": path.resolve().parent, "paths": set()}
            if not path.is_dir():
                if path.suffix not in exts:
                    r['paths'].add(path)
            else:
                for p in path.rglob("*"):
                    if p.suffix not in exts:
                        r['paths'].add(p.resolve())

            res.append(r)

        return res


class ScannerWhitelist:
    def __init__(self, extensions: list) -> None:
        self.extensions = extensions

    def run(self, paths):
        paths = [Path(el).resolve() for el in paths]
        res = []
        logging.getLogger("qop.scanners").debug(f"collecting files with extensions {','.join(self.extensions)}")
        exts = ["." + e for e in self.extensions]

        for path in paths:
            r = {"root": path.parent, "paths": set()}
            if not path.is_dir():
                if path.suffix in exts:
                    r['paths'].add(path)
            else:
                for p in path.rglob("*"):
                    if p.suffix in exts:
                        r['paths'].add(p.resolve())

            res.append(r)

        return res
