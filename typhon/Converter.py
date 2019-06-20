import shutil


class Converter:
    pass


class CopyConverter:
    """Dummy converter that only copies a file without any processing"""
    def __init__(self) -> None:
        pass

    def run(self, src, dst):
        shutil.copy(src, dst)
