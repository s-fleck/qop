import shutil


class Converter:
    pass


class CopyConverter:
    """Dummy converter that only copies a file without any processing"""

    def convert(self, src, dst):
        shutil.copy(src, dst)
