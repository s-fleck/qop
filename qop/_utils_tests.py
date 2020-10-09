import pydub
from pydub import generators

from pathlib import Path


def make_dummy_file(path) -> Path:
    path = Path(path)
    if not path.parent.exists():
        path.parent.mkdir(parents=True)
    f = open(path, "w+")
    f.write("foobar")
    return path


def make_dummy_flac(path) -> Path:
    path = Path(path)

    if not path.parent.exists():
        path.parent.mkdir(parents=True)
    sound = pydub.generators.Sine(440).to_audio_segment()
    sound.export(path, format="flac")

    return path