from typhon import Converter, utils
from pathlib import Path
import pytest


@pytest.mark.parametrize("src", utils.get_project_root("tests", "test_Converter").glob("*"))
def test_OggConverter(tmp_path, src):
    """OggConverter converts src to .ogg"""
    dst = Path(tmp_path.joinpath(src.stem + ".ogg"))

    co = Converter.OggConverter()

    assert src.exists()
    assert not dst.exists()
    co.run(src, dst)
    assert src.exists()
    assert dst.exists()
