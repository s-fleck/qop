from typhon import Converter, utils
import pytest


def test_OggConverter(tmp_path):
    """OggConverter converts src to .ogg"""
    src = utils.get_project_root("tests", "test_Converter", "16b.flac")
    dst = tmp_path.joinpath("16b.ogg")

    co = Converter.OggConverter()

    assert src.exists()
    assert not dst.exists()
    co.run(src, dst)
    assert src.exists()
    assert dst.exists()
