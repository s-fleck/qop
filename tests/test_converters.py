from qcp import converters, utils
from pathlib import Path
import pytest
import json


@pytest.mark.parametrize("src", utils.get_project_root("tests", "test_Converter").glob("*"))
def test_OggConverter(tmp_path, src):
    """OggConverter converts src to .ogg"""
    dst = Path(tmp_path.joinpath(src.stem + ".ogg"))

    co = converters.OggConverter()

    assert src.exists()
    assert not dst.exists()
    co.run(src, dst)
    assert src.exists()
    assert dst.exists()


def test_serialize_OggConverter_to_json():
    """OggConverter can be serialized and unserialized"""
    co1 = converters.OggConverter(bitrate="123k")
    cos = co1.to_dict()
    co2 = converters.from_json(json.dumps(cos))

    assert co1 == co2
    assert co2.bitrate == "123k"