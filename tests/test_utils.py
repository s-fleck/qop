import pydub
from pydub import generators
from mediafile import MediaFile
from qop import _utils


def test_transfer_tags_between_different_filetypes(tmp_path):

    src = tmp_path.joinpath("sine.flac")
    dst = tmp_path.joinpath("sine.mp3")
    sound = pydub.generators.Sine(10).to_audio_segment()

    sound.export(src, format="flac")
    sound.export(dst, format="mp3")

    f = MediaFile(src)
    f.artist = "foo"
    f.album = "bar"
    f.save()

    _utils.transfer_tags(src, dst)

    g = MediaFile(dst)
    assert g.artist == "foo"
    assert g.album == "bar"
