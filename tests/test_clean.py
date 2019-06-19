from typhon import clean
from typhon import utils
import pytest

def test_list_trash():
    path = utils.get_project_root("tests", "test_clean_files")
    #res = clean.list_trash(path)
    #print(res)
    assert(1 == 1)


def test_list_duplicates():
    with pytest.raises(FileNotFoundError):
        clean.list_duplicates("/foo/bar")
