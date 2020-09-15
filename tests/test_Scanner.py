from qop import Scanner, converters, utils
import pytest


@pytest.mark.skip
def test_Scanner_scanns(tmp_path):
    """Scanner converts a directory to an OperationQueue"""

    path = utils.get_project_root("tests", "test_Scanner")
    sc = Scanner.Scanner()

    sc.run(path)
    for el in sc.queue:
        print(el)


