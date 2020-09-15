from qop import converters, utils
from pathlib import Path
import pytest
import json
import subprocess

@pytest.mark.parametrize("src", utils.get_project_root("tests", "test_Converter").glob("*"))
def test_simple_copy_operation(tmp_path, src):
    subprocess.Popen(["python", "test.py"])


