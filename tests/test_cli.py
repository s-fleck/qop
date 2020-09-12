from qcp import converters, utils
from pathlib import Path
import pytest
import json
import subprocess

def test_simple_copy_operation(tmp_path, src):
    """qcp can copy a file"""

    daemon = subprocess.Popen(["python3", "../qcpd.py"])
    tf = tmp_path.joinpath("test.txt")
    cf = tmp_path.joinpath("copy.txt")

    f = open(tf, "w+")
    f.write("foobar")

    subprocess.Popen(["python3", "../qcp.py", "copy", tf, cf])

    assert cf.exists()
    daemon.kill()


