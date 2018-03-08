import os

import pycernan.config


def test_host_default():
    pycernan_host = pycernan.config.host()
    assert pycernan_host == "localhost"


def test_host_override():
    expected_host = "foobar.baz"
    os.environ["PYCERNAN_HOST"] = expected_host
    host = pycernan.config.host()
    assert host == expected_host
