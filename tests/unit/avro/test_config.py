import os

import pycernan.avro.config


def test_host_default():
    avro_host = pycernan.avro.config.host()
    pycernan_host = pycernan.config.host()
    assert avro_host == pycernan_host


def test_host_override():
    expected_host = "foobar.baz"
    os.environ["PYCERNAN_AVRO_HOST"] = expected_host
    avro_host = pycernan.avro.config.host()
    assert avro_host == expected_host


def test_port_default():
    assert pycernan.avro.config.port() == 2002


def test_port_override():
    expected_port = 4567
    os.environ['PYCERNAN_AVRO_PORT'] = str(expected_port)
    assert pycernan.avro.config.port() == expected_port
