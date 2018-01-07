import random
import struct

import mock
import pytest

import settings
from pycernan.avro.v1 import Client


def unpack_fmt(nbytes):
    return ">LLLQQ{}s".format(nbytes)

def random_id():
    return random.choice([None, random.randrange(2**64)])

def random_order_by():
    return random.choice([None, random.randrange(2**64)])

def test_params():
    return [
        (random_id(), random_order_by(), test_file) for test_file in settings.test_data]

@pytest.mark.parametrize("id, order_by, avro_file", test_params())
@mock.patch('pycernan.avro.v1.Client._connect', return_value=None)
@mock.patch('pycernan.avro.v1.Client._send', return_value=None)
def test_publish_blob(send_mock, connect_mock, id, order_by, avro_file):
    c = Client()

    with open(avro_file, 'rb') as file:
        file_contents = file.read()

    c.publish_blob(file_contents, id=id, order_by=order_by)

    send_calls = send_mock.mock_calls
    assert(len(send_calls) == 1)
    send_call = send_calls[0]
    (_, _, payload_raw) = send_call[1]

    payload = struct.unpack(
        unpack_fmt(len(file_contents)),
        payload_raw)

    assert((len(payload_raw) - 4) == payload[0]) # Length of the binary blob
    assert(payload[1] == 1) # Version number
    assert(payload[2] == (1 if id else 0)) # Control

    if id:
        assert(payload[3] == id)

    if order_by:
        assert(payload[4] == order_by)

    # Payload contents should match the avro we sent.
    assert(payload[5] == file_contents)
