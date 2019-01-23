import random
import struct

import mock
import pytest

import settings

from pycernan.avro.base_client import _hash_u64
from pycernan.avro.v1 import Client


def unpack_fmt(nbytes):
    return ">LLLQQ{}s".format(nbytes)


def random_payload_id():
    return random.choice([None, random.randrange(2 ** 64)])


def random_shard_by():
    return random.choice([None, random.randrange(2 ** 64)])


def test_params():
    return [
        (random_payload_id(), random_shard_by(), test_file) for test_file in settings.test_data]


@pytest.mark.parametrize("payload_id, shard_by, avro_file", test_params())
@mock.patch('pycernan.avro.client.TCPConnectionPool.connection', return_value=mock.MagicMock(), autospec=True)
@mock.patch('pycernan.avro.v1.Client._wait_for_ack', return_value=None, autospec=True)
@mock.patch('pycernan.avro.v1.Client._send_exact', return_value=None, autospec=True)
def test_publish_blob(send_mock, ack_mock, _, payload_id, shard_by, avro_file):
    c = Client()

    with open(avro_file, 'rb') as file:
        file_contents = file.read()

    if payload_id:
        ack_mock.return_value = struct.pack(">Q", payload_id)

    sync = payload_id is not None
    c.publish_blob(file_contents, sync=sync, payload_id=payload_id, shard_by=shard_by)
    send_calls = send_mock.mock_calls
    assert (len(send_calls) == 1)
    send_call = send_calls[0]
    (_self, _sock, payload_raw) = send_call[1]

    payload = struct.unpack(
        unpack_fmt(len(file_contents)),
        payload_raw)

    assert ((len(payload_raw) - 4) == payload[0])  # Length of the binary blob
    assert (payload[1] == 1)  # Version number
    assert (payload[2] == (1 if sync else 0))  # Control

    if payload_id:
        assert (payload[3] == payload_id)

    if shard_by:
        assert (payload[4] == _hash_u64(shard_by))

    # Payload contents should match the avro we sent.
    assert (payload[5] == file_contents)

    if payload_id:
        assert (len(ack_mock.mock_calls) == 1)
