import io
import random
import string
import struct

import mock
import pytest

import settings
from pycernan.avro.base_client import _hash_u64
from pycernan.avro.v2 import Client

HEADER_FMT = ">LLLQQ"
METADATA_ENTRIES_FMT = ">B"
KEY_LEN_FMT = ">B"
VAL_LEN_FMT = ">H"


def read_unpack(reader, fmt):
    raw = reader.read(struct.calcsize(fmt))
    return struct.unpack(fmt, raw)


def random_string(length):
    return ''.join([random.choice(string.ascii_letters) for _ in range(length)])


def random_payload_id():
    return random.choice([None, random.randrange(2 ** 64)])


def random_shard_by():
    return random.choice([None, random.randrange(2 ** 64)])


def random_metadata():
    return random.choice([None, {random_string(random.randrange(2 ** 8)): random_string(random.randrange(2 ** 16))}])


def test_params():
    return [
        (random_payload_id(), random_shard_by(), test_file, random_metadata()) for test_file in settings.test_data]


@pytest.mark.parametrize("payload_id, shard_by, avro_file, metadata", test_params())
@mock.patch('pycernan.avro.client.TCPConnectionPool.connection', return_value=mock.MagicMock(), autospec=True)
@mock.patch('pycernan.avro.v2.Client._wait_for_ack', return_value=None, autospec=True)
@mock.patch('pycernan.avro.v2.Client._send_exact', return_value=None, autospec=True)
def test_publish_blob(send_mock, ack_mock, _, payload_id, shard_by, avro_file, metadata):
    c = Client()

    with open(avro_file, 'rb') as file:
        file_contents = file.read()

    if payload_id:
        ack_mock.return_value = struct.pack(">Q", payload_id)

    sync = payload_id is not None
    c.publish_blob(file_contents, sync=sync, payload_id=payload_id, shard_by=shard_by, metadata=metadata)
    send_calls = send_mock.mock_calls
    assert (len(send_calls) == 1)
    send_call = send_calls[0]
    (_self, _sock, payload_raw) = send_call[1]

    payload_reader = io.BytesIO(payload_raw)

    header = read_unpack(payload_reader, HEADER_FMT)

    assert ((len(payload_raw) - 4) == header[0])  # Length of the binary blob
    assert (header[1] == 2)  # Version number
    assert (header[2] == (1 if sync else 0))  # Control

    if payload_id:
        assert (header[3] == payload_id)

    if shard_by:
        assert (header[4] == _hash_u64(shard_by))

    metadata_entries = read_unpack(payload_reader, METADATA_ENTRIES_FMT)[0]

    expected_metadata_entries = len(metadata) if metadata else 0
    assert metadata_entries == expected_metadata_entries

    decoded_metadata = {}
    for i in range(metadata_entries):
        key_len = read_unpack(payload_reader, KEY_LEN_FMT)[0]
        key = payload_reader.read(key_len)
        val_len = read_unpack(payload_reader, VAL_LEN_FMT)[0]
        val = payload_reader.read(val_len)
        decoded_metadata[key] = val

    payload_contents = payload_reader.read()

    # Payload contents should match the avro we sent.
    assert (payload_contents == file_contents)

    if payload_id:
        assert (len(ack_mock.mock_calls) == 1)
