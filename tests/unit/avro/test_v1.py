import random
import struct

import mock
import pytest

import settings
from pycernan.avro.v1 import Client, _hash_u64
from pycernan.avro.exceptions import InvalidAckException, ConnectionResetException


class V1DummyClient(Client):
    def _connect(self, host, port):
        return mock.MagicMock()


@pytest.fixture
def dummy_client():
    return V1DummyClient()


def unpack_fmt(nbytes):
    return ">LLLQQ{}s".format(nbytes)


def random_id():
    return random.choice([None, random.randrange(2**64)])


def random_shard_by():
    return random.choice([None, random.randrange(2**64)])


def test_params():
    return [
        (random_id(), random_shard_by(), test_file) for test_file in settings.test_data]


@pytest.mark.parametrize("id, shard_by, avro_file", test_params())
@mock.patch('pycernan.avro.client.TCPConnectionPool.connection', return_value=mock.MagicMock(), autospec=True)
@mock.patch('pycernan.avro.v1.Client._wait_for_ack', return_value=None, autospec=True)
@mock.patch('pycernan.avro.v1.Client._send_exact', return_value=None, autospec=True)
def test_publish_blob(send_mock, ack_mock, connect_mock, id, shard_by, avro_file):
    c = Client()

    with open(avro_file, 'rb') as file:
        file_contents = file.read()

    if id:
        ack_mock.return_value = struct.pack(">Q", id)

    sync = id is not None
    c.publish_blob(file_contents, sync=sync, id=id, shard_by=shard_by)
    send_calls = send_mock.mock_calls
    assert(len(send_calls) == 1)
    send_call = send_calls[0]
    (_self, _sock, payload_raw) = send_call[1]

    payload = struct.unpack(
        unpack_fmt(len(file_contents)),
        payload_raw)

    assert((len(payload_raw) - 4) == payload[0])  # Length of the binary blob
    assert(payload[1] == 1)  # Version number
    assert(payload[2] == (1 if sync else 0))  # Control

    if id:
        assert(payload[3] == id)

    if shard_by:
        assert(payload[4] == _hash_u64(shard_by))

    # Payload contents should match the avro we sent.
    assert(payload[5] == file_contents)

    if id:
        assert(len(ack_mock.mock_calls) == 1)


@mock.patch('pycernan.avro.v1.Client._recv_exact', autospec=True)
def test_wait_for_ack_raises_InvalidAckException_for_wrong_id_success_otherwise(m_recv, dummy_client):
    mock_sock = mock.MagicMock()
    expected_id = random.randrange(2**64)
    m_recv.return_value = struct.pack('>Q', expected_id)

    with pytest.raises(InvalidAckException):
        dummy_client._wait_for_ack(mock_sock, expected_id+1)

    dummy_client._wait_for_ack(mock_sock, expected_id)


def test_send_exact_just_calls_sendall(dummy_client):
    mock_sock = mock.MagicMock()
    dummy_client._send_exact(mock_sock, b'some buffer')
    assert mock_sock.sendall.call_args_list == [mock.call(b'some buffer')]


def test_recv_exact_can_exit_after_one_iteration(dummy_client):
    mock_sock = mock.MagicMock()
    expected_data = b'all the data'
    mock_sock.recv.return_value = expected_data
    dummy_client._recv_exact(mock_sock, len(expected_data)) == expected_data


def test_recv_exact_will_loop_if_necessary(dummy_client):
    import sys
    mock_sock = mock.MagicMock()
    expected_data = b'all the data'

    if sys.version_info >= (3, 0):
        def make_side_effect():
            return [bytearray(chr(b), encoding='utf8') for b in expected_data]
    else:
        def make_side_effect():
            return [bytearray(b) for b in expected_data]

    mock_sock.recv.side_effect = make_side_effect()
    assert dummy_client._recv_exact(mock_sock, len(expected_data)) == expected_data

    expected_recv_calls = [mock.call(x) for x in reversed(range(1, len(expected_data)+1))]
    assert mock_sock.recv.call_args_list == expected_recv_calls


def test_recv_exact_with_zero_length_will_raise_ConnectionResetException(dummy_client):
    mock_sock = mock.MagicMock()
    mock_sock.recv.side_effect = [bytearray(0)]
    with pytest.raises(ConnectionResetException):
        dummy_client._recv_exact(mock_sock, 5)
