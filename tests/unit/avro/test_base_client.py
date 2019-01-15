import random
import struct

import mock
import pytest

from pycernan.avro.base_client import BaseClient
from pycernan.avro.exceptions import InvalidAckException, ConnectionResetException


class BinaryDummyClient(BaseClient):
    def publish_blob(self, avro_blob, **kwargs):
        pass

    def _connect(self, host, port):
        return mock.MagicMock()


@pytest.fixture
def dummy_client():
    return BinaryDummyClient()


@mock.patch('pycernan.avro.base_client.BaseClient._recv_exact', autospec=True)
def test_wait_for_ack_raises_InvalidAckException_for_wrong_id_success_otherwise(m_recv, dummy_client):
    mock_sock = mock.MagicMock()
    expected_id = random.randrange(2 ** 64)
    m_recv.return_value = struct.pack('>Q', expected_id)

    with pytest.raises(InvalidAckException):
        dummy_client._wait_for_ack(mock_sock, expected_id + 1)

    dummy_client._wait_for_ack(mock_sock, expected_id)


def test_send_exact_just_calls_sendall(dummy_client):
    mock_sock = mock.MagicMock()
    dummy_client._send_exact(mock_sock, b'some buffer')
    assert mock_sock.sendall.call_args_list == [mock.call(b'some buffer')]


def test_recv_exact_can_exit_after_one_iteration(dummy_client):
    mock_sock = mock.MagicMock()
    expected_data = b'all the data'
    mock_sock.recv.return_value = expected_data
    assert dummy_client._recv_exact(mock_sock, len(expected_data)) == expected_data


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

    expected_recv_calls = [mock.call(x) for x in reversed(range(1, len(expected_data) + 1))]
    assert mock_sock.recv.call_args_list == expected_recv_calls


def test_recv_exact_with_zero_length_will_raise_ConnectionResetException(dummy_client):
    mock_sock = mock.MagicMock()
    mock_sock.recv.side_effect = [bytearray(0)]
    with pytest.raises(ConnectionResetException):
        dummy_client._recv_exact(mock_sock, 5)
