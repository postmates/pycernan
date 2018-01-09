import random
import struct

import mock
import pytest

import settings
from pycernan.avro.v1 import Client
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


def random_order_by():
    return random.choice([None, random.randrange(2**64)])


def test_params():
    return [
        (random_id(), random_order_by(), test_file) for test_file in settings.test_data]


@pytest.mark.parametrize("id, order_by, avro_file", test_params())
@mock.patch('pycernan.avro.v1.Client._connect', return_value=None, autospec=True)
@mock.patch('pycernan.avro.v1.Client._wait_for_ack', return_value=None, autospec=True)
@mock.patch('pycernan.avro.v1.Client._send_exact', return_value=None, autospec=True)
def test_publish_blob(send_mock, ack_mock, connect_mock, id, order_by, avro_file):
    c = Client()

    with open(avro_file, 'rb') as file:
        file_contents = file.read()

    if id:
        ack_mock.return_value = struct.pack(">Q", id)

    c.publish_blob(file_contents, id=id, order_by=order_by)
    send_calls = send_mock.mock_calls
    assert(len(send_calls) == 1)
    send_call = send_calls[0]
    (_self, payload_raw) = send_call[1]

    payload = struct.unpack(
        unpack_fmt(len(file_contents)),
        payload_raw)

    assert((len(payload_raw) - 4) == payload[0])  # Length of the binary blob
    assert(payload[1] == 1)  # Version number
    assert(payload[2] == (1 if id else 0))  # Control

    if id:
        assert(payload[3] == id)

    if order_by:
        assert(payload[4] == order_by)

    # Payload contents should match the avro we sent.
    assert(payload[5] == file_contents)

    if id:
        assert(len(ack_mock.mock_calls) == 1)


@mock.patch('pycernan.avro.v1.Client._recv_exact', autospec=True)
def test_wait_for_ack_raises_InvalidAckException_for_wrong_id_success_otherwise(m_recv, dummy_client):
    expected_id = random.randrange(2**64)
    m_recv.return_value = struct.pack('>Q', expected_id)

    with pytest.raises(InvalidAckException):
        dummy_client._wait_for_ack(expected_id+1)

    dummy_client._wait_for_ack(expected_id)


def test_send_exact_just_calls_sendall(dummy_client):
    dummy_client._send_exact(b'some buffer')
    assert dummy_client.sock.sendall.call_args_list == [mock.call(b'some buffer')]


def test_recv_exact_can_exit_after_one_iteration(dummy_client):
    expected_data = b'all the data'
    dummy_client.sock.recv.return_value = expected_data
    dummy_client._recv_exact(len(expected_data)) == expected_data


def test_recv_exact_will_loop_if_necessary(dummy_client):
    import sys
    expected_data = b'all the data'

    if sys.version_info >= (3, 0):
        def make_side_effect():
            return [bytearray(chr(b), encoding='utf8') for b in expected_data]
    else:
        def make_side_effect():
            return [bytearray(b) for b in expected_data]

    dummy_client.sock.recv.side_effect = make_side_effect()
    assert dummy_client._recv_exact(len(expected_data)) == expected_data

    expected_recv_calls = [mock.call(x) for x in reversed(range(1, len(expected_data)+1))]
    assert dummy_client.sock.recv.call_args_list == expected_recv_calls


def test_recv_exact_with_zero_length_will_raise_ConnectionResetException(dummy_client):
    dummy_client.sock.recv.side_effect = [bytearray(0)]
    with pytest.raises(ConnectionResetException):
        dummy_client._recv_exact(5)
