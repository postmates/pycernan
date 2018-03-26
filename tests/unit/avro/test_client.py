import mock
import pytest

import settings
from pycernan.avro import BaseDummyClient, DummyClient
from pycernan.avro.exceptions import SchemaParseException, DatumTypeException, EmptyBatchException


USER_SCHEMA = {
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number",  "type": ["int", "null"]},
        {"name": "favorite_color", "type": ["string", "null"]}
    ]
}


class FakeSocket(object):
    def __init__(self):
        self.call_args_list = []

    def settimeout(self, *args, **kwargs):
        self.call_args_list.append(('settimeout', mock.call(*args, **kwargs)))

    def close(self):
        self.call_args_list.append(('close', mock.call()))

    def _reset_mock(self):
        self.call_args_list = []


@pytest.mark.parametrize("avro_file", settings.test_data)
def test_publish_file(avro_file):
    c = DummyClient()
    c.publish_file(avro_file)


@mock.patch('pycernan.avro.client.serialize', autospec=True)
@pytest.mark.parametrize('ephemeral', [True, False])
def test_publish(m_serialize, ephemeral):
    expected_serialize_result = mock.MagicMock()
    m_serialize.return_value = expected_serialize_result

    user = {
        'name': 'Foo Bar Matic',
        'favorite_number': 24,
        'favorite_color': 'Nonyabusiness',
    }

    c = DummyClient()
    with mock.patch.object(c, 'publish_blob', autospec=True) as m_publish_blob:
        c.publish(USER_SCHEMA, [user], ephemeral_storage=ephemeral, kwarg1='one', kwarg2='two')
    assert m_serialize.call_args_list == [
        mock.call(USER_SCHEMA, [user], ephemeral)
    ]
    assert m_publish_blob.call_args_list == [
        mock.call(expected_serialize_result, kwarg1='one', kwarg2='two')
    ]


def test_publish_bad_schema():
    schema = "Not a dict"
    user = {}

    c = DummyClient()
    with pytest.raises(SchemaParseException):
        c.publish(schema, [user])


def test_publish_bad_datum_empty():
    user = {}

    c = DummyClient()
    with pytest.raises(DatumTypeException):
        c.publish(USER_SCHEMA, [user])


def test_publish_empty_batch():
    c = DummyClient()
    with pytest.raises(EmptyBatchException):
        c.publish(USER_SCHEMA, [])


@mock.patch('pycernan.avro.client.socket.create_connection', autospec=True)
def test_creating_a_client_connects_a_socket(m_connect):
    expected_sock = FakeSocket()
    m_connect.return_value = expected_sock

    client = BaseDummyClient(
        host='some fake host',
        port=31337,
        connect_timeout=666,
        publish_timeout=999)
    assert client.sock == expected_sock
    assert expected_sock.call_args_list == [('settimeout', mock.call(999))]
    assert m_connect.call_args_list == [mock.call(('some fake host', 31337), timeout=666)]


@mock.patch('pycernan.avro.client.socket.create_connection', autospec=True)
def test_closing_the_client_closes_the_socket_and_clears_it(m_connect):
    expected_sock = FakeSocket()
    m_connect.return_value = expected_sock

    client = BaseDummyClient(
        host='some fake host',
        port=31337,
        connect_timeout=666,
        publish_timeout=999)

    assert client.sock is not None
    expected_sock._reset_mock()
    client.close()

    assert expected_sock.call_args_list == [('close', mock.call())]
    assert client.sock is None


@mock.patch('pycernan.avro.client.socket.create_connection', autospec=True)
def test_closing_the_client_with_no_socket_does_not_crash(m_connect):
    expected_sock = FakeSocket()
    m_connect.return_value = expected_sock

    client = BaseDummyClient(
        host='some fake host',
        port=31337,
        connect_timeout=666,
        publish_timeout=999)
    client.close()

    assert client.sock is None
    client.close()
