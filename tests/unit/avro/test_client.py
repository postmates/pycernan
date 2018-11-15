import mock
import pytest

import settings

from queue import Queue, Empty

from pycernan.avro import BaseDummyClient, DummyClient
from pycernan.avro.client import TCPConnectionPool, _DefunctConnection, EmptyPoolException
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
    def __init__(self, set_failures=0, close_failures=0):
        self.set_failures = set_failures
        self.close_failures = close_failures
        self.call_args_list = []

    def settimeout(self, *args, **kwargs):
        self.call_args_list.append(('settimeout', mock.call(*args, **kwargs)))

        if self.set_failures > 0:
            self.set_failures -= 1
            raise Exception("settimeout Exception")

    def close(self):
        self.call_args_list.append(('close', mock.call()))

        if self.close_failures > 0:
            self.close_failures -= 1
            raise Exception("close Exception")

    def _reset_mock(self):
        self.call_args_list = []


class ForcedException(Exception):
    pass


class TestTCPConnectionPool():

    def test_pool_value_errors(self):
        with pytest.raises(ValueError):
            TCPConnectionPool('foobar', 80, -1, 1, 1)

    @mock.patch.object(TCPConnectionPool, '_create_connection', return_value=None, autospec=True)
    def test_pool_creation(self, connection_mock):
        size = 10
        pool = TCPConnectionPool('foobar', 80, size, 3, 1)
        assert pool.pool is not None
        for _ in range(size):
            assert pool.pool.get_nowait() == _DefunctConnection

        # Pool should be drained now.
        with pytest.raises(Empty):
            pool.pool.get_nowait()

    @mock.patch('pycernan.avro.client.socket.create_connection', autospec=True)
    def test_connection(self, connect_mock):
        mock_sock = FakeSocket()
        host, port = ('foobar', 9000)
        connect_timeout, recv_timeout = (1, 1)
        connect_mock.return_value = mock_sock
        pool = TCPConnectionPool(host, port, 1, connect_timeout, recv_timeout)

        with pool.connection() as sock:
            assert sock == mock_sock
            assert mock_sock.call_args_list == [('settimeout', mock.call(recv_timeout))]
            assert connect_mock.call_args_list == [mock.call((host, port), timeout=connect_timeout)]

        # After using a connection, it should be checked-in/cached for others to use
        connect_mock.return_value = FakeSocket()
        assert not pool.pool.empty()
        with pool.connection() as sock:
            assert sock == mock_sock

            # When we are at capacity, then callers will block
            # waiting for a connection to be returned.
            with pytest.raises(EmptyPoolException):
                with pool.connection(_block=False):
                    assert False  # Should not be reached

    @mock.patch('pycernan.avro.client.socket.create_connection', autospec=True)
    def test_connection_on_exception_closes_connections(self, connect_mock):
        pool = TCPConnectionPool('foobar', 8080, 1, 1, 1)

        # Connections are closed when application layer Exceptions occur.
        mock_sock = FakeSocket()
        connect_mock.return_value = mock_sock
        with pytest.raises(ForcedException):
            with pool.connection() as sock:
                raise ForcedException()
        assert mock_sock.call_args_list[-1] == ('close', mock.call())

        # Repeat the same experiment, this time simulating exception on close.
        # The semantics should match, namely, the exception is tolerated and
        # users get back the application level Exception.
        mock_sock.close_failures = 10
        mock_sock.call_args_list = []
        with pytest.raises(ForcedException):
            with pool.connection() as sock:
                raise ForcedException()
        assert mock_sock.call_args_list[-1] == ('close', mock.call())

    @mock.patch.object(TCPConnectionPool, '_create_connection', side_effect=ForcedException("Oops"), autospec=True)
    def test_create_connection_reraises(self, create_connection_mock):
        pool = TCPConnectionPool('foobar', 80, 10, 1, 1)
        with pytest.raises(ForcedException):
            with pool.connection():
                assert False  # Should not be reached.

            # No connections become cached.
            assert pool.pool.qsize() == 0
            assert pool.size == 0

    @mock.patch('pycernan.avro.client.socket.create_connection', autospec=True)
    def test_exceptions_result_in_defunct_connections(self, connect_mock):
        size = 10
        pool = TCPConnectionPool('foobar', 80, size, 1, 1)
        mock_sock = FakeSocket()
        connect_mock.return_value = mock_sock

        with pytest.raises(ForcedException):
            with pool.connection():
                raise ForcedException()

        assert pool.pool.qsize() == size
        assert pool.pool.get_nowait() == _DefunctConnection

    @mock.patch('pycernan.avro.client.socket.create_connection', autospec=True)
    def test_defunct_connections_are_regenerated(self, connect_mock):
        pool = TCPConnectionPool('foobar', 80, 10, 1, 1)
        mock_sock = FakeSocket()
        connect_mock.return_value = mock_sock

        pool.pool = Queue()
        pool.pool.put(_DefunctConnection)
        with pool.connection() as conn:
            assert conn is mock_sock

    @mock.patch('pycernan.avro.client.socket.create_connection', autospec=True)
    def test_regenerating_connections_tolerates_exceptions(self, connect_mock):
        num_failures = 20
        pool = TCPConnectionPool('foobar', 80, 10, 1, 1)
        mock_sock = FakeSocket(set_failures=num_failures)
        connect_mock.return_value = mock_sock

        # Simulate a prior failure
        pool.pool = Queue()
        pool.size = 1
        pool.pool._put(_DefunctConnection)

        # Try a number of failed operations.
        # Each time the defunct connection should be returned to the queue
        # for subsequent calls to reattempt.
        for i in range(num_failures):
            with pytest.raises(Exception):
                with pool.connection():
                    assert False  # Should not be reached

            assert pool.pool.qsize() == pool.size == 1
            assert pool.pool.get_nowait() is _DefunctConnection
            pool.pool._put(_DefunctConnection)

        # Failures are exhausted, we should be able to now
        # regenerate a valid context.
        with pool.connection():
            pass
        assert pool.pool.qsize() == 1
        assert pool.pool.get_nowait() is mock_sock

    @mock.patch('pycernan.avro.client.socket.create_connection', autospec=True)
    def test_closing_tolerates_close_exceptions(self, create_mock):
        expected_sock = FakeSocket(close_failures=10)
        create_mock.return_value = expected_sock
        pool = TCPConnectionPool('foobar', 80, 10, 1, 1)
        with pool.connection():
            pass

        pool.closeall()
        assert expected_sock.call_args_list[-1] == ('close', mock.call())


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
    schema = {}
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
    connect_timeout = 1
    publish_timeout = 50
    expected_sock = FakeSocket()
    m_connect.return_value = expected_sock

    client = BaseDummyClient(
        host='some fake host',
        port=31337,
        connect_timeout=connect_timeout,
        publish_timeout=publish_timeout)

    with client.pool.connection():
        assert expected_sock.call_args_list == [('settimeout', mock.call(publish_timeout))]
        assert m_connect.call_args_list == [mock.call(('some fake host', 31337), timeout=connect_timeout)]


@mock.patch('pycernan.avro.client.socket.create_connection', autospec=True)
def test_closing_the_client_closes_the_socket_and_clears_it(m_connect):
    expected_sock = FakeSocket()
    m_connect.return_value = expected_sock

    client = BaseDummyClient(
        host='some fake host',
        port=31337,
        connect_timeout=666,
        publish_timeout=999)

    with client.pool.connection():
        pass  # Generates a connection
    assert client.pool is not None
    assert client.pool.pool.qsize()

    # Close calls should gracefully handle defunct connections too
    client.pool._put(_DefunctConnection)
    expected_sock._reset_mock()
    client.close()

    assert expected_sock.call_args_list == [('close', mock.call())]
    assert client.pool is not None
    assert client.pool.pool.qsize() == 0


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
