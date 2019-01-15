"""
    Base Avro client from which all other clients derive.
"""
import contextlib
import socket

import pycernan.avro.config

from abc import ABCMeta, abstractmethod
from queue import Queue, Empty

from pycernan.avro.exceptions import EmptyBatchException, EmptyPoolException
from pycernan.avro.serde import serialize

_DefunctConnection = object()


class TCPConnectionPool(object):
    """
    Fork friendly TCP connection pool.

    Adapted from: https://github.com/gevent/gevent/blob/master/examples/psycopg2_pool.py

    All exceptions (application, operation (timeouts, etc)) are the responsibility of the user to
    handle responsibly.  One day, with sufficient time and motivation, this class could be made
    more intelligent to handle connection related exceptions distinct from application layer
    concerns.
    """

    def __init__(self, host, port, maxsize, connect_timeout, read_timeout):
        if maxsize <= 0:
            raise ValueError("maxsize must be > 0")

        self.host = host
        self.port = port
        self.maxsize = maxsize

        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.pool = Queue()
        for _ in range(maxsize):
            self._put(_DefunctConnection)

    def _create_connection(self):
        sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        sock.settimeout(self.read_timeout)
        return sock

    def _get(self, _block=True):
        try:
            connection = self.pool.get(_block)
        except Empty:
            # Expected to happen only when users override _block=False
            raise EmptyPoolException()

        # When a connection is defunct, we attempt to
        # regenerate it.  Note - it is important that we always return
        # something back to the queue here so that we don't errode our capacity.
        if connection is _DefunctConnection:
            try:
                connection = self._create_connection()
            except:
                # Always return a resource to the queue,
                # even if it is defunct.
                self._put(_DefunctConnection)
                raise

        return connection

    def _put(self, item):
        # Why this function?  It makes mocking in unit tests easier.
        self.pool.put(item)

    def closeall(self):
        """
            Close established connections currently not in-use.
        """
        try:
            while True:
                conn = self.pool.get_nowait()
                if conn is _DefunctConnection:
                    continue
                conn.close()
        except Exception:
            pass

    @contextlib.contextmanager
    def connection(self, _block=True):
        """
            Context manager for pooled connections.
        """
        garbage = None
        conn = self._get(_block)
        try:
            yield conn
        except:
            # Why do we defer closing conn here?
            # Python2 has different `raise` semantics than Python3.
            # Deferring garbage collection until the finally block
            # saves us from having to account for these differences.
            garbage = conn
            conn = _DefunctConnection
            raise
        finally:
            if garbage:
                try:
                    garbage.close()
                except:
                    pass
            self._put(conn)


class Client(object):
    """
        Interface specification for all Avro clients.
    """
    __metaclass__ = ABCMeta

    def __init__(self, host=None, port=None, connect_timeout=50, publish_timeout=10, maxsize=10):
        host = host or pycernan.avro.config.host()
        port = port or pycernan.avro.config.port()

        self.connect_timeout = connect_timeout
        self.publish_timeout = publish_timeout

        self.pool = TCPConnectionPool(
            host,
            port,
            maxsize=maxsize,
            connect_timeout=connect_timeout,
            read_timeout=publish_timeout)

    def close(self):
        """
            Closes all previously established connections not actively in use.
        """
        self.pool.closeall()

    def publish(self, schema_map, batch, ephemeral_storage=False, **kwargs):
        """
            Publishes a batch of records corresponding to the given schema.

            Args:
                schema_map: dict - Avro schema defintion.
                batch: list - List of Avro records (as dicts).

            Kwargs:
                ephemeral_storage: bool - Flag to indicate whether the batch
                                          should be stored long-term.
                Others  are version specific options.  See extending object.
        """
        if not batch:
            raise EmptyBatchException()

        blob = serialize(schema_map, batch, ephemeral_storage)
        self.publish_blob(blob, **kwargs)

    def publish_file(self, file_path, **kwargs):
        """
            Reads and publishes an Avro encoded file.

            Args:
                file_path : string  - Path to the file.

            Kwargs:
                Version specific options.  See extending object.
        """
        with open(file_path, "rb") as file:
            avro_blob = file.read()

        self.publish_blob(avro_blob, **kwargs)

    @abstractmethod
    def publish_blob(self, avro_blob, **kwargs):
        """
            Version specific payload generation / publication.
        """
        pass  # pragma: no cover
