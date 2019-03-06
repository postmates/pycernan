import contextlib
import socket
from queue import Queue, Empty

from pycernan.avro.exceptions import EmptyPoolException
from pycernan.avro import metrics

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
        metrics.conn_create_count.inc()
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
        # something back to the queue here so that we don't erode our capacity.
        if connection is _DefunctConnection:
            try:
                connection = self._create_connection()
            except Exception:
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
        except Exception:
            # Why do we defer closing conn here?
            # Python2 has different `raise` semantics than Python3.
            # Deferring garbage collection until the finally block
            # saves us from having to account for these differences.
            garbage = conn
            conn = _DefunctConnection
            metrics.conn_failure_count.inc()
            raise
        finally:
            if garbage:
                try:
                    garbage.close()
                    metrics.conn_close_count.inc()
                except Exception:
                    pass
            self._put(conn)
