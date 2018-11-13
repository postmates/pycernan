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


DefunctConnection = None


class TCPConnectionPool(object):
    """
    Fork friendly TCP connection pool.

    Adapted from: https://github.com/gevent/gevent/blob/master/examples/psycopg2_pool.py
    """
    def __init__(self, host, port, maxsize, connect_timeout, read_timeout):
        if maxsize <= 0:
            raise ValueError("maxsize must be > 0")

        self.host = host
        self.port = port
        self.size = 0
        self.maxsize = maxsize

        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.pool = None

    def create_connection(self):
        sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        sock.settimeout(self.read_timeout)
        return sock

    def get(self, _block=True):
        if not self.pool:
            # Lazy queue creation, ensuring we are fork() friendly.
            # Caveat - Users must not use the connection pool prior to
            # forking.
            self.pool = Queue()

        if self.size >= self.maxsize or self.pool.qsize():
            try:
                existing_item = self.pool.get(_block)
            except Empty:
                # Expected to happen only when users override _block=False
                raise EmptyPoolException()

            if not existing_item:
                try:
                    existing_item = self.create_connection()
                except Exception:
                    # Always return the resource to the queue,
                    # event if it is defunct.
                    self.put(DefunctConnection)
                    raise

            return existing_item

        self.size += 1
        try:
            new_item = self.create_connection()
        except Exception:
            self.size -= 1
            raise
        return new_item

    def put(self, item):
        self.pool.put(item)

    def closeall(self):
        if not self.pool:
            return

        while not self.pool.empty():
            conn = self.pool.get_nowait()

            if not conn:
                continue

            try:
                conn.close()
            except Exception:
                pass

    @contextlib.contextmanager
    def connection(self, _block=True):
        conn = self.get(_block)
        try:
            yield conn
        except Exception:
            conn = DefunctConnection
            raise
        finally:
            self.put(conn)


class Client(object):
    """
        Interface specification for all Avro clients.
    """
    __metaclass__ = ABCMeta

    def __init__(self, host=None, port=None, maxsize=10, connect_timeout=50, publish_timeout=10):
        host = host or pycernan.avro.config.host()
        port = port or pycernan.avro.config.port()

        self.connect_timeout = connect_timeout
        self.publish_timeout = publish_timeout

        self.pool = TCPConnectionPool(
            host,
            port,
            maxsize,
            connect_timeout,
            publish_timeout)

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
