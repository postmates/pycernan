"""
    Base Avro client from which all other clients derive.
"""

import pycernan.avro.config

from abc import ABCMeta, abstractmethod

from pycernan.avro.exceptions import EmptyBatchException
from pycernan.avro.serde import serialize
from pycernan.avro import metrics
from pycernan.avro.tcp_conn_pool import TCPConnectionPool


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

    @metrics.publish_failure_count.count_exceptions()
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
