"""
    Base Avro client from which all other clients derive.
"""
import json
import socket
import sys

import pycernan.avro.config

from abc import ABCMeta, abstractmethod
from io import BytesIO

from avro.io import DatumWriter
from avro.datafile import DataFileWriter
from pycernan.avro.exceptions import EmptyBatchException

if sys.version_info >= (3, 0):
    from avro.schema import Parse                   # pragma: no cover
else:
    from avro.schema import parse as Parse          # pragma: no cover


class Client(object):
    """
        Interface specification for all Avro clients.
    """
    __metaclass__ = ABCMeta
    sock = None

    def __init__(self, host=None, port=None, connect_timeout=50, publish_timeout=10):
        host = host or pycernan.avro.config.host()
        port = port or pycernan.avro.config.port()

        self.connect_timeout = connect_timeout
        self.publish_timeout = publish_timeout

        self.sock = self._connect(host, port)

    def _connect(self, host, port):
        """
            Establishes TCP connection to the given Cernan instance.
        """
        sock = socket.create_connection((host, port), timeout=self.connect_timeout)
        sock.settimeout(self.publish_timeout)
        return sock

    def close(self):
        """
            Closes a previously established connection.
        """
        if self.sock:
            self.sock.close()
            self.sock = None

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

        parsed_schema = Parse(json.dumps(schema_map))

        avro_buf = BytesIO()
        with DataFileWriter(avro_buf, DatumWriter(), parsed_schema, 'deflate') as writer:
            if ephemeral_storage:
                # handle py2/py3 interface discrepancy
                set_meta = getattr(writer, 'set_meta', None) or writer.SetMeta
                set_meta('postmates.storage.ephemeral', '1')
            for record in batch:
                writer.append(record)
            writer.flush()
            self.publish_blob(avro_buf.getvalue(), **kwargs)

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
