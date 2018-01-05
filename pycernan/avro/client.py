"""
    Base Avro client from which all other clients derive.
"""

import json
import socket

from io import BytesIO

from abc import ABCMeta
from avro import schema, io
from avro.io import DatumWriter
from avro.datafile import DataFileWriter
from pycernan.avro.exceptions import SchemaParseException, DatumTypeException, EmptyBatchException


class Client(metaclass=ABCMeta):
    """
        Interface specification for all Avro clients.
    """
    sock = None

    def __init__(self, host="127.0.0.1", port=2002, timeout=(50, 10)):
        self.connect_timeout = timeout[0]
        self.publish_timeout = timeout[1]

        self._connect(host, port)

    def _connect(self, host, port):
        """
            Establishes TCP connection to the given Cernan instance.
        """
        self.sock = socket.create_connection((host, port), timeout=self.connect_timeout)
        self.sock.settimeout(self.publish_timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def close(self):
        """
            Closes a previously established connection.
        """
        if self.sock:
            self.sock.close()
            self.sock = None

    def publish(self, schema_map, batch, **kwargs):
        """
            Publishes a batch of records corresponding to the given schema.

            Args:
                schema_map: dict - Avro schema defintion.
                batch: list - List of Avro records (as dicts).

            Kwargs:
                Version specific options.  See extending object.
        """
        if not batch:
            raise EmptyBatchException()

        try:
            parsed_schema = schema.Parse(json.dumps(schema_map))
        except schema.SchemaParseException as e:
            raise SchemaParseException from e

        avro_buf = BytesIO()
        writer = DataFileWriter(
            avro_buf,
            DatumWriter(),
            parsed_schema,
            'deflate'
        )
        try:
            for record in batch:
                writer.append(record)
        except io.AvroTypeException as e:
            raise DatumTypeException(e)

        self.publish_blob(avro_buf.getvalue(), **kwargs)
        avro_buf.close()

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

    def publish_blob(self, avro_blob, **kwargs):
        """
            Version specific payload generation / publication.
        """
        pass
