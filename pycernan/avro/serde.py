import json
import sys
import types

from fastavro import reader, writer, parse_schema
from io import BytesIO, IOBase

from pycernan.avro.exceptions import DatumTypeException


def serialize(schema_map, batch, ephemeral_storage=False, **metadata):
    """
        Serialize a batch of values, matching the given schema, as an
        Avro object container file.

        Note - This function reserves the `postmates.` metadata keyspace for
        for internal use.

        Args:
            schema_map: dict or pycernan.avro.serde.parse - Avro schema defintion.
            batch: list - List of concrete avro types or avro type generators.

        Kwargs:
            ephemeral_storage: bool - Flag to indicate whether the batch
                                      should be stored long-term.
            **metadata: dict - User defined metadata included in the header.

        Returns:
            bytes
    """
    parsed_schema = parse_schema(schema_map)
    avro_buf = BytesIO()

    if ephemeral_storage:
        metadata['postmates.storage.ephemeral'] = '1'

    for k, v in metadata.items():
        if not isinstance(v, str):
            metadata[k] = str(v)

    for record_or_generator in batch:
        write_data = [record_or_generator]
        if isinstance(record_or_generator, types.GeneratorType):
            # Fast avro doesn't handle iterators within iterators gracefully..
            write_data = record_or_generator
        try:
            writer(avro_buf, parsed_schema, write_data, codec='deflate', metadata=metadata)
        except (ValueError, TypeError) as e:
            raise DatumTypeException(e)

    return avro_buf.getvalue()


def deserialize(avro_bytes, decode_schema=False, reader_schema=None):
    """
        Deserialize encoded avro bytes.

        Args:
            avro_bytes: IOBase | bytes - Avro blob to decode.

        Kwargs:
            decode_schema: Bool - Load metadata['avro.schema'] as Python dictionary?.  Default = False.

            reader_schema: Dict - Schema to use when deserializing. If None, use writer_schema.  Default = None.

        Returns:
            (metadata, values) where:
                metadata: dict - Avro metadata as raw bytes.  When decode_schema is True,
                    the key 'avro.schema' value will be loaded as a Python dictionary instead of a string of JSON.

                values: generator - Generator for values corresponding to the schema contained
                    in metadata.

    """

    def _avro_generator(datafile_reader):
        for value in datafile_reader:
            yield value

    if isinstance(avro_bytes, IOBase):
        buffer = avro_bytes
    elif isinstance(avro_bytes, bytes):
        buffer = BytesIO(avro_bytes)
    else:
        raise ValueError("avro_bytes must be a bytes object or file-like io object")

    read = reader(buffer, reader_schema=reader_schema)
    values = _avro_generator(read)
    metadata = read.metadata

    if decode_schema:
        schema = metadata['avro.schema']
        if sys.version_info < (3, 0):
            schema = schema.decode('utf-8')
        metadata['avro.schema'] = json.loads(schema)

    return metadata, values
