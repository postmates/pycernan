import json
import sys

from avro.io import DatumWriter, DatumReader
from avro.datafile import DataFileWriter, DataFileReader
from io import BytesIO, IOBase


if sys.version_info >= (3, 0):
    from avro.schema import Parse as parse      # pragma: no cover
else:
    from avro.schema import parse               # pragma: no cover


def serialize(schema_map, batch, ephemeral_storage=False, **metadata):
    """
        Serialize a batch of values, matching the given schema, as an
        Avro object container file.

        Note - This function reserves the `postmates.` metadata keyspace for
        for internal use.

        Args:
            schema_map: dict or pycernan.avro.serde.parse - Avro schema defintion.
            batch: list - List of Avro types.

        Kwargs:
            ephemeral_storage: bool - Flag to indicate whether the batch
                                      should be stored long-term.
            **metadata: dict - User defined metadata included in the header.

        Returns:
            bytes
    """
    if isinstance(schema_map, dict):
        parsed_schema = parse(json.dumps(schema_map))
    else:
        parsed_schema = schema_map

    avro_buf = BytesIO()
    with DataFileWriter(avro_buf, DatumWriter(), parsed_schema, 'deflate') as writer:
        if ephemeral_storage:
            metadata['postmates.storage.ephemeral'] = '1'

        for k, v in metadata.items():
            # handle py2/py3 interface discrepancy
            set_meta = getattr(writer, 'set_meta', None) or writer.SetMeta
            set_meta(k, str(v))

        for record in batch:
            writer.append(record)

        writer.flush()
        encoded = avro_buf.getvalue()

    return encoded


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
        with datafile_reader:
            for value in datafile_reader:
                yield value

    if isinstance(avro_bytes, IOBase):
        buffer = avro_bytes
    elif isinstance(avro_bytes, bytes):
        buffer = BytesIO(avro_bytes)
    else:
        raise ValueError("avro_bytes must be a bytes object or file-like io object")

    if reader_schema:
        parsed_reader_schema = parse(json.dumps(reader_schema))
    else:
        parsed_reader_schema = None

    # NOTE: "reader_schema" in py3, "readers_schema" in py2! :facepalm:
    if sys.version_info >= (3, 0):
        reader = DataFileReader(buffer, DatumReader(reader_schema=parsed_reader_schema))
    else:
        reader = DataFileReader(buffer, DatumReader(readers_schema=parsed_reader_schema))

    values = _avro_generator(reader)
    metadata = reader.meta

    if decode_schema:
        metadata['avro.schema'] = json.loads(metadata['avro.schema'].decode('utf-8'))

    return metadata, values
