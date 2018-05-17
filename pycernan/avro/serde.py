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


def deserialize(avro_bytes, decode_schema=False, decode_values=True):
    """
        Deserialize encoded avro bytes.

        Args:
            avro_bytes: IOBase | bytes - Avro blob to decode.

        Kwargs:
            decode_schema: Bool - Load metadata['avro.schema'] as JSON?.  Default = False.
            decode_values: Bool - Decode & return values immediately?  Default = True.

        Returns:
            (metadata, values) where:
                metadata: dict - Avro metadata as raw bytes.  When decode_schema is True,
                    the key 'avro.schema' value will be loaded as JSON.

                values: List | DataFileReader - List of values corresponding to schema contained in
                    metadata.  When decode_values is False, a DataFileReader is returned which
                    users can leverage as an iterator over the values awaiting decode.  DataFileReaders
                    returned in this way can also be used as a context manager to ensure resources are
                    properly garbage collected.

    """
    if isinstance(avro_bytes, IOBase):
        buffer = avro_bytes
    elif isinstance(avro_bytes, bytes):
        buffer = BytesIO(avro_bytes)
    else:
        raise ValueError("avro_bytes must be a bytes object or file-like io object")

    reader = DataFileReader(buffer, DatumReader())
    values = reader
    metadata = reader.meta
    if decode_values:
        with reader:
            values = [r for r in reader]

    if decode_schema:
        metadata['avro.schema'] = json.loads(metadata['avro.schema'].decode('utf-8'))

    return metadata, values
