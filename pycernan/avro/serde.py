import json
import sys

from avro.io import DatumWriter, DatumReader
from avro.datafile import DataFileWriter, DataFileReader
from io import BytesIO, IOBase


if sys.version_info >= (3, 0):
    from avro.schema import Parse as parse      # pragma: no cover
else:
    from avro.schema import parse               # pragma: no cover


def serialize(schema_map, batch, ephemeral_storage=False):
    """
        Serialize a batch of values, matching the given schema, as an
        Avro object container file.

        Args:
            schema_map: dict or pycernan.avro.serde.parse - Avro schema defintion.
            batch: list - List of Avro types.

        Kwargs:
            ephemeral_storage: bool - Flag to indicate whether the batch
                                      should be stored long-term.

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
            # handle py2/py3 interface discrepancy
            set_meta = getattr(writer, 'set_meta', None) or writer.SetMeta
            set_meta('postmates.storage.ephemeral', '1')

        for record in batch:
            writer.append(record)

        writer.flush()
        encoded = avro_buf.getvalue()

    return encoded


def deserialize(avro_bytes):
    """
        Deserialize encoded avro bytes.
    """
    if isinstance(avro_bytes, IOBase):
        buffer = avro_bytes
    elif isinstance(avro_bytes, bytes):
        buffer = BytesIO(avro_bytes)
    else:
        raise ValueError("avro_bytes must be a bytes object or file-like io object")

    with DataFileReader(buffer, DatumReader()) as reader:
        records = [r for r in reader]
    return records
