import json
import sys

from avro.io import DatumWriter
from avro.datafile import DataFileWriter
from io import BytesIO

if sys.version_info >= (3, 0):
    from avro.schema import Parse                   # pragma: no cover
else:
    from avro.schema import parse as Parse          # pragma: no cover


def serialize(schema_map, batch, ephemeral_storage=False):
    """
        Serialize a batch of values, matching the given schema, as an
        Avro object container file.

        Args:
            schema_map: dict or avro.schema.Parse - Avro schema defintion.
            batch: list - List of Avro types.

        Kwargs:
            ephemeral_storage: bool - Flag to indicate whether the batch
                                      should be stored long-term.

        Returns:
            bytes
    """
    if isinstance(schema_map, dict):
        parsed_schema = Parse(json.dumps(schema_map))
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
