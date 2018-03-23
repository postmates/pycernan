import pytest

from avro.io import DatumReader
from avro.datafile import DataFileReader
from io import BytesIO

from pycernan.avro.exceptions import SchemaParseException, DatumTypeException
from pycernan.avro.serde import serialize


USER_SCHEMA = {
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number",  "type": ["int", "null"]},
        {"name": "favorite_color", "type": ["string", "null"]}
    ]
}


@pytest.mark.parametrize('ephemeral', [True, False])
def test_serialize(ephemeral):
    def inspect_publish_blob(avro_blob):
        user = {
            'name': 'Foo Bar Matic',
            'favorite_number': 24,
            'favorite_color': 'Nonyabusiness',
        }

        buf = BytesIO()
        buf.write(avro_blob)
        buf.seek(0)
        with DataFileReader(buf, DatumReader()) as reader:
            get_meta = getattr(reader, 'get_meta', None) or reader.GetMeta
            value = get_meta('postmates.storage.ephemeral')
            assert value is (b'1' if ephemeral else None)
            records = [r for r in reader]
            assert records == [user]

        blob = serialize(USER_SCHEMA, [user], ephemeral_storage=ephemeral)
        inspect_publish_blob(blob)


def test_bad_schema():
    schema = "Not a dict"
    user = {}
    with pytest.raises(SchemaParseException):
        serialize(schema, [user])


def test_bad_datum_empty():
    user = {}
    with pytest.raises(DatumTypeException):
        serialize(USER_SCHEMA, [user])
