import json
import pytest

from avro.io import DatumReader
from avro.datafile import DataFileReader
from io import BytesIO

from pycernan.avro.exceptions import SchemaParseException, DatumTypeException
from pycernan.avro.serde import parse, serialize, deserialize


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


@pytest.mark.parametrize('schema', [USER_SCHEMA, parse(json.dumps(USER_SCHEMA))])
@pytest.mark.parametrize('ephemeral', [True, False])
def test_serialize(ephemeral, schema):
    user = {
        'name': 'Foo Bar Matic',
        'favorite_number': 24,
        'favorite_color': 'Nonyabusiness',
    }

    avro_blob = serialize(schema, [user], ephemeral_storage=ephemeral)
    buf = BytesIO()
    buf.write(avro_blob)
    buf.seek(0)
    with DataFileReader(buf, DatumReader()) as reader:
        get_meta = getattr(reader, 'get_meta', None) or reader.GetMeta
        value = get_meta('postmates.storage.ephemeral')
        assert value is (b'1' if ephemeral else None)
        records = [r for r in reader]
        assert records == [user]


def test_serialize_and_deserialize():
    user = {
        'name': 'Foo Bar Matic',
        'favorite_number': 24,
        'favorite_color': 'Nonyabusiness',
    }

    avro_blob = serialize(USER_SCHEMA, [user])
    test_records = deserialize(avro_blob)
    assert isinstance(test_records, list)
    assert len(test_records) == 1
    assert test_records[0] == user

    test_buffer = BytesIO(avro_blob)
    test_records2 = deserialize(test_buffer)
    assert isinstance(test_records2, list)
    assert len(test_records2) == 1
    assert test_records2[0] == user


def test_bad_schema():
    schema = {}
    user = {}
    with pytest.raises(SchemaParseException):
        serialize(schema, [user])


def test_bad_datum_empty():
    user = {}
    with pytest.raises(DatumTypeException):
        serialize(USER_SCHEMA, [user])


def test_bad_arg_to_deserialize():
    with pytest.raises(ValueError):
        deserialize(47)
